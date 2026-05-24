/**
 * Retry-stuck primitives — single-row pick + send/refund/recharge mutation.
 *
 * Selection criteria (LOCAL DB only — no Instantly preflight):
 *   - `delivery_status = 'contacted'` (lead pushed, not yet observed sending)
 *   - `status = 'active'` (not in terminal error state locally)
 *   - `created_at < NOW() - INTERVAL '72 hours'` (3 days = beyond Instantly's
 *     weekday/business-hours dispatch window)
 *   - `campaign_id`, `lead_email`, `org_id` are NOT NULL (filter out orphaned
 *     rows that can't be re-sent)
 *   - NOT EXISTS any silver event in `instantly_events` proving the lead
 *     already moved off `contacted` (email_sent / email_opened / link_clicked
 *     / reply_received / auto_reply_received / email_bounced /
 *     lead_unsubscribed). Belt-and-suspenders: if the column stayed stale
 *     for any reason, silver still gates us.
 *
 * Per row:
 *   1. Read live Instantly campaign once to recover the sequence (subject +
 *      step bodies + delays). This is the ONLY Instantly call we make for
 *      observability — `not_sending_status` is NOT consulted (reconcile owns
 *      it for /stats; retry-stuck operates purely on local signals).
 *   2. Read the lead's profile from `instantly_leads`.
 *   3. Call `sendLeadToInstantly` to provision a new campaign on a different
 *      healthy account.
 *   4. On success: cancel the old cost rows (refund), provision fresh costs
 *      on new step runs (re-charge), mute the local row in place to point at
 *      the new Instantly campaign, append a `redispatchHistory` entry.
 *      `delivery_status` stays `'contacted'`.
 *   5. On failure (no healthy account, all attempts hit NSS, no sequence,
 *      no local lead profile, getCampaign throws): the row is LEFT ALONE.
 *      No terminal cancel. The worker loop will pick up another row and the
 *      stuck row is re-visited on the next sweep.
 *
 * Concurrency: this module exposes a `selectOneStuckRow` + `processRow` pair
 * that the worker loop in `lib/retry-stuck-worker.ts` calls sequentially —
 * one row at a time, no batching, no advisory lock. With a single replica
 * this is race-free by construction. (Multi-replica safety would need
 * `FOR UPDATE SKIP LOCKED` on the SELECT — not currently required.)
 */

import { db } from "../db";
import { instantlyCampaigns, instantlyLeads, sequenceCosts } from "../db/schema";
import { and, eq, or, sql } from "drizzle-orm";
import { getCampaign as getInstantlyCampaign, type Lead } from "./instantly-client";
import { resolveInstantlyApiKey, KeyServiceError } from "./key-client";
import {
  sendLeadToInstantly,
  stripAccountSignature,
  type SortedSequenceStep,
} from "./send-lead";
import {
  addCosts,
  createRun,
  updateCostStatus,
  updateRun,
  type IdentityContext,
} from "./runs-client";

/** Age (hours) a row must reach before retry-stuck picks it up. */
export const STUCK_AGE_HOURS = 72;

const SYSTEM_USER_ID = "00000000-0000-0000-0000-000000000000";

export interface StuckCampaignRow {
  id: string;
  instantlyCampaignId: string;
  campaignId: string | null;
  leadEmail: string | null;
  orgId: string | null;
  userId: string | null;
  runId: string | null;
  brandIds: string[] | null;
  metadata: Record<string, unknown> | null;
}

export type RowOutcome =
  | { kind: "redispatched"; newInstantlyCampaignId: string; account: string }
  | { kind: "skipped_no_key" }
  | { kind: "failed"; reason: string };

/**
 * Pick one stuck row (oldest first). Returns `null` when the backlog is
 * empty — caller should back off and retry later.
 */
export async function selectOneStuckRow(): Promise<StuckCampaignRow | null> {
  const result = await db.execute(sql`
    SELECT
      id,
      instantly_campaign_id AS "instantlyCampaignId",
      campaign_id           AS "campaignId",
      lead_email            AS "leadEmail",
      org_id                AS "orgId",
      user_id               AS "userId",
      run_id                AS "runId",
      brand_ids             AS "brandIds",
      metadata
    FROM instantly_campaigns c
    WHERE c.delivery_status = 'contacted'
      AND c.status = 'active'
      AND c.created_at < NOW() - INTERVAL '${sql.raw(`${STUCK_AGE_HOURS} hours`)}'
      AND c.campaign_id IS NOT NULL
      AND c.lead_email IS NOT NULL
      AND c.org_id IS NOT NULL
      AND NOT EXISTS (
        SELECT 1 FROM instantly_events e
        WHERE e.campaign_id = c.instantly_campaign_id
          AND e.event_type IN (
            'email_sent',
            'email_opened',
            'link_clicked',
            'reply_received',
            'auto_reply_received',
            'email_bounced',
            'lead_unsubscribed'
          )
      )
    ORDER BY created_at ASC
    LIMIT 1
  `);
  const rows = Array.isArray(result) ? result : (result as { rows?: unknown[] }).rows ?? [];
  if (rows.length === 0) return null;
  return rows[0] as StuckCampaignRow;
}

function getRedispatchCount(metadata: Record<string, unknown> | null): number {
  if (!metadata) return 0;
  const raw = metadata.redispatchCount;
  if (typeof raw === "number" && Number.isFinite(raw)) return raw;
  return 0;
}

/**
 * Translate the Instantly campaign config returned by `getCampaign` into the
 * normalized `SortedSequenceStep[]` shape expected by `sendLeadToInstantly`.
 *
 * Instantly's per-step `delay` is "days AFTER this step before the NEXT one".
 * Our `daysSinceLastStep` on step N is "days BEFORE step N (since step N-1)".
 * So `sortedSequence[i].daysSinceLastStep = liveSteps[i-1].delay` for `i >= 1`,
 * and `0` for the first step.
 *
 * Each body has the previous account's signature appended (`\n\n--\n<sig>`);
 * we strip that so the new account's signature can be re-injected by
 * `buildSequenceSteps` inside the send helper.
 */
function extractSequenceFromLive(
  live: Record<string, unknown>,
): { subject: string; sortedSequence: SortedSequenceStep[] } | null {
  const sequences = live.sequences as
    | Array<{ steps?: Array<{ delay?: number; variants?: Array<{ subject?: string; body?: string }> }> }>
    | undefined;
  const steps = sequences?.[0]?.steps;
  if (!steps || steps.length === 0) return null;

  const subject = steps[0]?.variants?.[0]?.subject ?? "(no subject)";

  const sortedSequence: SortedSequenceStep[] = steps.map((s, i) => ({
    step: i + 1,
    bodyHtml: stripAccountSignature(s.variants?.[0]?.body ?? ""),
    daysSinceLastStep: i === 0 ? 0 : steps[i - 1]?.delay ?? 0,
  }));

  return { subject, sortedSequence };
}

/**
 * Cancel the (provisioned | actual) cost rows tied to (campaignId, leadEmail)
 * via runs-service (refunds the customer) and flip the local rows to
 * `cancelled`.
 */
async function cancelExistingCosts(
  row: StuckCampaignRow,
  identity: IdentityContext,
): Promise<void> {
  if (!row.campaignId || !row.leadEmail) return;

  const existing = await db
    .select()
    .from(sequenceCosts)
    .where(
      and(
        eq(sequenceCosts.campaignId, row.campaignId),
        eq(sequenceCosts.leadEmail, row.leadEmail),
        or(
          eq(sequenceCosts.status, "provisioned"),
          eq(sequenceCosts.status, "actual"),
        ),
      ),
    );

  for (const cost of existing) {
    const costIdentity = { ...identity, runId: cost.runId };
    await updateCostStatus(cost.runId, cost.costId, "cancelled", costIdentity);
    await db
      .update(sequenceCosts)
      .set({ status: "cancelled", updatedAt: new Date() })
      .where(eq(sequenceCosts.id, cost.id));
  }
}

/**
 * Provision fresh cost rows for each step of the re-sent campaign on a
 * new per-step run. Mirrors the /send entry-point pattern.
 */
async function provisionFreshCosts(
  row: StuckCampaignRow,
  parentIdentity: IdentityContext,
  keySource: "platform" | "org",
  stepCount: number,
): Promise<void> {
  if (!row.campaignId || !row.leadEmail) return;

  for (let step = 1; step <= stepCount; step++) {
    const stepRun = await createRun(
      {
        serviceName: "instantly-service",
        taskName: `email-send-step-${step}`,
        brandId: row.brandIds?.join(",") ?? undefined,
        campaignId: row.campaignId,
      },
      parentIdentity,
    );

    const stepIdentity: IdentityContext = { ...parentIdentity, runId: stepRun.id };

    const costResult = await addCosts(
      stepRun.id,
      [
        {
          costName: "instantly-account-email-sent",
          quantity: 1,
          costSource: keySource,
          status: "provisioned",
        },
        {
          costName: "instantly-domain-email-sent",
          quantity: 1,
          costSource: keySource,
          status: "provisioned",
        },
      ],
      stepIdentity,
    );

    for (const cost of costResult.costs) {
      await db.insert(sequenceCosts).values({
        campaignId: row.campaignId,
        leadEmail: row.leadEmail,
        step,
        runId: stepRun.id,
        costId: cost.id,
        status: "provisioned",
      });
    }

    await updateRun(stepRun.id, "completed", stepIdentity);
  }
}

/**
 * Process one stuck row: resolve key, recover sequence + lead, send on a
 * fresh healthy account, refund old costs, provision new ones, mute the
 * local row.
 *
 * Returns a discriminated outcome instead of throwing so the worker loop
 * can keep iterating without try/catch around each call.
 */
export async function processRow(row: StuckCampaignRow): Promise<RowOutcome> {
  try {
    if (!row.campaignId || !row.leadEmail || !row.orgId) {
      return { kind: "failed", reason: "missing_identifiers" };
    }

    // Resolve the org's Instantly key.
    let apiKey: string;
    let keySource: "platform" | "org";
    try {
      const keyResult = await resolveInstantlyApiKey(row.orgId, "system", {
        method: "POST",
        path: "/internal/campaigns/retry-stuck",
      });
      apiKey = keyResult.key;
      keySource = keyResult.keySource;
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : String(error);
      const isKeyMissing = error instanceof KeyServiceError && error.statusCode === 404;
      const logFn = isKeyMissing ? console.warn : console.error;
      logFn(
        `[instantly-service] retry-stuck: skipping row=${row.id} org=${row.orgId} — ${message}`,
      );
      return { kind: "skipped_no_key" };
    }

    // 1. Pull the live campaign once to recover the sequence. NOT used for
    //    any NSS decision — reconcile owns that signal independently.
    const live = (await getInstantlyCampaign(
      apiKey,
      row.instantlyCampaignId,
    )) as unknown as Record<string, unknown>;

    const seq = extractSequenceFromLive(live);
    if (!seq) {
      console.warn(
        `[instantly-service] retry-stuck: row=${row.id} instantly=${row.instantlyCampaignId} no sequence on live campaign — leaving alone`,
      );
      return { kind: "failed", reason: "no_sequence" };
    }

    // 2. Read the lead's profile data from the local instantly_leads row.
    const [storedLead] = await db
      .select()
      .from(instantlyLeads)
      .where(eq(instantlyLeads.instantlyCampaignId, row.instantlyCampaignId))
      .limit(1);

    if (!storedLead) {
      console.warn(
        `[instantly-service] retry-stuck: row=${row.id} lead profile not found in instantly_leads — leaving alone`,
      );
      return { kind: "failed", reason: "lead_profile_not_found" };
    }

    const lead: Lead = {
      email: storedLead.email,
      first_name: storedLead.firstName ?? undefined,
      last_name: storedLead.lastName ?? undefined,
      company_name: storedLead.companyName ?? undefined,
      variables: (storedLead.customVariables as Record<string, string> | null) ?? undefined,
    };

    const redispatchCount = getRedispatchCount(row.metadata);
    const campaignName = `Campaign ${row.campaignId} (retry ${redispatchCount + 1})`;

    // 3. Re-send onto a fresh healthy account.
    const result = await sendLeadToInstantly({
      apiKey,
      campaignName,
      subject: seq.subject,
      sortedSequence: seq.sortedSequence,
      lead,
    });

    if (!result.ok) {
      console.warn(
        `[instantly-service] retry-stuck: row=${row.id} send failed (${result.reason}) — leaving alone`,
      );
      return { kind: "failed", reason: result.reason };
    }

    const identity: IdentityContext = {
      orgId: row.orgId,
      userId: row.userId ?? SYSTEM_USER_ID,
      runId: row.runId ?? undefined,
    };

    // 4. Cancel old costs (refund), provision fresh costs (recharge).
    await cancelExistingCosts(row, identity);
    await provisionFreshCosts(
      row,
      identity,
      keySource,
      seq.sortedSequence.length,
    );

    // 5. Mirror the lead onto the new Instantly campaign so subsequent
    //    re-sends still resolve the profile data.
    await db
      .insert(instantlyLeads)
      .values({
        instantlyCampaignId: result.value.instantlyCampaignId,
        email: storedLead.email,
        firstName: storedLead.firstName,
        lastName: storedLead.lastName,
        companyName: storedLead.companyName,
        customVariables: storedLead.customVariables,
        orgId: row.orgId,
        runId: null,
      })
      .onConflictDoNothing();

    // 6. Mute the campaign row in place: new Instantly campaign ID, metadata
    //    bumped with the redispatch history entry. delivery_status stays
    //    `'contacted'` — the lead is back to actively being attempted.
    const existingMetadata = (row.metadata ?? {}) as Record<string, unknown>;
    const existingHistory = Array.isArray(existingMetadata.redispatchHistory)
      ? (existingMetadata.redispatchHistory as Array<Record<string, unknown>>)
      : [];

    await db
      .update(instantlyCampaigns)
      .set({
        instantlyCampaignId: result.value.instantlyCampaignId,
        name: campaignName,
        metadata: {
          ...existingMetadata,
          redispatchCount: redispatchCount + 1,
          redispatchHistory: [
            ...existingHistory,
            {
              from: row.instantlyCampaignId,
              to: result.value.instantlyCampaignId,
              account: result.value.account.email,
              at: new Date().toISOString(),
            },
          ],
        },
        updatedAt: new Date(),
      })
      .where(eq(instantlyCampaigns.id, row.id));

    console.log(
      `[instantly-service] retry-stuck: re-sent row=${row.id} ` +
        `from=${row.instantlyCampaignId} to=${result.value.instantlyCampaignId} ` +
        `account=${result.value.account.email}`,
    );
    return {
      kind: "redispatched",
      newInstantlyCampaignId: result.value.instantlyCampaignId,
      account: result.value.account.email,
    };
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(
      `[instantly-service] retry-stuck: row=${row.id} threw: ${message}`,
    );
    return { kind: "failed", reason: message };
  }
}
