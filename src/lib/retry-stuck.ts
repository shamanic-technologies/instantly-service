/**
 * Retry-stuck primitives — single-row pick + send/refund/recharge mutation.
 *
 * Selection criteria:
 *   - `delivery_status = 'contacted'` (lead pushed, not yet observed sending)
 *   - `status = 'active'` (not in terminal error state locally)
 *   - `created_at < NOW() - INTERVAL '72 hours'` (3 days = beyond Instantly's
 *     weekday/business-hours dispatch window)
 *   - `campaign_id`, `lead_email`, `org_id` are NOT NULL (filter out orphaned
 *     rows that can't be re-sent)
 *   - NOT EXISTS any silver event for THIS campaign proving the lead already
 *     moved off `contacted` (email_sent / opened / clicked / reply / auto-reply
 *     / bounced / unsubscribed).
 *   - NOT EXISTS any reply / auto-reply / unsubscribe / bounce for this
 *     LEAD_EMAIL in ANY campaign (DIS-148 person-level opt-out gate — keyed on
 *     the atomic member, since redispatch repoints `instantly_campaign_id`
 *     and orphans the opt-out signal on the predecessor).
 *
 * Per row:
 *   1. Read live Instantly campaign once. Used for two things: (a) recover the
 *      sequence (subject + step bodies + delays); (b) live-status preflight —
 *      if the campaign is no longer active (e.g. paused in Instantly's UI; the
 *      local `status` is NOT synced from Instantly by reconcile), sync local
 *      status and SKIP the redispatch rather than resurrect it. `not_sending_
 *      status` is still NOT consulted (reconcile owns it for /stats).
 *   2. Read the lead's profile from `instantly_leads`.
 *   3. Call `sendLeadToInstantly` to provision a new campaign on a different
 *      healthy account, then PAUSE the predecessor Instantly campaign so the
 *      prospect is not contacted by both (best-effort — a pause failure does
 *      not abort the redispatch).
 *   4. On success: cancel the old cost rows (refund), provision fresh costs
 *      on new step runs (re-charge), mute the local row in place to point at
 *      the new Instantly campaign, append a `redispatchHistory` entry.
 *      `delivery_status` stays `'contacted'`.
 *   5. Terminal-cancel when the row is un-retriable (parent gone, key gone, no
 *      sequence, no lead profile, runs-service 409) OR has already been
 *      redispatched `MAX_REDISPATCHES` times without ever sending. On a
 *      transient failure (no healthy account) the row is LEFT ALONE and
 *      re-visited next sweep.
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
import {
  getCampaign as getInstantlyCampaign,
  updateCampaignStatus,
  type Lead,
} from "./instantly-client";
import { resolveInstantlyApiKey } from "./key-client";
import {
  sendLeadToInstantly,
  stripAccountSignature,
  type SortedSequenceStep,
} from "./send-lead";
import {
  addCosts,
  createRun,
  getRun,
  updateCostStatus,
  updateRun,
  type IdentityContext,
} from "./runs-client";
import { handleCampaignError } from "./campaign-error-handler";
import { deleteLeadStatusCurrent, refreshLeadStatusCurrent } from "./status-gold";

/** Age (hours) a row must reach before retry-stuck picks it up. */
export const STUCK_AGE_HOURS = 72;

/**
 * Minimum gap (minutes) between two consecutive attempts on the same row.
 * Set on `metadata.lastAttemptAt` at the start of every `processRow` call.
 * The SELECT excludes rows whose last attempt is more recent than this,
 * preventing any single broken row from hogging the loop.
 *
 * 72h. Matches the STUCK_AGE_HOURS floor — after a redispatch lands on a
 * fresh Instantly campaign, give Instantly the full dispatch window (3
 * full business days) to actually fire the new send before considering
 * the row stuck again. Each redispatch consumes a fresh contact-upload
 * slot on the customer's Instantly workspace, so re-attempting more
 * aggressively burns billable slots without any new signal.
 */
export const ATTEMPT_COOLDOWN_MINUTES = 4320;

/**
 * Hard cap on redispatches per row. A row that has been redispatched this many
 * times and STILL never reached `email_sent` is presumed un-sendable (dead
 * lead, perma-NSS, etc.) — it is terminal-cancelled instead of looping forever.
 *
 * Each redispatch creates a fresh Instantly campaign + consumes a billable
 * contact-upload slot, so an uncapped loop both spams the prospect and burns
 * the customer's quota. DIS-41 showed rows historically hit 70+ redispatches
 * under the old 30-min cooldown; this cap is the structural fix.
 */
export const MAX_REDISPATCHES = 3;

/**
 * Instantly campaign status codes (from the live `GET /campaigns/{id}` payload).
 * 1 = Active (actively contacting). Anything else (2 paused, 3 completed,
 * 4 running-subsequences-only, negative = suspended) means Instantly is NOT
 * sending fresh first-touches, so retry-stuck must NOT redispatch — it would
 * resurrect a campaign the operator (or Instantly) deliberately stopped.
 */
const INSTANTLY_STATUS_ACTIVE = 1;
const INSTANTLY_STATUS_COMPLETED = 3;

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
  | { kind: "skipped_paused"; liveStatus: number }
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
      AND (
        c.metadata->>'lastAttemptAt' IS NULL
        OR (c.metadata->>'lastAttemptAt')::timestamptz < NOW() - INTERVAL '${sql.raw(`${ATTEMPT_COOLDOWN_MINUTES} minutes`)}'
      )
      AND NOT EXISTS (
        SELECT 1 FROM instantly_events e
        WHERE e.campaign_id = c.instantly_campaign_id
          AND e.event_type IN (
            'email_sent',
            'email_opened',
            'email_link_clicked',
            'reply_received',
            'auto_reply_received',
            'email_bounced',
            'lead_unsubscribed'
          )
      )
      -- Person-level opt-out gate (DIS-148). Keyed on the ATOMIC member
      -- (lead_email), NOT the campaign instance: once a prospect has replied,
      -- auto-replied, unsubscribed, or bounced under ANY campaign, they are
      -- done -- never redispatch them onto a fresh campaign. The per-campaign
      -- gate above misses this because retry-stuck repoints the campaign id
      -- to the new campaign (0 events), orphaning the opt-out signal on the
      -- predecessor (cf. DIS-57).
      AND NOT EXISTS (
        SELECT 1 FROM instantly_events e2
        WHERE e2.lead_email = c.lead_email
          AND e2.event_type IN (
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
 *
 * Step 1 also charges a fresh `instantly-contact-uploaded` (actual): each
 * re-send creates a brand-new Instantly campaign and uploads the lead to
 * it, consuming a workspace lead slot from Instantly's quota. The cost is
 * NOT stored in `sequence_costs` — same convention as /send (the upload
 * is a one-shot actual, never refunded). Without this charge, every
 * re-dispatch consumes a slot the customer doesn't pay for.
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

    const costItems: Array<{
      costName: string;
      quantity: number;
      costSource: "platform" | "org";
      status: "actual" | "provisioned";
    }> = [
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
    ];

    // Step 1 also charges for the fresh lead upload to the new Instantly
    // campaign (actual, one-shot — mirrors /send).
    if (step === 1) {
      costItems.push({
        costName: "instantly-contact-uploaded",
        quantity: 1,
        costSource: keySource,
        status: "actual",
      });
    }

    const costResult = await addCosts(stepRun.id, costItems, stepIdentity);

    // Store email costs in sequence_costs for webhook lifecycle management.
    // Contact upload cost is NOT stored — it is actual + never cancelled
    // (consistent with /send).
    for (const cost of costResult.costs) {
      if (cost.costName === "instantly-contact-uploaded") continue;
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
 * Stamp `metadata.lastAttemptAt = NOW()` on the row so the SELECT can
 * exclude it for at least `ATTEMPT_COOLDOWN_MINUTES` minutes. Prevents
 * any single broken row from monopolizing the worker loop.
 */
async function markAttempt(row: StuckCampaignRow): Promise<void> {
  const existingMetadata = (row.metadata ?? {}) as Record<string, unknown>;
  await db
    .update(instantlyCampaigns)
    .set({
      metadata: { ...existingMetadata, lastAttemptAt: new Date().toISOString() },
      updatedAt: new Date(),
    })
    .where(eq(instantlyCampaigns.id, row.id));
}

/**
 * Resolve the identity context retry-stuck should use for runs-service
 * writes on this row. Reuses the original parent run's identity so child
 * runs match parent identity (avoids 409 even if the row has been
 * transferred to a different org since the original /send).
 *
 * Returns `null` if the parent run no longer exists or its identity can't
 * be read — caller should treat the row as un-retriable and cancel it.
 */
async function resolveParentIdentity(
  row: StuckCampaignRow,
): Promise<IdentityContext | null> {
  if (!row.orgId) return null;

  if (!row.runId) {
    // No parent — new runs will be top-level under the row's current
    // identity. No conflict possible.
    return {
      orgId: row.orgId,
      userId: row.userId ?? SYSTEM_USER_ID,
    };
  }

  const parent = await getRun(row.runId, {
    orgId: row.orgId,
    userId: row.userId ?? SYSTEM_USER_ID,
  });

  if (!parent || !parent.organizationId) {
    return null;
  }

  return {
    orgId: parent.organizationId,
    userId: parent.userId ?? SYSTEM_USER_ID,
    runId: row.runId,
  };
}

/**
 * Cancel the row as a terminal `delivery_status='cancelled'`. Refunds
 * remaining costs via `handleCampaignError`. Reserved for "this row
 * cannot be retried by us" outcomes (parent run gone, parent identity
 * unusable, key unavailable for parent org, runs-service rejects the
 * write with 409, etc.) — once cancelled the row falls outside the
 * retry-stuck SELECT and lead-service can re-attempt with a fresh /send.
 */
async function cancelRowAsTerminal(
  row: StuckCampaignRow,
  reason: string,
): Promise<void> {
  console.warn(
    `[instantly-service] retry-stuck: row=${row.id} instantly=${row.instantlyCampaignId} cancelling — ${reason}`,
  );
  try {
    await handleCampaignError(row.instantlyCampaignId, reason, {
      terminalStatus: "cancelled",
      extraMetadata: { retryStuckCancelReason: reason },
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(
      `[instantly-service] retry-stuck: row=${row.id} cancellation handler threw: ${message}`,
    );
  }
}

/**
 * Process one stuck row: stamp lastAttemptAt, resolve parent identity,
 * resolve key, recover sequence + lead, send on a fresh healthy account,
 * refund old costs, provision new ones, mute the local row.
 *
 * Failures fall into two buckets:
 *   - **Transient** (no_healthy_accounts_available): row is left alone.
 *     lastAttemptAt holds it out of SELECT for ATTEMPT_COOLDOWN_MINUTES,
 *     then it's eligible again.
 *   - **Terminal-for-us** (parent run gone, parent identity unusable,
 *     key unavailable, runs-service 409, no sequence, no local lead):
 *     row is flipped to `delivery_status='cancelled'` via
 *     `handleCampaignError`. lead-service can re-send under current
 *     ownership.
 *
 * Returns a discriminated outcome instead of throwing so the worker loop
 * can keep iterating without try/catch around each call.
 */
export async function processRow(row: StuckCampaignRow): Promise<RowOutcome> {
  if (!row.campaignId || !row.leadEmail || !row.orgId) {
    await cancelRowAsTerminal(row, "missing_identifiers");
    return { kind: "failed", reason: "missing_identifiers" };
  }

  // Bound retries (DIS-148): a row redispatched MAX_REDISPATCHES times that
  // STILL never reached `email_sent` is presumed un-sendable. Terminal-cancel
  // it instead of looping — once cancelled it leaves the SELECT pool and
  // lead-service can re-attempt with a fresh /send.
  const priorRedispatches = getRedispatchCount(row.metadata);
  if (priorRedispatches >= MAX_REDISPATCHES) {
    await cancelRowAsTerminal(
      row,
      `max_redispatches_exceeded (${priorRedispatches} >= ${MAX_REDISPATCHES})`,
    );
    return { kind: "failed", reason: "max_redispatches_exceeded" };
  }

  // Rate-limit per-row: stamp lastAttemptAt up front so SELECT excludes
  // this row for ATTEMPT_COOLDOWN_MINUTES even if processRow throws.
  await markAttempt(row);

  // Reuse the original parent run's identity so child runs match parent
  // identity (no 409 even after brand/org transfer).
  const parentIdentity = await resolveParentIdentity(row);
  if (!parentIdentity) {
    await cancelRowAsTerminal(row, "parent_run_gone_or_unreadable");
    return { kind: "failed", reason: "parent_run_gone" };
  }

  // Resolve Instantly key for the parent's org (= the org the original
  // /send was for). If the key isn't configured anymore, we can't retry.
  let apiKey: string;
  let keySource: "platform" | "org";
  try {
    const keyResult = await resolveInstantlyApiKey(parentIdentity.orgId, "system", {
      method: "POST",
      path: "/internal/campaigns/retry-stuck",
    });
    apiKey = keyResult.key;
    keySource = keyResult.keySource;
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    await cancelRowAsTerminal(row, `key_unavailable: ${message}`);
    return { kind: "failed", reason: "key_unavailable" };
  }

  try {
    // 1. Pull the live campaign once to recover the sequence. NOT used for
    //    any NSS decision — reconcile owns that signal independently.
    const live = (await getInstantlyCampaign(
      apiKey,
      row.instantlyCampaignId,
    )) as unknown as Record<string, unknown>;

    // Live-status preflight (DIS-148). The SELECT gates on the LOCAL
    // `status='active'`, but reconcile never syncs Instantly-side pause/stop
    // back into local, so a campaign the operator paused in Instantly's UI
    // still reads `active` locally. Read the live status directly: if the
    // campaign is no longer active on Instantly, respect that — sync local
    // status and skip the redispatch rather than resurrecting it on a fresh
    // campaign. (A missing/unknown status → NaN → also treated as not-active,
    // i.e. fail-safe toward NOT sending.)
    const liveStatus = Number((live as { status?: unknown }).status);
    if (liveStatus !== INSTANTLY_STATUS_ACTIVE) {
      const localStatus =
        liveStatus === INSTANTLY_STATUS_COMPLETED ? "completed" : "paused";
      await db
        .update(instantlyCampaigns)
        .set({ status: localStatus, updatedAt: new Date() })
        .where(eq(instantlyCampaigns.id, row.id));
      console.log(
        `[instantly-service] retry-stuck: row=${row.id} instantly=${row.instantlyCampaignId} ` +
          `live status=${liveStatus} (not active) — skipping redispatch, synced local status='${localStatus}'`,
      );
      return { kind: "skipped_paused", liveStatus };
    }

    const seq = extractSequenceFromLive(live);
    if (!seq) {
      await cancelRowAsTerminal(row, "no_sequence_on_live_campaign");
      return { kind: "failed", reason: "no_sequence" };
    }

    // 2. Read the lead's profile data from the local instantly_leads row.
    const [storedLead] = await db
      .select()
      .from(instantlyLeads)
      .where(eq(instantlyLeads.instantlyCampaignId, row.instantlyCampaignId))
      .limit(1);

    if (!storedLead) {
      await cancelRowAsTerminal(row, "lead_profile_not_found");
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
      // Transient — leave the row alone. The cooldown stamp holds it out
      // of SELECT for ATTEMPT_COOLDOWN_MINUTES, then it's eligible again.
      console.warn(
        `[instantly-service] retry-stuck: row=${row.id} send failed (${result.reason}) — cooling down`,
      );
      return { kind: "failed", reason: result.reason };
    }

    // 3b. Stop the bleed (DIS-148): pause the PREDECESSOR Instantly campaign
    //     now that the lead lives on a fresh one. Without this, every
    //     redispatch left the old campaign active, so the prospect could be
    //     contacted by BOTH — multiplied across retries into many
    //     simultaneously-active campaigns. Best-effort: the redispatch has
    //     already succeeded, so a pause failure must NOT abort the flow — log
    //     it (warn = actionable) and continue. Worst case is one extra active
    //     campaign, not the unbounded loop.
    try {
      await updateCampaignStatus(apiKey, row.instantlyCampaignId, "paused");
      console.log(
        `[instantly-service] retry-stuck: paused predecessor campaign=${row.instantlyCampaignId} ` +
          `after redispatch to ${result.value.instantlyCampaignId}`,
      );
    } catch (pauseError: unknown) {
      const msg = pauseError instanceof Error ? pauseError.message : String(pauseError);
      console.warn(
        `[instantly-service] retry-stuck: row=${row.id} failed to pause predecessor ` +
          `${row.instantlyCampaignId}: ${msg}`,
      );
    }

    // 4. Cancel old costs (refund), provision fresh costs (recharge).
    await cancelExistingCosts(row, parentIdentity);
    await provisionFreshCosts(
      row,
      parentIdentity,
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
        orgId: parentIdentity.orgId,
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
          lastAttemptAt: new Date().toISOString(),
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

    await deleteLeadStatusCurrent(row.instantlyCampaignId, row.leadEmail);
    await refreshLeadStatusCurrent(result.value.instantlyCampaignId, row.leadEmail);

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
    // 409 from runs-service (parent/child identity drift) — cancel the row.
    // Anything else is unexpected; cool down via lastAttemptAt and let the
    // next pass try again with fresh state.
    if (/failed: 409\b/.test(message)) {
      await cancelRowAsTerminal(row, `runs_service_409: ${message}`);
      return { kind: "failed", reason: "runs_service_409" };
    }
    console.error(
      `[instantly-service] retry-stuck: row=${row.id} threw: ${message}`,
    );
    return { kind: "failed", reason: message };
  }
}
