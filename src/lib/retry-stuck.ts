/**
 * Retry-stuck job — heartbeat-driven sweep that re-sends leads stuck in
 * `delivery_status='contacted'` past the age floor onto a fresh healthy
 * Instantly account.
 *
 * Selection criteria (LOCAL DB only — no Instantly preflight):
 *   - `delivery_status = 'contacted'` (lead pushed, not yet observed sending)
 *   - `status = 'active'` (not in terminal error state locally)
 *   - `created_at < NOW() - INTERVAL '72 hours'` (3 days = beyond Instantly's
 *     weekday/business-hours dispatch window)
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
 *      No terminal cancel. Next tick retries. Instantly is free; with 100+
 *      accounts random sampling converges.
 *
 * Concurrency: a Postgres advisory lock (`pg_try_advisory_lock(8729, 1)`)
 * gates each tick so overlapping invocations can't double-process the same
 * row. The lock is held for the duration of one tick (~seconds), released
 * in `finally`. Heartbeat worker (lib/retry-stuck-worker.ts) drives ticks
 * every RETRY_STUCK_TICK_INTERVAL_MS; manual triggers via
 * `POST /internal/campaigns/retry-stuck` use the same path.
 *
 * Throughput: rows are processed in `Promise.all` batches of BATCH_SIZE per
 * org. The instantly-client throttle (process-global) paces concurrent
 * Instantly API calls below per-workspace caps.
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

/** Cap rows processed per tick so the advisory lock window stays short. */
export const MAX_ROWS_PER_TICK = 100;

/** Per-tick batch size for parallel Instantly calls within a single org. */
export const BATCH_SIZE = 10;

/** Postgres advisory-lock keyspace for the retry-stuck sweep singleton. */
const SWEEP_LOCK_KEY_1 = 8729;
const SWEEP_LOCK_KEY_2 = 1;

const SYSTEM_USER_ID = "00000000-0000-0000-0000-000000000000";

export interface RetryStuckSummary {
  scanned: number;
  redispatched: number;
  skippedNoKey: number;
  failed: number;
  durationMs: number;
  /** When set, no work was done because another sweep holds the lock. */
  skipped?: "sweep_in_progress";
}

interface StuckCampaignRow {
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

async function tryAcquireSweepLock(): Promise<boolean> {
  const result = await db.execute(
    sql`SELECT pg_try_advisory_lock(${SWEEP_LOCK_KEY_1}, ${SWEEP_LOCK_KEY_2}) AS locked`,
  );
  const rows = Array.isArray(result)
    ? (result as Array<{ locked?: boolean }>)
    : ((result as { rows?: Array<{ locked?: boolean }> }).rows ?? []);
  return rows[0]?.locked === true;
}

async function releaseSweepLock(): Promise<void> {
  await db.execute(
    sql`SELECT pg_advisory_unlock(${SWEEP_LOCK_KEY_1}, ${SWEEP_LOCK_KEY_2})`,
  );
}

async function selectStuckRows(): Promise<StuckCampaignRow[]> {
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
    LIMIT ${MAX_ROWS_PER_TICK}
  `);
  const rows = Array.isArray(result) ? result : (result as { rows?: unknown[] }).rows ?? [];
  return rows as StuckCampaignRow[];
}

function getRedispatchCount(metadata: Record<string, unknown> | null): number {
  if (!metadata) return 0;
  const raw = metadata.redispatchCount;
  if (typeof raw === "number" && Number.isFinite(raw)) return raw;
  return 0;
}

function emptySummary(durationMs: number): RetryStuckSummary {
  return {
    scanned: 0,
    redispatched: 0,
    skippedNoKey: 0,
    failed: 0,
    durationMs,
  };
}

interface RowOutcome {
  redispatched: number;
  failed: number;
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
 * `cancelled`. Returns the count of cost rows transitioned.
 */
async function cancelExistingCosts(
  row: StuckCampaignRow,
  identity: IdentityContext,
): Promise<number> {
  if (!row.campaignId || !row.leadEmail) return 0;

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

  return existing.length;
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

async function processRow(
  apiKey: string,
  keySource: "platform" | "org",
  row: StuckCampaignRow,
): Promise<RowOutcome> {
  try {
    // 1. Pull the live campaign once to recover the sequence. NOT used for any
    //    NSS decision — reconcile owns that signal independently.
    const live = (await getInstantlyCampaign(
      apiKey,
      row.instantlyCampaignId,
    )) as unknown as Record<string, unknown>;

    const seq = extractSequenceFromLive(live);
    if (!seq) {
      console.warn(
        `[instantly-service] retry-stuck: row=${row.id} instantly=${row.instantlyCampaignId} no sequence on live campaign — leaving alone`,
      );
      return { redispatched: 0, failed: 1 };
    }

    if (!row.campaignId || !row.leadEmail || !row.orgId) {
      console.warn(
        `[instantly-service] retry-stuck: row=${row.id} missing campaignId/leadEmail/orgId — leaving alone`,
      );
      return { redispatched: 0, failed: 1 };
    }

    // 2. Read the lead's profile data (firstName/lastName/etc.) from the local
    //    instantly_leads row originally inserted at /send time (or by a previous
    //    re-send). Match against the CURRENT instantlyCampaignId — every
    //    successful re-send mirrors a fresh instantly_leads row for the new
    //    campaign so future re-sends still resolve.
    const [storedLead] = await db
      .select()
      .from(instantlyLeads)
      .where(eq(instantlyLeads.instantlyCampaignId, row.instantlyCampaignId))
      .limit(1);

    if (!storedLead) {
      console.warn(
        `[instantly-service] retry-stuck: row=${row.id} lead profile not found in instantly_leads — leaving alone`,
      );
      return { redispatched: 0, failed: 1 };
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
        `[instantly-service] retry-stuck: row=${row.id} send failed (${result.reason}) — leaving alone, next tick will retry`,
      );
      return { redispatched: 0, failed: 1 };
    }

    // 4. Identity for cost & run writes: prefer the row's userId so cost
    //    lineage points back to the originating user, fall back to the system
    //    UUID when missing (legacy rows pre-dating the column).
    const identity: IdentityContext = {
      orgId: row.orgId,
      userId: row.userId ?? SYSTEM_USER_ID,
      runId: row.runId ?? undefined,
    };

    // 5. Cancel old costs (refund), provision fresh costs (recharge).
    await cancelExistingCosts(row, identity);
    await provisionFreshCosts(
      row,
      identity,
      keySource,
      seq.sortedSequence.length,
    );

    // 6. Mirror the lead onto the new Instantly campaign so the next sweep can
    //    still resolve the lead's profile data.
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

    // 7. Mute the campaign row in place: new Instantly campaign ID, metadata
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
    return { redispatched: 1, failed: 0 };
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(
      `[instantly-service] retry-stuck: row=${row.instantlyCampaignId} threw: ${message}`,
    );
    return { redispatched: 0, failed: 1 };
  }
}

/**
 * Run one retry-stuck tick. Returns counters for logging / response payloads.
 *
 * Acquires a Postgres advisory lock on entry; if another tick holds it,
 * returns immediately with `skipped: "sweep_in_progress"` and an otherwise-
 * zeroed summary. The lock is released in `finally`.
 *
 * Errors per row are caught + counted (`failed++`) — a single bad row must
 * not halt the tick.
 */
export async function runRetryStuck(): Promise<RetryStuckSummary> {
  const startedAt = Date.now();

  const acquired = await tryAcquireSweepLock();
  if (!acquired) {
    console.warn(
      `[instantly-service] retry-stuck: skipped (another tick is in progress)`,
    );
    return {
      ...emptySummary(Date.now() - startedAt),
      skipped: "sweep_in_progress",
    };
  }

  try {
    const rows = await selectStuckRows();

    console.log(
      `[instantly-service] retry-stuck: starting, candidates=${rows.length} (cap=${MAX_ROWS_PER_TICK})`,
    );

    let scanned = 0;
    let redispatched = 0;
    let skippedNoKey = 0;
    let failed = 0;

    // Group by orgId so each org's Instantly key is resolved once per tick.
    const byOrg = new Map<string | null, StuckCampaignRow[]>();
    for (const r of rows) {
      const k = r.orgId ?? null;
      if (!byOrg.has(k)) byOrg.set(k, []);
      byOrg.get(k)!.push(r);
    }

    for (const [orgId, orgRows] of byOrg) {
      let apiKey: string;
      let keySource: "platform" | "org";
      try {
        if (!orgId) throw new Error("Campaign missing orgId");
        const keyResult = await resolveInstantlyApiKey(orgId, "system", {
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
          `[instantly-service] retry-stuck: skipping org=${orgId} (${orgRows.length} rows) — ${message}`,
        );
        skippedNoKey += orgRows.length;
        continue;
      }

      // Parallel batches per org. The instantly-client throttle paces the
      // concurrent Instantly API calls within each batch.
      for (let i = 0; i < orgRows.length; i += BATCH_SIZE) {
        const batch = orgRows.slice(i, i + BATCH_SIZE);
        const outcomes = await Promise.all(
          batch.map((row) => processRow(apiKey, keySource, row)),
        );
        for (const outcome of outcomes) {
          scanned++;
          redispatched += outcome.redispatched;
          failed += outcome.failed;
        }
      }
    }

    const durationMs = Date.now() - startedAt;
    console.log(
      `[instantly-service] retry-stuck: tick processed=${scanned} redispatched=${redispatched} ` +
        `skippedNoKey=${skippedNoKey} failed=${failed} duration=${durationMs}ms`,
    );

    return {
      scanned,
      redispatched,
      skippedNoKey,
      failed,
      durationMs,
    };
  } finally {
    await releaseSweepLock();
  }
}
