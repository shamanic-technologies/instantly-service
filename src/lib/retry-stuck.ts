/**
 * Retry-stuck job — detects campaigns that Instantly is refusing to send and
 * re-dispatches the lead onto a fresh healthy account when possible.
 *
 * Selection: rows with `delivery_status='contacted' AND status='active'` that
 * are older than 24h, oldest first, capped at MAX_ROWS_PER_RUN per sweep so a
 * single tick can't run for hours on a large backlog.
 *
 * Concurrency: a Postgres advisory lock (`pg_try_advisory_lock(8729, 1)`)
 * gates the sweep so overlapping cron ticks can't double-cancel + double-
 * refund the same row. The second caller short-circuits with
 * `{ skipped: "sweep_in_progress" }`. Released in `finally`.
 *
 * Per row:
 *   1. Resolve the org's Instantly API key (once per org, with its keySource).
 *   2. Fetch the live campaign from Instantly. If `not_sending_status IS NULL`,
 *      the campaign is still actively trying — leave it alone.
 *   3. Pause the original Instantly campaign (`status=paused`) so no further
 *      dispatch can race the next steps.
 *   4. Write the observed `not_sending_status` onto the row (diagnostic).
 *   5. **Re-dispatch attempt** — read the sequence from the failing Instantly
 *      campaign, strip the old account's signature from each body, look up the
 *      lead's profile data locally, and call `dispatchLeadToInstantly()` to
 *      provision a new campaign on a different healthy account.
 *      - On success: cancel the old cost rows (refund), provision fresh costs
 *        on new step runs (re-charge), mute the local row in place to point at
 *        the new Instantly campaign, append a `redispatchHistory` entry. Row's
 *        `delivery_status` stays `'contacted'`.
 *      - On failure (no healthy account or all attempts hit
 *        `not_sending_status`): fall through to terminal cancellation via
 *        `handleCampaignError(cancelled)` — refund only, no re-charge.
 *
 * Throughput: rows are processed in `Promise.all` batches of BATCH_SIZE per
 * org. The instantly-client throttle paces concurrent API calls below the
 * 600 req/min general cap.
 *
 * The endpoint that drives this lives at POST /internal/campaigns/retry-stuck
 * (cron-driven, daily 02:00 UTC) in routes/campaigns.ts.
 */

import { db } from "../db";
import { instantlyCampaigns, instantlyLeads, sequenceCosts } from "../db/schema";
import { and, eq, or, sql } from "drizzle-orm";
import {
  getCampaign as getInstantlyCampaign,
  updateCampaignStatus as updateInstantlyStatus,
  type Lead,
} from "./instantly-client";
import { resolveInstantlyApiKey, KeyServiceError } from "./key-client";
import { handleCampaignError } from "./campaign-error-handler";
import {
  dispatchLeadToInstantly,
  stripAccountSignature,
  type SortedSequenceStep,
} from "./dispatch-lead";
import {
  addCosts,
  createRun,
  updateCostStatus,
  updateRun,
  type IdentityContext,
} from "./runs-client";

/** Age (hours) a row must reach before the cron picks it up. */
export const STUCK_AGE_HOURS = 24;

/**
 * Cap rows processed per sweep so a daily tick has a bounded runtime.
 *
 * At ~2.6s per row (observed prod: 351 re-dispatches in 22min on 2026-05-23),
 * 5000 rows ≈ 3.5h per sweep. Comfortably under the daily window and large
 * enough that a typical backlog drains in a single tick instead of dragging
 * across N days.
 */
export const MAX_ROWS_PER_RUN = 5000;

/** Per-tick batch size for parallel Instantly calls. */
export const BATCH_SIZE = 10;

/** Postgres advisory-lock keyspace for the retry-stuck sweep singleton. */
const SWEEP_LOCK_KEY_1 = 8729;
const SWEEP_LOCK_KEY_2 = 1;

export interface RetryStuckSummary {
  scanned: number;
  cancelled: number;
  redispatched: number;
  stillSending: number;
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
    FROM instantly_campaigns
    WHERE delivery_status = 'contacted'
      AND status = 'active'
      AND created_at < NOW() - INTERVAL '${sql.raw(`${STUCK_AGE_HOURS} hours`)}'
    ORDER BY created_at ASC
    LIMIT ${MAX_ROWS_PER_RUN}
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
    cancelled: 0,
    redispatched: 0,
    stillSending: 0,
    skippedNoKey: 0,
    failed: 0,
    durationMs,
  };
}

interface RowOutcome {
  cancelled: number;
  redispatched: number;
  stillSending: number;
  failed: number;
}

/**
 * Translate the Instantly campaign config returned by `getCampaign` into the
 * normalized `SortedSequenceStep[]` shape expected by `dispatchLeadToInstantly`.
 *
 * Instantly's per-step `delay` is "days AFTER this step before the NEXT one".
 * Our `daysSinceLastStep` on step N is "days BEFORE step N (since step N-1)".
 * So `sortedSequence[i].daysSinceLastStep = liveSteps[i-1].delay` for `i >= 1`,
 * and `0` for the first step.
 *
 * Each body has the previous account's signature appended (`\n\n--\n<sig>`);
 * we strip that so the new account's signature can be re-injected by
 * `buildSequenceSteps` inside the dispatch helper.
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
 * Provision fresh cost rows for each step of the re-dispatched campaign on a
 * new per-step run. Mirrors the /send entry-point pattern.
 */
async function provisionFreshCostsForRedispatch(
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

interface RedispatchOutcome {
  ok: boolean;
  newInstantlyCampaignId?: string;
  accountEmail?: string;
  reason?: string;
}

async function tryRedispatch(
  apiKey: string,
  keySource: "platform" | "org",
  row: StuckCampaignRow,
  live: Record<string, unknown>,
): Promise<RedispatchOutcome> {
  if (!row.campaignId || !row.leadEmail || !row.orgId) {
    return { ok: false, reason: "row_missing_identifiers" };
  }

  const seq = extractSequenceFromLive(live);
  if (!seq) {
    return { ok: false, reason: "no_sequence_on_live_campaign" };
  }

  // Look up the lead's profile data (firstName/lastName/etc.) from the local
  // instantly_leads row originally inserted at /send time (or by a previous
  // re-dispatch). Matches against the CURRENT instantlyCampaignId — every
  // successful re-dispatch also writes a fresh instantly_leads row for the
  // new campaign so future re-dispatches still resolve.
  const [storedLead] = await db
    .select()
    .from(instantlyLeads)
    .where(eq(instantlyLeads.instantlyCampaignId, row.instantlyCampaignId))
    .limit(1);

  if (!storedLead) {
    return { ok: false, reason: "lead_profile_not_found" };
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

  const dispatch = await dispatchLeadToInstantly({
    apiKey,
    campaignName,
    subject: seq.subject,
    sortedSequence: seq.sortedSequence,
    lead,
  });

  if (!dispatch.ok) {
    return { ok: false, reason: dispatch.reason };
  }

  // Identity for cost & run writes: prefer the row's userId so cost lineage
  // points back to the originating user, fall back to the system uuid when
  // missing (legacy rows pre-dating the column).
  const identity: IdentityContext = {
    orgId: row.orgId,
    userId: row.userId ?? "00000000-0000-0000-0000-000000000000",
    runId: row.runId ?? undefined,
  };

  // 1. Cancel old costs first (refund customer for the failed attempt).
  await cancelExistingCosts(row, identity);

  // 2. Provision fresh costs on new step runs (re-charge for the new attempt).
  await provisionFreshCostsForRedispatch(
    row,
    identity,
    keySource,
    seq.sortedSequence.length,
  );

  // 3. Mirror the lead onto the new Instantly campaign so the next sweep can
  // still resolve the lead's profile data.
  await db
    .insert(instantlyLeads)
    .values({
      instantlyCampaignId: dispatch.value.instantlyCampaignId,
      email: storedLead.email,
      firstName: storedLead.firstName,
      lastName: storedLead.lastName,
      companyName: storedLead.companyName,
      customVariables: storedLead.customVariables,
      orgId: row.orgId,
      runId: null,
    })
    .onConflictDoNothing();

  // 4. Mute the campaign row in place: new Instantly campaign ID, NSS cleared,
  // metadata bumped with the redispatch history entry. delivery_status stays
  // `'contacted'` — the lead is back to actively being dispatched.
  const existingMetadata = (row.metadata ?? {}) as Record<string, unknown>;
  const existingHistory = Array.isArray(existingMetadata.redispatchHistory)
    ? (existingMetadata.redispatchHistory as Array<Record<string, unknown>>)
    : [];

  await db
    .update(instantlyCampaigns)
    .set({
      instantlyCampaignId: dispatch.value.instantlyCampaignId,
      name: campaignName,
      notSendingStatus: null,
      notSendingStatusSeenAt: null,
      metadata: {
        ...existingMetadata,
        redispatchCount: redispatchCount + 1,
        redispatchHistory: [
          ...existingHistory,
          {
            from: row.instantlyCampaignId,
            to: dispatch.value.instantlyCampaignId,
            account: dispatch.value.account.email,
            at: new Date().toISOString(),
          },
        ],
      },
      updatedAt: new Date(),
    })
    .where(eq(instantlyCampaigns.id, row.id));

  return {
    ok: true,
    newInstantlyCampaignId: dispatch.value.instantlyCampaignId,
    accountEmail: dispatch.value.account.email,
  };
}

async function processRow(
  apiKey: string,
  keySource: "platform" | "org",
  row: StuckCampaignRow,
): Promise<RowOutcome> {
  try {
    const live = (await getInstantlyCampaign(
      apiKey,
      row.instantlyCampaignId,
    )) as unknown as Record<string, unknown>;

    const notSendingStatus = live.not_sending_status;
    if (notSendingStatus === undefined || notSendingStatus === null) {
      return { cancelled: 0, redispatched: 0, stillSending: 1, failed: 0 };
    }

    // Pause the original Instantly campaign before any cancel/re-dispatch
    // work so no in-flight Instantly send can race us.
    try {
      await updateInstantlyStatus(apiKey, row.instantlyCampaignId, "paused");
    } catch (pauseErr: unknown) {
      const msg = pauseErr instanceof Error ? pauseErr.message : String(pauseErr);
      console.warn(
        `[instantly-service] retry-stuck: failed to pause campaign=${row.instantlyCampaignId}: ${msg} — proceeding`,
      );
    }

    // Persist the observed diagnostic onto the row for dashboards/queries.
    const nssNumeric = typeof notSendingStatus === "number" ? notSendingStatus : null;
    await db
      .update(instantlyCampaigns)
      .set({
        notSendingStatus: nssNumeric,
        notSendingStatusSeenAt: new Date(),
      })
      .where(eq(instantlyCampaigns.id, row.id));

    // Try re-dispatching the lead onto a different healthy account.
    const redispatch = await tryRedispatch(apiKey, keySource, row, live);

    if (redispatch.ok) {
      console.log(
        `[instantly-service] retry-stuck: re-dispatched row=${row.id} ` +
          `from=${row.instantlyCampaignId} to=${redispatch.newInstantlyCampaignId} ` +
          `account=${redispatch.accountEmail}`,
      );
      return { cancelled: 0, redispatched: 1, stillSending: 0, failed: 0 };
    }

    // Re-dispatch impossible — fall through to terminal cancellation.
    const reason = `not_sending_status: ${JSON.stringify(notSendingStatus)} (redispatch_failed: ${redispatch.reason})`;
    await handleCampaignError(row.instantlyCampaignId, reason, {
      terminalStatus: "cancelled",
      extraMetadata: {
        notSendingStatus,
        redispatchCount: getRedispatchCount(row.metadata),
        lastRedispatchFailure: redispatch.reason,
      },
    });

    console.log(
      `[instantly-service] retry-stuck: cancelled campaign=${row.instantlyCampaignId} ` +
        `lead=${row.leadEmail} reason=${reason}`,
    );
    return { cancelled: 1, redispatched: 0, stillSending: 0, failed: 0 };
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(
      `[instantly-service] retry-stuck: row=${row.instantlyCampaignId} failed: ${message}`,
    );
    return { cancelled: 0, redispatched: 0, stillSending: 0, failed: 1 };
  }
}

/**
 * Run the retry-stuck sweep. Returns counters for logging / response payloads.
 *
 * Acquires a Postgres advisory lock on entry; if another sweep holds it,
 * returns immediately with `skipped: "sweep_in_progress"` and an otherwise-
 * zeroed summary. The lock is released in `finally`.
 *
 * Errors per row are caught + counted (`failed++`) — a single bad campaign
 * must not halt the sweep.
 */
export async function runRetryStuck(): Promise<RetryStuckSummary> {
  const startedAt = Date.now();

  const acquired = await tryAcquireSweepLock();
  if (!acquired) {
    console.warn(
      `[instantly-service] retry-stuck: skipped (another sweep is in progress)`,
    );
    return {
      ...emptySummary(Date.now() - startedAt),
      skipped: "sweep_in_progress",
    };
  }

  try {
    const rows = await selectStuckRows();

    console.log(
      `[instantly-service] retry-stuck: starting, candidates=${rows.length} (cap=${MAX_ROWS_PER_RUN})`,
    );

    let scanned = 0;
    let cancelled = 0;
    let redispatched = 0;
    let stillSending = 0;
    let skippedNoKey = 0;
    let failed = 0;

    // Group by orgId so we resolve the Instantly key once per org, not per row.
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
          cancelled += outcome.cancelled;
          redispatched += outcome.redispatched;
          stillSending += outcome.stillSending;
          failed += outcome.failed;
        }
      }
    }

    const durationMs = Date.now() - startedAt;
    console.log(
      `[instantly-service] retry-stuck: done scanned=${scanned} cancelled=${cancelled} ` +
        `redispatched=${redispatched} stillSending=${stillSending} ` +
        `skippedNoKey=${skippedNoKey} failed=${failed} duration=${durationMs}ms`,
    );

    return {
      scanned,
      cancelled,
      redispatched,
      stillSending,
      skippedNoKey,
      failed,
      durationMs,
    };
  } finally {
    await releaseSweepLock();
  }
}
