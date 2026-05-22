/**
 * Retry-stuck job — detects campaigns that Instantly is refusing to send and
 * cancels the customer's reserved spend.
 *
 * Selection: rows with `delivery_status='contacted' AND status='active'` that
 * are older than 24h, oldest first, capped at MAX_ROWS_PER_RUN per sweep so a
 * single tick can't run for hours on a large backlog.
 *
 * Concurrency: a Postgres advisory lock (`pg_try_advisory_lock(8729, 1)`)
 * gates the sweep so overlapping cron ticks can't double-cancel + double-
 * refund the same row. The second caller short-circuits with
 * `{ skipped: "sweep_in_progress" }` and releases nothing. The lock is held
 * for the full sweep duration (including background processing after the
 * HTTP 202 returns) and released in a `finally` block.
 *
 * Per row:
 *   1. Resolve the org's Instantly API key (once per org).
 *   2. Fetch the live campaign from Instantly. If `not_sending_status IS NULL`,
 *      the campaign is still actively trying — leave it alone.
 *   3. If `metadata.retryCount >= MAX_RETRIES`, leave it alone (idempotency cap).
 *   4. Pause the Instantly campaign (`status=paused`) to stop further dispatch.
 *   5. Write the observed `not_sending_status` + seen-at onto the row so
 *      observers (status endpoint, dashboards) can see the diagnostic
 *      without having to re-fetch Instantly.
 *   6. Delegate to `handleCampaignError(..., { terminalStatus: 'cancelled' })`
 *      to cancel both actual+provisioned costs, fail step runs. The handler
 *      suppresses its admin email on the cancelled path so the cron sweep
 *      does not flood the inbox.
 *
 * Throughput: rows are processed in `Promise.all` batches of BATCH_SIZE. The
 * Instantly client's internal throttle (`/emails` 20 req/min, general 600
 * req/min) paces the concurrent calls — bursts within a batch flatten out
 * to the throttle's min-interval gate.
 *
 * The endpoint that drives this lives at POST /internal/campaigns/retry-stuck
 * (cron-driven, daily 02:00 UTC) in routes/campaigns.ts.
 */

import { db } from "../db";
import { instantlyCampaigns } from "../db/schema";
import { eq, sql } from "drizzle-orm";
import {
  getCampaign as getInstantlyCampaign,
  updateCampaignStatus as updateInstantlyStatus,
} from "./instantly-client";
import { resolveInstantlyApiKey, KeyServiceError } from "./key-client";
import { handleCampaignError } from "./campaign-error-handler";

/** Max times a single (campaignId, leadEmail) row can be processed by this job. */
export const MAX_RETRIES = 2;

/** Age (hours) a row must reach before the cron picks it up. */
export const STUCK_AGE_HOURS = 24;

/** Cap rows processed per sweep so a daily tick has a bounded runtime. */
export const MAX_ROWS_PER_RUN = 500;

/** Per-tick batch size for parallel Instantly calls. */
export const BATCH_SIZE = 10;

/** Postgres advisory-lock keyspace for the retry-stuck sweep singleton. */
const SWEEP_LOCK_KEY_1 = 8729;
const SWEEP_LOCK_KEY_2 = 1;

export interface RetryStuckSummary {
  scanned: number;
  cancelled: number;
  stillSending: number;
  capped: number;
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

function getRetryCount(metadata: Record<string, unknown> | null): number {
  if (!metadata) return 0;
  const raw = metadata.retryCount;
  if (typeof raw === "number" && Number.isFinite(raw)) return raw;
  return 0;
}

function emptySummary(durationMs: number): RetryStuckSummary {
  return {
    scanned: 0,
    cancelled: 0,
    stillSending: 0,
    capped: 0,
    skippedNoKey: 0,
    failed: 0,
    durationMs,
  };
}

interface RowOutcome {
  cancelled: number;
  stillSending: number;
  capped: number;
  failed: number;
}

async function processRow(
  apiKey: string,
  row: StuckCampaignRow,
): Promise<RowOutcome> {
  try {
    const retryCount = getRetryCount(row.metadata);
    if (retryCount >= MAX_RETRIES) {
      return { cancelled: 0, stillSending: 0, capped: 1, failed: 0 };
    }

    const live = (await getInstantlyCampaign(
      apiKey,
      row.instantlyCampaignId,
    )) as unknown as Record<string, unknown>;

    const notSendingStatus = live.not_sending_status;
    if (notSendingStatus === undefined || notSendingStatus === null) {
      return { cancelled: 0, stillSending: 1, capped: 0, failed: 0 };
    }

    // Pause the Instantly campaign before cancelling costs so no further
    // dispatch can race the cancel write.
    try {
      await updateInstantlyStatus(apiKey, row.instantlyCampaignId, "paused");
    } catch (pauseErr: unknown) {
      const msg = pauseErr instanceof Error ? pauseErr.message : String(pauseErr);
      console.warn(
        `[instantly-service] retry-stuck: failed to pause campaign=${row.instantlyCampaignId}: ${msg} — proceeding with cost cancel`,
      );
    }

    // Persist the observed diagnostic onto the row BEFORE flipping
    // delivery_status. Idempotent: re-running the sweep against the same
    // row writes the same value (and handleCampaignError's idempotency
    // guard prevents the second cancel).
    const nssNumeric = typeof notSendingStatus === "number" ? notSendingStatus : null;
    await db
      .update(instantlyCampaigns)
      .set({
        notSendingStatus: nssNumeric,
        notSendingStatusSeenAt: new Date(),
      })
      .where(eq(instantlyCampaigns.id, row.id));

    const reason = `not_sending_status: ${JSON.stringify(notSendingStatus)}`;
    await handleCampaignError(row.instantlyCampaignId, reason, {
      terminalStatus: "cancelled",
      extraMetadata: {
        notSendingStatus,
        retryCount: retryCount + 1,
      },
    });

    console.log(
      `[instantly-service] retry-stuck: cancelled campaign=${row.instantlyCampaignId} ` +
        `lead=${row.leadEmail} reason=${reason}`,
    );
    return { cancelled: 1, stillSending: 0, capped: 0, failed: 0 };
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(
      `[instantly-service] retry-stuck: row=${row.instantlyCampaignId} failed: ${message}`,
    );
    return { cancelled: 0, stillSending: 0, capped: 0, failed: 1 };
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
    let stillSending = 0;
    let capped = 0;
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
      try {
        if (!orgId) throw new Error("Campaign missing orgId");
        const keyResult = await resolveInstantlyApiKey(orgId, "system", {
          method: "POST",
          path: "/internal/campaigns/retry-stuck",
        });
        apiKey = keyResult.key;
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

      // Parallel batches per org. Inside a batch, the Instantly client's
      // throttle paces the concurrent calls below the rate cap.
      for (let i = 0; i < orgRows.length; i += BATCH_SIZE) {
        const batch = orgRows.slice(i, i + BATCH_SIZE);
        const outcomes = await Promise.all(
          batch.map((row) => processRow(apiKey, row)),
        );
        for (const outcome of outcomes) {
          scanned++;
          cancelled += outcome.cancelled;
          stillSending += outcome.stillSending;
          capped += outcome.capped;
          failed += outcome.failed;
        }
      }
    }

    const durationMs = Date.now() - startedAt;
    console.log(
      `[instantly-service] retry-stuck: done scanned=${scanned} cancelled=${cancelled} ` +
        `stillSending=${stillSending} capped=${capped} skippedNoKey=${skippedNoKey} ` +
        `failed=${failed} duration=${durationMs}ms`,
    );

    return {
      scanned,
      cancelled,
      stillSending,
      capped,
      skippedNoKey,
      failed,
      durationMs,
    };
  } finally {
    await releaseSweepLock();
  }
}
