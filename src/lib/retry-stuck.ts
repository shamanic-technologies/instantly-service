/**
 * Retry-stuck job — detects campaigns that Instantly is refusing to send and
 * cancels the customer's reserved spend.
 *
 * Selection: rows with `delivery_status='contacted' AND status='active'` that
 * are older than 24h (default) — i.e. lead was pushed to Instantly but the
 * `email_sent` webhook never arrived. The 24h floor avoids racing the normal
 * dispatch window. The retro path (`all=true`) skips the age filter so the
 * one-shot endpoint can sweep historical rows in a single pass.
 *
 * Per row:
 *   1. Resolve the org's Instantly API key.
 *   2. Fetch the live campaign from Instantly. If `not_sending_status IS NULL`,
 *      the campaign is still actively trying — leave it alone.
 *   3. If `metadata.retryCount >= MAX_RETRIES`, leave it alone (idempotency cap).
 *   4. Pause the Instantly campaign (`status=paused`) to stop further dispatch.
 *   5. Delegate to `handleCampaignError(..., { terminalStatus: 'cancelled' })`
 *      to cancel both actual+provisioned costs, fail step runs, send admin email.
 *      `terminalStatus='cancelled'` + extra metadata (`notSendingStatus`,
 *      `retryCount`) keep this path distinguishable from the inline-error path.
 *
 * The endpoint that drives this lives at POST /internal/campaigns/retry-stuck
 * (cron-driven, every 1h) and POST /internal/campaigns/retry-stuck-now (retro,
 * sync) — both in routes/campaigns.ts.
 */

import { db } from "../db";
import { instantlyCampaigns } from "../db/schema";
import { sql } from "drizzle-orm";
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

export interface RetryStuckSummary {
  scanned: number;
  cancelled: number;
  stillSending: number;
  capped: number;
  skippedNoKey: number;
  failed: number;
  durationMs: number;
}

export interface RetryStuckOptions {
  /** When true, ignore the 24h age filter (retro one-shot). */
  all?: boolean;
}

interface StuckCampaignRow {
  id: string;
  instantlyCampaignId: string;
  campaignId: string | null;
  leadEmail: string | null;
  orgId: string | null;
  metadata: Record<string, unknown> | null;
}

async function selectStuckRows(opts: RetryStuckOptions): Promise<StuckCampaignRow[]> {
  // Cron path: 24h age floor. Retro path (`all=true`): no age filter so a
  // single sweep can cancel every currently-stuck row.
  const ageClause = opts.all
    ? sql``
    : sql`AND created_at < NOW() - INTERVAL '${sql.raw(`${STUCK_AGE_HOURS} hours`)}'`;

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
      ${ageClause}
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

/**
 * Run the retry-stuck sweep. Returns counters for logging / response payloads.
 *
 * Cron callers should invoke with no options; retro callers with `{ all: true }`.
 * Errors per row are caught + counted (`failed++`) — a single bad campaign must
 * not halt the sweep.
 */
export async function runRetryStuck(
  opts: RetryStuckOptions = {},
): Promise<RetryStuckSummary> {
  const startedAt = Date.now();

  const rows = await selectStuckRows(opts);

  console.log(
    `[instantly-service] retry-stuck: starting, candidates=${rows.length} all=${!!opts.all}`,
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

    for (const row of orgRows) {
      scanned++;
      try {
        const retryCount = getRetryCount(row.metadata);
        if (retryCount >= MAX_RETRIES) {
          capped++;
          continue;
        }

        const live = (await getInstantlyCampaign(
          apiKey,
          row.instantlyCampaignId,
        )) as unknown as Record<string, unknown>;

        const notSendingStatus = live.not_sending_status;
        if (notSendingStatus === undefined || notSendingStatus === null) {
          stillSending++;
          continue;
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

        const reason = `not_sending_status: ${JSON.stringify(notSendingStatus)}`;
        await handleCampaignError(row.instantlyCampaignId, reason, {
          terminalStatus: "cancelled",
          extraMetadata: {
            notSendingStatus,
            retryCount: retryCount + 1,
          },
        });

        cancelled++;
        console.log(
          `[instantly-service] retry-stuck: cancelled campaign=${row.instantlyCampaignId} ` +
            `lead=${row.leadEmail} reason=${reason}`,
        );
      } catch (error: unknown) {
        failed++;
        const message = error instanceof Error ? error.message : String(error);
        console.error(
          `[instantly-service] retry-stuck: row=${row.instantlyCampaignId} failed: ${message}`,
        );
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
}
