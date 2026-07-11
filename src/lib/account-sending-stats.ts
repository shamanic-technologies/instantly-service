/**
 * Per-account sending-throughput reads for the staff account-health table
 * (GET /internal/audit/account-health). Both figures are derived from OUR
 * authoritative silver + cost-hold data — Instantly's V2 account object exposes
 * neither a sent-today counter nor a queue size, so these are computed locally,
 * fail-loud, never fabricated.
 *
 *   fetchSentTodayByAccount  — real (non-inferred) `email_sent` events observed
 *                              today (UTC) grouped by sending account_email.
 *   fetchQueueSizeByAccount  — still-provisioned sequence-cost holds on active
 *                              campaigns, attributed to each campaign's observed
 *                              sending account (1 campaign = 1 lead = 1 account).
 *
 * IO glue only — the pure mapping (buildAccountHealth) lives in account-health.ts.
 */

import { sql } from "drizzle-orm";
import { db } from "../db";
import { getOrSetCachedStats } from "./stats-cache";
import {
  aggregateQueueBreakdown,
  type QueueBreakdown,
  type QueuedSequenceInput,
} from "./queue-breakdown";

interface CountRow {
  account_email: string;
  count: number | string;
}

function rowsOf(result: unknown): CountRow[] {
  if (!result) return [];
  return Array.isArray(result)
    ? (result as CountRow[])
    : (((result as { rows?: CountRow[] }).rows) ?? []);
}

function toMap(rows: CountRow[]): Map<string, number> {
  const out = new Map<string, number>();
  for (const row of rows) {
    if (!row.account_email) continue;
    out.set(row.account_email, Number(row.count));
  }
  return out;
}

/**
 * Real `email_sent` events observed today (UTC) per sending account. Excludes
 * inferred (synthetic-predecessor) rows so the count reflects actual observed
 * dispatches, matching Instantly's UI "N/dailyLimit" reading.
 */
export async function fetchSentTodayByAccount(): Promise<Map<string, number>> {
  const result = await db.execute(sql`
    SELECT e.account_email, COUNT(*) AS count
    FROM instantly_events e
    WHERE e.event_type = 'email_sent'
      AND e.inferred = false
      AND e.account_email IS NOT NULL
      AND e.timestamp >= date_trunc('day', (now() AT TIME ZONE 'UTC'))
    GROUP BY e.account_email
  `);
  return toMap(rowsOf(result));
}

/**
 * Real `email_sent` events observed YESTERDAY (the full previous UTC calendar
 * day) per sending account. Same provenance/filters as fetchSentTodayByAccount
 * (real, non-inferred dispatches) — only the time window differs, bounded to
 * [prev-midnight, today-midnight) so it excludes both today and older days.
 */
export async function fetchSentYesterdayByAccount(): Promise<Map<string, number>> {
  const result = await db.execute(sql`
    SELECT e.account_email, COUNT(*) AS count
    FROM instantly_events e
    WHERE e.event_type = 'email_sent'
      AND e.inferred = false
      AND e.account_email IS NOT NULL
      AND e.timestamp >= date_trunc('day', (now() AT TIME ZONE 'UTC')) - interval '1 day'
      AND e.timestamp <  date_trunc('day', (now() AT TIME ZONE 'UTC'))
    GROUP BY e.account_email
  `);
  return toMap(rowsOf(result));
}

/**
 * Queued-but-not-sent step count per sending account. Reuses the EXACT pending
 * gate from loadPendingLeads (active campaign + delivery_status in
 * contacted/sent + status='provisioned'), collapses each lead's provisioned
 * holds to its distinct un-sent steps, then attributes those steps to the
 * campaign's sending account.
 *
 * Attribution is `COALESCE(persisted account_email, observed email_sent account)`:
 *   - The account is persisted on the campaign row at send time (send.ts phase-2,
 *     retry-stuck redispatch), so a lead is attributed the instant it is
 *     `contacted` — no dependency on the first `email_sent` webhook.
 *   - Historical rows written before the account_email column existed are NULL;
 *     they fall back to the account observed from their real email_sent events
 *     (the pre-column behaviour). The LEFT JOIN keeps a persisted-but-not-yet-
 *     sent campaign in the result — the INNER JOIN used to drop it, which was
 *     the attribution gap that let a burst over-concentrate on one account.
 * A row with neither a persisted nor an observed account is unattributable and
 * excluded (never fabricated).
 */
export async function fetchQueueSizeByAccount(): Promise<Map<string, number>> {
  const result = await db.execute(sql`
    WITH campaign_account AS (
      SELECT e.campaign_id AS instantly_campaign_id,
             MIN(e.account_email) AS account_email
      FROM instantly_events e
      WHERE e.event_type = 'email_sent'
        AND e.inferred = false
        AND e.account_email IS NOT NULL
        AND e.campaign_id IS NOT NULL
      GROUP BY e.campaign_id
    ),
    pending AS (
      SELECT c.instantly_campaign_id,
             MIN(c.account_email) AS persisted_account,
             COUNT(DISTINCT sc.step) AS pending_steps
      FROM sequence_costs sc
      JOIN instantly_campaigns c
        ON c.lead_email = sc.lead_email
       AND c.campaign_id IS NOT DISTINCT FROM sc.campaign_id
       AND c.status = 'active'
       AND c.delivery_status IN ('contacted', 'sent')
      WHERE sc.status = 'provisioned'
      GROUP BY c.instantly_campaign_id
    )
    SELECT COALESCE(p.persisted_account, ca.account_email) AS account_email,
           SUM(p.pending_steps)::int AS count
    FROM pending p
    LEFT JOIN campaign_account ca
      ON ca.instantly_campaign_id = p.instantly_campaign_id
    WHERE COALESCE(p.persisted_account, ca.account_email) IS NOT NULL
    GROUP BY COALESCE(p.persisted_account, ca.account_email)
  `);
  return toMap(rowsOf(result));
}

interface BreakdownRow {
  account_email: string;
  last_sent_step: number | string | null;
  last_sent_at: string | Date | null;
  provisioned_steps: (number | string)[] | null;
  step_config: Array<{ delay?: number | string | null }> | null;
}

/**
 * Per-account queue BREAKDOWN — splits the queued STEPS (every remaining un-sent
 * email across the account's queued sequences) into firstUnsent / nextToday /
 * nextTomorrow / nextLater by the projected send date of EACH step. Partitions
 * the account's queued-STEPS total (= its queueSize) — the four buckets sum to
 * that total, not to the sequence count. See queue-breakdown.ts for the
 * invariant + the compounding nominal-cadence-lower-bound caveat.
 *
 * Same queued gate + account attribution as fetchQueueSizeByAccount (active +
 * delivery_status in contacted/sent, COALESCE persisted/observed account). Per
 * SEQUENCE it loads:
 *   - the distinct un-sent (provisioned) step numbers (the same set
 *     fetchQueueSizeByAccount counts — so `breakdown.steps` equals `queueSize`);
 *   - last-sent step + timestamp (the projection anchor);
 *   - the FULL per-step `delay` array from the campaign's LATEST bronze sequence
 *     config (0-based `steps[].delay`), so the pure layer can CHAIN delays across
 *     every remaining step, not just the immediate next one.
 * Projection + classification are pure (aggregateQueueBreakdown); this only reads.
 */
export async function fetchQueueBreakdownByAccount(
  asOf: Date = new Date(),
): Promise<Map<string, QueueBreakdown>> {
  const result = await db.execute(sql`
    WITH campaign_account AS (
      SELECT e.campaign_id AS instantly_campaign_id,
             MIN(e.account_email) AS account_email
      FROM instantly_events e
      WHERE e.event_type = 'email_sent'
        AND e.inferred = false
        AND e.account_email IS NOT NULL
        AND e.campaign_id IS NOT NULL
      GROUP BY e.campaign_id
    ),
    seq AS (
      SELECT c.instantly_campaign_id,
             MIN(c.account_email) AS persisted_account,
             MAX(sc.step) FILTER (WHERE sc.status = 'actual') AS last_sent_step,
             MAX(sc.updated_at) FILTER (WHERE sc.status = 'actual') AS last_sent_at,
             array_agg(DISTINCT sc.step) FILTER (WHERE sc.status = 'provisioned')
               AS provisioned_steps
      FROM sequence_costs sc
      JOIN instantly_campaigns c
        ON c.lead_email = sc.lead_email
       AND c.campaign_id IS NOT DISTINCT FROM sc.campaign_id
       AND c.status = 'active'
       AND c.delivery_status IN ('contacted', 'sent')
      GROUP BY c.instantly_campaign_id
      HAVING array_agg(DISTINCT sc.step) FILTER (WHERE sc.status = 'provisioned')
               IS NOT NULL
    )
    SELECT COALESCE(s.persisted_account, ca.account_email) AS account_email,
           s.last_sent_step,
           s.last_sent_at,
           s.provisioned_steps,
           cfg.payload->'sequences'->0->'steps' AS step_config
    FROM seq s
    LEFT JOIN campaign_account ca
      ON ca.instantly_campaign_id = s.instantly_campaign_id
    LEFT JOIN LATERAL (
      SELECT payload FROM instantly_campaigns_config_raw r
      WHERE r.instantly_campaign_id = s.instantly_campaign_id
      ORDER BY r.fetched_at DESC
      LIMIT 1
    ) cfg ON true
    WHERE COALESCE(s.persisted_account, ca.account_email) IS NOT NULL
  `);
  const rows = rowsAsBreakdown(result);
  const inputs: QueuedSequenceInput[] = rows.map((r) => ({
    account: r.account_email,
    lastSentStep:
      r.last_sent_step === null || r.last_sent_step === undefined
        ? null
        : Number(r.last_sent_step),
    lastSentAt: r.last_sent_at ? new Date(r.last_sent_at) : null,
    provisionedSteps: (r.provisioned_steps ?? []).map(Number),
    stepDelays: Array.isArray(r.step_config)
      ? r.step_config.map((s) => {
          const d = s?.delay;
          return d === null || d === undefined ? null : Number(d);
        })
      : null,
  }));
  return aggregateQueueBreakdown(inputs, asOf);
}

function rowsAsBreakdown(result: unknown): BreakdownRow[] {
  if (!result) return [];
  return Array.isArray(result)
    ? (result as BreakdownRow[])
    : (((result as { rows?: BreakdownRow[] }).rows) ?? []);
}

/** Default TTL for the send-selection load snapshot (per replica). */
export const ACCOUNT_LOAD_TTL_MS = 60_000;
const ACCOUNT_LOAD_CACHE_KEY = "account-load|send-selection";

/**
 * Combined per-account load = sentToday + queueSize, merged into one map. Used
 * as the input to least-loaded account selection on the send path. Absent from
 * both maps ⇒ load 0 (never sent, nothing queued) ⇒ maximally preferred.
 */
export async function fetchAccountLoad(): Promise<Map<string, number>> {
  const [sent, queue] = await Promise.all([
    fetchSentTodayByAccount(),
    fetchQueueSizeByAccount(),
  ]);
  const merged = new Map<string, number>(sent);
  for (const [email, n] of queue) {
    merged.set(email, (merged.get(email) ?? 0) + n);
  }
  return merged;
}

/**
 * Short-TTL cached wrapper around fetchAccountLoad for the send hot-path. Two
 * fleet-wide aggregations per uncached call (email_sent-today scan + a
 * sequence_costs join) would saturate the 0.25-1 CU Neon compute under a send
 * burst — the same flood the /stats cache guards against — so a 60s window
 * collapses a burst down to ~1 snapshot per replica. Reuses the /stats TTL cache
 * (getOrSetCachedStats) rather than a second cache module.
 */
export function fetchAccountLoadCached(): Promise<Map<string, number>> {
  return getOrSetCachedStats(
    ACCOUNT_LOAD_CACHE_KEY,
    fetchAccountLoad,
    ACCOUNT_LOAD_TTL_MS,
  );
}
