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
 * Queued-but-not-sent step count per sending account. Reuses the EXACT pending
 * gate from loadPendingLeads (active campaign + delivery_status in
 * contacted/sent + status='provisioned'), collapses each lead's provisioned
 * holds to its distinct un-sent steps, then attributes those steps to the
 * account that sent the campaign's already-observed emails. Campaigns with no
 * observed real send yet have an unknown account and are not attributed.
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
    SELECT ca.account_email, SUM(p.pending_steps)::int AS count
    FROM pending p
    JOIN campaign_account ca
      ON ca.instantly_campaign_id = p.instantly_campaign_id
    GROUP BY ca.account_email
  `);
  return toMap(rowsOf(result));
}
