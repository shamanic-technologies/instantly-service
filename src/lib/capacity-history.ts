/**
 * Capacity-over-time (Gold, derived-on-read). Reconstructs the fleet's
 * `in_production` daily capacity for each of the last N days from the append-only
 * Bronze layers:
 *   - instantly_account_lifecycle_events → each account's STATUS as-of any day
 *     (the latest transition with created_at <= end-of-day).
 *   - instantly_accounts_raw            → each account's daily_limit as-of any day
 *     (the latest snapshot with fetched_at <= end-of-day).
 *
 * For each day: dailyCapacity = Σ daily_limit over accounts whose as-of-that-day
 * status is in_production; inProductionCount = how many. Days are UTC calendar
 * days. No fabrication — a day before an account's first event contributes 0.
 */

import { sql } from "drizzle-orm";
import { db } from "../db";

export interface CapacityHistoryPoint {
  /** YYYY-MM-DD (UTC). */
  date: string;
  inProductionCount: number;
  dailyCapacity: number;
}

/** `days` is clamped to [1, 365]. Returns one point per UTC day, oldest first. */
export async function fetchCapacityHistory(
  days: number,
): Promise<CapacityHistoryPoint[]> {
  const window = Math.max(1, Math.min(365, Math.floor(days)));
  const result = await db.execute(sql`
    WITH days AS (
      SELECT generate_series(
        (CURRENT_DATE - (${window}::int - 1) * INTERVAL '1 day'),
        CURRENT_DATE,
        INTERVAL '1 day'
      )::date AS day
    ),
    accounts AS (
      SELECT DISTINCT account_email FROM instantly_account_lifecycle_events
    ),
    per AS (
      SELECT
        d.day,
        a.account_email,
        (SELECT e.to_status
           FROM instantly_account_lifecycle_events e
          WHERE e.account_email = a.account_email
            AND e.created_at < (d.day + INTERVAL '1 day')
          ORDER BY e.created_at DESC
          LIMIT 1) AS status,
        (SELECT r.daily_limit
           FROM instantly_accounts_raw r
          WHERE r.account_email = a.account_email
            AND r.fetched_at < (d.day + INTERVAL '1 day')
          ORDER BY r.fetched_at DESC
          LIMIT 1) AS daily_limit
      FROM days d CROSS JOIN accounts a
    )
    SELECT
      to_char(day, 'YYYY-MM-DD') AS "date",
      COUNT(*) FILTER (WHERE status = 'in_production')::int AS "inProductionCount",
      COALESCE(
        SUM(CASE WHEN status = 'in_production' THEN COALESCE(daily_limit, 0) ELSE 0 END),
        0
      )::int AS "dailyCapacity"
    FROM per
    GROUP BY day
    ORDER BY day
  `);

  const rows = Array.isArray(result) ? result : ((result as any).rows ?? []);
  return rows.map((r: Record<string, unknown>) => ({
    date: String(r.date),
    inProductionCount: Number(r.inProductionCount),
    dailyCapacity: Number(r.dailyCapacity),
  }));
}
