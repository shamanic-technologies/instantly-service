/**
 * Every sending domain's sends and bounces over the bounce-budget windows
 * (`bounce-budget.ts`), read from silver.
 *
 * Both transports converge on `instantly_events` through `promoteEvent`, so one
 * read covers them. Real events only (`inferred = false`, not withdrawn), timed
 * by the event's own `"timestamp"`: a backfilled bounce from last month is not
 * a bounce of today. `created_at` bounds the scan (indexed; a bounce is
 * promoted after its event happened, never before), then `"timestamp"` decides
 * the window. ~14 ms over 7 days on prod (2026-10-15, 5k rows); the 14-day window doubles the rows.
 *
 * Cached 60 s per replica: the send path asks it per send, the dispatcher once
 * per selection. The selection then spends the budget in memory, send by send.
 */

import { sql } from "drizzle-orm";
import { db } from "../db";
import { getOrSetCachedStats } from "./stats-cache";
import {
  BOUNCE_BUDGET_WINDOWS_HOURS,
  HARD_CEILING_WINDOW_HOURS,
  type FleetBounceHistory,
  type WindowCounts,
} from "./bounce-budget";

const WINDOWS_HOURS = [...new Set<number>([...BOUNCE_BUDGET_WINDOWS_HOURS, HARD_CEILING_WINDOW_HOURS])];
const LONGEST_HOURS = Math.max(...WINDOWS_HOURS);

export async function loadFleetBounceHistory(): Promise<FleetBounceHistory> {
  const result = await db.execute(sql`
    SELECT split_part(lower(account_email), '@', 2)          AS "domain",
           event_type                                         AS "eventType",
           EXTRACT(EPOCH FROM (now() AT TIME ZONE 'UTC' - "timestamp")) / 3600 AS "ageHours"
    FROM instantly_events
    WHERE created_at > now() - make_interval(hours => ${LONGEST_HOURS + 24})
      AND "timestamp" > (now() AT TIME ZONE 'UTC') - make_interval(hours => ${LONGEST_HOURS})
      AND event_type IN ('email_sent', 'email_bounced')
      AND inferred = false
      AND withdrawn_at IS NULL
      AND account_email IS NOT NULL
  `);
  const rows = (result as { rows?: Record<string, unknown>[] }).rows ?? [];
  const fleet = new Map<string, Map<number, WindowCounts>>();
  for (const row of rows) {
    const domain = String(row.domain);
    const age = Number(row.ageHours);
    const bounced = row.eventType === "email_bounced";
    let windows = fleet.get(domain);
    if (!windows) {
      windows = new Map(WINDOWS_HOURS.map((h) => [h, { sends: 0, bounces: 0 }]));
      fleet.set(domain, windows);
    }
    for (const hours of WINDOWS_HOURS) {
      if (age > hours) continue;
      const w = windows.get(hours)!;
      if (bounced) w.bounces += 1;
      else w.sends += 1;
    }
  }
  return fleet;
}

const CACHE_KEY = "fleet-bounce-history";

export async function loadFleetBounceHistoryCached(): Promise<FleetBounceHistory> {
  return getOrSetCachedStats(CACHE_KEY, loadFleetBounceHistory);
}
