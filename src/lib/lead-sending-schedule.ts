/**
 * The sending schedule that governs cold email to ONE lead —
 * `GET /orgs/sending-schedule`.
 *
 * A dashboard shows a customer WHEN we are allowed to email a prospect (local
 * weekdays, local business hours, in the prospect's own timezone). Every value
 * here is read from the constants the send path itself uses
 * (`SENDING_WEEKDAYS`, `SEND_WINDOW_START_HOUR`/`END_HOUR`,
 * `DEFAULT_LEAD_TIMEZONE`, `resolveLeadTimezone`), so the day the window moves
 * this read moves with it. A consumer must never re-type the window.
 *
 * The zone is the one persisted on the lead's most recent sequence for this
 * org (and brand, when given), falling back to the zone we shipped to Instantly
 * in the campaign schedule — the same `COALESCE` the capacity loader uses. A
 * lead we hold no sequence for, or whose sequence carries no usable zone, gets
 * the default zone flagged `timezoneIsDefault: true`: that is the zone such a
 * send WOULD run in, not a guess about the prospect.
 */
import { sql } from "drizzle-orm";

import { db } from "../db";
import { SENDING_WEEKDAYS } from "./sending-calendar";
import {
  DEFAULT_LEAD_TIMEZONE,
  SEND_WINDOW_END_HOUR,
  SEND_WINDOW_START_HOUR,
  resolveLeadTimezone,
} from "./sending-window";

const WEEKDAY_NAMES = [
  "sunday",
  "monday",
  "tuesday",
  "wednesday",
  "thursday",
  "friday",
  "saturday",
] as const;

export type WeekdayName = (typeof WEEKDAY_NAMES)[number];

export interface LeadSendingSchedule {
  weekdays: WeekdayName[];
  startHour: number;
  endHour: number;
  timezone: string;
  timezoneIsDefault: boolean;
  hasSequence: boolean;
}

/** True when the runtime can evaluate a window in this zone. */
function isUsableZone(zone: string): boolean {
  try {
    new Intl.DateTimeFormat("en-US", { timeZone: zone });
    return true;
  } catch {
    return false;
  }
}

/**
 * Pure: the schedule for a lead given the raw zone stored on its sequence
 * (`null` when none is stored) and whether we hold a sequence at all.
 */
export function buildLeadSendingSchedule(
  rawTimezone: string | null,
  hasSequence: boolean,
): LeadSendingSchedule {
  const raw = rawTimezone?.trim() ? rawTimezone.trim() : null;
  const resolved = raw ? resolveLeadTimezone(raw) : null;
  const own = resolved && isUsableZone(resolved) ? resolved : null;
  return {
    weekdays: SENDING_WEEKDAYS.map((d) => WEEKDAY_NAMES[d]),
    startHour: SEND_WINDOW_START_HOUR,
    endHour: SEND_WINDOW_END_HOUR,
    timezone: own ?? DEFAULT_LEAD_TIMEZONE,
    timezoneIsDefault: own === null,
    hasSequence,
  };
}

function rowsOf(result: unknown): Record<string, unknown>[] {
  return ((result as { rows?: Record<string, unknown>[] }).rows ?? []);
}

/** The schedule for `email` within `orgId` (and `brandId`, when given). */
export async function fetchLeadSendingSchedule(params: {
  orgId: string;
  email: string;
  brandId?: string;
}): Promise<LeadSendingSchedule> {
  const brandFilter = params.brandId
    ? sql`AND ${params.brandId} = ANY(c.brand_ids)`
    : sql``;
  const result = await db.execute(sql`
    SELECT COALESCE(
             c.timezone,
             cfg.payload->'campaign_schedule'->'schedules'->0->>'timezone'
           ) AS timezone
    FROM instantly_campaigns c
    LEFT JOIN LATERAL (
      SELECT payload FROM instantly_campaigns_config_raw r
      WHERE r.instantly_campaign_id = c.instantly_campaign_id
      ORDER BY r.fetched_at DESC
      LIMIT 1
    ) cfg ON true
    WHERE c.org_id = ${params.orgId}
      AND lower(c.lead_email) = lower(trim(${params.email}))
      AND c.instantly_campaign_id NOT LIKE 'reserving:%'
      ${brandFilter}
    ORDER BY c.created_at DESC
    LIMIT 1
  `);
  const row = rowsOf(result)[0];
  if (!row) return buildLeadSendingSchedule(null, false);
  const tz = row.timezone;
  return buildLeadSendingSchedule(
    tz === null || tz === undefined ? null : String(tz),
    true,
  );
}
