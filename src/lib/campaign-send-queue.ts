/**
 * What is waiting to be sent for a set of campaigns RIGHT NOW, and when the
 * sending window next opens for it — `GET /orgs/send-queue`
 * (CAMPAIGN-NEXT-STEPS-1018: our assistant tells a client "N leads are queued
 * and go out from tomorrow, in their business hours").
 *
 * A queued sequence is one on a LIVE per-lead campaign (the same gate the queued
 * email count, the sending forecast and reconcile use: `status='active'` and
 * `delivery_status IN ('contacted','sent')`) holding at least one PROVISIONED
 * step in `sequence_costs` (the send queue). It is then either:
 *   - awaiting its FIRST email (`delivery_status='contacted'`: handed to the
 *     sender, nothing gone out yet), or
 *   - a lead already emailed whose next step is queued.
 *
 * When: every send waits for the lead's own local window (`SENDING_WEEKDAYS`,
 * `SEND_WINDOW_START_HOUR`..`SEND_WINDOW_END_HOUR`, in the lead's timezone, the
 * zone persisted on the sequence; none = `DEFAULT_LEAD_TIMEZONE`, flagged).
 * `firstEmailsFrom` is the EARLIEST instant that window opens for a lead
 * awaiting its first email (`nextLocalSendInstant`, the send path's own
 * function). It is the earliest the email MAY leave, never a promise: mailbox
 * capacity can push a send later. No lead awaiting = null.
 *
 * Fail loud: a read failure throws (the route answers 500), never a 0 that
 * would read as "nothing queued".
 */
import { sql } from "drizzle-orm";

import { db } from "../db";
import { SENDING_WEEKDAYS } from "./sending-calendar";
import {
  DEFAULT_LEAD_TIMEZONE,
  SEND_WINDOW_END_HOUR,
  SEND_WINDOW_START_HOUR,
  nextLocalSendInstant,
  resolveLeadTimezone,
} from "./sending-window";

const WEEKDAY_NAMES = ["sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"] as const;

/** One queued sequence as read from the DB. */
export interface QueuedSequenceRow {
  campaignId: string;
  timezone: string | null;
  deliveryStatus: string;
  queuedSteps: number;
}

export interface SendQueueTimezone {
  timezone: string;
  /** True when no usable zone is stored for these leads: the fleet default the send runs in. */
  timezoneIsDefault: boolean;
  leadsAwaitingFirstEmail: number;
  /** When the window next opens in this zone (at or after `asOf`), ISO. */
  windowOpensAt: string;
}

export interface CampaignSendQueue {
  campaignIds: string[];
  asOf: string;
  /** Leads whose first email is written and queued, not yet sent. */
  leadsAwaitingFirstEmail: number;
  /** Leads already emailed whose next step is queued. */
  leadsWithFollowUpQueued: number;
  /** Every queued email (all steps), the grain of `emailStats.queued`. */
  queuedEmails: number;
  /** Earliest instant the window opens for a lead awaiting its first email; null when none awaits. */
  firstEmailsFrom: string | null;
  sendWindow: {
    weekdays: (typeof WEEKDAY_NAMES)[number][];
    startHour: number;
    endHour: number;
    /** Hours are LOCAL to each lead (its own timezone). */
    timezoneRule: "lead_local";
  };
  /** Leads awaiting their first email, per timezone, earliest window first. */
  timezones: SendQueueTimezone[];
}

function usableZone(raw: string | null): string | null {
  if (!raw || !raw.trim()) return null;
  try {
    const zone = resolveLeadTimezone(raw);
    new Intl.DateTimeFormat("en-US", { timeZone: zone });
    return zone;
  } catch {
    return null;
  }
}

/** PURE: the queue answer for these rows at `asOf`. */
export function buildCampaignSendQueue(campaignIds: string[], rows: QueuedSequenceRow[], asOf: Date): CampaignSendQueue {
  let awaiting = 0;
  let followUps = 0;
  let queuedEmails = 0;
  const byZone = new Map<string, { isDefault: boolean; leads: number }>();
  for (const row of rows) {
    if (row.queuedSteps <= 0) continue;
    queuedEmails += row.queuedSteps;
    if (row.deliveryStatus !== "contacted") {
      followUps += 1;
      continue;
    }
    awaiting += 1;
    const own = usableZone(row.timezone);
    const zone = own ?? DEFAULT_LEAD_TIMEZONE;
    const key = `${zone}|${own === null}`;
    const entry = byZone.get(key) ?? { isDefault: own === null, leads: 0 };
    entry.leads += 1;
    byZone.set(key, entry);
  }
  const timezones: SendQueueTimezone[] = [...byZone.entries()]
    .map(([key, e]) => {
      const timezone = key.slice(0, key.lastIndexOf("|"));
      return {
        timezone,
        timezoneIsDefault: e.isDefault,
        leadsAwaitingFirstEmail: e.leads,
        windowOpensAt: nextLocalSendInstant(asOf, timezone).toISOString(),
      };
    })
    .sort((a, b) => a.windowOpensAt.localeCompare(b.windowOpensAt) || b.leadsAwaitingFirstEmail - a.leadsAwaitingFirstEmail || a.timezone.localeCompare(b.timezone));
  return {
    campaignIds,
    asOf: asOf.toISOString(),
    leadsAwaitingFirstEmail: awaiting,
    leadsWithFollowUpQueued: followUps,
    queuedEmails,
    firstEmailsFrom: timezones[0]?.windowOpensAt ?? null,
    sendWindow: {
      weekdays: SENDING_WEEKDAYS.map((d) => WEEKDAY_NAMES[d]),
      startHour: SEND_WINDOW_START_HOUR,
      endHour: SEND_WINDOW_END_HOUR,
      timezoneRule: "lead_local",
    },
    timezones,
  };
}

/** IO: the org's queued sequences on these campaigns. Throws on a read failure. */
export async function fetchCampaignSendQueue(input: { orgId: string; campaignIds: string[]; asOf?: Date }): Promise<CampaignSendQueue> {
  const asOf = input.asOf ?? new Date();
  const ids = sql`ARRAY[${sql.join(input.campaignIds.map((id) => sql`${id}`), sql`, `)}]::text[]`;
  const result = await db.execute(sql`
    SELECT c.campaign_id AS "campaignId",
           c.timezone AS "timezone",
           c.delivery_status AS "deliveryStatus",
           q.steps AS "queuedSteps"
    FROM instantly_campaigns c
    CROSS JOIN LATERAL (
      SELECT COUNT(DISTINCT sc.step)::int AS steps
      FROM sequence_costs sc
      WHERE sc.status = 'provisioned'
        AND (sc.instantly_campaign_id = c.instantly_campaign_id
             OR (sc.instantly_campaign_id IS NULL
                 AND sc.campaign_id = c.campaign_id
                 AND sc.lead_email = c.lead_email))
    ) q
    WHERE c.org_id = ${input.orgId}
      AND c.campaign_id = ANY(${ids})
      AND c.status = 'active'
      AND c.delivery_status IN ('contacted', 'sent')
      AND q.steps > 0
  `);
  const rows = (Array.isArray(result) ? result : (result as { rows?: unknown[] }).rows) as Record<string, unknown>[] | undefined;
  if (!Array.isArray(rows)) throw new Error("[instantly-service] send-queue read returned no rows array");
  return buildCampaignSendQueue(
    input.campaignIds,
    rows.map((r) => ({
      campaignId: String(r.campaignId),
      timezone: typeof r.timezone === "string" ? r.timezone : null,
      deliveryStatus: String(r.deliveryStatus),
      queuedSteps: Number(r.queuedSteps),
    })),
    asOf,
  );
}
