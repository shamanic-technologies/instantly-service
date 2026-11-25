import { sql } from "drizzle-orm";

import { db } from "../../db";
import { PERSON_SEND_SPACING_MS, type RecentPersonSend } from "./dispatch";

/**
 * Every email we sent a person since one spacing before `asOf`
 * (`PERSON_SEND_SPACING_MS`, FOLLOWUP-SAME-MAILBOX-1125): sequence steps and
 * answers alike, as the dispatch log records them (`outcome = 'sent'`, served
 * by the `dispatched_at` index), one row per (person, sequence). Read by the
 * dispatch run for `holdSecondEmailToPerson`. Fail loud: an unreadable log
 * fails the run, nothing is sent.
 */
export async function loadRecentPersonSends(asOf: Date): Promise<RecentPersonSend[]> {
  const since = new Date(asOf.getTime() - PERSON_SEND_SPACING_MS).toISOString();
  const result = await db.execute(sql`
    SELECT lower(d.lead_email) AS "leadEmail",
           d.instantly_campaign_id AS "instantlyCampaignId",
           MAX(d.dispatched_at) AT TIME ZONE 'UTC' AS "sentAt"
    FROM smtp_dispatch_raw d
    WHERE d.outcome = 'sent'
      AND d.dispatched_at > (${since}::timestamptz AT TIME ZONE 'UTC')
    GROUP BY 1, 2
  `);
  return (result.rows as Record<string, unknown>[]).map((row) => ({
    leadEmail: String(row.leadEmail),
    instantlyCampaignId: String(row.instantlyCampaignId),
    sentAt: new Date(row.sentAt as string),
  }));
}
