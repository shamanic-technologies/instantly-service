/**
 * Take back `email_bounced` silver events that were not a bounce. ONE home for
 * the mechanics, shared by every correction that finds a bounce was wrong:
 * `delayed-dsn-backfill.ts` (a temporary delay read as permanent) and
 * `self-send/dsn-judgment-sweep.ts` (a refused MESSAGE read as a dead address,
 * BOUNCE-SPAM-BLOCK-1018).
 *
 * Per event, in order:
 *  1. copied WHOLE into `instantly_events_retracted` (with the campaign row's
 *     prior delivery status), so it is reversible:
 *     `INSERT INTO instantly_events SELECT (jsonb_populate_record(NULL::instantly_events, event)).*
 *      FROM instantly_events_retracted WHERE action='retracted' AND reason=...`;
 *  2. deleted from silver. The outreach fact feed then emits a `withdrawn` fact
 *     for its `email_bounced` fact (`eventWithdrawalsInsertSql`), and
 *     `findBouncedEmails` (human-service's serve) stops listing the address;
 *  3. the campaign row's `delivery_status` goes back from `bounced` to `sent`
 *     while nothing else still says it bounced (a DSN proves the email left);
 *  4. gold is rebuilt (`POST /orgs/status`, what lead-service's suppression
 *     reads live) and lead-service is told the address's evidence changed.
 *
 * NOT undone: the bounce already stopped that sequence and refunded its
 * remaining holds. Restarting outreach is a new decision (a new send), not a
 * correction.
 */
import { sql } from "drizzle-orm";

import { db } from "../db";
import { instantlyEventsRetracted } from "../db/schema";
import { announceEvidenceChanged } from "./evidence-changed";
import { refreshLeadStatusCurrent } from "./status-gold";

function rowsOf(result: unknown): Record<string, unknown>[] {
  if (Array.isArray(result)) return result as Record<string, unknown>[];
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

export interface RetractableBounce {
  id: string;
  campaignId: string | null;
  leadEmail: string | null;
}

/** Copy one silver event whole into the retraction archive. Idempotent. */
export async function archiveSilverEvent(
  eventId: string,
  action: "retracted" | "reattributed",
  reason: string,
  replacementSourceRowId: string | null,
): Promise<void> {
  const [row] = rowsOf(
    await db.execute(sql`
      SELECT to_jsonb(e.*) AS event, c.delivery_status
      FROM instantly_events e
      LEFT JOIN instantly_campaigns c ON c.instantly_campaign_id = e.campaign_id
      WHERE e.id = ${eventId}
    `),
  );
  if (!row) return;
  await db
    .insert(instantlyEventsRetracted)
    .values({
      eventId,
      action,
      reason,
      event: row.event as Record<string, unknown>,
      priorDeliveryStatus: (row.delivery_status as string | null) ?? null,
      replacementSourceRowId,
    })
    .onConflictDoNothing({ target: instantlyEventsRetracted.eventId });
}

/**
 * Archive, delete, restore delivery status, rebuild gold, announce. Steps 1-3
 * per event, then 4 once per (campaign, lead).
 */
export async function retractBouncedEvents(
  events: readonly RetractableBounce[],
  reason: string,
  announceTag: string,
): Promise<void> {
  const touched = new Map<string, string | null>(); // campaignId -> leadEmail
  for (const event of events) {
    await archiveSilverEvent(event.id, "retracted", reason, null);
    await db.execute(sql`DELETE FROM instantly_events WHERE id = ${event.id}`);
    if (!event.campaignId) continue;
    // Back to `sent` only while nothing else still says it bounced.
    await db.execute(sql`
      UPDATE instantly_campaigns
      SET delivery_status = 'sent', updated_at = now()
      WHERE instantly_campaign_id = ${event.campaignId}
        AND delivery_status = 'bounced'
        AND NOT EXISTS (
          SELECT 1 FROM instantly_events b
          WHERE b.campaign_id = ${event.campaignId} AND b.event_type = 'email_bounced'
        )
    `);
    touched.set(event.campaignId, event.leadEmail);
  }

  for (const [campaignId, leadEmail] of touched) {
    await refreshLeadStatusCurrent(campaignId, leadEmail);
  }

  if (touched.size === 0) return;
  const orgs = rowsOf(
    await db.execute(sql`
      SELECT instantly_campaign_id, org_id FROM instantly_campaigns
      WHERE instantly_campaign_id = ANY(${sql.param([...touched.keys()])}::text[])
    `),
  );
  const orgOf = new Map(orgs.map((r) => [String(r.instantly_campaign_id), r.org_id as string | null]));
  for (const [campaignId, leadEmail] of touched) {
    await announceEvidenceChanged(orgOf.get(campaignId) ?? null, [leadEmail], announceTag);
  }
}
