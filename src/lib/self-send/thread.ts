/**
 * Rebuilding a conversation thread for a sequence WE dispatched.
 *
 * On the Instantly transport the thread comes from `GET /emails` — Instantly
 * holds every message. On this one there is no such store, but we already keep
 * both halves in bronze: `smtp_dispatch_raw` is everything we sent, and
 * `imap_messages_raw` is everything that came back. Interleaving them by time
 * reconstructs the same conversation.
 *
 * This is not a second thread format. It produces the SAME `ThreadMessage`
 * shape the Instantly path already renders, so the forward email, its subject
 * and its body are identical whichever transport the lead was on.
 */

import { sql } from "drizzle-orm";

import { db } from "../../db";
import { htmlToText, type ThreadMessage } from "../forward-positive-reply";

/**
 * Every message of one self-dispatched conversation, oldest first.
 *
 * Outbound comes from the dispatch log, restricted to attempts that actually
 * went out — a refused attempt produced no email, so including it would show the
 * prospect a message they never received.
 *
 * Inbound comes from the IMAP mirror, restricted to messages we CORRELATED to
 * this sequence. An `unrelated` row is stored in the same table on purpose (it
 * is the evidence of what we ignored) and must never leak into a thread.
 */
export async function fetchSelfSendThread(
  instantlyCampaignId: string,
): Promise<ThreadMessage[]> {
  const result = await db.execute(sql`
    SELECT
      'outbound'                          AS "direction",
      d.account_email                     AS "from",
      d.lead_email                        AS "to",
      COALESCE(s.subject, '')             AS "subject",
      COALESCE(s.body_html, '')           AS "bodyHtml",
      d.dispatched_at                     AS "at"
    FROM smtp_dispatch_raw d
    LEFT JOIN sequence_steps s
      ON s.instantly_campaign_id = d.instantly_campaign_id AND s.step = d.step
    WHERE d.instantly_campaign_id = ${instantlyCampaignId}
      AND d.outcome = 'sent'

    UNION ALL

    SELECT
      'inbound'                                        AS "direction",
      COALESCE(m.from_address, m.account_email)        AS "from",
      m.account_email                                  AS "to",
      COALESCE(m.subject, '')                          AS "subject",
      COALESCE(m.payload->>'textSnippet', '')          AS "bodyHtml",
      COALESCE(m.received_at, m.polled_at)             AS "at"
    FROM imap_messages_raw m
    WHERE m.instantly_campaign_id = ${instantlyCampaignId}
      AND m.kind IN ('reply', 'auto_reply')

    ORDER BY "at"
  `);

  return (result.rows as Record<string, unknown>[]).map((row) => ({
    direction: row.direction === "inbound" ? "inbound" : "outbound",
    from: String(row.from ?? ""),
    to: String(row.to ?? ""),
    // Same ISO form the Instantly path produces, so the renderer cannot tell the
    // two transports apart.
    date: row.at ? new Date(row.at as string).toISOString() : "",
    subject: String(row.subject ?? ""),
    // Our stored outbound body is HTML; the inbound snippet is already text.
    // Both go through the SAME stripper the Instantly path uses, so a forwarded
    // thread reads identically whichever pipe carried it.
    bodyText: htmlToText(String(row.bodyHtml ?? "")),
  }));
}
