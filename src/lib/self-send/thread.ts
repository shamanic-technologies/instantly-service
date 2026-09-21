/**
 * Rebuilding a conversation thread out of what WE put on the wire.
 *
 * On the Instantly transport the thread comes from `GET /emails` — Instantly
 * holds every message. On this one there is no such store, but we already keep
 * both halves in bronze: `smtp_dispatch_raw` is everything we sent, and
 * `imap_messages_raw` is everything that came back. Interleaving them by time
 * reconstructs the same conversation.
 *
 * ⚠️ `smtp_dispatch_raw` IS NOT SELF-SEND-ONLY, which is why the outbound half
 * lives in its own exported function rather than inside `fetchSelfSendThread`.
 * `POST /orgs/replies` records a manual reply there on BOTH transports (step 0,
 * `MANUAL_REPLY_STEP`), so on the Instantly transport that table holds exactly
 * the answers a human or a worker wrote — the messages Instantly's own mirror
 * does not know about until the prospect writes back and the thread is
 * re-mirrored. Reading only the mirror therefore hid our own answer from the
 * customer's timeline AND from the worker drafting the next follow-up, which
 * would have made it re-draft a first reply to somebody already answered.
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
      COALESCE(s.subject, d.payload->>'subject', '')     AS "subject",
      COALESCE(s.body_html, d.payload->>'bodyHtml', '')  AS "bodyHtml",
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

  return (result.rows as Record<string, unknown>[]).map(toThreadMessage);
}

/**
 * Turn one stored row into the shape the Instantly path already renders.
 *
 * Shared by both readers below so the two can never drift about what a stored
 * message looks like.
 */
function toThreadMessage(row: Record<string, unknown>): ThreadMessage {
  return {
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
  };
}

/** One answer we dispatched, beside the provider id that would duplicate it. */
export interface OwnDispatchedMessage {
  /**
   * The id the PROVIDER gave this message, when a provider carried it.
   *
   * It is the exact dedup key against the mirror: once the prospect writes back
   * and the thread is re-mirrored, Instantly returns our own answer too (as
   * `ue_type: 3`, manual-sent), and rendering both copies would show the
   * customer the same message twice. Null on the self-send transport, where no
   * provider ever saw the message, so nothing can duplicate it.
   */
  instantlyEmailId: string | null;
  message: ThreadMessage;
}

/**
 * Every answer WE dispatched on this sequence, oldest first.
 *
 * Restricted to attempts that actually went out: a refused attempt produced no
 * email, so including it would show a message the prospect never received. The
 * same restriction `fetchSelfSendThread` applies, for the same reason.
 */
export async function fetchOwnDispatchedMessages(
  instantlyCampaignId: string,
): Promise<OwnDispatchedMessage[]> {
  const result = await db.execute(sql`
    SELECT
      'outbound'                                         AS "direction",
      d.account_email                                    AS "from",
      d.lead_email                                       AS "to",
      COALESCE(s.subject, d.payload->>'subject', '')     AS "subject",
      COALESCE(s.body_html, d.payload->>'bodyHtml', '')  AS "bodyHtml",
      d.dispatched_at                                    AS "at",
      d.payload->>'instantlyEmailId'                     AS "instantlyEmailId"
    FROM smtp_dispatch_raw d
    LEFT JOIN sequence_steps s
      ON s.instantly_campaign_id = d.instantly_campaign_id AND s.step = d.step
    WHERE d.instantly_campaign_id = ${instantlyCampaignId}
      AND d.outcome = 'sent'
    ORDER BY d.dispatched_at
  `);

  return (result.rows as Record<string, unknown>[]).map((row) => ({
    instantlyEmailId:
      typeof row.instantlyEmailId === "string" && row.instantlyEmailId.length > 0
        ? row.instantlyEmailId
        : null,
    message: toThreadMessage(row),
  }));
}
