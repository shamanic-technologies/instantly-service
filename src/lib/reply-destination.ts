/**
 * Where the prospect wrote LAST, when that is one of our people's own address
 * rather than the sending mailbox.
 *
 * Owner rule 2026-10-13 (STAFF-GMAIL-THREAD-1013, caps): ANY answer to a
 * prospect goes in the thread where they wrote last, from the address they wrote
 * to. Lukas wrote last to kevin@distribute.you, in Kevin's own Gmail thread
 * (folded in by self-send/gmail-threads); an answer was then sent from
 * allie@leansignalio.com into the old cold-email thread. Never again.
 *
 * So `POST /orgs/replies` answers such a prospect IN THAT PERSON'S NAME, from
 * their address, through our own sender, threaded on the prospect's last
 * message (its Message-ID, its References, its subject): never from the sending
 * alias, never in the old outreach thread. When we hold no credential for that
 * address, it refuses loudly (`prospect_wrote_elsewhere`): the person answers
 * from their own mailbox.
 *
 * Reads the SOURCES (Instantly's inbound mirror, our inbound bronze), never the
 * lagging `messages` projection. Fails loud.
 */

import { sql } from "drizzle-orm";

import { db } from "../db";
import { bareAddress, isStaffSender, staffSenderSql } from "./staff-senders";

/** Provenance of the rows self-send/gmail-threads files (kept here: a leaf, no import cycle). */
export const OWN_MAILBOX_THREAD = "own_mailbox_thread";

export interface ElsewhereDestination {
  /** Our person's address they wrote to (one of our own people), or null when none of the recipients is one. */
  ownAddress: string | null;
  /** Every recipient as recorded. */
  to: string;
  /** When they wrote it, ISO 8601 UTC. */
  at: string;
  /** The thread to answer in: their message, its references, its subject. */
  messageId: string;
  references: string[];
  subject: string;
}

function rowsOf(result: unknown): Record<string, unknown>[] {
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

/** Pure: the first recipient that is one of our own people, or null. */
export function ownRecipient(to: string, leadEmail: string): string | null {
  for (const part of to.split(",")) {
    const address = bareAddress(part);
    if (address && isStaffSender(address, leadEmail)) return address;
  }
  return null;
}

/**
 * The prospect's newest message across this org's sequences with them, when it
 * was written to one of our people's own mailboxes; null when it went to a
 * sending mailbox (or they never wrote).
 */
export async function findProspectWroteElsewhere(
  orgId: string,
  leadEmail: string,
): Promise<ElsewhereDestination | null> {
  const lead = leadEmail.trim().toLowerCase();
  const result = await db.execute(sql`
    WITH seqs AS (
      SELECT instantly_campaign_id
      FROM instantly_campaigns
      WHERE org_id = ${orgId} AND lower(lead_email) = ${lead}
    )
    SELECT at, own, "to", message_id AS "messageId", subject, refs
    FROM (
      SELECT (e.payload->>'timestamp_email')::timestamptz AS at, false AS own,
             NULL::text AS "to", NULL::text AS message_id, NULL::text AS subject, NULL::jsonb AS refs
      FROM instantly_emails_raw e
      WHERE e.instantly_campaign_id IN (SELECT instantly_campaign_id FROM seqs)
        AND e.payload->>'ue_type' = '2'
        AND NOT ${staffSenderSql(sql`e.payload->>'from_address_email'`)}

      UNION ALL

      SELECT COALESCE(m.received_at, m.polled_at) AT TIME ZONE 'UTC' AS at,
             (m.payload->>'recoveredFrom') IS NOT DISTINCT FROM ${OWN_MAILBOX_THREAD} AS own,
             m.payload->>'to' AS "to",
             m.message_id,
             m.subject,
             m.payload->'referencedMessageIds' AS refs
      FROM imap_messages_raw m
      WHERE m.instantly_campaign_id IN (SELECT instantly_campaign_id FROM seqs)
        AND m.kind IN ('reply', 'auto_reply')
    ) t
    WHERE at IS NOT NULL
    ORDER BY at DESC
    LIMIT 1
  `);
  const row = rowsOf(result)[0];
  if (!row || row.own !== true) return null;
  const at = row.at instanceof Date ? row.at : new Date(String(row.at));
  const to = String(row.to ?? "");
  const messageId = String(row.messageId ?? "");
  const refs = Array.isArray(row.refs) ? (row.refs as unknown[]).map(String) : [];
  return {
    ownAddress: ownRecipient(to, lead),
    to: to || "one of our people's own mailbox",
    at: at.toISOString(),
    messageId,
    // RFC 5322: the parent's References, then the parent itself.
    references: [...refs.filter((r) => r !== messageId), ...(messageId.startsWith("<") ? [messageId] : [])],
    subject: String(row.subject ?? ""),
  };
}
