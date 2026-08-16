/**
 * The IMAP poller — IO glue around the pure classification in `inbound.ts`.
 *
 * Reads each self-send mailbox, correlates what it finds against the Message-Ids
 * we know we sent, and promotes replies and bounces into the same silver every
 * other ingestion path writes to.
 *
 * Idempotent WITHOUT a cursor. Every run re-reads an overlapping window and
 * relies on the unique `(account_email, message_id)` index in bronze to make a
 * re-read a no-op. A stored cursor that drifts — a clock skew, a failed run, a
 * message that arrives out of order — silently loses replies, and a lost reply
 * means we keep emailing someone who already answered. An overlapping window
 * costs a few redundant reads and cannot lose anything.
 */

import { ImapFlow } from "imapflow";
import { simpleParser, type ParsedMail } from "mailparser";
import { sql } from "drizzle-orm";

import { db } from "../../db";
import { imapMessagesRaw } from "../../db/schema";
import { promoteEvent } from "../silver-promote";
import type { CallerInfo } from "../key-client";
import {
  GMAIL_IMAP_PORT,
  resolveMailboxCredential,
  type MailboxCredential,
} from "./mailbox-credentials";
import { classifyInbound, eventTypeForInbound, type InboundHeaders } from "./inbound";
import { SEND_TRANSPORT_SMTP } from "./transport";

const CALLER: CallerInfo = { method: "POST", path: "/internal/self-send/poll" };

/**
 * How far back each run looks.
 *
 * Comfortably wider than the hourly cadence, so a missed run, a slow delivery or
 * a clock skew cannot open a gap. The unique index absorbs the overlap.
 */
const POLL_WINDOW_DAYS = 3;

export interface PollSummary {
  accountsPolled: number;
  messagesRead: number;
  replies: number;
  autoReplies: number;
  bounces: number;
  unrelated: number;
  accountsFailed: number;
}

interface KnownSend {
  instantlyCampaignId: string;
  leadEmail: string;
  step: number;
}

/** Our own sends from this mailbox, keyed by the Message-Id the server accepted. */
async function loadKnownSends(accountEmail: string): Promise<Map<string, KnownSend>> {
  const result = await db.execute(sql`
    SELECT message_id            AS "messageId",
           instantly_campaign_id AS "instantlyCampaignId",
           lead_email            AS "leadEmail",
           step                  AS "step"
    FROM smtp_dispatch_raw
    WHERE account_email = ${accountEmail}
      AND outcome = 'sent'
      AND message_id IS NOT NULL
  `);

  const sends = new Map<string, KnownSend>();
  for (const row of result.rows as Record<string, unknown>[]) {
    sends.set(String(row.messageId), {
      instantlyCampaignId: String(row.instantlyCampaignId),
      leadEmail: String(row.leadEmail),
      step: Number(row.step),
    });
  }
  return sends;
}

/** Mailboxes on the self-send transport. */
async function loadSelfSendAccounts(): Promise<string[]> {
  const result = await db.execute(sql`
    SELECT email AS "email"
    FROM instantly_accounts
    WHERE send_transport = ${SEND_TRANSPORT_SMTP}
      AND absent_since IS NULL
  `);
  return (result.rows as Record<string, unknown>[]).map((row) => String(row.email));
}

/** Lowercase every header name once, so the pure classifier can index directly. */
function normalizeHeaders(headers: Map<string, unknown>): InboundHeaders {
  const out: Record<string, string> = {};
  for (const [key, value] of headers) {
    out[key.toLowerCase()] =
      typeof value === "string" ? value : JSON.stringify(value ?? "");
  }
  return out;
}

async function pollAccount(
  accountEmail: string,
  credential: MailboxCredential,
  since: Date,
  summary: PollSummary,
): Promise<void> {
  const knownSends = await loadKnownSends(accountEmail);

  const client = new ImapFlow({
    host: credential.imapHost,
    port: GMAIL_IMAP_PORT,
    secure: true,
    auth: { user: credential.address, pass: credential.appPassword },
    logger: false,
  });

  await client.connect();

  try {
    const lock = await client.getMailboxLock("INBOX");
    try {
      for await (const message of client.fetch({ since }, { envelope: true, source: true })) {
        // The server can return a fetch row without a body (a race with an
        // expunge, a partial response). Nothing to classify, so skip rather than
        // parse an empty buffer into an empty message.
        if (!message.source) continue;

        const parsed: ParsedMail = await simpleParser(message.source);
        const messageId = parsed.messageId;

        // No Message-Id means nothing to dedup on, so a re-read would insert it
        // again every run. Skipping is the honest choice — and a message with no
        // Message-Id cannot thread onto one of our sends anyway.
        if (!messageId) continue;

        summary.messagesRead += 1;

        const headers = normalizeHeaders(parsed.headers as Map<string, unknown>);
        const body = `${parsed.text ?? ""}\n${parsed.html || ""}`;
        const classification = classifyInbound(headers, body, new Set(knownSends.keys()));

        const send = classification.referencedMessageIds
          .map((id) => knownSends.get(id))
          .find((s): s is KnownSend => s !== undefined);

        // Bronze first, and for EVERY message including `unrelated`: the row is
        // both the dedup key and the record of what we chose to ignore.
        const [row] = await db
          .insert(imapMessagesRaw)
          .values({
            accountEmail,
            messageId,
            fromAddress: parsed.from?.text ?? null,
            subject: parsed.subject ?? null,
            kind: classification.kind,
            instantlyCampaignId: send?.instantlyCampaignId ?? null,
            step: send?.step ?? null,
            payload: {
              headers,
              subject: parsed.subject ?? null,
              from: parsed.from?.text ?? null,
              referencedMessageIds: classification.referencedMessageIds,
              textSnippet: (parsed.text ?? "").slice(0, 4000),
            },
            receivedAt: parsed.date ?? null,
          })
          .onConflictDoNothing({
            target: [imapMessagesRaw.accountEmail, imapMessagesRaw.messageId],
          })
          .returning({ id: imapMessagesRaw.id });

        // No row back means this message was already ingested on an earlier run.
        // Re-promoting would be harmless (silver dedups too) but pointless.
        if (!row) continue;

        if (classification.kind === "unrelated" || !send) {
          summary.unrelated += 1;
          continue;
        }

        const eventType = eventTypeForInbound(classification.kind);
        if (!eventType) {
          summary.unrelated += 1;
          continue;
        }

        await promoteEvent({
          eventType,
          instantlyCampaignId: send.instantlyCampaignId,
          leadEmail: send.leadEmail,
          accountEmail,
          step: send.step,
          variant: null,
          timestamp: parsed.date ?? new Date(),
          source: "self_send",
          sourceRowId: row.id,
        });

        if (classification.kind === "reply") summary.replies += 1;
        else if (classification.kind === "auto_reply") summary.autoReplies += 1;
        else summary.bounces += 1;
      }
    } finally {
      lock.release();
    }
  } finally {
    await client.logout().catch(() => {
      // The session is being torn down either way; a failed logout must not mask
      // the outcome of the poll itself.
    });
  }
}

/**
 * Poll every self-send mailbox.
 *
 * Fail-loud PER ACCOUNT: one mailbox with a broken credential is counted and
 * logged, and the sweep moves on — the alternative is that a single bad mailbox
 * stops the fleet from noticing anyone's reply.
 */
export async function runPoll(options: { asOf?: Date } = {}): Promise<PollSummary> {
  const asOf = options.asOf ?? new Date();
  const since = new Date(asOf.getTime() - POLL_WINDOW_DAYS * 24 * 60 * 60 * 1000);

  const accounts = await loadSelfSendAccounts();

  const summary: PollSummary = {
    accountsPolled: 0,
    messagesRead: 0,
    replies: 0,
    autoReplies: 0,
    bounces: 0,
    unrelated: 0,
    accountsFailed: 0,
  };

  for (const accountEmail of accounts) {
    try {
      const credential = await resolveMailboxCredential(accountEmail, CALLER);
      await pollAccount(accountEmail, credential, since, summary);
      summary.accountsPolled += 1;
    } catch (error) {
      console.error(
        `[instantly-service] self-send-poll: account=${accountEmail} failed: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      summary.accountsFailed += 1;
    }
  }

  console.log(`[instantly-service] self-send-poll: done ${JSON.stringify(summary)}`);

  return summary;
}
