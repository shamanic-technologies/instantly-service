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
  loginFor,
  resolveMailboxCredential,
  type MailboxCredential,
} from "./mailbox-credentials";
import { classifyInbound, eventTypeForInbound, type InboundHeaders } from "./inbound";
import { parseInstantlySequenceStep } from "./instantly-sends";
import { qualifyReply } from "./qualify-reply";
import { SEND_TRANSPORT_SMTP } from "./transport";

const CALLER: CallerInfo = { method: "POST", path: "/internal/self-send/poll" };

/**
 * `db.execute` resolves to a node-postgres `QueryResult`, never a bare array —
 * see the repo rule. Read through this rather than casting.
 */
function rowsOf(result: unknown): Record<string, unknown>[] {
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

/**
 * How far back each run looks.
 *
 * Comfortably wider than the hourly cadence, so a missed run, a slow delivery or
 * a clock skew cannot open a gap. The unique index absorbs the overlap.
 */
const POLL_WINDOW_DAYS = 3;

/**
 * The widest window a caller may ask for.
 *
 * A mailbox nobody has ever read holds replies far older than the routine
 * window — and for those mailboxes the mail exists NOWHERE ELSE. Instantly
 * cannot log in either, so its Unibox never mirrored them and no bronze backfill
 * can reach them: the only instrument that can is this poller, pointed further
 * back. Hence an explicit, bounded override rather than a second sweep.
 *
 * Bounded because the window is an IMAP fetch per mailbox, and unbounded means
 * re-reading and re-parsing an entire inbox on every routine run if the value
 * ever leaks into the cron. A year is past the age of the fleet.
 */
const MAX_POLL_WINDOW_DAYS = 365;

export interface PollSummary {
  accountsPolled: number;
  messagesRead: number;
  replies: number;
  autoReplies: number;
  bounces: number;
  /** Replies we obtained a trustworthy sentiment for. */
  qualified: number;
  /** Replies recorded and stopped, but left without a sentiment. */
  unqualified: number;
  unrelated: number;
  accountsFailed: number;
}

interface KnownSend {
  instantlyCampaignId: string;
  leadEmail: string;
  step: number;
}

/**
 * Sends from this mailbox, keyed by the Message-Id that went on the wire.
 *
 * TWO sources, and the second one is what makes a reply on an Instantly-sent
 * sequence correlatable at all — see `instantly-sends.ts`. Both are exact
 * Message-Id keys; neither is an address heuristic.
 */
export async function loadKnownSends(accountEmail: string): Promise<Map<string, KnownSend>> {
  const sends = new Map<string, KnownSend>();

  const result = await db.execute(sql`
    SELECT d.message_id            AS "messageId",
           d.instantly_campaign_id AS "instantlyCampaignId",
           d.lead_email            AS "leadEmail",
           -- A manual reply is recorded at step 0 (it is not a step of the
           -- sequence). An answer threading onto it belongs, honestly, to the
           -- last sequence step the prospect actually received — attributing it
           -- to step 0 would put a step that does not exist into silver, and the
           -- inference rule would then project a step-0 email_sent nobody sent.
           CASE WHEN d.step = 0 THEN COALESCE((
                  SELECT MAX(x.step)
                  FROM smtp_dispatch_raw x
                  WHERE x.instantly_campaign_id = d.instantly_campaign_id
                    AND x.outcome = 'sent'
                ), 1)
                ELSE d.step
           END                     AS "step"
    FROM smtp_dispatch_raw d
    WHERE d.account_email = ${accountEmail}
      AND d.outcome = 'sent'
      AND d.message_id IS NOT NULL
  `);

  for (const row of rowsOf(result)) {
    sends.set(String(row.messageId), {
      instantlyCampaignId: String(row.instantlyCampaignId),
      leadEmail: String(row.leadEmail),
      step: Number(row.step),
    });
  }

  // Second source: what INSTANTLY sent from this mailbox. `eaccount` is the
  // sending mailbox on Instantly's own email object, and the lead comes from our
  // campaign row rather than the payload — one Instantly campaign is one lead, so
  // the join is authoritative and does not depend on a payload field that is
  // absent on some shapes.
  //
  // Our own dispatch wins a collision (the loop below only fills a key we do not
  // already hold): a message-id we put on the wire is a fact about a send we
  // made, and `smtp_dispatch_raw` additionally resolves the step-0 manual-reply
  // case that this mirror knows nothing about.
  const instantlyResult = await db.execute(sql`
    SELECT m.payload->>'message_id' AS "messageId",
           m.instantly_campaign_id  AS "instantlyCampaignId",
           c.lead_email             AS "leadEmail",
           m.payload->>'step'       AS "rawStep"
    FROM instantly_emails_raw m
    JOIN instantly_campaigns c ON c.instantly_campaign_id = m.instantly_campaign_id
    WHERE m.payload->>'eaccount' = ${accountEmail}
      AND m.payload->>'ue_type' = '1'
      AND m.payload->>'message_id' IS NOT NULL
  `);

  for (const row of rowsOf(instantlyResult)) {
    const messageId = String(row.messageId);
    if (sends.has(messageId)) continue;

    const step = parseInstantlySequenceStep(
      row.rawStep === null || row.rawStep === undefined ? null : String(row.rawStep),
    );
    // An unreadable step is dropped rather than defaulted. Correlating the reply
    // to the wrong step would make the inference rule project `email_sent` rows
    // for steps nobody sent.
    if (step === null) continue;

    sends.set(messageId, {
      instantlyCampaignId: String(row.instantlyCampaignId),
      leadEmail: String(row.leadEmail),
      step,
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
    auth: { user: loginFor(credential), pass: credential.appPassword },
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

        if (classification.kind === "reply") {
          summary.replies += 1;

          // Qualify the reply so a hot one reaches the agency inbox. This does
          // NOT decide whether to stop the sequence — `reply_received` above
          // already did that, whatever the sentiment. It only decides what the
          // reply MEANS, which drives the forward and the gold stats.
          //
          // Fail-soft and AFTER the stop: a classification we cannot obtain
          // leaves the reply recorded and the sequence correctly stopped, just
          // unlabelled. Promoting a guessed sentiment would be worse — a
          // fabricated "neutral" on a hot reply reads as a real judgement.
          try {
            const qualification = await qualifyReply(parsed.text ?? "");
            if (qualification) {
              await promoteEvent({
                eventType: qualification,
                instantlyCampaignId: send.instantlyCampaignId,
                leadEmail: send.leadEmail,
                accountEmail,
                step: send.step,
                variant: null,
                timestamp: parsed.date ?? new Date(),
                source: "self_send",
                sourceRowId: row.id,
              });
              summary.qualified += 1;
            } else {
              console.warn(
                `[instantly-service] self-send-poll: no usable qualification for campaign=${send.instantlyCampaignId} — reply recorded and sequence stopped, sentiment left unset`,
              );
              summary.unqualified += 1;
            }
          } catch (error) {
            console.error(
              `[instantly-service] self-send-poll: qualification failed for campaign=${send.instantlyCampaignId}: ${
                error instanceof Error ? error.message : String(error)
              }`,
            );
            summary.unqualified += 1;
          }
        } else if (classification.kind === "auto_reply") summary.autoReplies += 1;
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
export async function runPoll(
  options: { asOf?: Date; sinceDays?: number } = {},
): Promise<PollSummary> {
  const asOf = options.asOf ?? new Date();

  // A caller may reach further back for a one-shot catch-up on a mailbox that
  // has just become readable — clamped, never trusted raw. Anything absent or
  // unusable falls to the routine window; a wider-than-max ask is clamped rather
  // than refused, since the caller's intent ("as far back as you can") is clear.
  const windowDays = Math.min(
    Math.max(
      Number.isFinite(options.sinceDays) && (options.sinceDays as number) > 0
        ? (options.sinceDays as number)
        : POLL_WINDOW_DAYS,
      POLL_WINDOW_DAYS,
    ),
    MAX_POLL_WINDOW_DAYS,
  );
  const since = new Date(asOf.getTime() - windowDays * 24 * 60 * 60 * 1000);

  const accounts = await loadSelfSendAccounts();

  const summary: PollSummary = {
    accountsPolled: 0,
    messagesRead: 0,
    replies: 0,
    autoReplies: 0,
    bounces: 0,
    qualified: 0,
    unqualified: 0,
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

  console.log(
    `[instantly-service] self-send-poll: done windowDays=${windowDays} ${JSON.stringify(summary)}`,
  );

  return summary;
}
