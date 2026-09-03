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
import {
  classifyInbound,
  correlateSend,
  eventTypeForInbound,
  type CorrelatedSend,
  type InboundHeaders,
} from "./inbound";
import { stopLeadSequence } from "../stop-lead-sequence";
import { qualifyReply } from "./qualify-reply";
import { isSelfSendCampaignId, SEND_TRANSPORT_SMTP } from "./transport";

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
  /** Replies we obtained a trustworthy sentiment for. */
  qualified: number;
  /** Replies recorded and stopped, but left without a sentiment. */
  unqualified: number;
  unrelated: number;
  /**
   * Messages whose references reached MORE THAN ONE sequence on this mailbox.
   * Stored in bronze, promoted to nothing — see `correlateSend`.
   */
  ambiguous: number;
  /**
   * Sequences we told the SENDER to stop, on top of our own hold cancel. Only
   * an Instantly-transport sequence needs it, and only when Instantly did not
   * see the reply itself.
   */
  sequencesStopped: number;
  accountsFailed: number;
}

/** A correlated send, plus the org that owns it (needed to pause on Instantly). */
interface KnownSend extends CorrelatedSend {
  orgId: string | null;
}

/**
 * Every send that left this mailbox, keyed by the Message-Id it carried —
 * BOTH transports.
 *
 * ⚠️ THE SECOND HALF IS LOAD-BEARING AND IS NOT A NICE-TO-HAVE. A mailbox on
 * `send_transport='smtp'` holds, almost always, sequences Instantly dispatched
 * BEFORE the flip — the transport is frozen per campaign at send time, so a
 * mailbox rescued onto our own sender keeps draining Instantly-sent sequences
 * for weeks. Reading only `smtp_dispatch_raw` therefore classifies every reply
 * to those sequences as `unrelated` and touches nothing, which is precisely how
 * a real "I would be interested" reply reached nobody: Instantly had lost its
 * IMAP link to the mailbox (so it emitted no `reply_received`) and our own
 * poller had no id to match against.
 *
 * The key is EXACT on both sides, never a heuristic. Instantly hands back the
 * `Message-Id` of the mail it sent in its own Unibox payload, which the reconcile
 * poll and the Unibox backfill mirror into `instantly_emails_raw` — 39,749 such
 * ids across 65 of these mailboxes in prod at the time of writing. So the
 * widening buys the Instantly-transport case without inventing a single
 * inference about senders, subjects or timing.
 *
 * The lead email comes from the CAMPAIGN ROW, not from the payload's `lead`
 * field: the campaign row is this service's own record of who the sequence is
 * for, and it is the value every other silver writer uses.
 */
async function loadKnownSends(accountEmail: string): Promise<Map<string, KnownSend>> {
  const result = await db.execute(sql`
    -- Our own dispatches.
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
           END                     AS "step",
           dc.org_id               AS "orgId"
    FROM smtp_dispatch_raw d
    LEFT JOIN instantly_campaigns dc
      ON dc.instantly_campaign_id = d.instantly_campaign_id
    WHERE d.account_email = ${accountEmail}
      AND d.outcome = 'sent'
      AND d.message_id IS NOT NULL

    UNION ALL

    -- Sends Instantly made from this same mailbox, as Instantly itself recorded
    -- them. ue_type = 1 is its outbound marker.
    SELECT m.payload->>'message_id'  AS "messageId",
           c.instantly_campaign_id   AS "instantlyCampaignId",
           c.lead_email              AS "leadEmail",
           -- Instantly's step is a 0-based "<sequence>_<step>_<variant>" string,
           -- ours is 1-based. Verified against prod: 0_0_0 pairs with silver
           -- step 1 (36,766 rows), 0_1_0 with 2 (31,570), 0_2_0 with 3 (30,025).
           -- An unparseable value falls back to the campaign's last real send
           -- rather than to a guessed 1 — the same reasoning as the step-0 case
           -- above, since inventing a step corrupts step accounting downstream.
           COALESCE(
             NULLIF(split_part(m.payload->>'step', '_', 2), '')::int + 1,
             (SELECT MAX(e.step) FROM instantly_events e
              WHERE e.campaign_id = c.instantly_campaign_id
                AND e.event_type = 'email_sent'),
             1
           )                         AS "step",
           c.org_id                  AS "orgId"
    FROM instantly_emails_raw m
    JOIN instantly_campaigns c
      ON c.instantly_campaign_id = m.instantly_campaign_id
    WHERE m.payload->>'eaccount' = ${accountEmail}
      AND m.payload->>'ue_type' = '1'
      AND m.payload->>'message_id' IS NOT NULL
  `);

  const sends = new Map<string, KnownSend>();
  for (const row of result.rows as Record<string, unknown>[]) {
    const messageId = String(row.messageId);
    // Our own dispatch row wins a collision: it is first-party evidence of what
    // we put on the wire, where the Instantly row is a mirror of a third party's
    // record. In practice they never collide — a Message-Id is unique.
    if (sends.has(messageId)) continue;
    sends.set(messageId, {
      instantlyCampaignId: row.instantlyCampaignId as string,
      leadEmail: row.leadEmail as string,
      step: Number(row.step),
      orgId: (row.orgId as string | null) ?? null,
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

        const correlation = correlateSend(
          classification.referencedMessageIds,
          knownSends,
        );

        // Ambiguous is NOT "pick the first". Two live sequences on one mailbox
        // both claiming this message means we cannot say whose reply it is, and
        // a wrong attribution both fabricates a reply on a lead AND stops the
        // wrong sequence. Record it, say so, promote nothing.
        if (correlation.outcome === "ambiguous") {
          console.warn(
            `[instantly-service] self-send-poll: AMBIGUOUS inbound on account=${accountEmail} message=${messageId} — references reach ${correlation.campaignIds.length} sequences (${correlation.campaignIds.join(", ")}); stored in bronze, nothing promoted`,
          );
        }

        const send = correlation.outcome === "matched" ? correlation.send : undefined;

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

        if (correlation.outcome === "ambiguous") {
          summary.ambiguous += 1;
          continue;
        }

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

          // `promoteEvent` above did OUR half of the stop — the row is marked
          // and the remaining holds are cancelled. On an Instantly-transport
          // sequence that tells the SENDER nothing, and we only ever read this
          // reply ourselves because Instantly could not: its IMAP link to the
          // mailbox is broken, so its own stop-on-reply never fired and it will
          // keep dispatching the rest of the sequence at someone who has
          // already answered. Exactly the manual-qualification situation, and
          // it takes the same second half.
          //
          // A `self:` sequence needs nothing here: `stopSelfSendSequence` is
          // reached through `cancelRemainingProvisions` + the row status that
          // `promoteEvent` already applied, and there is no campaign to pause.
          if (!isSelfSendCampaignId(send.instantlyCampaignId) && send.orgId) {
            const stopped = await stopLeadSequence({
              orgId: send.orgId,
              instantlyCampaignId: send.instantlyCampaignId,
              leadEmail: send.leadEmail,
              reason: "reply read from our own mailbox; Instantly never saw it",
              caller: CALLER,
            });
            if (stopped) summary.sequencesStopped += 1;
          }

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
    qualified: 0,
    unqualified: 0,
    unrelated: 0,
    ambiguous: 0,
    sequencesStopped: 0,
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
