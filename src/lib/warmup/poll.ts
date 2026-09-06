/**
 * The warmup mesh — receiving side. This half is what actually warms a mailbox.
 *
 * The sending half puts ordinary mail on the wire; that alone teaches a filter
 * almost nothing. What a provider grades is what the RECIPIENT does with it:
 * mail that is opened, moved out of spam and occasionally answered is mail from
 * a sender people want. So this module reads each partner's inbox and spam
 * folder, records where the message landed, then acts.
 *
 * ⚠️ THE OBSERVATION IS FROZEN BEFORE THE RESCUE. Where the message landed at
 * delivery is a fact; moving it afterwards is our action. Writing the receipt
 * first (with the unique index making the first observation win, exactly as the
 * seed harness does) keeps the two apart — otherwise a re-read after a rescue
 * would report every message as having arrived in the inbox, and the mesh would
 * look like it was working precisely when it was not.
 *
 * ⚠️ THE JUDGES ARE NOT POLLED HERE. The seed harness grades placement by
 * reading a small held-out set of receivers; teaching those filters would make
 * their verdicts a reflection of our training rather than a measurement. The
 * exclusion is applied when the mesh is PLANNED (`partnerCandidates`), so no
 * warmup mail is ever addressed to a judge and there is nothing here to rescue.
 *
 * Idempotent WITHOUT a cursor, same reasoning as the self-send poller: each run
 * re-reads a fixed window and the unique index makes the overlap a no-op. A
 * stored cursor that drifts silently loses messages, and a lost message here
 * means a mailbox quietly stops being warmed.
 */

import { sql } from "drizzle-orm";
import { createImapClient } from "../self-send/imap-client";
import { simpleParser, type ParsedMail } from "mailparser";

import { db } from "../../db";
import { warmupDispatches, warmupReceipts } from "../../db/schema";
import type { CallerInfo } from "../key-client";
import {
  GMAIL_IMAP_PORT,
  loginFor,
  type MailboxCredential,
} from "../self-send/mailbox-credentials";
import { loadSeedCredentialResolver } from "../seed-placement/credentials";
import { dispatchMessage } from "../self-send/smtp";
import { buildWarmupReply } from "./message";
import { shouldReplyTo, warmupDayKey } from "./plan";

const CALLER: CallerInfo = { method: "POST", path: "/internal/audit/warmup/poll" };

/**
 * How far back each run reads.
 *
 * Wide enough that a run skipped by a deploy or a delayed cron loses nothing,
 * narrow enough that the re-read is cheap. Same bound and same reasoning as the
 * self-send poller's window.
 */
export const WARMUP_POLL_WINDOW_DAYS = 3;

/**
 * Mailboxes read at once.
 *
 * Sequential took ~2 minutes per mailbox over 164 receivers — a five-hour sweep,
 * which a daily job cannot reliably finish before the next one starts. Unbounded
 * would open 164 IMAP connections at once, which providers throttle and which is
 * the shape that produces the socket timeouts this poller already has to survive.
 */
export const WARMUP_POLL_CONCURRENCY = 8;

/**
 * Folders worth opening, and what landing there means.
 *
 * `[Gmail]/All Mail` is deliberately absent: it MIRRORS both inbox and spam, so
 * reading it would double-count every message. Servers differ on the spam
 * folder's name, hence both spellings.
 */
const FOLDERS: ReadonlyArray<{ path: string; placement: "inbox" | "spam" }> = [
  { path: "INBOX", placement: "inbox" },
  { path: "[Gmail]/Spam", placement: "spam" },
  { path: "Junk", placement: "spam" },
  { path: "Spam", placement: "spam" },
];

export interface WarmupPollSummary {
  accountsPolled: number;
  messagesRead: number;
  /** Warmup messages we recognised as ours. */
  matched: number;
  /** Found in spam and moved back to the inbox. */
  rescued: number;
  /** Answered, creating a real thread. */
  replied: number;
  accountsFailed: number;
}

interface PendingWarmup {
  messageId: string;
  senderEmail: string;
  receiverEmail: string;
  subject: string | null;
}

/**
 * Warmup mail we sent inside the window, keyed by Message-Id.
 *
 * The correlation key is a Message-Id we know went out — never the sender
 * address. These mailboxes also receive ordinary mail, and matching on the
 * sender would act on anything that happened to come from a fleet address.
 */
async function loadPendingWarmup(since: Date): Promise<Map<string, PendingWarmup>> {
  const result = await db.execute(sql`
    SELECT message_id AS "messageId",
           sender_email AS "senderEmail",
           receiver_email AS "receiverEmail",
           subject AS "subject"
    FROM warmup_dispatches
    WHERE outcome = 'sent' AND dispatched_at >= ${since}
  `);

  const out = new Map<string, PendingWarmup>();
  for (const row of result.rows as Record<string, unknown>[]) {
    out.set(String(row.messageId), {
      messageId: String(row.messageId),
      senderEmail: String(row.senderEmail),
      receiverEmail: String(row.receiverEmail),
      subject: row.subject === null ? null : String(row.subject),
    });
  }
  return out;
}

/**
 * Read one mailbox and act on the warmup messages we sent to it.
 *
 * ⚠️ ASK THE SERVER FOR OUR MESSAGES; DO NOT WALK ITS MAIL. We know exactly
 * which Message-Ids we sent to this mailbox — a handful — so `SEARCH HEADER
 * Message-ID` answers in one indexed round trip each. Iterating the window
 * instead means the cost scales with how much ORDINARY mail the mailbox
 * receives, which has nothing to do with our traffic: at ~2 minutes per mailbox
 * over 164 receivers that is a five-hour sweep, and a daily job that takes five
 * hours will not reliably finish before the next one starts. Measured on the
 * first armed runs, in three successive shapes: full source, then envelopes,
 * then this.
 */
async function pollReceiver(
  receiverEmail: string,
  credential: MailboxCredential,
  asOf: Date,
  expected: readonly PendingWarmup[],
  summary: WarmupPollSummary,
): Promise<void> {
  if (expected.length === 0) return;

  const client = createImapClient({
    host: credential.imapHost,
    port: GMAIL_IMAP_PORT,
    secure: true,
    auth: { user: loginFor(credential), pass: credential.appPassword },
    logger: false,
  }, receiverEmail);

  await client.connect();

  try {
    // Ask the server what it HAS, once, rather than probing each name and
    // swallowing failures — a per-folder catch would also hide an auth drop.
    const available = new Set((await client.list()).map((m) => m.path));

    // Messages still to place. A message is found in exactly one folder, so it
    // leaves this set as soon as it is handled and later folders skip it.
    const outstanding = new Map(expected.map((w) => [w.messageId, w]));

    for (const folder of FOLDERS) {
      if (outstanding.size === 0) break;
      if (!available.has(folder.path)) continue;

      const lock = await client.getMailboxLock(folder.path);
      try {
        for (const [messageId, warmup] of [...outstanding]) {
          const uids = (await client.search(
            { header: { "message-id": messageId } },
            { uid: true },
          )) as number[] | false;

          const uid = Array.isArray(uids) ? uids[0] : undefined;
          if (uid === undefined) continue;

          outstanding.delete(messageId);
          summary.messagesRead += 1;
          summary.matched += 1;

          // FIRST: freeze where it landed. The rescue below changes the folder,
          // so a receipt written afterwards would record our own action as the
          // delivery outcome.
          const [inserted] = await db
            .insert(warmupReceipts)
            .values({
              messageId,
              receiverEmail,
              folder: folder.path,
              placement: folder.placement,
            })
            .onConflictDoNothing({
              target: [warmupReceipts.messageId, warmupReceipts.receiverEmail],
            })
            .returning({ id: warmupReceipts.id });

          // ⚠️ THE RECEIPT EXISTING IS NOT THE SAME FACT AS THE MESSAGE HAVING
          // BEEN ACTED ON. The receipt is written before the rescue on purpose,
          // so a run that dies between the two leaves a row saying "landed in
          // spam" with nothing done about it — and treating the conflict as
          // "already handled" means no later run ever rescues it. The skip is
          // keyed on the ACTION; both the flag and the move are idempotent.
          if (!inserted) {
            const [existing] = await db
              .select({
                placement: warmupReceipts.placement,
                rescued: warmupReceipts.rescued,
              })
              .from(warmupReceipts)
              .where(
                sql`${warmupReceipts.messageId} = ${messageId} AND ${warmupReceipts.receiverEmail} = ${receiverEmail}`,
              );

            if (!existing || existing.placement !== "spam" || existing.rescued) continue;
          }

          // THEN act. Mark read first — an unread message that jumps folders is
          // not what a person doing their inbox looks like.
          await client.messageFlagsAdd({ uid: String(uid) }, ["\\Seen"], { uid: true });

          let rescued = false;
          if (folder.placement === "spam") {
            await client.messageMove({ uid: String(uid) }, "INBOX", { uid: true });
            rescued = true;
            summary.rescued += 1;
          }

          // A reply is the strongest signal in the set, which is exactly why not
          // every message gets one: a mailbox that answers EVERYTHING is itself
          // a pattern. Keyed on the message id so the decision is stable across
          // the re-read window and nothing is answered twice.
          let replied = false;
          if (shouldReplyTo(messageId)) {
            // The body is needed ONLY here, so it is fetched for this one
            // message rather than for every message in the window.
            let originalText = "";
            try {
              const full = await client.fetchOne(String(uid), { source: true }, { uid: true });
              if (full && full.source) {
                const parsed: ParsedMail = await simpleParser(full.source);
                originalText = typeof parsed.text === "string" ? parsed.text : "";
              }
            } catch {
              // Answering without quoting is still a reply; failing to fetch the
              // body must not cost the thread.
            }

            const reply = await buildWarmupReply(warmup.subject ?? "", originalText);
            const sent = await dispatchMessage(credential, {
              from: receiverEmail,
              to: warmup.senderEmail,
              subject: reply.subject,
              html: `<p>${reply.text.split("\n").join("<br>")}</p>`,
              headers: {},
              inReplyTo: messageId,
              references: [messageId],
            });

            // ⚠️ A REPLY IS A SEND, so it comes out of the replying mailbox's
            // daily quota like any other. Both the mesh's own budget and the
            // outreach dispatcher read `warmup_dispatches` for the day, so a
            // reply left unrecorded is quota neither job can see.
            await db
              .insert(warmupDispatches)
              .values({
                senderEmail: receiverEmail,
                senderMailbox: loginFor(credential),
                receiverEmail: warmup.senderEmail,
                dayKey: warmupDayKey(asOf),
                messageId: sent.messageId,
                subject: reply.subject,
                outcome: "sent",
                response: sent.response,
              })
              .onConflictDoNothing();

            replied = true;
            summary.replied += 1;
          }

          if (rescued || replied) {
            await db.execute(sql`
              UPDATE warmup_receipts
                 SET rescued = ${rescued}, replied = ${replied}
               WHERE message_id = ${messageId} AND receiver_email = ${receiverEmail}
            `);
          }
        }
      } finally {
        lock.release();
      }
    }
  } finally {
    await client.logout().catch(() => undefined);
  }
}

export async function runWarmupPoll(
  options: { asOf?: Date; sinceDays?: number } = {},
): Promise<WarmupPollSummary> {
  const asOf = options.asOf ?? new Date();
  const windowDays = Math.min(
    365,
    Math.max(WARMUP_POLL_WINDOW_DAYS, options.sinceDays ?? WARMUP_POLL_WINDOW_DAYS),
  );
  const since = new Date(asOf.getTime() - windowDays * 24 * 60 * 60 * 1000);

  const pending = await loadPendingWarmup(since);
  const resolve = await loadSeedCredentialResolver(CALLER);

  const summary: WarmupPollSummary = {
    accountsPolled: 0,
    messagesRead: 0,
    matched: 0,
    rescued: 0,
    replied: 0,
    accountsFailed: 0,
  };

  // Only mailboxes we actually sent warmup TO have anything to find, so the poll
  // is bounded by the mesh rather than by the fleet. Each receiver is handed
  // exactly the messages addressed to it, so the mailbox is never searched for
  // something that could not be there.
  const byReceiver = new Map<string, PendingWarmup[]>();
  for (const warmup of pending.values()) {
    const list = byReceiver.get(warmup.receiverEmail);
    if (list) list.push(warmup);
    else byReceiver.set(warmup.receiverEmail, [warmup]);
  }

  const receivers = [...byReceiver.keys()].sort();

  // ⚠️ BOUNDED CONCURRENCY, not a `Promise.all` over every receiver. Each worker
  // holds an open IMAP connection and its own SMTP dispatches, so an unbounded
  // fan-out over ~164 mailboxes would open 164 sockets at once — which the
  // providers throttle and which is exactly the shape that produces the socket
  // timeouts this poller already had to be hardened against. Sequential was the
  // other extreme and took ~5 hours.
  let cursor = 0;

  const worker = async (): Promise<void> => {
    for (;;) {
      const receiverEmail = receivers[cursor++];
      if (receiverEmail === undefined) return;

      const credential = resolve(receiverEmail);
      if (!credential) continue;

      // Fail-loud PER MAILBOX: one unreachable inbox must not stop the rest of
      // the fleet being warmed, and the failure has to stay visible rather
      // than read as "nothing arrived".
      try {
        await pollReceiver(
          receiverEmail,
          credential,
          asOf,
          byReceiver.get(receiverEmail) ?? [],
          summary,
        );
        summary.accountsPolled += 1;
      } catch (error) {
        summary.accountsFailed += 1;
        console.warn(
          `[warmup] poll: ${receiverEmail} failed: ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
      }
    }
  };

  await Promise.all(
    Array.from({ length: Math.min(WARMUP_POLL_CONCURRENCY, receivers.length) }, () =>
      worker(),
    ),
  );

  return summary;
}
