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

async function pollReceiver(
  receiverEmail: string,
  credential: MailboxCredential,
  since: Date,
  asOf: Date,
  pending: ReadonlyMap<string, PendingWarmup>,
  summary: WarmupPollSummary,
): Promise<void> {
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

    for (const folder of FOLDERS) {
      if (!available.has(folder.path)) continue;

      const lock = await client.getMailboxLock(folder.path);
      try {
        for await (const message of client.fetch({ since }, { source: true, uid: true })) {
          if (!message.source) continue;

          const parsed: ParsedMail = await simpleParser(message.source);
          const messageId = parsed.messageId;
          if (!messageId) continue;

          summary.messagesRead += 1;

          const warmup = pending.get(messageId);
          // Real mailboxes also receive ordinary mail. A message that is not one
          // of ours is not our business — and acting on it would mean moving a
          // stranger's email between a customer's folders.
          if (!warmup) continue;
          if (warmup.receiverEmail !== receiverEmail) continue;

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
          // BEEN ACTED ON, and conflating them permanently strands mail in spam.
          //
          // The receipt is written BEFORE the rescue on purpose (the placement
          // must be frozen before we move anything). So a run that dies between
          // the two — which is exactly what an unhandled IMAP socket error used
          // to do — leaves a row saying "landed in spam" with nothing done about
          // it, and treating the insert conflict as "already handled" means no
          // later run ever rescues it. Observed 2026-09-06: two messages stuck
          // at `rescued: false` across three killed sweeps.
          //
          // So the skip is keyed on the ACTION, not on the row: a message
          // already in the inbox needs nothing, and one already rescued is done.
          // Everything else is retried, which is safe because both the flag and
          // the move are idempotent.
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
          await client.messageFlagsAdd({ uid: String(message.uid) }, ["\\Seen"], {
            uid: true,
          });

          let rescued = false;
          if (folder.placement === "spam") {
            await client.messageMove({ uid: String(message.uid) }, "INBOX", { uid: true });
            rescued = true;
            summary.rescued += 1;
          }

          // A reply is the strongest signal in the set, which is exactly why not
          // every message gets one: a mailbox that answers EVERYTHING is itself
          // a pattern. Keyed on the message id so the decision is stable across
          // the re-read window and nothing is answered twice.
          let replied = false;
          if (shouldReplyTo(messageId)) {
            const reply = await buildWarmupReply(
              warmup.subject ?? "",
              typeof parsed.text === "string" ? parsed.text : "",
            );
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
            // daily quota like any other. Recording it here is what makes that
            // true: both the mesh's own budget and the outreach dispatcher read
            // `warmup_dispatches` for the day, so a reply left unrecorded is
            // quota spent that neither job can see — the same blind spend the
            // per-alias fan-out produced before it was measured.
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
  // is bounded by the mesh rather than by the fleet.
  const receivers = [...new Set([...pending.values()].map((p) => p.receiverEmail))].sort();

  for (const receiverEmail of receivers) {
    const credential = resolve(receiverEmail);
    if (!credential) continue;

    // Fail-loud PER MAILBOX: one unreachable inbox must not stop the rest of the
    // fleet being warmed, and the failure has to stay visible rather than read
    // as "nothing arrived".
    try {
      await pollReceiver(receiverEmail, credential, since, asOf, pending, summary);
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

  return summary;
}
