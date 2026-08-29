/**
 * Read the seeds back and promote them to silver (IO glue).
 *
 * For each receiver mailbox, open INBOX and the spam folder, match what is there
 * against the Message-Ids we dispatched, and record the verdict. Then aggregate
 * every test that has un-promoted observations into
 * `instantly_placement_results` — the SAME silver the Instantly path writes, so
 * the lifecycle delivery gate and account-health read it with no code change.
 *
 * Idempotent WITHOUT a cursor, exactly like the self-send IMAP poller: each run
 * re-reads a bounded window and the unique `(receiver_email, message_id)` index
 * absorbs the overlap. A stored cursor that drifts loses observations, and a
 * lost observation is indistinguishable from a seed that went missing — i.e. it
 * silently penalises a healthy mailbox.
 */

import { ImapFlow } from "imapflow";
import { simpleParser, type ParsedMail } from "mailparser";
import { sql } from "drizzle-orm";

import { db } from "../../db";
import {
  instantlyPlacementResults,
  seedPlacementObservations,
} from "../../db/schema";
import type { CallerInfo } from "../key-client";
import { GMAIL_IMAP_PORT, loginFor, type MailboxCredential } from "../self-send/mailbox-credentials";
import { loadSeedCredentialResolver } from "./credentials";
import {
  aggregateSeedPlacement,
  classifySeedFolder,
  parseAuthResults,
  type SeedDispatchRecord,
  type SeedObservationRecord,
} from "./classify";

const CALLER: CallerInfo = {
  method: "POST",
  path: "/internal/audit/seed-placement/sync",
};

/** Local row unwrapper — same shape every other module in this repo defines. */
function rowsOf<T = Record<string, unknown>>(result: unknown): T[] {
  if (!result) return [];
  return Array.isArray(result)
    ? (result as T[])
    : (((result as { rows?: T[] }).rows) ?? []);
}

/**
 * How far back each run reads.
 *
 * Wider than the weekly send cadence would strictly need, so a missed run or a
 * slow delivery cannot open a gap. The unique index makes the overlap free.
 */
const READ_WINDOW_DAYS = 10;

/** Folders worth opening. Anything else classifies to null anyway (see `classifySeedFolder`). */
const CANDIDATE_FOLDERS = ["INBOX", "[Gmail]/Spam", "Junk", "Junk E-mail", "Spam"];

export interface SeedPlacementSyncSummary {
  receiversPolled: number;
  receiversFailed: number;
  messagesRead: number;
  observations: number;
  testsPromoted: number;
  silverRows: number;
}

interface PendingSeed {
  messageId: string;
  testId: string;
  receiverEmail: string;
}

/**
 * Seeds dispatched inside the read window that we have not yet observed.
 *
 * Only `outcome = 'sent'` rows: a failed dispatch has a synthetic message id
 * that will never appear in a mailbox, so including it would make every run
 * re-scan for something that cannot exist.
 */
async function loadPendingSeeds(): Promise<PendingSeed[]> {
  const result = await db.execute(sql`
    SELECT d.message_id AS "messageId", d.test_id AS "testId", d.receiver_email AS "receiverEmail"
    FROM seed_placement_dispatches d
    LEFT JOIN seed_placement_observations o
      ON o.message_id = d.message_id AND o.receiver_email = d.receiver_email
    WHERE d.outcome = 'sent'
      AND d.dispatched_at >= now() - make_interval(days => ${READ_WINDOW_DAYS})
      AND o.id IS NULL
  `);
  return rowsOf<PendingSeed>(result);
}

async function pollReceiver(
  receiverEmail: string,
  credential: MailboxCredential,
  since: Date,
  wanted: Map<string, PendingSeed>,
  summary: SeedPlacementSyncSummary,
): Promise<void> {
  const client = new ImapFlow({
    host: credential.imapHost,
    port: GMAIL_IMAP_PORT,
    secure: true,
    auth: { user: loginFor(credential), pass: credential.appPassword },
    logger: false,
  });

  await client.connect();

  try {
    // Ask the server which mailboxes it HAS, once, rather than probing each
    // candidate and swallowing the failures. Not every server exposes every
    // name (Gmail has `[Gmail]/Spam`, others `Junk`), but a `.catch(() => null)`
    // per folder would also swallow an auth drop or a network blip — which would
    // silently file every spam-foldered seed as `missing` instead, misreporting
    // a real spam verdict as a vanished message.
    const available = new Set(
      (await client.list()).map((mailbox) => mailbox.path),
    );

    for (const folder of CANDIDATE_FOLDERS) {
      const placement = classifySeedFolder(folder);
      if (!placement) continue;
      if (!available.has(folder)) continue;

      const lock = await client.getMailboxLock(folder);
      try {
        for await (const message of client.fetch({ since }, { source: true })) {
          if (!message.source) continue;

          const parsed: ParsedMail = await simpleParser(message.source);
          const messageId = parsed.messageId;
          if (!messageId) continue;

          summary.messagesRead += 1;

          const seed = wanted.get(messageId);
          // Real mailboxes also receive ordinary mail. A message that is not one
          // of our outstanding seeds is not our business.
          if (!seed) continue;

          const headers = parsed.headers as Map<string, unknown>;
          const authHeader = headers.get("authentication-results");
          const auth = parseAuthResults(
            typeof authHeader === "string" ? authHeader : null,
          );

          const [inserted] = await db
            .insert(seedPlacementObservations)
            .values({
              testId: seed.testId,
              messageId,
              receiverEmail,
              folder,
              placement,
              spfPass: auth.spfPass,
              dkimPass: auth.dkimPass,
              dmarcPass: auth.dmarcPass,
            })
            .onConflictDoNothing({
              target: [
                seedPlacementObservations.receiverEmail,
                seedPlacementObservations.messageId,
              ],
            })
            .returning({ id: seedPlacementObservations.id });

          // Count only what this run actually recorded. No row back means an
          // earlier run already observed this seed (first observation wins), and
          // reporting it again would overstate what the sweep learned.
          if (inserted) summary.observations += 1;

          // Found it — a later folder in this same run must not re-classify it.
          wanted.delete(messageId);
        }
      } finally {
        lock.release();
      }
    }
  } finally {
    await client.logout().catch(() => undefined);
  }
}

/**
 * Promote every seed test touched by this run into silver.
 *
 * Re-aggregates the WHOLE test each time rather than incrementally, so a seed
 * observed on a later run correctly moves out of `missing` — the counts are a
 * function of the bronze pair, never an accumulator.
 */
async function promoteSeedTests(
  testIds: readonly string[],
  summary: SeedPlacementSyncSummary,
): Promise<void> {
  for (const testId of testIds) {
    const dispatchRows = rowsOf<SeedDispatchRecord & { dispatchedAt: string | Date }>(
      await db.execute(sql`
        SELECT message_id AS "messageId", sender_email AS "senderEmail",
               recipient_esp::int AS "recipientEsp", dispatched_at AS "dispatchedAt"
        FROM seed_placement_dispatches
        WHERE test_id = ${testId} AND outcome = 'sent'
      `),
    );
    if (dispatchRows.length === 0) continue;

    const observationRows = rowsOf<SeedObservationRecord>(
      await db.execute(sql`
        SELECT message_id AS "messageId", placement,
               spf_pass AS "spfPass", dkim_pass AS "dkimPass", dmarc_pass AS "dmarcPass"
        FROM seed_placement_observations
        WHERE test_id = ${testId}
      `),
    );

    // The test's timestamp is when its seeds went out, so a re-promote days later
    // does not make an old measurement look fresh to the 16-day staleness gate.
    const testedAt = dispatchRows
      .map((d) => new Date(d.dispatchedAt))
      .reduce((min, d) => (d < min ? d : min));

    const silver = aggregateSeedPlacement(dispatchRows, observationRows, testId, testedAt);

    for (const s of silver) {
      await db
        .insert(instantlyPlacementResults)
        .values(s)
        .onConflictDoUpdate({
          target: [
            instantlyPlacementResults.testId,
            instantlyPlacementResults.accountEmail,
            instantlyPlacementResults.recipientEsp,
          ],
          set: {
            testedAt: s.testedAt,
            seedTotal: s.seedTotal,
            inboxCount: s.inboxCount,
            spamCount: s.spamCount,
            missingCount: s.missingCount,
            inboxPct: s.inboxPct,
            spamPct: s.spamPct,
            missingPct: s.missingPct,
            spfPass: s.spfPass,
            dkimPass: s.dkimPass,
            dmarcPass: s.dmarcPass,
          },
        });
    }

    summary.silverRows += silver.length;
    summary.testsPromoted += 1;
  }
}

export async function syncSeedPlacement(): Promise<SeedPlacementSyncSummary> {
  const summary: SeedPlacementSyncSummary = {
    receiversPolled: 0,
    receiversFailed: 0,
    messagesRead: 0,
    observations: 0,
    testsPromoted: 0,
    silverRows: 0,
  };

  const pending = await loadPendingSeeds();
  const touchedTests = new Set(pending.map((p) => p.testId));

  if (pending.length > 0) {
    const resolveCredential = await loadSeedCredentialResolver(CALLER);
    const since = new Date(Date.now() - READ_WINDOW_DAYS * 24 * 60 * 60 * 1000);

    const byReceiver = new Map<string, Map<string, PendingSeed>>();
    for (const seed of pending) {
      const list = byReceiver.get(seed.receiverEmail) ?? new Map<string, PendingSeed>();
      list.set(seed.messageId, seed);
      byReceiver.set(seed.receiverEmail, list);
    }

    for (const [receiverEmail, wanted] of byReceiver) {
      try {
        const credential = resolveCredential(receiverEmail);
        if (!credential) {
          // A receiver is chosen from the sendable set, so this should not
          // happen — but a credential can be revoked between the send and the
          // read. Loud, and its seeds stay pending rather than being written off.
          throw new Error(`no credential for receiver ${receiverEmail}`);
        }
        await pollReceiver(receiverEmail, credential, since, wanted, summary);
        summary.receiversPolled += 1;
      } catch (error) {
        // Fail loud PER RECEIVER. One unreachable mailbox must not stop the rest,
        // and its seeds simply stay pending for the next run rather than being
        // written off as missing.
        summary.receiversFailed += 1;
        console.error(
          `[seed-placement] receiver ${receiverEmail} failed: ${
            error instanceof Error ? error.message : String(error)
          }`,
        );
      }
    }
  }

  await promoteSeedTests([...touchedTests], summary);

  return summary;
}
