/**
 * Dispatch one in-house seed placement test (IO glue around the pure planning).
 *
 * Sends one seed from every testable mailbox to every receiver, and records each
 * attempt in bronze. Reading the results is a SEPARATE run (`sync.ts`) on its own
 * schedule — chaining the read onto the send in one job is the trap that made
 * the Instantly placement cron ingest week-old results for months: the poll runs
 * seconds after the create and finds nothing, so result latency silently becomes
 * the job interval rather than the real delivery time.
 *
 * Fail loud PER SEED. One dead mailbox must not cost the whole fleet its weekly
 * measurement, and a mailbox whose seeds all failed to send simply has no
 * denominator this week — which reads as "untested", the honest answer, rather
 * than as a bad score.
 */

import { db } from "../../db";
import { seedPlacementDispatches } from "../../db/schema";
import type { CallerInfo } from "../key-client";
import { fetchTestablePoolEmails } from "../account-lifecycle-sync";
import { dispatchMessage, SmtpDispatchError } from "../self-send/smtp";
import { buildSeedMessage } from "./message";
import { loadSeedCredentialResolver } from "./credentials";
import {
  MAX_SEED_RECEIVERS,
  mintSeedTestId,
  planSeedSends,
  selectSeedReceivers,
} from "./seeds";

const CALLER: CallerInfo = {
  method: "POST",
  path: "/internal/audit/seed-placement/run",
};

/**
 * Kill-switch. Default OFF.
 *
 * Seed rows land in the SAME silver the lifecycle delivery gate reads, so an
 * unproven harness could demote the whole sending fleet. Arming is an env change
 * on the box, reversible without a deploy.
 */
export function isSeedPlacementEnabled(): boolean {
  return process.env.SEED_PLACEMENT_ENABLED === "true";
}

export interface SeedPlacementRunSummary {
  testId: string;
  senders: number;
  receivers: number;
  planned: number;
  sent: number;
  failed: number;
  /** Mailboxes we hold no credential for — expected for the legacy fleet, not an error. */
  skippedNoCredential: number;
}

export async function runSeedPlacementTest(
  options: { limit?: number } = {},
): Promise<SeedPlacementRunSummary> {
  const testId = mintSeedTestId();
  const pool = await fetchTestablePoolEmails();

  const summary: SeedPlacementRunSummary = {
    testId,
    senders: 0,
    receivers: 0,
    planned: 0,
    sent: 0,
    failed: 0,
    skippedNoCredential: 0,
  };

  if (pool.length === 0) {
    // An empty pool produces an empty test, not a fabricated one. No bronze rows
    // means no silver rows means no account's delivery changes.
    console.log("[seed-placement] testable pool is empty — nothing to seed");
    return summary;
  }

  const resolveCredential = await loadSeedCredentialResolver(CALLER);

  // ⚠️ NARROW THE POOL TO WHAT WE CAN ACTUALLY AUTHENTICATE, BEFORE PLANNING.
  // The testable pool is the whole fleet (~200 accounts), but credentials exist
  // only for Primeforge plus the manual key — the legacy Gandi/Mailforge
  // accounts have no retrievable app password. Planning against the full pool
  // and discovering that per-seed would attempt ~10 doomed sends per dead
  // mailbox and write ~1,900 failure rows that mean nothing.
  const credentials = new Map<string, ReturnType<typeof resolveCredential>>();
  const sendable: string[] = [];
  for (const email of pool) {
    const credential = resolveCredential(email);
    if (!credential) {
      summary.skippedNoCredential += 1;
      continue;
    }
    credentials.set(email, credential);
    sendable.push(email);
  }

  // Receivers are drawn from the SENDABLE set, because a receiver has to be read
  // over IMAP with the same credential — a mailbox we cannot authenticate is no
  // more readable than it is sendable. The credential also carries the real
  // HOST, which is what decides whether the mailbox can grade at all (only a
  // Google/Microsoft-hosted one can; see `selectSeedReceivers`).
  const receivers = selectSeedReceivers(
    sendable.map((email) => ({
      email,
      imapHost: credentials.get(email)?.imapHost ?? "",
    })),
    MAX_SEED_RECEIVERS,
  );

  if (receivers.length === 0) {
    // No gradeable receiver means no measurement is possible. Say so and stop,
    // rather than sending mail nobody can score.
    console.warn(
      `[seed-placement] no receiver on a consumer ESP among ${sendable.length} sendable mailboxes — nothing to measure`,
    );
    return summary;
  }
  const planned = planSeedSends(sendable, receivers);
  const work = options.limit ? planned.slice(0, options.limit) : planned;

  summary.senders = new Set(work.map((w) => w.senderEmail)).size;
  summary.receivers = receivers.length;
  summary.planned = work.length;

  if (work.length === 0) {
    console.log(
      `[seed-placement] nothing to seed (pool=${pool.length}, sendable=${sendable.length})`,
    );
    return summary;
  }

  for (const send of work) {
    try {
      const credential = credentials.get(send.senderEmail);
      if (!credential) continue;

      const message = buildSeedMessage({
        testId,
        senderEmail: send.senderEmail,
        receiverEmail: send.receiverEmail,
      });

      const result = await dispatchMessage(credential, message);

      await db.insert(seedPlacementDispatches).values({
        testId,
        senderEmail: send.senderEmail,
        receiverEmail: send.receiverEmail,
        recipientEsp: send.recipientEsp,
        messageId: result.messageId,
        outcome: "sent",
        response: result.response,
      });

      summary.sent += 1;
    } catch (error) {
      summary.failed += 1;

      // Recorded, but NOT counted toward the denominator — the aggregation reads
      // `outcome = 'sent'` only. A seed that never left the building tells us
      // nothing about where mail from this mailbox lands, and scoring it as a
      // miss would blame placement for an SMTP or credential problem.
      const kind = error instanceof SmtpDispatchError ? error.kind : "error";
      const response =
        error instanceof SmtpDispatchError
          ? error.response
          : error instanceof Error
            ? error.message
            : String(error);

      await db.insert(seedPlacementDispatches).values({
        testId,
        senderEmail: send.senderEmail,
        receiverEmail: send.receiverEmail,
        recipientEsp: send.recipientEsp,
        // No Message-Id exists for a send that failed, and the column is unique,
        // so a synthetic one keeps the audit row without colliding.
        messageId: `failed:${crypto.randomUUID()}`,
        outcome: kind,
        response,
      });

      console.warn(
        `[seed-placement] seed ${send.senderEmail} -> ${send.receiverEmail} failed (${kind}): ${response}`,
      );
    }
  }

  return summary;
}
