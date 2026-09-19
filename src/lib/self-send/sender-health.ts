/**
 * When to stop trying a mailbox the relay keeps refusing.
 *
 * ⚠️ WHY THIS EXISTS AT ALL. A permanent SENDER-side refusal correctly leaves
 * the hold `provisioned` — the prospect's address is fine and untested, so
 * marking them bounced would poison a reachable lead with a fact about our own
 * mailbox. The consequence is that the step is re-selected on the NEXT run, and
 * the one after, forever: nothing in the ledger ever records that this mailbox
 * cannot send. Prod 2026-09-16 → 09-18, two Mailforge mailboxes deprovisioned
 * at the vendor: 4,210 attempts against 56 steps, every one `535 5.7.8 Error:
 * authentication failed`, at ~13 seconds per attempt. Entire hours of the
 * dispatcher (16:00, 20:00 UTC on 09-18) produced 240 failures and ZERO emails,
 * because the failing steps are the most overdue and therefore sort FIRST.
 *
 * The rule is the warmup mesh's, `selectSilencedSenders`, with one difference
 * forced by this table: warmup only ever sends to our own mailboxes, so every
 * permanent failure there IS about the sender. Here `outcome = 'permanent'`
 * also covers a dead PROSPECT address, and silencing a working mailbox because
 * three prospects are unreachable is exactly the misfiling a prod audit already
 * found in the other direction (9 of 12 deactivated accounts were refused over
 * `Recipient address rejected: Domain not found`). So the failures are re-read
 * through `classifyPermanentFailure` and only the `sender` ones count.
 *
 * Self-healing with no state to clear: the window rolls, so a silenced mailbox
 * is retried automatically and re-silenced the same day if the relay still
 * refuses. Requiring ZERO successes is what keeps a mailbox that mostly works
 * from being silenced by a bad afternoon.
 */

import { classifyPermanentFailure } from "./dispatch";

/** Consecutive-ish sender-side refusals, with no success, before we stop. */
export const SMTP_SILENCE_MIN_FAILURES = 3;

/** One distinct refusal this mailbox received, and how often. */
export interface SmtpFailureRow {
  response: string;
  responseCode: number | null;
  count: number;
}

/** One real mailbox's recent dispatch record. */
export interface SmtpSenderHealth {
  /** The SASL login, not the sending address — aliases share one relay quota. */
  mailbox: string;
  sent: number;
  failures: readonly SmtpFailureRow[];
}

export function selectSilencedSmtpSenders(
  health: readonly SmtpSenderHealth[],
  minFailures: number = SMTP_SILENCE_MIN_FAILURES,
): Set<string> {
  const silenced = new Set<string>();

  for (const row of health) {
    if (row.sent > 0) continue;

    let senderSide = 0;
    for (const failure of row.failures) {
      if (classifyPermanentFailure(failure.response, failure.responseCode) === "sender") {
        senderSide += failure.count;
      }
    }

    if (senderSide >= minFailures) silenced.add(row.mailbox);
  }

  return silenced;
}
