/**
 * Correlating a reply to a sequence INSTANTLY sent.
 *
 * The poller correlates inbound mail by matching `In-Reply-To`/`References`
 * against the `Message-Id` of a send WE made, which lives in
 * `smtp_dispatch_raw`. That row only exists for mail our own dispatcher put on
 * the wire — so on a mailbox whose sequences were sent by Instantly there is
 * nothing to match and every reply classifies `unrelated`, however carefully we
 * read the inbox.
 *
 * That was not a theoretical gap. A mailbox Instantly has DISABLED (its own IMAP
 * link broken) stops emitting `reply_received`, and the rescue that moves such a
 * mailbox onto our own transport does NOT move the sequences already in flight —
 * they stay frozen on `send_transport='instantly'`. So the mailbox is read by
 * nobody: not by Instantly, which cannot log in, and not by us, because the
 * sequence left no dispatch row. Measured 2026-09-02, that was 296 live campaigns
 * across 65 mailboxes.
 *
 * ⚠️ THE KEY IS EXACT, NOT A HEURISTIC. Instantly mirrors every message it sends
 * into `instantly_emails_raw` and the payload carries the RFC 5322 `Message-Id`
 * it actually put on the wire (present on 100% of 112,626 outbound rows measured
 * in prod). So a reply threading onto an Instantly-sent email references an id we
 * hold, exactly as it would for one of our own — same predicate, second source.
 * Do NOT relax this into an address match: the repo's existing rule ("do NOT
 * match on the recipient address, the same prospect can legitimately be in two
 * sequences") stands, and a prospect frequently replies from a DIFFERENT address
 * than the one we mailed, so a sender match is wrong in both directions.
 */

/**
 * Instantly encodes a sequence position as `<variant>_<stepIndex>_<subStep>` —
 * `0_0_0` is the first email, `0_1_0` the first followup. Our own `step` is
 * 1-based everywhere (`sequence_costs.step`, `sequence_steps.step`), so the
 * middle field plus one is the step we record.
 *
 * Verified against prod: the only shapes present are `0_0_0`, `0_1_0`, `0_2_0`
 * and a single `0_3_0`, and for the campaign that motivated this work the two
 * mirrored sends (`0_0_0`, `0_1_0`) line up with silver's `email_sent` steps 1
 * and 2.
 *
 * Returns null on anything it cannot read. A step we cannot resolve must not be
 * guessed at: the inference rule projects predecessors from it, so a wrong step
 * fabricates sends that never happened.
 */
export function parseInstantlySequenceStep(raw: string | null | undefined): number | null {
  if (!raw) return null;

  const parts = raw.split("_");
  if (parts.length !== 3) return null;

  const index = Number(parts[1]);
  if (!Number.isInteger(index) || index < 0) return null;

  return index + 1;
}
