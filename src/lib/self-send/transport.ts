/**
 * Send transport — which pipe actually dispatches a sequence.
 *
 * Two columns carry this, and they are NOT redundant:
 *
 *   - `instantly_accounts.send_transport` is the POLICY: "new sends assigned to
 *     this mailbox should go out over X". Flipping it is how an operator moves a
 *     mailbox onto the self-hosted sender, and moving it back is the rollback.
 *
 *   - `instantly_campaigns.send_transport` is the DECISION, frozen at send time
 *     from the chosen account's policy. It is what every later step of that
 *     lead's sequence reads.
 *
 * Freezing it on the campaign row is load-bearing, not defensive: a sequence
 * spans days, so reading the account policy live would re-route a lead's
 * followups mid-flight the moment an operator flips the mailbox — and for a
 * lead already pushed to Instantly we hold no local step bodies to send from,
 * so those followups would simply stop. Same persist-at-write reasoning as
 * `instantly_campaigns.account_email` (migration 0025).
 */

export const SEND_TRANSPORT_INSTANTLY = "instantly";
export const SEND_TRANSPORT_SMTP = "smtp";

export type SendTransport =
  | typeof SEND_TRANSPORT_INSTANTLY
  | typeof SEND_TRANSPORT_SMTP;

/**
 * Resolve a stored transport value to the pipe a send must actually use.
 *
 * Anything that is not exactly `"smtp"` resolves to Instantly. This is a
 * deliberate asymmetry rather than a silent fallback: the column defaults to
 * `'instantly'`, so the only way to reach the self-send path is an explicit,
 * reversible `UPDATE`. An unrecognised value must never divert live customer
 * traffic onto the newer pipe — the safe direction is the one already proven in
 * production.
 */
export function resolveTransportForSend(
  value: string | null | undefined,
): SendTransport {
  return value === SEND_TRANSPORT_SMTP ? SEND_TRANSPORT_SMTP : SEND_TRANSPORT_INSTANTLY;
}
