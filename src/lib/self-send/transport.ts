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

/**
 * Prefix of the `instantly_campaigns.instantly_campaign_id` value used when a
 * sequence is dispatched by US: `self:<uuid>`.
 *
 * There IS no Instantly campaign on this transport, but the column is
 * `notNull().unique()` and is the join key for `sequence_costs`, the silver event
 * log, the bronze mirrors and the IMAP correlation. So the id stays — it just
 * stops being an Instantly id and becomes a purely local one.
 *
 * ⚠️ THE SAME RULE AS THE `reserving:` SENTINEL APPLIES, and for the same reason:
 * ANY sweep that selects `instantly_campaigns` and then calls the Instantly API
 * per row MUST exclude these, or it will ask Instantly about a campaign that was
 * never there. `GET /campaigns/self:<uuid>` 400s on the uuid format, exactly as
 * it does for a reservation sentinel. Every such caller is listed in CLAUDE.md.
 */
export const SELF_SEND_CAMPAIGN_PREFIX = "self:";

/** SQL-side form of the same exclusion, for a `NOT LIKE` predicate. */
export const SELF_SEND_CAMPAIGN_LIKE = "self:%";

/** True when this id names one of OUR sequences rather than an Instantly campaign. */
export function isSelfSendCampaignId(instantlyCampaignId: string): boolean {
  return instantlyCampaignId.startsWith(SELF_SEND_CAMPAIGN_PREFIX);
}

/** Mint a fresh local id for a sequence we will dispatch ourselves. */
export function mintSelfSendCampaignId(): string {
  return `${SELF_SEND_CAMPAIGN_PREFIX}${crypto.randomUUID()}`;
}
