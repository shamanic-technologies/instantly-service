/**
 * Which pipe a NEW sequence goes out on — one fact, derived, not stored.
 *
 * We are cancelling the Instantly Email Outreach subscription, so the question
 * "Instantly or us?" stopped being an experiment and became a consequence of a
 * single capability: do we hold a credential for this mailbox?
 *
 *   credential  → smtp      (ours — the mailbox estate we own)
 *   none        → instantly (the Instantly DFY pool, whose Workspace is theirs)
 *
 * ⚠️ THIS REPLACED A PER-SEQUENCE A/B SPLIT, AND THE REPLACEMENT IS THE POINT.
 * That split assigned `smtp` to a campaign while leaving the ACCOUNT's policy
 * column on `instantly` — deliberately, so both arms rode the same mailboxes and
 * mailbox quality could not confound the comparison. But the dispatch worker
 * loaded capacity only for accounts whose POLICY said `smtp`, so every sequence
 * the experiment assigned to a mailbox that was not explicitly flipped could
 * never be selected. Measured in prod 2026-09-06: 1,486 sequences frozen, 1,176
 * of them prospects who had never received their first email, some waiting a
 * week — and silently, because the worker reported `due: 0` with zero errors.
 *
 * Two rules that would each have prevented it, and which this module keeps:
 *
 *   - ONE fact decides the transport, read the same way by whoever asks. A
 *     decision split across a campaign column and an account column is two
 *     answers to one question, and they drifted.
 *   - Capability is POSITIVE evidence (we hold a password), never a stored
 *     opinion. `instantly_accounts.send_transport` no longer takes part in the
 *     decision; it survives only as the frozen record on the campaign row.
 *
 * Fail-safe direction: a mailbox we cannot authenticate stays on Instantly,
 * because self-sending from it is not merely undesirable, it is impossible.
 */

import { getOrSetCachedStats } from "../stats-cache";
import { loadCredentialedMailboxes } from "./mailbox-credentials";
import type { CallerInfo } from "../key-client";
import {
  SEND_TRANSPORT_INSTANTLY,
  SEND_TRANSPORT_SMTP,
  type SendTransport,
} from "./transport";

/** Cache key for the credentialed-mailbox set, shared with the capacity snapshot's TTL. */
export const CREDENTIALED_MAILBOXES_CACHE_KEY = "self-send-credentialed-mailboxes";

/**
 * Is this mailbox usable by the self-send pipe at all?
 *
 * Cached for the same 60s as the send-selection capacity snapshot, and for the
 * same reason: it is two network reads (key-service plus a vendor pagination)
 * that would otherwise run on every single send.
 *
 * ⚠️ `loadCredentialedMailboxes` fails LOUD on anything but a key-service 404,
 * and that is what makes the derivation trustworthy. A silent empty set would
 * read as "no mailbox is ours" and route the entire fleet back to a vendor we
 * are about to cancel.
 */
export async function isSelfSendCapable(
  accountEmail: string,
  caller: CallerInfo,
): Promise<boolean> {
  const addresses = await getOrSetCachedStats(CREDENTIALED_MAILBOXES_CACHE_KEY, () =>
    loadCredentialedMailboxes(caller),
  );
  return addresses.has(accountEmail.trim().toLowerCase());
}

/**
 * The transport a NEW sequence should use on this mailbox.
 *
 * Note what is absent: the account's stored `send_transport`. It used to take
 * precedence here as a manual pin, but its column default is `'instantly'`, so
 * "explicitly pinned to Instantly" and "never touched" are the same value and
 * the pin could never be read. A lever that cannot be distinguished from the
 * default is not a lever.
 *
 * The rollback is therefore the one that always worked: `SELF_SEND_DISPATCH_ENABLED`
 * on the service, which stops the whole sweep in one env change without touching
 * an account row.
 */
export async function resolveTransportForNewSequence(
  account: { email: string },
  caller: CallerInfo,
): Promise<SendTransport> {
  return (await isSelfSendCapable(account.email, caller))
    ? SEND_TRANSPORT_SMTP
    : SEND_TRANSPORT_INSTANTLY;
}
