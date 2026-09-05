/**
 * The agency inbox — the one address a human reads.
 *
 * It surfaces in three different ways and they must all name the SAME mailbox:
 * a positive reply is forwarded to it, a campaign error is notified to it, and
 * every one-to-one reply we send into a prospect's thread carries it in CC. The
 * address had been written out at each of those sites, so a change of inbox
 * meant finding every copy — this module is the one home.
 *
 * Read at USE, never captured at module load, so an env change on the box takes
 * effect on the next call rather than on the next boot, and so a test can set it
 * without re-importing anything.
 */

/** Fallback when `ADMIN_NOTIFICATION_EMAIL` is unset. */
export const DEFAULT_AGENCY_INBOX = "kevin@distribute.you";

/** The mailbox a human reads. */
export function agencyInbox(): string {
  const configured = process.env.ADMIN_NOTIFICATION_EMAIL?.trim();
  return configured ? configured : DEFAULT_AGENCY_INBOX;
}
