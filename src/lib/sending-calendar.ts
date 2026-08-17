/**
 * The fleet's sending CALENDAR — which weekdays a campaign can actually
 * dispatch on, and therefore which day send SELECTION must measure capacity
 * against.
 *
 * ── Why this exists ───────────────────────────────────────────────────────────
 * Campaigns are created with a Mon-Fri window (`createAndActivateCampaign` sends
 * `days: { "0": false, "1": true … "5": true, "6": false }`), so nothing
 * dispatches on a Saturday or a Sunday. Send selection, however, compares an
 * account's load against its daily cap for the CALENDAR day — so on a weekend it
 * measured a day that can never consume the capacity it was handing out, and
 * granted Saturday's AND Sunday's slots on top of Monday's. All three landed on
 * Monday's single cap, which is one of the ways a head-of-fill-order account
 * ends up carrying more queued work than it can drain.
 *
 * `nextSendingDay` answers "which day will a lead assigned right now actually
 * first send on?" — itself on a weekday, the following Monday on a weekend.
 *
 * ── Scope: SEND SELECTION ONLY. Do NOT use this in the ops projections ────────
 * `aggregateQueueBreakdown` (per-account queue table) and `sending-forecast`
 * (fleet day-by-day chart) deliberately bucket on the RAW nominal UTC day with
 * no weekend snap, because that is what makes the two surfaces agree with each
 * other step-for-step (see CLAUDE.md: "Do NOT reintroduce a weekend snap").
 * Reintroducing a snap there would make a weekend day report 0 scheduled steps
 * while the account table still counted them as due — the exact incoherence that
 * was removed. This module is consumed only by the capacity snapshot and the
 * account picker, which answer a different question: not "when is this step
 * nominally due" but "can this mailbox absorb one more lead".
 */

/** Days a campaign is allowed to dispatch on, as JS `getUTCDay()` values (0=Sun). */
export const SENDING_WEEKDAYS: readonly number[] = [1, 2, 3, 4, 5];

/** True when `d` falls on a day the fleet's campaigns can dispatch (Mon-Fri, UTC). */
export function isSendingDay(d: Date): boolean {
  return SENDING_WEEKDAYS.includes(d.getUTCDay());
}

/**
 * The day a lead assigned at `asOf` will first be able to send.
 *
 * On a sending day this is `asOf` itself, returned UNCHANGED — so weekday
 * behaviour is byte-identical to before this module existed. On a Saturday or a
 * Sunday it is UTC midnight of the following Monday, which is both the correct
 * bucketing key (`dateKeyUTC`) for "what will be due then" and the correct clock
 * for the age ramp (a mailbox is two days older by Monday).
 *
 * Idempotent: snapping an already-snapped instant returns it unchanged.
 */
export function nextSendingDay(asOf: Date): Date {
  if (isSendingDay(asOf)) return asOf;
  const d = new Date(
    Date.UTC(asOf.getUTCFullYear(), asOf.getUTCMonth(), asOf.getUTCDate()),
  );
  // At most two hops (Sat -> Sun -> Mon); the loop keeps it honest if the
  // sending week ever changes shape.
  while (!isSendingDay(d)) d.setUTCDate(d.getUTCDate() + 1);
  return d;
}
