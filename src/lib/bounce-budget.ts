/**
 * A sending DOMAIN's bounce budget: may the first email to THIS address leave
 * from a mailbox on THIS domain right now? (CATCHALL-BOUNCE-GUARD-1015)
 *
 * ⚠️ WHY. Until 2026-10-13 we only emailed verifier-valid addresses; on
 * 2026-10-14 campaigns were allowed to email catch-all addresses. That day the
 * fleet bounced 7.2 % (37 / 516) against 0.3-1 % every day before, and the
 * bounces were not spread: boostdistribute.com took 21 in 1h30,
 * growdistribute.com 7, agileconsultco.com 4 (raquel@: 4 of 21 sends),
 * fuseconnectio.com 4. The same evening ruth@agileconsultco.com, who had sent
 * only valid addresses and bounced NOTHING herself, landed in Junk at Outlook
 * and Spam at Titan: the damage is the DOMAIN's, not the mailbox's.
 *
 * The thresholds are the deliverability market's consensus, not a fit to our
 * own (thin) history (owner 2026-10-15: "we don't have enough data to know"):
 *
 *   - 2 % is the line. Smartlead ("a good cold email bounce rate is below 2 %
 *     on a rolling 14-day window", for a mailbox with 100+ sends in it; 2-4 % a
 *     warning zone where providers score the sender down, > 4 % reputation damage
 *     compounds), BounceZero and Suped (2 % is where Gmail and Outlook start to
 *     correlate bounces with low-quality sending; crossing it degrades the WHOLE
 *     sending domain's reputation).
 *   - 5 % is a stop. Smartlead's "High Bounce Rate Auto Protection" pauses at a
 *     user threshold, its example 5 %; > 4-6 % = weeks before placement drops.
 *   - Catch-alls are mixed into clean volume, never sent in bulk: MillionVerifier
 *     ("a strict 10 % maximum of the total send, mixed with clearly valid
 *     emails"), Hunter / ListKit (separate segment, small share, measured).
 *   - Reputation is the DOMAIN's: Google Postmaster Tools and Microsoft SNDS
 *     report per domain/IP, and ruth@agileconsultco.com (zero bounces of her
 *     own) landed in Junk at Outlook on 2026-10-14 beside a bouncing sibling.
 *
 * Two facts of ours shape the MECHANISM, not the numbers: 70 % of our bounces
 * land within one minute of the send (Google accepts, a DSN comes back), and
 * every 2026-10-14 burst happened inside ONE dispatch tick (dozens of first
 * emails per mailbox in two minutes). So the budget is spent at SELECTION time,
 * email by email, with a 24 h window beside the experts' 14 days.
 *
 * The rule, per sending DOMAIN (every alias and mailbox of it summed):
 *
 *   1. RISK BUDGET for an address that is not verifier-valid (catch_all,
 *      unknown, no verdict): for each window w in {24 h, 14 d},
 *        bounces_w + pending + p(verdict) <= 2 % x (sends_w + 1)
 *      i.e. "after this email, the EXPECTED bounce rate stays under 2 %".
 *      p = the verdict's bounce rate at send (`GET /internal/delivery-by-verdict`
 *      2026-10-15: valid 2.1 %, catch_all 10.7 %, unknown 71 %). `pending` = the
 *      expected bounces of risky emails already planned in this selection (their
 *      DSN is not back yet). A clean domain thus takes about one catch-all per
 *      8-10 clean sends (the experts' ~10 % mix) and a domain that just bounced
 *      takes none until clean volume dilutes it or the window rolls. The
 *      sample-size problem solves itself: on 3 sends the budget is 0.08 of a
 *      bounce, so a risky email waits for volume, while a valid one is never
 *      frozen by it.
 *   2. HARD CEILING for EVERY first email (valid included): a domain at >= 5 %
 *      bounces over 14 days with >= 100 sends in them takes no new first email
 *      at all. Follow-ups to addresses that already accepted our email keep going
 *      (they do not bounce, and they finish threads people read).
 *
 * Replay of 2026-10-14 against the rule (`bounce-budget-replay.test.ts`, the
 * four domains): catch-all/unknown/no-verdict first-email bounces 35 -> 3, every valid
 * email still sent. Fleet-wide over 2026-10-01..13 (valid-only era) the hard
 * ceiling would have held 125 of 11,274 emails (1.1 %), all on domains already
 * past 5 %.
 *
 * Nothing here drops a lead: a refused first email stays queued and is either
 * moved to a mailbox on a domain with budget (the dispatcher's rehome) or sent
 * from its own once the domain is back under budget.
 */

/** Expected bounce rate of one email by the verifier verdict held at send. */
export const EXPECTED_BOUNCE_RATE_BY_VERDICT: Readonly<Record<string, number>> = {
  valid: 0.021,
  catch_all: 0.107,
  unknown: 0.714,
};

/** No usable verdict: priced like the worst sendable verdict, never like valid. */
export const EXPECTED_BOUNCE_RATE_NO_VERDICT = EXPECTED_BOUNCE_RATE_BY_VERDICT.unknown!;

/** Expected bounce rate a domain may run at after a risky email, per window. */
export const BOUNCE_BUDGET_MAX_RATE = 0.02;

/** The windows the risk budget must hold on, in hours. */
export const BOUNCE_BUDGET_WINDOWS_HOURS = [24, 14 * 24] as const;

/** Hard ceiling: no first email of any verdict from a domain past it. */
export const HARD_CEILING_WINDOW_HOURS = 14 * 24;
export const HARD_CEILING_RATE = 0.05;
/** Below this many sends in the window a rate is not a judgment (Smartlead: 100+). */
export const HARD_CEILING_MIN_SENDS = 100;

/** Sends and bounces of one domain in one window. */
export interface WindowCounts {
  sends: number;
  bounces: number;
}

/** One domain's counts, keyed by window length in hours. */
export type DomainBounceHistory = ReadonlyMap<number, WindowCounts>;

/** Every sending domain's history (lowercased domain -> windows). */
export type FleetBounceHistory = ReadonlyMap<string, DomainBounceHistory>;

/** The sending domain of an address, lowercased. */
export function senderDomain(accountEmail: string): string {
  const at = accountEmail.lastIndexOf("@");
  return (at >= 0 ? accountEmail.slice(at + 1) : accountEmail).trim().toLowerCase();
}

/** Whether an address's verdict needs the risk budget (anything but valid). */
export function isRiskyVerdict(verdict: string | null): boolean {
  return verdict !== "valid";
}

export function expectedBounceRate(verdict: string | null): number {
  if (verdict === null) return EXPECTED_BOUNCE_RATE_NO_VERDICT;
  return EXPECTED_BOUNCE_RATE_BY_VERDICT[verdict] ?? EXPECTED_BOUNCE_RATE_NO_VERDICT;
}

export type BounceBudgetRefusal = "hard_ceiling" | "risk_budget";

/**
 * Pure verdict for one first email on one domain. `pending` = expected bounces
 * of risky emails already planned on this domain and not yet in `history`;
 * `plannedSends` = emails already planned on it (they count as sends).
 */
export function bounceBudgetRefusal(
  history: DomainBounceHistory | undefined,
  verdict: string | null,
  planned: { sends: number; pending: number } = { sends: 0, pending: 0 },
): BounceBudgetRefusal | null {
  const counts = (hours: number): WindowCounts => history?.get(hours) ?? { sends: 0, bounces: 0 };

  const hard = counts(HARD_CEILING_WINDOW_HOURS);
  const hardSends = hard.sends + planned.sends;
  if (hardSends >= HARD_CEILING_MIN_SENDS && hard.bounces / hardSends >= HARD_CEILING_RATE) {
    return "hard_ceiling";
  }

  if (!isRiskyVerdict(verdict)) return null;
  const p = expectedBounceRate(verdict);
  for (const hours of BOUNCE_BUDGET_WINDOWS_HOURS) {
    const w = counts(hours);
    const sends = w.sends + planned.sends;
    if (w.bounces + planned.pending + p > BOUNCE_BUDGET_MAX_RATE * (sends + 1)) {
      return "risk_budget";
    }
  }
  return null;
}

/**
 * The budget as a selection spends it: one instance per selection, so a tick
 * that plans sixty first emails on one domain sees each one it already planned.
 */
export class BounceBudget {
  private readonly planned = new Map<string, { sends: number; pending: number }>();

  constructor(private readonly history: FleetBounceHistory) {}

  /** Why a first email to an address with `verdict` may not leave from `accountEmail`. */
  refusal(accountEmail: string, verdict: string | null): BounceBudgetRefusal | null {
    const domain = senderDomain(accountEmail);
    return bounceBudgetRefusal(this.history.get(domain), verdict, this.planned.get(domain));
  }

  /** Book a planned email on its domain (any step: every send dilutes the rate). */
  spend(accountEmail: string, verdict: string | null, firstEmail: boolean): void {
    const domain = senderDomain(accountEmail);
    const entry = this.planned.get(domain) ?? { sends: 0, pending: 0 };
    entry.sends += 1;
    if (firstEmail && isRiskyVerdict(verdict)) entry.pending += expectedBounceRate(verdict);
    this.planned.set(domain, entry);
  }
}
