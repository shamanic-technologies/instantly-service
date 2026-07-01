/**
 * Sending-forecast logic (pure — no IO). Powers GET
 * /internal/audit/sending-forecast, the staff-only "Audit → Instantly" ops view
 * that compares the fleet's upcoming scheduled send VOLUME against its available
 * daily CAPACITY.
 *
 * Two independent halves:
 *   1. Capacity — sum of the daily send limit over ONLY healthy senders
 *      (`filterHealthyAccounts`: Instantly-active + warmup ≥ 100 + domain not in
 *      `BLOCKED_DOMAINS`). Reuses the exact live-send gate, not a copy.
 *   2. Future volume — a TRUE per-day projection of every active campaign's
 *      remaining (un-sent) sequence steps onto future business days, using the
 *      fleet's real send schedule (business-hours weekdays only). This is NOT a
 *      "backlog ÷ capacity" approximation: capacity is currently near-zero (the
 *      2026-06-29 Gmail-spam full halt), so a capacity-paced drain would never
 *      terminate. The projection is capacity-INDEPENDENT and bounded by the
 *      sequence structure — that decoupling is the whole point of the ops view.
 */

import type { Account } from "./instantly-client";
import { filterHealthyAccounts, isBlockedDomain } from "./send-lead";

/**
 * Canonical inter-step gap (calendar days) used to project a lead's remaining
 * sequence steps onto future days.
 *
 * The real per-campaign step delays (`daysSinceLastStep`) are caller-driven and
 * NOT persisted locally, so the forecast models every follow-up as landing this
 * many calendar days after its predecessor, then snaps the landing forward off
 * weekends (the fleet sends business-hours weekdays only — see
 * `instantly-client.createCampaign`'s `campaign_schedule`). v1 modeling constant;
 * observed real delays cluster at 3 days (`daysSinceLastStep: 3`). Bump here if
 * the standard cadence changes.
 */
export const STEP_GAP_CALENDAR_DAYS = 3;

export interface CapacitySummary {
  /** Emails/day the healthy fleet can send (Σ daily_limit over healthy accounts). */
  dailyCapacity: number;
  /** Accounts passing `filterHealthyAccounts`. */
  healthyAccountCount: number;
  /** All accounts before any filtering. */
  totalAccountCount: number;
  /** Accounts whose email domain is in `BLOCKED_DOMAINS`. */
  blockedDomainCount: number;
}

/**
 * Fleet capacity summary. `dailyCapacity` sums `daily_limit` over healthy
 * senders only; a healthy account missing `daily_limit` contributes 0 (Instantly
 * always returns it in practice — 0 fails loud-ish as "no capacity", never a
 * fabricated number).
 */
export function computeCapacitySummary(accounts: Account[]): CapacitySummary {
  const healthy = filterHealthyAccounts(accounts);
  const dailyCapacity = healthy.reduce((sum, a) => sum + (a.daily_limit ?? 0), 0);
  const blockedDomainCount = accounts.filter((a) => isBlockedDomain(a.email)).length;
  return {
    dailyCapacity,
    healthyAccountCount: healthy.length,
    totalAccountCount: accounts.length,
    blockedDomainCount,
  };
}

/** One active-campaign lead carrying pending (un-sent) sequence steps. */
export interface PendingLead {
  /** Distinct provisioned (un-sent) step numbers, e.g. `[2, 3]`. Must be non-empty. */
  provisionedSteps: number[];
  /** Highest already-sent (actualized) step, or null when nothing has sent yet. */
  lastSentStep: number | null;
  /** Timestamp of the last sent step, or null when nothing has sent yet. */
  lastSentAt: Date | null;
}

export interface ForecastDay {
  /** Calendar day, `YYYY-MM-DD` (UTC). */
  date: string;
  /** Emails scheduled to send that day across the whole fleet. */
  scheduledCount: number;
}

const MS_PER_DAY = 86_400_000;

function isWeekendUTC(d: Date): boolean {
  const day = d.getUTCDay();
  return day === 0 || day === 6; // 0=Sun, 6=Sat
}

/** Snap forward to the next weekday (no-op when already a weekday). */
function snapToWeekdayUTC(d: Date): Date {
  const out = new Date(d.getTime());
  while (isWeekendUTC(out)) out.setUTCDate(out.getUTCDate() + 1);
  return out;
}

/** Add `n` calendar days, then snap forward off any weekend landing. */
function addDaysSnapWeekday(d: Date, n: number): Date {
  return snapToWeekdayUTC(new Date(d.getTime() + n * MS_PER_DAY));
}

function dateKeyUTC(d: Date): string {
  return d.toISOString().slice(0, 10);
}

/**
 * Project one lead's remaining steps onto calendar dates.
 *
 * - Never-contacted lead (`lastSentStep === null`): the first pending step fires
 *   ~now (asOf, snapped to a weekday); each later step +GAP after the previous.
 * - Contacted lead: the next step fires GAP days after the last sent step (or
 *   after asOf if that slot already elapsed — a past-due follow-up schedules
 *   from today, never in the past).
 */
export function scheduleLead(lead: PendingLead, asOf: Date): Date[] {
  const steps = [...lead.provisionedSteps].sort((a, b) => a - b);
  if (steps.length === 0) return [];

  const contacted = lead.lastSentStep !== null && lead.lastSentAt !== null;
  const anchorStep = contacted ? (lead.lastSentStep as number) : steps[0];
  const rawBase = contacted ? (lead.lastSentAt as Date) : asOf;
  // A follow-up whose nominal slot already passed still schedules from today.
  const baseDate = rawBase.getTime() < asOf.getTime() ? asOf : rawBase;

  return steps.map((s) => {
    const gaps = s - anchorStep; // fresh: >=0 (first step 0); contacted: >=1
    return gaps <= 0
      ? snapToWeekdayUTC(baseDate)
      : addDaysSnapWeekday(baseDate, gaps * STEP_GAP_CALENDAR_DAYS);
  });
}

/**
 * Bucket every pending lead's projected step-sends into calendar days, from
 * today (asOf) forward. Returns chronological `ForecastDay[]`; `[]` when nothing
 * is scheduled. Horizon is bounded by the sequence structure (finite steps ×
 * finite gap) — there is no unbounded tail.
 */
export function projectDailySchedule(leads: PendingLead[], asOf: Date): ForecastDay[] {
  const buckets = new Map<string, number>();
  for (const lead of leads) {
    for (const when of scheduleLead(lead, asOf)) {
      const key = dateKeyUTC(when);
      buckets.set(key, (buckets.get(key) ?? 0) + 1);
    }
  }
  return [...buckets.entries()]
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([date, scheduledCount]) => ({ date, scheduledCount }));
}
