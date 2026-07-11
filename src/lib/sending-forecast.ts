/**
 * Sending-forecast logic (pure — no IO). Powers GET
 * /internal/audit/sending-forecast, the staff-only "Audit → Instantly" ops view
 * that compares the fleet's upcoming scheduled send VOLUME against its available
 * daily CAPACITY.
 *
 * Two independent halves:
 *   1. Capacity — sum of the daily send limit over ONLY `in_production` senders
 *      (the exact live-send gate: silver lifecycle_status == 'in_production').
 *      Reuses the lifecycle projection, not a copy.
 *   2. Future volume — a TRUE per-day projection of every active campaign's
 *      remaining (un-sent) sequence steps onto future business days, using the
 *      fleet's real send schedule (business-hours weekdays only). This is NOT a
 *      "backlog ÷ capacity" approximation: capacity is currently near-zero (the
 *      2026-06-29 Gmail-spam full halt), so a capacity-paced drain would never
 *      terminate. The projection is capacity-INDEPENDENT and bounded by the
 *      sequence structure — that decoupling is the whole point of the ops view.
 *
 * ── Cadence source = the SAME real per-step delays the per-account queue
 *    breakdown uses ─────────────────────────────────────────────────────────
 * The inter-step gap is the REAL configured `delay` (calendar days) of each
 * sequence step, read from the campaign's LATEST bronze config
 * (`instantly_campaigns_config_raw.payload->'sequences'->0->'steps'`) and passed
 * in per lead as `PendingLead.stepDelays`. This is IDENTICAL to the cadence the
 * per-account queue-bucket projection (`queue-breakdown.ts`) uses, so the two ops
 * views agree on WHEN the same future steps land. `STEP_GAP_CALENDAR_DAYS` is now
 * only the per-gap FALLBACK for a sequence whose bronze config is missing that
 * step's delay (in practice bronze covers the whole active queued set).
 *
 * Same honesty caveat as the queue breakdown: the projected date is a
 * NOMINAL-CADENCE LOWER BOUND (the earliest a step is eligible per its sequence
 * cadence), NOT Instantly's exact dispatch date — the real send slips LATER under
 * daily-limit saturation / throttling / pauses. Additionally, this forecast
 * (unlike the breakdown, which buckets on the raw nominal UTC day) snaps each
 * landing forward off weekends, modeling the fleet's business-hours-weekday send
 * schedule; that weekday snap is a deliberate presentation choice of THIS view,
 * orthogonal to the now-shared cadence source.
 */

import type { Account } from "./instantly-client";
import type { LifecycleView } from "./account-lifecycle-sync";

/**
 * Per-gap FALLBACK inter-step gap (calendar days), used ONLY when a sequence's
 * bronze config does not carry a real `delay` for a given step.
 *
 * The forecast's primary cadence source is the REAL per-step `delay` from bronze
 * config (`PendingLead.stepDelays`), matching the per-account queue-breakdown
 * projection. This constant is the last-resort per-gap default so a step is never
 * dropped when its config delay is absent; it is also the value the queue
 * breakdown falls back to (`queue-breakdown.ts` imports it), so both views share
 * ONE fallback too. Observed real delays cluster at 3 days. Bump here only to
 * change the missing-config default — the standard cadence now comes from config.
 */
export const STEP_GAP_CALENDAR_DAYS = 3;

/**
 * Resolve the real gap (calendar days) from cost-step `fromStep` to `fromStep+1`.
 *
 * Cost steps are 1-based; the bronze config `steps` array (surfaced here as
 * `stepDelays`) is 0-based, and the gap from cost-step `k` to `k+1` is
 * `steps[k-1].delay` (verified empirically — same indexing the queue breakdown
 * uses). A missing / null / negative / non-finite delay falls back to
 * `STEP_GAP_CALENDAR_DAYS` so no step is ever dropped from the projection.
 */
export function delayForGap(fromStep: number, stepDelays: readonly (number | null)[]): number {
  const d = stepDelays[fromStep - 1];
  return d === null || d === undefined || !Number.isFinite(d) || d < 0
    ? STEP_GAP_CALENDAR_DAYS
    : d;
}

export interface CapacitySummary {
  /** Emails/day the fleet can send (Σ daily_limit over in_production accounts). */
  dailyCapacity: number;
  /** Accounts currently in_production (send-eligible). */
  healthyAccountCount: number;
  /** All accounts before any filtering. */
  totalAccountCount: number;
  /** Accounts blocked by domain policy (lifecycle == deactivated_by_user). */
  blockedDomainCount: number;
}

/**
 * Fleet capacity summary. `dailyCapacity` sums `daily_limit` over `in_production`
 * senders only (the live-send gate); a production account missing `daily_limit`
 * contributes 0 (Instantly always returns it in practice — 0 fails loud-ish as
 * "no capacity", never a fabricated number). Lifecycle is read from silver and
 * passed in as `lifecycleByEmail`; an account absent from the map (never
 * classified) is NOT in_production and contributes no capacity.
 */
export function computeCapacitySummary(
  accounts: Account[],
  lifecycleByEmail: Map<string, LifecycleView>,
): CapacitySummary {
  const statusOf = (email: string) => lifecycleByEmail.get(email)?.status ?? null;
  const production = accounts.filter((a) => statusOf(a.email) === "in_production");
  const dailyCapacity = production.reduce((sum, a) => sum + (a.daily_limit ?? 0), 0);
  const blockedDomainCount = accounts.filter(
    (a) => statusOf(a.email) === "deactivated_by_user",
  ).length;
  return {
    dailyCapacity,
    healthyAccountCount: production.length,
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
  /**
   * Real per-step delays (calendar days) from the campaign's LATEST bronze
   * sequence config, config-ordered 0-based: `stepDelays[i]` = the gap from
   * cost-step `i+1` to cost-step `i+2` (i.e. `steps[i].delay`). A null / missing
   * entry falls back per-gap to `STEP_GAP_CALENDAR_DAYS`. Optional / empty ⇒ the
   * whole sequence uses the fallback (config unavailable). Same source + indexing
   * as the per-account queue breakdown, so the two views share one cadence model.
   */
  stepDelays?: (number | null)[];
}

export interface ForecastDay {
  /** Calendar day, `YYYY-MM-DD` (UTC). */
  date: string;
  /** Emails scheduled to send that day across the whole fleet. */
  scheduledCount: number;
}

export const MS_PER_DAY = 86_400_000;

/** `YYYY-MM-DD` (UTC) key for a Date. Shared with the queue-breakdown projection. */
export function dateKeyUTC(d: Date): string {
  return d.toISOString().slice(0, 10);
}

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

/**
 * Project one lead's remaining steps onto calendar dates, using the REAL
 * configured per-step delays (`lead.stepDelays`) as the inter-step cadence.
 *
 * - Never-contacted lead (`lastSentStep === null`): the first pending step fires
 *   ~now (asOf, snapped to a weekday); each later step lands after accumulating
 *   the real configured delays of the gaps between it and the first step.
 * - Contacted lead: each un-sent step lands after accumulating the real delays of
 *   the gaps from the last sent step (base = last-sent timestamp, or asOf if that
 *   slot already elapsed — a past-due follow-up schedules from today, never in
 *   the past).
 *
 * The accumulated gap from cost-step `anchor` to cost-step `s` is
 * `Σ delayForGap(j)` for `j` in `[anchor, s-1]`, each resolved from config with a
 * per-gap `STEP_GAP_CALENDAR_DAYS` fallback. Each landing is snapped forward off
 * weekends (this view's business-hours-weekday model).
 */
export function scheduleLead(lead: PendingLead, asOf: Date): Date[] {
  const steps = [...lead.provisionedSteps].sort((a, b) => a - b);
  if (steps.length === 0) return [];

  const stepDelays = lead.stepDelays ?? [];
  const contacted = lead.lastSentStep !== null && lead.lastSentAt !== null;
  const anchorStep = contacted ? (lead.lastSentStep as number) : steps[0];
  const rawBase = contacted ? (lead.lastSentAt as Date) : asOf;
  // A follow-up whose nominal slot already passed still schedules from today.
  const baseDate = rawBase.getTime() < asOf.getTime() ? asOf : rawBase;

  return steps.map((s) => {
    if (s <= anchorStep) return snapToWeekdayUTC(baseDate); // fresh first step / anchor
    let cumulativeDays = 0;
    for (let j = anchorStep; j < s; j++) cumulativeDays += delayForGap(j, stepDelays);
    return addDaysSnapWeekday(baseDate, cumulativeDays);
  });
}

/**
 * Bucket every pending lead's projected step-sends into calendar days, from
 * today (asOf) forward, then ZERO-FILL every gap so the series is a CONTIGUOUS
 * one-bar-per-calendar-day range (the ops chart renders exactly the array it
 * receives and must not fabricate missing days client-side).
 *
 * Range = asOf's UTC date (inclusive) through the LAST scheduled day
 * (inclusive). Every UTC calendar day in between — INCLUDING weekends, which the
 * projection snaps sends off of, so Sat/Sun surface as real `scheduledCount:0`
 * bars — is present. Returns chronological `ForecastDay[]`; `[]` when nothing is
 * scheduled at all (empty forecast → chart shows its "nothing scheduled" empty
 * state). Horizon is bounded by the sequence structure (finite steps × finite
 * gap) — there is no unbounded tail.
 */
export function projectDailySchedule(leads: PendingLead[], asOf: Date): ForecastDay[] {
  const buckets = new Map<string, number>();
  for (const lead of leads) {
    for (const when of scheduleLead(lead, asOf)) {
      const key = dateKeyUTC(when);
      buckets.set(key, (buckets.get(key) ?? 0) + 1);
    }
  }
  if (buckets.size === 0) return [];

  // Contiguous range: asOf's UTC day → the max scheduled day, both inclusive.
  const lastKey = [...buckets.keys()].reduce((max, k) => (k > max ? k : max));
  const start = new Date(`${dateKeyUTC(asOf)}T00:00:00.000Z`);
  const end = new Date(`${lastKey}T00:00:00.000Z`);

  const days: ForecastDay[] = [];
  for (let d = start; d.getTime() <= end.getTime(); d = new Date(d.getTime() + MS_PER_DAY)) {
    const key = dateKeyUTC(d);
    days.push({ date: key, scheduledCount: buckets.get(key) ?? 0 });
  }
  return days;
}
