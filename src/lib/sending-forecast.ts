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
 *      remaining (un-sent) sequence steps onto future calendar days. This is NOT
 *      a "backlog ÷ capacity" approximation: capacity is currently near-zero (the
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
 * daily-limit saturation / throttling / pauses.
 *
 * ── Day bucketing = the raw nominal UTC day, IDENTICAL to the queue breakdown ──
 * Each projected send is bucketed on its raw nominal UTC calendar day, with NO
 * weekend snap. This makes the two staff ops surfaces COHERENT BY CONSTRUCTION:
 * a step the per-account queue breakdown counts as due today/tomorrow
 * (`queuedNextToday`/`queuedNextTomorrow`) lands on that same UTC day in this
 * forecast series — so the "Emails sent per day" chart and the "Sending accounts"
 * queued-today/tomorrow table agree for the same pending steps. The former
 * business-hours-weekday snap (which pushed a weekend-due step forward to Monday,
 * dropping today/tomorrow to 0 on a weekend while the table still showed the
 * step queued) was removed for exactly this reason — do NOT reintroduce it.
 */

import { rampCapForVolume, IN_PRODUCTION_DAILY_LIMIT } from "./account-lifecycle";
import { sustainedForMailbox, type DailyVolume } from "./recent-send-volume";
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
 * Fleet capacity summary — what the fleet can REALLY send today, not what its
 * limits would allow if every mailbox were already at full volume.
 *
 * ⚠️ `dailyCapacity` used to be `Σ daily_limit` over the `in_production` pool,
 * which is the theoretical ceiling and overstated the fleet twice over:
 *
 *   - It ignored the RAMP. A mailbox is only offered `rampCapForVolume(what it
 *     has been sending)`, so a quiet one carries 5/day whatever its stated limit
 *     says. Measured 2026-09-08: the page read **2,980/day** against **1,696**
 *     once the ramp was applied.
 *   - It counted per ADDRESS. Several aliases share one relay login and
 *     therefore ONE daily quota — 62 production addresses sat on 22 domains, so
 *     the per-address sum counted the same quota up to three times. This is the
 *     same trap the dispatcher's own capacity had, one surface along.
 *
 * So capacity is summed over real MAILBOXES: for each, the operator limit is the
 * MINIMUM across its aliases (an operator lowering one means it for the mailbox)
 * and the ramp reads the mailbox's own summed daily volume. `healthyAccountCount`
 * deliberately stays an ACCOUNT count — it answers "how many senders passed the
 * gate", a different question from "how much can go out".
 *
 * `mailboxOf` maps an address to the login it authenticates as; an address it
 * does not know is its own mailbox, which is both the Primeforge case and the
 * safe reading — the account is never silently dropped from the total.
 */
export function computeCapacitySummary(
  accounts: Account[],
  lifecycleByEmail: Map<string, LifecycleView>,
  volume: DailyVolume = new Map(),
  mailboxOf: ReadonlyMap<string, string> = new Map(),
): CapacitySummary {
  const statusOf = (email: string) => lifecycleByEmail.get(email)?.status ?? null;
  const production = accounts.filter((a) => statusOf(a.email) === "in_production");

  const addressesByMailbox = new Map<string, string[]>();
  const limitByMailbox = new Map<string, number>();
  for (const a of production) {
    const mailbox = mailboxOf.get(a.email.trim().toLowerCase()) ?? a.email.trim().toLowerCase();
    const group = addressesByMailbox.get(mailbox) ?? [];
    group.push(a.email);
    addressesByMailbox.set(mailbox, group);
    const limit = a.daily_limit ?? 0;
    const known = limitByMailbox.get(mailbox);
    limitByMailbox.set(mailbox, known === undefined ? limit : Math.min(known, limit));
  }

  let dailyCapacity = 0;
  for (const [mailbox, limit] of limitByMailbox) {
    const sustained = sustainedForMailbox(volume, addressesByMailbox.get(mailbox) ?? []);
    dailyCapacity += Math.min(limit, rampCapForVolume(sustained, IN_PRODUCTION_DAILY_LIMIT));
  }

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

/**
 * Project one lead's remaining steps onto calendar dates, using the REAL
 * configured per-step delays (`lead.stepDelays`) as the inter-step cadence. The
 * projection is IDENTICAL to the per-account queue breakdown's `projectStepDate`
 * (`queue-breakdown.ts`) — same anchor, same chained real per-gap delays, same
 * raw nominal UTC day (NO weekend snap) — plus a past-due clamp to asOf that
 * mirrors the breakdown's `projKey <= todayKey ⇒ due today` bucketing. This keeps
 * the two ops surfaces coherent for the same pending steps by construction.
 *
 * - Contacted lead: each un-sent step's NOMINAL date is
 *   `lastSentAt + Σ delayForGap(j)` for `j` in `[lastSentStep, s-1]` — the exact
 *   `projectStepDate` computation. A step whose nominal date already passed
 *   (overdue) clamps to asOf (today), so it buckets on today just like the
 *   breakdown counts it under `nextToday`. Never lands in the past.
 * - Never-contacted lead (`lastSentStep === null`): no send anchor exists, so the
 *   first pending step fires ~now (asOf) and each later step lands after
 *   accumulating the real gaps from the first step. (In practice the forecast's
 *   pending-lead gate is `delivery_status IN ('contacted','sent')`, so this
 *   branch is effectively unused — the breakdown keeps never-contacted steps in
 *   `firstUnsent` rather than date-projecting them.)
 *
 * Each gap is resolved from config via the shared `delayForGap` (per-gap
 * `STEP_GAP_CALENDAR_DAYS` fallback), so cadence AND day bucketing match the
 * breakdown. Do NOT reintroduce a weekend snap or a base-clamp (clamping the
 * anchor to today then re-adding the full gap over-shoots the nominal date and
 * breaks reconciliation with the breakdown).
 */
export function scheduleLead(lead: PendingLead, asOf: Date): Date[] {
  const steps = [...lead.provisionedSteps].sort((a, b) => a - b);
  if (steps.length === 0) return [];

  const stepDelays = lead.stepDelays ?? [];
  const contacted = lead.lastSentStep !== null && lead.lastSentAt !== null;

  if (!contacted) {
    // No send anchor — project the first pending step at asOf, later steps after
    // accumulating the real gaps from that first step.
    const anchorStep = steps[0];
    return steps.map((s) => {
      let days = 0;
      for (let j = anchorStep; j < s; j++) days += delayForGap(j, stepDelays);
      return new Date(asOf.getTime() + days * MS_PER_DAY);
    });
  }

  const anchorStep = lead.lastSentStep as number;
  const anchor = lead.lastSentAt as Date;
  return steps.map((s) => {
    let days = 0;
    for (let j = anchorStep; j < s; j++) days += delayForGap(j, stepDelays);
    const projected = new Date(anchor.getTime() + days * MS_PER_DAY);
    // Overdue nominal slot → schedule today (mirrors the breakdown's overdue →
    // nextToday); never in the past.
    return projected.getTime() < asOf.getTime() ? asOf : projected;
  });
}

/**
 * Bucket every pending lead's projected step-sends into calendar days, from
 * today (asOf) forward, then ZERO-FILL every gap so the series is a CONTIGUOUS
 * one-bar-per-calendar-day range (the ops chart renders exactly the array it
 * receives and must not fabricate missing days client-side).
 *
 * Range = asOf's UTC date (inclusive) through the LAST scheduled day
 * (inclusive). Every UTC calendar day in between is present, INCLUDING weekends —
 * and because there is no weekend snap, a weekend day carries its REAL due-step
 * count (matching the queue breakdown's "due that day" semantics), not a forced
 * 0. Returns chronological `ForecastDay[]`; `[]` when nothing is scheduled at all
 * (empty forecast → chart shows its "nothing scheduled" empty state). Horizon is
 * bounded by the sequence structure (finite steps × finite gap) — there is no
 * unbounded tail.
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
