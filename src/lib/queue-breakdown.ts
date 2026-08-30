/**
 * Per-account queue BREAKDOWN (pure — no IO). Splits each account's queued
 * STEPS (every remaining un-sent email across all its queued sequences) into
 * four mutually-exclusive buckets by the projected send date of THAT step:
 *
 *   firstUnsent  (Q0-first) — steps of a sequence that has not sent its first
 *                             email yet (no send-date anchor → not projected).
 *   nextToday    (Q0-next)  — step projected today (UTC) or overdue.
 *   nextTomorrow (Q1-next)  — step projected tomorrow (UTC).
 *   nextLater    (Q-next)   — step projected after tomorrow (UTC).
 *
 * INVARIANT (the whole point): the four buckets PARTITION the account's queued
 * STEPS, so `steps === firstUnsent + nextToday + nextTomorrow + nextLater` for
 * every account — no gap, no double-count. Each queued step lands in exactly
 * one bucket. `steps` equals the account's `queueSize` (total pending steps), so
 * the ops table's four bucket columns add up to the queued-steps total.
 *
 * `sequences` is ALSO exposed — the count of queued sequences (leads) for the
 * account, a DIFFERENT granularity kept side by side (one Instantly campaign =
 * one lead = one sequence). Only the bucket partition is per-STEP; the sequence
 * count is unchanged.
 *
 * ── Projecting EVERY remaining step, not just the immediate next one ──────────
 * A contacted sequence at last-sent step `k` (sent at `lastSentAt`) still has
 * steps k+1, k+2, … queued. We project EACH of them by CHAINING the real
 * per-step nominal delays from the campaign's bronze sequence config
 * (`instantly_campaigns_config_raw.payload->sequences->0->steps[].delay`, the
 * same cadence source the immediate-next-step projection already used):
 *   projected(s) = lastSentAt + Σ_{hop=k..s-1} delay(hop → hop+1)
 * where delay(hop → hop+1) = config `steps[hop-1].delay` (cost steps are 1-based,
 * the config `steps` array is 0-based). Bucketing only the immediate next step
 * (the pre-fix behaviour) made the four buckets partition SEQUENCES, not steps —
 * so they summed to the sequence count, not the queued-steps total.
 *
 * ── The projected date is a NOMINAL-CADENCE LOWER BOUND, and chaining COMPOUNDS
 *    the drift ──────────────────────────────────────────────────────────────
 * Instantly does NOT expose a per-lead scheduled-send timestamp (verified: the
 * lead/email/campaign objects carry only PAST timestamps + a business-hours
 * window). Each projected date is the EARLIEST the step is eligible to send per
 * the sequence cadence; the ACTUAL Instantly dispatch slips LATER under
 * daily-limit saturation / throttling / pauses (empirically: a nominal 3-day gap
 * actualizes at ~7.6 days during the current backlog). Because a step k+2 sums
 * TWO nominal gaps off `lastSentAt`, its lower bound compounds the drift of the
 * step before it — later steps slide progressively more. So `nextToday` reads as
 * "step is DUE today-or-overdue", not "will certainly send today", and the later
 * a step sits in the sequence the softer its date. This is the honest framing —
 * do NOT relabel the buckets as an exact schedule.
 *
 * v1 modeling assumptions (deliberate, documented):
 *   1. Per-campaign non-sending-day windows (`campaign_schedule.days`/timezone)
 *      are NOT modeled — the projection is the raw nominal date. Many campaigns
 *      send 7 days/week, so a weekday-only snap (as the fleet forecast uses)
 *      would be wrong here; UTC-day bucketing is used instead.
 *   2. Missing sequence config → the per-hop delay falls back to the fleet
 *      forecast's canonical `STEP_GAP_CALENDAR_DAYS` (same assumption already
 *      shipped), so a step is never dropped from the partition. In practice
 *      bronze config covers the entire active queued set (fallback ~never fires).
 *   3. A never-contacted sequence (no first email sent) has NO `lastSentAt`
 *      anchor to project from — anchoring on "now" would fabricate a date for a
 *      sequence Instantly has not even started. All of its un-sent steps are
 *      counted under `firstUnsent` ("not started yet"), never date-projected.
 */

import { MS_PER_DAY, dateKeyUTC, delayForGap } from "./sending-forecast";
import { chainBookedDays, nextLocalSendInstant, resolveLeadTimezone } from "./sending-window";

/** One queued sequence's projection inputs (resolved from the cost ledger + config). */
export interface QueuedSequenceInput {
  /** Account the sequence is attributed to (persisted or observed). */
  account: string;
  /** Highest already-sent (actualized) step, or null when nothing has sent yet. */
  lastSentStep: number | null;
  /** Timestamp of the last sent step, or null when nothing has sent yet. */
  lastSentAt: Date | null;
  /** Distinct un-sent (provisioned) step numbers for this sequence — 1-based, non-empty. */
  provisionedSteps: number[];
  /**
   * Per-step configured delays (calendar days) from the campaign's bronze
   * sequence config, 0-based: `stepDelays[i]` is config `steps[i].delay`, i.e.
   * the gap from cost-step `i+1` to cost-step `i+2`. Individual null entries (or
   * a null array when config is unavailable) fall back per hop to
   * `STEP_GAP_CALENDAR_DAYS` so a step is never dropped from the partition.
   */
  stepDelays: (number | null)[] | null;
  /**
   * The lead's IANA timezone — the zone the campaign's Instantly schedule runs
   * in, so the zone that decides which UTC day each of its sends actually books
   * on the mailbox. Null when we hold none (historical rows, a caller that sent
   * no timezone); `resolveLeadTimezone` then uses the same default the schedule
   * itself degraded to, never a guess about the prospect.
   *
   * Read ONLY by the capacity projection (`aggregateQueueCapacity`). The ops
   * breakdown deliberately ignores it — see the note on `aggregateQueueCapacity`.
   */
  timezone?: string | null;
}

export type QueueBucket = "firstUnsent" | "nextToday" | "nextTomorrow" | "nextLater";

/** The four-way per-STEP partition of one account's queue + its two totals. */
export interface QueueBreakdown {
  /** Distinct queued sequences (leads) for the account. NOT the bucket sum. */
  sequences: number;
  /** Total queued STEPS for the account (= sum of the four buckets = queueSize). */
  steps: number;
  /** Q0-first — steps of sequences whose first email has not sent yet. */
  firstUnsent: number;
  /**
   * Q0-first as SEQUENCES — how many never-contacted leads sit on this account,
   * i.e. how many FIRST emails are actually due. `firstUnsent` counts every
   * remaining step of those sequences (a 3-step sequence contributes 3), which
   * over-states today by the whole future sequence; this is the number send
   * SELECTION uses for today's load (`q0first` in `aggregateQueueCapacity`).
   *
   * Exposed so an ops surface can render the same "due today" quantity the
   * selector decides on, instead of a step total that reads as saturation
   * (an account showing 102 queued-today steps against a 45 cap is seen by the
   * selector as ~32 and legitimately keeps taking leads).
   */
  firstUnsentSequences: number;
  /** Q0-next — step projected today (UTC) or overdue. */
  nextToday: number;
  /**
   * The BACKLOG subset of `nextToday` — steps projected STRICTLY BEFORE today,
   * i.e. already past their nominal due date. Deliberately a subset counter and
   * NOT a fifth bucket, so the four-way partition
   * (`steps === firstUnsent + nextToday + nextTomorrow + nextLater`) is
   * untouched; `nextOverdue <= nextToday` always.
   *
   * Exposed because "Followups due today" and "followups we owed days ago and
   * never dispatched" are operationally different facts that the merged
   * today-or-overdue bucket cannot tell apart. A `nextOverdue` that grows week
   * over week means dispatch is not keeping up with assignment — the signal an
   * ops surface needs to answer "is this account draining or drowning?".
   */
  nextOverdue: number;
  /** Q1-next — step projected tomorrow (UTC). */
  nextTomorrow: number;
  /** Q-next — step projected after tomorrow (UTC). */
  nextLater: number;
}

/**
 * Resolve the projected send date of one un-sent `step` of a CONTACTED sequence
 * by chaining the real per-hop delays from `lastSentAt`. Caller guarantees the
 * sequence is contacted (`lastSentStep`/`lastSentAt` non-null) and `step >
 * lastSentStep`. Each hop's delay comes from the SHARED `delayForGap` resolver
 * (same per-gap indexing + `STEP_GAP_CALENDAR_DAYS` fallback the fleet
 * sending-forecast uses — one cadence model, not a second copy).
 */
export function projectStepDate(seq: QueuedSequenceInput, step: number): Date {
  const k = seq.lastSentStep as number;
  const anchor = seq.lastSentAt as Date;
  const stepDelays = seq.stepDelays ?? [];
  let days = 0;
  for (let hop = k; hop < step; hop++) days += delayForGap(hop, stepDelays);
  return new Date(anchor.getTime() + days * MS_PER_DAY);
}

/** Classify one un-sent `step` of a queued sequence into exactly one bucket. */
export function classifyQueuedStep(
  seq: QueuedSequenceInput,
  step: number,
  asOf: Date,
): QueueBucket {
  // Never-contacted sequence: no anchor to project from → "not started yet".
  if (seq.lastSentStep === null || seq.lastSentAt === null) return "firstUnsent";

  const projected = projectStepDate(seq, step);
  const projKey = dateKeyUTC(projected);
  const todayKey = dateKeyUTC(asOf);
  const tomorrowKey = dateKeyUTC(new Date(asOf.getTime() + MS_PER_DAY));

  // YYYY-MM-DD strings compare lexicographically in date order.
  if (projKey <= todayKey) return "nextToday"; // due today OR overdue (slipped past)
  if (projKey === tomorrowKey) return "nextTomorrow";
  return "nextLater";
}

function emptyBreakdown(): QueueBreakdown {
  return {
    sequences: 0,
    steps: 0,
    firstUnsent: 0,
    firstUnsentSequences: 0,
    nextToday: 0,
    nextOverdue: 0,
    nextTomorrow: 0,
    nextLater: 0,
  };
}

/**
 * True when an un-sent `step` of a CONTACTED sequence was nominally due on a day
 * STRICTLY BEFORE `asOf`'s UTC day — i.e. it is backlog, not work due today.
 * A never-contacted sequence has no anchor to project from and is never overdue
 * (it lands in `firstUnsent`), so it returns false rather than fabricating a
 * date.
 */
export function isOverdueStep(
  seq: QueuedSequenceInput,
  step: number,
  asOf: Date,
): boolean {
  if (seq.lastSentStep === null || seq.lastSentAt === null) return false;
  return dateKeyUTC(projectStepDate(seq, step)) < dateKeyUTC(asOf);
}

/**
 * How far ahead the capacity projection books. A three-step sequence at the
 * fleet's D0 / D+3 / D+10 cadence lands inside two weeks; 21 days leaves room
 * for a weekend snap on every hop and for a longer sequence than any campaign
 * currently configures.
 *
 * A step beyond the horizon is simply absent from `byDay`, which selection reads
 * as room. That is the never-block direction: refusing an account over a day we
 * did not project would be a decision made on missing information.
 */
export const CAPACITY_HORIZON_DAYS = 21;

/**
 * Per-account CAPACITY feeding the sequential-fill send-selection policy
 * (see send-lead.ts `pickSequentialFillAccount`). A projection of the same queued
 * sequences as the ops breakdown, answering a different question: not "when is
 * this step nominally due" but "which day of this mailbox's quota does it spend".
 *
 * ── Why this diverges from `aggregateQueueBreakdown`, deliberately ────────────
 * Two differences, both required for selection and both WRONG for the ops table:
 *
 *   1. A never-contacted sequence IS projected here. The breakdown refuses to
 *      date one (anchoring on "now" would fabricate a date for a sequence
 *      Instantly has not started). But selection has already decided such a lead
 *      sends imminently — that is exactly what assigning it meant — so its
 *      followups land on real future days and must be booked. Leaving them out
 *      makes the projection blind to precisely the leads the head of the fill
 *      order accumulates, which is the over-booking this whole shape exists to
 *      stop.
 *
 *   2. Every step is booked on the day it will ACTUALLY leave — resolved through
 *      the lead's own timezone window (`sending-window.ts`) — not on its raw
 *      nominal UTC day. A New Zealand lead's local Monday is the mailbox's
 *      Sunday. The ops table keeps the raw day because that is what makes it
 *      agree with the fleet forecast step-for-step; selection needs the day the
 *      quota is really spent.
 *
 * A step already past its nominal date is booked on the next window at or after
 * `asOf`, never in the past: an overdue followup competes for TODAY's capacity,
 * which is the same thing the breakdown's today-or-overdue bucket says.
 */
export interface QueueCapacity {
  /**
   * Committed un-sent steps per UTC day key (`YYYY-MM-DD`), within
   * `CAPACITY_HORIZON_DAYS` of `asOf`. The single source of an account's future
   * load — there is deliberately no second, separately-maintained "today" or
   * "tomorrow" counter to drift away from it.
   */
  byDay: Record<string, number>;
}

function emptyCapacity(): QueueCapacity {
  return { byDay: {} };
}

/**
 * The UTC day each un-sent step of one queued sequence books on its mailbox.
 *
 * Contacted sequence: chain the real per-hop delays off `lastSentAt` exactly as
 * the ops projection does, clamp anything overdue up to `asOf`, then resolve the
 * lead's next open window. Never-contacted: anchor on `asOf`'s window and chain
 * the sequence's own gaps from its first pending step.
 */
function bookedDaysForSequence(
  seq: QueuedSequenceInput,
  asOf: Date,
): Map<number, string> {
  const tz = resolveLeadTimezone(seq.timezone);
  const steps = [...seq.provisionedSteps].sort((a, b) => a - b);
  const out = new Map<number, string>();
  if (steps.length === 0) return out;

  if (seq.lastSentStep !== null && seq.lastSentAt !== null) {
    for (const step of steps) {
      const nominal = projectStepDate(seq, step);
      // An overdue step is owed NOW, not on the day it was owed.
      const from = nominal.getTime() < asOf.getTime() ? asOf : nominal;
      out.set(step, dateKeyUTC(nextLocalSendInstant(from, tz)));
    }
    return out;
  }

  // Never contacted: the first pending step goes out at the next open window and
  // the rest chain off it, hop by hop, through the same shared delay resolver.
  const stepDelays = seq.stepDelays ?? [];
  const gaps: number[] = [];
  for (let i = 1; i < steps.length; i += 1) {
    let days = 0;
    for (let hop = steps[i - 1]!; hop < steps[i]!; hop += 1) {
      days += delayForGap(hop, stepDelays);
    }
    gaps.push(days);
  }
  const keys = chainBookedDays(asOf, tz, gaps);
  steps.forEach((step, i) => out.set(step, keys[i]!));
  return out;
}

/** Day keys strictly inside the projection horizon, as a lookup set. */
function horizonKeys(asOf: Date): Set<string> {
  const keys = new Set<string>();
  for (let i = 0; i <= CAPACITY_HORIZON_DAYS; i += 1) {
    keys.add(dateKeyUTC(new Date(asOf.getTime() + i * MS_PER_DAY)));
  }
  return keys;
}

/**
 * Aggregate queued sequences into the per-account capacity shape above. Pure.
 *
 * Every un-sent step of every queued sequence is booked onto exactly one UTC day
 * (or dropped, when it falls past the horizon). Days before `asOf` cannot occur:
 * an overdue step is clamped up to today before its window is resolved.
 */
export function aggregateQueueCapacity(
  rows: QueuedSequenceInput[],
  asOf: Date,
): Map<string, QueueCapacity> {
  const out = new Map<string, QueueCapacity>();
  const inHorizon = horizonKeys(asOf);

  for (const row of rows) {
    if (!row.account) continue;
    const c = out.get(row.account) ?? emptyCapacity();
    for (const key of bookedDaysForSequence(row, asOf).values()) {
      if (!inHorizon.has(key)) continue;
      c.byDay[key] = (c.byDay[key] ?? 0) + 1;
    }
    out.set(row.account, c);
  }
  return out;
}

/**
 * Aggregate queued sequences into a per-account STEP breakdown. Each sequence
 * increments its account's `sequences` count once; each of its un-sent steps
 * increments `steps` AND exactly one date bucket — so both the step-partition
 * invariant (`steps === firstUnsent + nextToday + nextTomorrow + nextLater`) and
 * the sequence count hold for every account by construction.
 *
 * `nextOverdue` rides alongside as a SUBSET of `nextToday` (the strictly-past
 * half of today-or-overdue). It is counted, never bucketed, so it cannot break
 * the partition.
 */
export function aggregateQueueBreakdown(
  rows: QueuedSequenceInput[],
  asOf: Date,
): Map<string, QueueBreakdown> {
  const out = new Map<string, QueueBreakdown>();
  for (const row of rows) {
    if (!row.account) continue;
    const b = out.get(row.account) ?? emptyBreakdown();
    b.sequences += 1;
    // A never-contacted sequence owes exactly ONE first email — count it once,
    // independently of how many un-sent steps it still carries.
    if (row.lastSentStep === null || row.lastSentAt === null) b.firstUnsentSequences += 1;
    for (const step of row.provisionedSteps) {
      b.steps += 1;
      const bucket = classifyQueuedStep(row, step, asOf);
      b[bucket] += 1;
      // Subset counter, NOT a fifth bucket — the partition above stays intact.
      if (bucket === "nextToday" && isOverdueStep(row, step, asOf)) b.nextOverdue += 1;
    }
    out.set(row.account, b);
  }
  return out;
}
