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
  /** Q0-next — step projected today (UTC) or overdue. */
  nextToday: number;
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
  return { sequences: 0, steps: 0, firstUnsent: 0, nextToday: 0, nextTomorrow: 0, nextLater: 0 };
}

/**
 * Aggregate queued sequences into a per-account STEP breakdown. Each sequence
 * increments its account's `sequences` count once; each of its un-sent steps
 * increments `steps` AND exactly one date bucket — so both the step-partition
 * invariant (`steps === firstUnsent + nextToday + nextTomorrow + nextLater`) and
 * the sequence count hold for every account by construction.
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
    for (const step of row.provisionedSteps) {
      b.steps += 1;
      b[classifyQueuedStep(row, step, asOf)] += 1;
    }
    out.set(row.account, b);
  }
  return out;
}
