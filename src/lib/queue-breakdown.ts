/**
 * Per-account queue BREAKDOWN (pure — no IO). Splits each account's queued
 * sequences (one Instantly campaign = one lead = one sequence) into four
 * mutually-exclusive buckets by the timing of that sequence's NEXT un-sent step:
 *
 *   firstUnsent  (Q0-first) — no email sent yet (the first email is pending).
 *   nextToday    (Q0-next)  — ≥1 email sent, next step projected today (UTC) or
 *                             overdue.
 *   nextTomorrow (Q1-next)  — ≥1 email sent, next step projected tomorrow (UTC).
 *   nextLater    (Q-next)   — ≥1 email sent, next step projected after tomorrow.
 *
 * INVARIANT (the whole point): the four buckets PARTITION the account's queued
 * sequences, so `sequences === firstUnsent + nextToday + nextTomorrow +
 * nextLater` for every account — no gap, no double-count. Each queued sequence
 * lands in exactly one bucket.
 *
 * ── The projected date is a NOMINAL-CADENCE LOWER BOUND, not Instantly's exact
 *    dispatch date ──────────────────────────────────────────────────────────
 * Instantly does NOT expose a per-lead scheduled-send timestamp (verified: the
 * lead/email/campaign objects carry only PAST timestamps + a business-hours
 * window). We PROJECT the next send from data we DO hold locally:
 *   projected_next = lastSentAt + <next step's configured delay in days>
 * where the delay is the real per-step `delay` from the campaign's sequence
 * config (bronze `instantly_campaigns_config_raw`), resolved by the caller.
 * The projection is the EARLIEST the next step is eligible to send per the
 * sequence cadence; the ACTUAL Instantly dispatch can slip LATER under
 * daily-limit saturation / throttling / pauses (empirically observed: a nominal
 * 3-day gap actualizes at ~7.6 days during the current backlog). So `nextToday`
 * reads as "next step is DUE today-or-overdue", not "will certainly send today".
 *
 * v1 modeling assumptions (deliberate, documented):
 *   1. Per-campaign non-sending-day windows (`campaign_schedule.days`/timezone)
 *      are NOT modeled — the projection is the raw nominal date. Many campaigns
 *      send 7 days/week, so a weekday-only snap (as the fleet forecast uses)
 *      would be wrong here; UTC-day bucketing is used instead.
 *   2. Missing sequence config → the delay falls back to the fleet forecast's
 *      canonical `STEP_GAP_CALENDAR_DAYS` (same assumption already shipped), so a
 *      sequence is never dropped from the partition. In practice bronze config
 *      covers the entire active queued set (fallback ~never fires).
 */

import { MS_PER_DAY, dateKeyUTC, STEP_GAP_CALENDAR_DAYS } from "./sending-forecast";

/** One queued sequence's projection inputs (resolved from the cost ledger + config). */
export interface QueuedSequenceInput {
  /** Account the sequence is attributed to (persisted or observed). */
  account: string;
  /** Highest already-sent (actualized) step, or null when nothing has sent yet. */
  lastSentStep: number | null;
  /** Timestamp of the last sent step, or null when nothing has sent yet. */
  lastSentAt: Date | null;
  /**
   * Configured delay (calendar days) from the last-sent step to the next step,
   * i.e. the sequence's `steps[lastSentStep-1].delay`. Null when the config is
   * unavailable → the canonical `STEP_GAP_CALENDAR_DAYS` fallback is used.
   */
  nextDelayDays: number | null;
}

export type QueueBucket = "firstUnsent" | "nextToday" | "nextTomorrow" | "nextLater";

/** The four-way partition of one account's queued sequences + their total. */
export interface QueueBreakdown {
  /** Qtotal — distinct queued sequences for the account (= sum of the four). */
  sequences: number;
  /** Q0-first — queued sequences whose first email has not sent yet. */
  firstUnsent: number;
  /** Q0-next — sent ≥1, next step projected today (UTC) or overdue. */
  nextToday: number;
  /** Q1-next — sent ≥1, next step projected tomorrow (UTC). */
  nextTomorrow: number;
  /** Q-next — sent ≥1, next step projected after tomorrow (UTC). */
  nextLater: number;
}

/** Classify one queued sequence into exactly one bucket. */
export function classifyQueuedSequence(
  row: QueuedSequenceInput,
  asOf: Date,
): QueueBucket {
  if (row.lastSentStep === null || row.lastSentAt === null) return "firstUnsent";

  const gapDays = row.nextDelayDays ?? STEP_GAP_CALENDAR_DAYS;
  const projected = new Date(row.lastSentAt.getTime() + gapDays * MS_PER_DAY);

  const projKey = dateKeyUTC(projected);
  const todayKey = dateKeyUTC(asOf);
  const tomorrowKey = dateKeyUTC(new Date(asOf.getTime() + MS_PER_DAY));

  // YYYY-MM-DD strings compare lexicographically in date order.
  if (projKey <= todayKey) return "nextToday"; // due today OR overdue (slipped past)
  if (projKey === tomorrowKey) return "nextTomorrow";
  return "nextLater";
}

function emptyBreakdown(): QueueBreakdown {
  return { sequences: 0, firstUnsent: 0, nextToday: 0, nextTomorrow: 0, nextLater: 0 };
}

/**
 * Aggregate queued sequences into a per-account breakdown. Each row increments
 * its account's total (`sequences`) AND exactly one bucket, so the partition
 * invariant holds for every account by construction.
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
    b[classifyQueuedSequence(row, asOf)] += 1;
    out.set(row.account, b);
  }
  return out;
}
