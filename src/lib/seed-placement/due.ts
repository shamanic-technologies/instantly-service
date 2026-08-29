/**
 * When is a seed placement test DUE? (pure — no IO)
 *
 * ⚠️ THE CRON CANNOT BE TRUSTED TO FIRE, AND THAT IS WHY THIS EXISTS.
 * GitHub Actions silently delays or skips scheduled workflows under load — on
 * 2026-08-29 the 06:00 Saturday placement job never ran at all and the 05:30
 * prime job fired at 11:45. A weekly cron with no catch-up therefore does not
 * mean "weekly", it means "most weeks", and the consequence here is not a missed
 * report: `DELIVERY_EVIDENCE_MAX_AGE_DAYS` is 16, so TWO consecutive misses age
 * every account into `delivery_evidence_stale` and the whole fleet stops
 * sending.
 *
 * So the schedule is inverted. The cron fires DAILY and asks this module whether
 * a test is due; almost every day the answer is no and the call is a cheap
 * no-op. A skipped day costs nothing because the next day asks again. That makes
 * the cadence a property of the DATA (when did we last measure?) rather than of
 * the scheduler firing on the exact minute we hoped.
 */

import { isSendingDay } from "../sending-calendar";
import { DELIVERY_EVIDENCE_MAX_AGE_DAYS } from "../account-lifecycle";

/**
 * Normal cadence: a test is due once this many days have passed.
 *
 * Six rather than seven so a Saturday run is due again the following Saturday
 * even if the previous one landed a few hours late — a 7-day gate plus any
 * lateness silently pushes the cadence to 8 days and then 9.
 */
export const SEED_TEST_INTERVAL_DAYS = 6;

/**
 * Hard ceiling: once evidence is this old, run TODAY whatever day it is.
 *
 * Comfortably below the 16-day staleness limit so there is room for the test to
 * run, land, and be ingested by the daily sync before anything expires. Below
 * this we prefer a send-free day; past it, weekday volume is the lesser evil
 * against the entire fleet dropping out of the sending pool.
 */
export const SEED_EVIDENCE_URGENT_AGE_DAYS = 12;

export type SeedDueReason =
  | "no_previous_test"
  | "weekend_cadence"
  | "evidence_urgent"
  | "recently_tested"
  | "waiting_for_weekend";

export interface SeedDueDecision {
  due: boolean;
  reason: SeedDueReason;
  ageDays: number | null;
}

function daysBetween(from: Date, to: Date): number {
  return (to.getTime() - from.getTime()) / (24 * 60 * 60 * 1000);
}

/**
 * Decide whether to run a seed test now.
 *
 * `lastTestedAt` is the newest seed test we have, or null if we have never run
 * one. Never tested ⇒ always due: an unmeasured fleet is the state this whole
 * mechanism exists to prevent, and waiting for a preferred weekday would leave
 * it unmeasured for up to a week.
 *
 * Otherwise: not due inside the interval; due on a NON-sending day once the
 * interval has passed (Saturday and Sunday are both send-free, so a missed
 * Saturday is picked up by Sunday); and due on ANY day once the evidence is
 * urgently old.
 */
export function decideSeedTestDue(
  lastTestedAt: Date | null,
  asOf: Date,
): SeedDueDecision {
  if (!lastTestedAt) return { due: true, reason: "no_previous_test", ageDays: null };

  const ageDays = daysBetween(lastTestedAt, asOf);

  if (ageDays >= SEED_EVIDENCE_URGENT_AGE_DAYS) {
    return { due: true, reason: "evidence_urgent", ageDays };
  }
  if (ageDays < SEED_TEST_INTERVAL_DAYS) {
    return { due: false, reason: "recently_tested", ageDays };
  }
  // Interval elapsed. Prefer a send-free day so the seed volume lands on a
  // mailbox that is otherwise idle, exactly as the weekly cadence intended.
  if (isSendingDay(asOf)) {
    return { due: false, reason: "waiting_for_weekend", ageDays };
  }
  return { due: true, reason: "weekend_cadence", ageDays };
}

/** Sanity bound: the urgent trigger must leave room before evidence expires. */
export const SEED_URGENT_MARGIN_DAYS =
  DELIVERY_EVIDENCE_MAX_AGE_DAYS - SEED_EVIDENCE_URGENT_AGE_DAYS;
