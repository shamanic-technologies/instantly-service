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

/**
 * On a send-free day (Sunday) a test runs unless one already ran in the last
 * day: the cadence re-anchors to Sunday after ANY off-cycle test.
 *
 * MON-SEED-PLACEMENT-1120: the 6-day interval used to gate Sundays too. A
 * catch-up test forced on Wednesday 2026-11-11 made Sunday 11-15 decline
 * (`recently_tested`, 4 days), every weekday then waited for Sunday 11-22
 * (11 days after the test), and the health check went red at 8 days. Under
 * « interval, then Sunday only » an off-cycle test opens a gap of up to 13 days
 * against an 8-day limit. Over-seeding a Sunday is the cheap direction: every
 * sender and receiver is our own mailbox and Sunday carries no cold volume.
 */
export const SEED_WEEKEND_MIN_AGE_DAYS = 1;

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
 * Otherwise: due on ANY day once the evidence is urgently old; never on a
 * sending day below that; on the send-free day (Sunday) due unless a test ran
 * in the last day, so the weekly cadence holds whatever day the last test
 * landed on (MON-SEED-PLACEMENT-1120).
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
  if (isSendingDay(asOf)) {
    // Seeds land on the send-free day, where the mailbox is otherwise idle.
    return {
      due: false,
      reason: ageDays < SEED_TEST_INTERVAL_DAYS ? "recently_tested" : "waiting_for_weekend",
      ageDays,
    };
  }
  // Send-free day: run every week, whatever weekday the last test landed on
  // (see SEED_WEEKEND_MIN_AGE_DAYS); only a test from the last day declines.
  if (ageDays < SEED_WEEKEND_MIN_AGE_DAYS) {
    return { due: false, reason: "recently_tested", ageDays };
  }
  return { due: true, reason: "weekend_cadence", ageDays };
}

/**
 * When was the FLEET last measured? (pure, MON-PLACEMENT-STOPPED-1111)
 *
 * The oldest "newest seed" among the testable mailboxes seeded inside the
 * evidence window: the cadence asks how old the fleet's evidence is, and that is
 * the evidence of its least recently measured mailbox, never the newest seed of
 * any one mailbox. Keyed on the newest seed of ANY test, a hand-run probe of 3
 * mailboxes on 2026-11-03 (30 seeds) read as « tested 2 days ago » on Sunday
 * 11-08, the full weekly test declined (`recently_tested`), and no mailbox was
 * measured for 10 days while the hub stayed calm.
 *
 * Only mailboxes still in the testable pool count (one that left cannot be
 * measured again), and only seeds inside {@link DELIVERY_EVIDENCE_MAX_AGE_DAYS}
 * (a mailbox measured once long ago and never since, e.g. its credential gone,
 * would otherwise force a test every day forever). No such seed = the newest
 * seed of any mailbox (`fallbackAt`), else never tested.
 */
export function fleetLastTestedAt(
  newestSeedBySender: ReadonlyArray<{ sender: string; lastAt: Date }>,
  pool: ReadonlyArray<string>,
  asOf: Date,
): Date | null {
  const inPool = new Set(pool.map((e) => e.trim().toLowerCase()));
  const windowStart = asOf.getTime() - DELIVERY_EVIDENCE_MAX_AGE_DAYS * 24 * 60 * 60 * 1000;
  let oldest: Date | null = null;
  let newest: Date | null = null;
  for (const { sender, lastAt } of newestSeedBySender) {
    if (!newest || lastAt > newest) newest = lastAt;
    if (!inPool.has(sender.trim().toLowerCase()) || lastAt.getTime() < windowStart) continue;
    if (!oldest || lastAt < oldest) oldest = lastAt;
  }
  return oldest ?? newest;
}


/**
 * A run RESUMES: senders already seeded inside the last
 * {@link SEED_WEEKEND_MIN_AGE_DAYS} are dropped from the plan (pure).
 *
 * MON-SEED-PLACEMENT-1120: the test dispatches in-process for ~45 min; a deploy
 * of an unrelated commit recreated the container 35 min into the 2026-11-20 run
 * (1,475 seeds of ~2,200), and the mailboxes it never reached kept the fleet's
 * age at 9 days. Re-running then would have seeded the 1,475 a second time on a
 * sending day. With this, any re-run (forced or the next tick) sends only what
 * is missing, and a mailbox never carries two tests in a day.
 */
export function dropRecentlySeededSenders<T extends { senderEmail: string }>(
  planned: ReadonlyArray<T>,
  recentlySeeded: ReadonlySet<string>,
): T[] {
  return planned.filter((p) => !recentlySeeded.has(p.senderEmail.trim().toLowerCase()));
}

/**
 * Past this age the weekly test MISSED: a test lands every Sunday (interval 6
 * days, then the next non-sending day), so a healthy gap is at most 7 days plus
 * the day the sync takes to read it.
 */
export const SEED_TEST_OVERDUE_AGE_DAYS = SEED_TEST_INTERVAL_DAYS + 2;

/** Sanity bound: the urgent trigger must leave room before evidence expires. */
export const SEED_URGENT_MARGIN_DAYS =
  DELIVERY_EVIDENCE_MAX_AGE_DAYS - SEED_EVIDENCE_URGENT_AGE_DAYS;
