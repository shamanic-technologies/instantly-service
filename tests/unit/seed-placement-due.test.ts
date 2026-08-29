/**
 * Seed-test cadence — the schedule the scheduler cannot be trusted with.
 *
 * These pin the property the whole design exists for: a SKIPPED cron tick must
 * cost nothing, because GitHub Actions demonstrably skips them (2026-08-29, the
 * 06:00 Saturday job never fired) and two consecutive misses would age the fleet
 * past DELIVERY_EVIDENCE_MAX_AGE_DAYS and stop all sending.
 */

import { describe, it, expect } from "vitest";

import {
  SEED_EVIDENCE_URGENT_AGE_DAYS,
  SEED_TEST_INTERVAL_DAYS,
  SEED_URGENT_MARGIN_DAYS,
  decideSeedTestDue,
} from "../../src/lib/seed-placement/due";
import { DELIVERY_EVIDENCE_MAX_AGE_DAYS } from "../../src/lib/account-lifecycle";

// 2026-08-29 is a Saturday; 2026-08-31 a Monday.
const SAT = new Date("2026-08-29T08:00:00.000Z");
const SUN = new Date("2026-08-30T08:00:00.000Z");
const MON = new Date("2026-08-31T08:00:00.000Z");
const WED = new Date("2026-09-02T08:00:00.000Z");

const daysBefore = (d: Date, n: number) => new Date(d.getTime() - n * 86400000);

describe("decideSeedTestDue", () => {
  it("is due when we have NEVER tested, whatever day it is", () => {
    // An unmeasured fleet is the state this mechanism exists to prevent, so
    // waiting for a preferred weekday would leave it unmeasured for a week.
    expect(decideSeedTestDue(null, MON)).toMatchObject({
      due: true,
      reason: "no_previous_test",
    });
  });

  it("declines inside the interval", () => {
    expect(decideSeedTestDue(daysBefore(SAT, 1), SAT)).toMatchObject({
      due: false,
      reason: "recently_tested",
    });
  });

  it("is due on a Saturday once the interval has passed", () => {
    expect(decideSeedTestDue(daysBefore(SAT, 7), SAT)).toMatchObject({
      due: true,
      reason: "weekend_cadence",
    });
  });

  it("SELF-HEALS: a skipped Saturday is picked up by Sunday", () => {
    // This is the whole point. The 2026-08-29 Saturday tick never fired.
    expect(decideSeedTestDue(daysBefore(SUN, 8), SUN)).toMatchObject({
      due: true,
      reason: "weekend_cadence",
    });
  });

  it("waits for the weekend on a normal sending day, so seeds land on an idle mailbox", () => {
    expect(decideSeedTestDue(daysBefore(MON, 7), MON)).toMatchObject({
      due: false,
      reason: "waiting_for_weekend",
    });
  });

  it("OVERRIDES the weekend preference once evidence is urgently old", () => {
    // A weekday seed spike is the lesser evil against the entire fleet ageing
    // into delivery_evidence_stale and dropping out of the sending pool.
    expect(decideSeedTestDue(daysBefore(WED, SEED_EVIDENCE_URGENT_AGE_DAYS), WED)).toMatchObject({
      due: true,
      reason: "evidence_urgent",
    });
  });

  it("reports the evidence age so the log says WHY", () => {
    const d = decideSeedTestDue(daysBefore(SAT, 7), SAT);
    expect(d.ageDays).toBeCloseTo(7, 5);
    expect(decideSeedTestDue(null, SAT).ageDays).toBeNull();
  });
});

describe("cadence constants leave room to recover", () => {
  it("the urgent trigger fires well before evidence expires", () => {
    // The test still has to run, land, and be ingested by the daily sync before
    // the 16-day limit bites.
    expect(SEED_EVIDENCE_URGENT_AGE_DAYS).toBeLessThan(DELIVERY_EVIDENCE_MAX_AGE_DAYS);
    expect(SEED_URGENT_MARGIN_DAYS).toBeGreaterThanOrEqual(3);
  });

  it("the interval is under a week so a late run does not drift the cadence", () => {
    // A 7-day gate plus any lateness silently becomes 8 days, then 9.
    expect(SEED_TEST_INTERVAL_DAYS).toBeLessThan(7);
  });

  it("two consecutive missed weekends still cannot expire the evidence", () => {
    // 6-day interval + a fortnight of skipped ticks still trips `evidence_urgent`
    // at 12 days, which is inside the 16-day limit.
    const stale = daysBefore(WED, 14);
    expect(decideSeedTestDue(stale, WED).due).toBe(true);
  });
});
