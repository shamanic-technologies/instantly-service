import type { DailyVolume } from "../../src/lib/recent-send-volume";
import { RAMP_FLOOR_PER_DAY, capForAccount } from "../../src/lib/account-lifecycle";
import { describe, it, expect } from "vitest";
import type { Account } from "../../src/lib/instantly-client";
import type { LifecycleView } from "../../src/lib/account-lifecycle-sync";
import {
  computeCapacitySummary,
  projectDailySchedule,
  scheduleLead,
  delayForGap,
  dateKeyUTC,
  STEP_GAP_CALENDAR_DAYS,
  type PendingLead,
} from "../../src/lib/sending-forecast";
import {
  classifyQueuedStep,
  projectStepDate,
  type QueuedSequenceInput,
} from "../../src/lib/queue-breakdown";

function acct(overrides: Partial<Account>): Account {
  return {
    email: "x@example.com",
    warmup_status: 1,
    status: 1,
    stat_warmup_score: 100,
    daily_limit: 30,
    ...overrides,
  };
}

function lc(status: LifecycleView["status"]): LifecycleView {
  return { status, reason: null, updatedAt: "2026-07-05T00:00:00.000Z" };
}

/**
 * Every listed address already at volume, so the ramp is saturated and the
 * operator limit is what binds. The ramp gets its own cases below rather than
 * colouring every capacity assertion.
 */
const atVolume = (...emails: string[]): DailyVolume =>
  new Map(
    emails.map((e) => [
      e,
      // TWO days, because the statistic is the SECOND-highest.
      new Map([
        ["2026-09-01", 50],
        ["2026-09-02", 50],
      ]),
    ]),
  );

describe("computeCapacitySummary", () => {
  it("sums daily_limit over ONLY in_production accounts; counts totals + deactivated-by-user", () => {
    const accounts: Account[] = [
      acct({ email: "a@good.com", daily_limit: 30 }), // in_production
      acct({ email: "b@good.com", daily_limit: 20 }), // in_production
      acct({ email: "c@good.com", daily_limit: 50 }), // in_recovery → excluded
      acct({ email: "d@good.com", daily_limit: 50 }), // deactivated_by_instantly → excluded
      acct({ email: "e@good.com", daily_limit: 40 }), // deactivated_by_user → excluded, blocked
      acct({ email: "f@good.com", daily_limit: 40 }), // deactivated_by_user → excluded, blocked
    ];
    const lifecycle = new Map<string, LifecycleView>([
      ["a@good.com", lc("in_production")],
      ["b@good.com", lc("in_production")],
      ["c@good.com", lc("in_recovery")],
      ["d@good.com", lc("deactivated_by_instantly")],
      ["e@good.com", lc("deactivated_by_user")],
      ["f@good.com", lc("deactivated_by_user")],
    ]);
    const s = computeCapacitySummary(
      accounts,
      lifecycle,
      atVolume("a@good.com", "b@good.com"),
    );
    expect(s.dailyCapacity).toBe(50); // 30 + 20 only
    expect(s.healthyAccountCount).toBe(2);
    expect(s.totalAccountCount).toBe(6);
    expect(s.blockedDomainCount).toBe(2); // two deactivated_by_user
  });

  it("in_production account missing daily_limit contributes 0 (no fabricated number)", () => {
    const s = computeCapacitySummary(
      [
        acct({ email: "a@good.com", daily_limit: undefined }),
        acct({ email: "b@good.com", daily_limit: 15 }),
      ],
      new Map<string, LifecycleView>([
        ["a@good.com", lc("in_production")],
        ["b@good.com", lc("in_production")],
      ]),
      atVolume("a@good.com", "b@good.com"),
    );
    expect(s.dailyCapacity).toBe(15);
    expect(s.healthyAccountCount).toBe(2);
  });

  // ─── Capacity is what the fleet can REALLY send ────────────────────────────
  //
  // ⚠️ `Σ daily_limit` is the theoretical ceiling and overstated the fleet twice:
  // it ignored the ramp, and it counted per ADDRESS when several aliases share
  // one relay login and therefore ONE quota. Measured 2026-09-08 the page read
  // 2,980/day against 1,696 with the ramp, and 62 production addresses sat on 22
  // domains — so the per-address sum counted the same quota up to three times.

  it("counts a mailbox ONCE however many aliases it carries", () => {
    const accounts = [
      acct({ email: "kevin@ga.forum", daily_limit: 50 }),
      acct({ email: "klourd@ga.forum", daily_limit: 50 }),
      acct({ email: "kevinl@ga.forum", daily_limit: 50 }),
    ];
    const lifecycle = new Map<string, LifecycleView>(
      accounts.map((a) => [a.email, lc("in_production")]),
    );
    const logins = new Map(accounts.map((a) => [a.email, "kevin@ga.forum"]));

    const s = computeCapacitySummary(
      accounts,
      lifecycle,
      atVolume("kevin@ga.forum", "klourd@ga.forum", "kevinl@ga.forum"),
      logins,
    );
    // One mailbox, one quota: 50. Per address it would have read 150.
    expect(s.dailyCapacity).toBe(50);
    // The ACCOUNT count is a different question and stays 3.
    expect(s.healthyAccountCount).toBe(3);
  });

  it("takes the LOWEST operator limit across a mailbox's aliases", () => {
    const accounts = [
      acct({ email: "kevin@ga.forum", daily_limit: 50 }),
      acct({ email: "klourd@ga.forum", daily_limit: 12 }),
    ];
    const lifecycle = new Map<string, LifecycleView>(
      accounts.map((a) => [a.email, lc("in_production")]),
    );
    const logins = new Map(accounts.map((a) => [a.email, "kevin@ga.forum"]));
    const s = computeCapacitySummary(
      accounts,
      lifecycle,
      atVolume("kevin@ga.forum", "klourd@ga.forum"),
      logins,
    );
    // An operator who lowered one alias meant it for the mailbox behind it.
    expect(s.dailyCapacity).toBe(12);
  });

  it("reports the RAMPED figure, not the stated limit, for a mailbox still climbing", () => {
    const accounts = [acct({ email: "ramping@x.com", daily_limit: 50 })];
    const lifecycle = new Map<string, LifecycleView>([
      ["ramping@x.com", lc("in_production")],
    ]);
    // Sustained 12/day ⇒ may attempt 18. Reporting 50 would claim capacity the
    // selector will not offer.
    const volume: DailyVolume = new Map([
      [
        "ramping@x.com",
        new Map([
          ["2026-09-01", 30],
          ["2026-09-02", 12],
        ]),
      ],
    ]);
    expect(computeCapacitySummary(accounts, lifecycle, volume).dailyCapacity).toBe(18);
  });

  it("reports the FLOOR for a mailbox nobody has measured, never its stated limit", () => {
    const s = computeCapacitySummary(
      [acct({ email: "cold@x.com", daily_limit: 50 })],
      new Map<string, LifecycleView>([["cold@x.com", lc("in_production")]]),
    );
    expect(s.dailyCapacity).toBe(RAMP_FLOOR_PER_DAY);
  });

  it("an address absent from the login map is its own mailbox — never dropped", () => {
    // The Primeforge case, and the safe reading: an unknown address still
    // contributes, rather than vanishing from the fleet total.
    const accounts = [
      acct({ email: "solo1@primeforge.com", daily_limit: 50 }),
      acct({ email: "solo2@primeforge.com", daily_limit: 50 }),
    ];
    const lifecycle = new Map<string, LifecycleView>(
      accounts.map((a) => [a.email, lc("in_production")]),
    );
    const s = computeCapacitySummary(
      accounts,
      lifecycle,
      atVolume("solo1@primeforge.com", "solo2@primeforge.com"),
      new Map(),
    );
    expect(s.dailyCapacity).toBe(100);
  });

  it("counts a NON-production alias's volume toward its mailbox — the quota is shared", () => {
    // ⚠️ An alias in recovery still does warmup and still gets seeded by the
    // placement test, and every one of those messages comes out of the SAME
    // relay quota. Leaving it out under-states what the mailbox has carried:
    // measured 2026-09-08, this summary read 1,447/day against the
    // capacity-over-time series' 1,501 for the same day, purely because the
    // series counted every alias and this one counted a subset.
    const accounts = [
      acct({ email: "kevin@ga.forum", daily_limit: 50 }),
      acct({ email: "klourd@ga.forum", daily_limit: 50 }), // in_recovery, warms up
    ];
    const lifecycle = new Map<string, LifecycleView>([
      ["kevin@ga.forum", lc("in_production")],
      ["klourd@ga.forum", lc("in_recovery")],
    ]);
    const logins = new Map(accounts.map((a) => [a.email, "kevin@ga.forum"]));
    const volume: DailyVolume = new Map([
      ["kevin@ga.forum", new Map([["2026-09-01", 6], ["2026-09-02", 6]])],
      ["klourd@ga.forum", new Map([["2026-09-01", 6], ["2026-09-02", 6]])],
    ]);

    // The mailbox carried 12/day, so it may attempt 18. Counting only the
    // production alias would have read 6/day ⇒ 9.
    expect(computeCapacitySummary(accounts, lifecycle, volume, logins).dailyCapacity).toBe(18);
  });

  it("offers nothing at all for a mailbox with no production alias", () => {
    // Volume is not permission: a mailbox that sends warmup but has no alias
    // through the lifecycle gate is assigned no leads.
    const accounts = [acct({ email: "klourd@ga.forum", daily_limit: 50 })];
    const lifecycle = new Map<string, LifecycleView>([
      ["klourd@ga.forum", lc("in_recovery")],
    ]);
    const volume: DailyVolume = new Map([
      ["klourd@ga.forum", new Map([["2026-09-01", 30], ["2026-09-02", 30]])],
    ]);
    const s = computeCapacitySummary(accounts, lifecycle, volume, new Map());
    expect(s.dailyCapacity).toBe(0);
    expect(s.healthyAccountCount).toBe(0);
  });

  it("account absent from the lifecycle map contributes no capacity and is not blocked-domain", () => {
    const s = computeCapacitySummary(
      [acct({ email: "a@good.com", daily_limit: 30 })],
      new Map(),
    );
    expect(s.dailyCapacity).toBe(0);
    expect(s.healthyAccountCount).toBe(0);
    expect(s.totalAccountCount).toBe(1);
    expect(s.blockedDomainCount).toBe(0);
  });

  it("empty fleet → all zeros (real number, blocked ≤ total)", () => {
    const s = computeCapacitySummary([], new Map());
    expect(s).toEqual({
      dailyCapacity: 0,
      healthyAccountCount: 0,
      totalAccountCount: 0,
      blockedDomainCount: 0,
    });
  });
});

describe("scheduleLead", () => {
  // 2026-07-01 is a Wednesday (UTC).
  const wed = new Date("2026-07-01T12:00:00.000Z");
  const key = (d: Date) => d.toISOString().slice(0, 10);

  it("never-contacted lead: first pending step fires ~today, next +GAP (raw nominal day)", () => {
    const lead: PendingLead = {
      provisionedSteps: [1, 2],
      lastSentStep: null,
      lastSentAt: null,
    };
    const dates = scheduleLead(lead, wed).map(key);
    expect(dates[0]).toBe("2026-07-01"); // step 1 today (Wed)
    // +3 calendar days from Wed = Sat 07-04 — RAW nominal day, no weekend snap
    expect(dates[1]).toBe("2026-07-04");
  });

  it("contacted lead: next step is GAP calendar-days after the last sent step (raw nominal day)", () => {
    const lead: PendingLead = {
      provisionedSteps: [2],
      lastSentStep: 1,
      lastSentAt: new Date("2026-07-01T09:00:00.000Z"), // Wed
    };
    // +3 cal days = Sat 07-04 — bucketed on the raw nominal UTC day, no snap
    expect(scheduleLead(lead, wed).map(key)).toEqual(["2026-07-04"]);
  });

  it("past-due follow-up schedules from today, never in the past", () => {
    const lead: PendingLead = {
      provisionedSteps: [2],
      lastSentStep: 1,
      lastSentAt: new Date("2026-06-01T09:00:00.000Z"), // long ago
    };
    const [d] = scheduleLead(lead, wed).map(key);
    expect(d >= "2026-07-01").toBe(true); // from today forward, not June
  });

  it("weekend landings are kept on their raw nominal day (NO snap)", () => {
    // Fresh sequence, steps 1..4, canonical +3 gap. From Wed 07-01:
    //   step1 07-01 (Wed), step2 +3 07-04 (Sat), step3 +6 07-07 (Tue), step4 +9 07-10 (Fri).
    const lead: PendingLead = {
      provisionedSteps: [1, 2, 3, 4],
      lastSentStep: null,
      lastSentAt: null,
    };
    expect(scheduleLead(lead, wed).map(key)).toEqual([
      "2026-07-01",
      "2026-07-04", // Saturday — NOT snapped forward
      "2026-07-07",
      "2026-07-10",
    ]);
  });

  it("a step due on the asOf weekend day surfaces on that same weekend day (AC)", () => {
    // Reproduces the reported bug: asOf is a Saturday. A step whose last send was
    // Wed 07-08 with a +3 gap is due Sat 07-11 — it must land on 07-11, not be
    // pushed forward to Monday (which would zero out the weekend bar).
    const sat = new Date("2026-07-11T12:00:00.000Z"); // Saturday
    const lead: PendingLead = {
      provisionedSteps: [2],
      lastSentStep: 1,
      lastSentAt: new Date("2026-07-08T09:00:00.000Z"), // Wed
      stepDelays: [3],
    };
    expect(scheduleLead(lead, sat).map(key)).toEqual(["2026-07-11"]); // Sat, on the day
  });

  it("GAP constant is the documented 3 calendar days", () => {
    expect(STEP_GAP_CALENDAR_DAYS).toBe(3);
  });

  it("uses the REAL configured per-step delays, not the canonical gap", () => {
    // Contacted lead, last sent step 1 on Wed 07-01; config delay for step 1→2 is 7.
    const lead: PendingLead = {
      provisionedSteps: [2],
      lastSentStep: 1,
      lastSentAt: new Date("2026-07-01T09:00:00.000Z"), // Wed
      stepDelays: [7], // steps[0].delay — gap cost-step 1 → 2
    };
    // +7 cal days from Wed 07-01 = Wed 07-08 (weekday, no snap). NOT the canonical +3.
    expect(scheduleLead(lead, wed).map(key)).toEqual(["2026-07-08"]);
  });

  it("accumulates real per-gap delays across a fresh multi-step sequence", () => {
    // Fresh lead, steps 1,2,3; config delays 2 (1→2) then 3 (2→3), both weekday-safe.
    const lead: PendingLead = {
      provisionedSteps: [1, 2, 3],
      lastSentStep: null,
      lastSentAt: null,
      stepDelays: [2, 3], // steps[0].delay=2, steps[1].delay=3
    };
    const dates = scheduleLead(lead, new Date("2026-07-06T12:00:00.000Z")).map(key); // Mon
    expect(dates[0]).toBe("2026-07-06"); // step 1 today (Mon)
    expect(dates[1]).toBe("2026-07-08"); // +2 → Wed 07-08
    expect(dates[2]).toBe("2026-07-11"); // +2+3 = +5 → Sat 07-11, raw nominal day (no snap)
  });

  it("empty / missing stepDelays falls back to the canonical gap (config unavailable)", () => {
    const withEmpty: PendingLead = {
      provisionedSteps: [2],
      lastSentStep: 1,
      lastSentAt: new Date("2026-07-01T09:00:00.000Z"),
      stepDelays: [],
    };
    const withMissing: PendingLead = {
      provisionedSteps: [2],
      lastSentStep: 1,
      lastSentAt: new Date("2026-07-01T09:00:00.000Z"),
      // stepDelays omitted entirely
    };
    // Both fall back to +3 → Sat 07-04, kept on the raw nominal day (no snap).
    expect(scheduleLead(withEmpty, wed).map(key)).toEqual(["2026-07-04"]);
    expect(scheduleLead(withMissing, wed).map(key)).toEqual(["2026-07-04"]);
  });

  it("a null delay entry inside the array falls back per-gap without dropping the step", () => {
    const lead: PendingLead = {
      provisionedSteps: [2],
      lastSentStep: 1,
      lastSentAt: new Date("2026-07-01T09:00:00.000Z"),
      stepDelays: [null], // step 1→2 delay missing → per-gap fallback 3
    };
    expect(scheduleLead(lead, wed).map(key)).toEqual(["2026-07-04"]); // +3 Sat, raw nominal day
  });
});

describe("delayForGap", () => {
  it("indexes cost-step k → k+1 gap as stepDelays[k-1] (0-based config, 1-based cost)", () => {
    const delays = [3, 7, 5];
    expect(delayForGap(1, delays)).toBe(3); // 1→2 = steps[0]
    expect(delayForGap(2, delays)).toBe(7); // 2→3 = steps[1]
    expect(delayForGap(3, delays)).toBe(5); // 3→4 = steps[2]
  });

  it("falls back to STEP_GAP_CALENDAR_DAYS for missing / null / negative / non-finite delays", () => {
    expect(delayForGap(1, [])).toBe(STEP_GAP_CALENDAR_DAYS); // out of range
    expect(delayForGap(1, [null])).toBe(STEP_GAP_CALENDAR_DAYS);
    expect(delayForGap(1, [-2])).toBe(STEP_GAP_CALENDAR_DAYS);
    expect(delayForGap(1, [Number.NaN])).toBe(STEP_GAP_CALENDAR_DAYS);
  });

  it("honors a zero delay (same-day follow-up) — not treated as missing", () => {
    expect(delayForGap(1, [0])).toBe(0);
  });
});

describe("cadence coherence with the per-account queue breakdown", () => {
  // Both ops views derive the same NEXT-step nominal date from the same config
  // delay AND bucket it on the same raw nominal UTC day (no weekend snap on
  // either side), so a spot-checked sequence lands byte-equal.
  it("forecast next-step date == queue-breakdown projected next-send for the same sequence", () => {
    const asOf = new Date("2026-07-06T12:00:00.000Z"); // Mon
    const lastSentAt = new Date("2026-07-06T09:00:00.000Z"); // Mon
    const configDelay = 2; // steps[0].delay — lands Wed 07-08 (weekday)

    // Forecast side: contacted lead, next un-sent step 2.
    const lead: PendingLead = {
      provisionedSteps: [2],
      lastSentStep: 1,
      lastSentAt,
      stepDelays: [configDelay],
    };
    const forecastNext = scheduleLead(lead, asOf)[0];

    // Queue-breakdown side: same sequence, same config delay, next un-sent step 2.
    const seq: QueuedSequenceInput = {
      account: "a@x.com",
      lastSentStep: 1,
      lastSentAt,
      provisionedSteps: [2],
      stepDelays: [configDelay],
    };
    const breakdownProjected = projectStepDate(seq, 2);

    // Same nominal UTC day from the same shared delayForGap resolver.
    expect(dateKeyUTC(forecastNext)).toBe(dateKeyUTC(breakdownProjected));
    // And the breakdown classifies step 2 as a future (nextLater) send, consistent
    // with the forecast placing it two days out.
    expect(classifyQueuedStep(seq, 2, asOf)).toBe("nextLater");
  });
});

describe("projectDailySchedule", () => {
  const wed = new Date("2026-07-01T12:00:00.000Z");

  it("returns [] when there are no pending leads", () => {
    expect(projectDailySchedule([], wed)).toEqual([]);
  });

  it("buckets multiple leads per day, chronological, from today forward", () => {
    const leads: PendingLead[] = [
      { provisionedSteps: [1], lastSentStep: null, lastSentAt: null }, // today
      { provisionedSteps: [1], lastSentStep: null, lastSentAt: null }, // today
      {
        provisionedSteps: [2],
        lastSentStep: 1,
        lastSentAt: new Date("2026-07-01T09:00:00.000Z"),
      }, // +3 → Sat 07-04 (raw nominal day)
    ];
    const days = projectDailySchedule(leads, wed);
    expect(days[0]).toEqual({ date: "2026-07-01", scheduledCount: 2 });
    expect(days).toContainEqual({ date: "2026-07-04", scheduledCount: 1 });
    // chronological
    const keys = days.map((d) => d.date);
    expect([...keys].sort()).toEqual(keys);
  });

  it("horizon is bounded — total scheduled == total provisioned steps", () => {
    const leads: PendingLead[] = [
      { provisionedSteps: [1, 2, 3], lastSentStep: null, lastSentAt: null },
      { provisionedSteps: [2, 3], lastSentStep: 1, lastSentAt: new Date("2026-07-01T09:00:00.000Z") },
    ];
    const total = projectDailySchedule(leads, wed).reduce(
      (sum, d) => sum + d.scheduledCount,
      0,
    );
    expect(total).toBe(5); // 3 + 2 steps, all placed, none dropped, no tail
  });

  describe("contiguous zero-fill", () => {
    it("returns a gapless UTC-day series from asOf through the last scheduled day", () => {
      // step 1 today (Wed 07-01), step 2 +3 → Sat 07-04 (raw nominal day, no snap).
      const leads: PendingLead[] = [
        { provisionedSteps: [1, 2], lastSentStep: null, lastSentAt: null },
      ];
      const days = projectDailySchedule(leads, wed);
      expect(days.map((d) => d.date)).toEqual([
        "2026-07-01",
        "2026-07-02",
        "2026-07-03",
        "2026-07-04",
      ]);
      // no missing calendar days between first and last
      const step = 86_400_000;
      for (let i = 1; i < days.length; i++) {
        const prev = new Date(`${days[i - 1].date}T00:00:00.000Z`).getTime();
        const cur = new Date(`${days[i].date}T00:00:00.000Z`).getTime();
        expect(cur - prev).toBe(step);
      }
    });

    it("zero-fills a gap between two scheduled days", () => {
      const leads: PendingLead[] = [
        { provisionedSteps: [1, 2], lastSentStep: null, lastSentAt: null },
      ];
      const days = projectDailySchedule(leads, wed);
      // 07-01 and 07-04 carry sends; the days between are real zero bars.
      expect(days).toContainEqual({ date: "2026-07-01", scheduledCount: 1 });
      expect(days).toContainEqual({ date: "2026-07-02", scheduledCount: 0 });
      expect(days).toContainEqual({ date: "2026-07-03", scheduledCount: 0 });
      expect(days).toContainEqual({ date: "2026-07-04", scheduledCount: 1 });
    });

    it("a weekend day carries its REAL due-step count, not a forced 0", () => {
      // A step due on Sat 07-04 (Wed 07-01 last-send + 3-day gap) must show a
      // nonzero bar on Saturday — the whole point of dropping the weekend snap.
      const leads: PendingLead[] = [
        {
          provisionedSteps: [2],
          lastSentStep: 1,
          lastSentAt: new Date("2026-07-01T09:00:00.000Z"), // Wed
          stepDelays: [3],
        },
      ];
      const days = projectDailySchedule(leads, wed);
      const sat = days.find((d) => d.date === "2026-07-04"); // Saturday
      expect(sat).toEqual({ date: "2026-07-04", scheduledCount: 1 });
    });

    it("zero-fills a leading gap when the first scheduled day is after asOf", () => {
      // Contacted lead, next step +3 → Sat 07-04 (raw nominal); nothing on asOf (07-01).
      const leads: PendingLead[] = [
        {
          provisionedSteps: [2],
          lastSentStep: 1,
          lastSentAt: new Date("2026-07-01T09:00:00.000Z"),
        },
      ];
      const days = projectDailySchedule(leads, wed);
      expect(days[0]).toEqual({ date: "2026-07-01", scheduledCount: 0 });
      expect(days[days.length - 1]).toEqual({ date: "2026-07-04", scheduledCount: 1 });
      // every day from asOf through the send is present, all zero except the last
      expect(days.map((d) => d.date)).toEqual([
        "2026-07-01",
        "2026-07-02",
        "2026-07-03",
        "2026-07-04",
      ]);
    });

    it("empty input still returns [] (chart shows its empty state)", () => {
      expect(projectDailySchedule([], wed)).toEqual([]);
    });
  });
});

describe("computeCapacitySummary — the ramp follows the transport", () => {
  const lc = (status: string, sendTransport: string, vendorPrewarmedAt: Date | null = null) =>
    ({ status, sendTransport, vendorPrewarmedAt }) as never;

  it("ramps a mailbox WE dispatch: quiet ⇒ the floor, not its stated limit", () => {
    const summary = computeCapacitySummary(
      [{ email: "mine@a.com", daily_limit: 50 } as never],
      new Map([["mine@a.com", lc("in_production", "smtp")]]),
      new Map(),
    );
    expect(summary.dailyCapacity).toBe(5);
  });

  it("does NOT ramp a mailbox Instantly dispatches — it would contradict the selector", () => {
    // Measured 2026-09-13: eleven DFY mailboxes read 5/day here while the
    // selector offered them 50. The ops table reads the selector, never a
    // second derivation of it. See `rampAppliesToTransport`.
    const summary = computeCapacitySummary(
      [{ email: "dfy@a.com", daily_limit: 50 } as never],
      new Map([["dfy@a.com", lc("in_production", "instantly")]]),
      new Map(),
    );
    expect(summary.dailyCapacity).toBe(50);
  });

  it("still honours a lower operator-set limit on the un-ramped transport", () => {
    const summary = computeCapacitySummary(
      [{ email: "dfy@a.com", daily_limit: 20 } as never],
      new Map([["dfy@a.com", lc("in_production", "instantly")]]),
      new Map(),
    );
    expect(summary.dailyCapacity).toBe(20);
  });
});

describe("computeCapacitySummary — a mailbox the vendor pre-warmed", () => {
  const lc2 = (status: string, sendTransport: string, vendorPrewarmedAt: Date | null = null) =>
    ({ status, sendTransport, vendorPrewarmedAt }) as never;
  const PREWARMED = new Date("2026-08-13T00:00:00Z");
  const IMPORTED = "2026-09-13T11:26:21Z";
  const DAY_AFTER_IMPORT = new Date("2026-09-14T08:00:00Z");
  const LONG_AFTER_IMPORT = new Date("2026-10-23T08:00:00Z");

  const acct = (email: string, extra: Record<string, unknown> = {}) =>
    ({ email, daily_limit: 50, timestamp_created: IMPORTED, ...extra }) as never;

  it("reports its full limit before it has sent anything for us", () => {
    // 0 measured volume means we watched none of the vendor's month of warmup.
    // Ramping on it reported thirty 100%-inbox mailboxes at 5/day while the
    // selector offered 50 — the ops table contradicting the selector.
    const summary = computeCapacitySummary(
      [acct("new@a.com")],
      new Map([["new@a.com", lc2("in_production", "smtp", PREWARMED)]]),
      new Map(),
      new Map(),
      DAY_AFTER_IMPORT,
    );
    expect(summary.dailyCapacity).toBe(50);
  });

  it("KEEPS the full limit once it starts sending — no cliff on day two", () => {
    // The second-highest day of a two-day history is the SMALLER one, so a
    // mailbox that sent 14 seed+warmup on day one and 1 outreach on day two
    // reads sustained 1 and the old form floored it at 5.
    const summary = computeCapacitySummary(
      [acct("new@a.com")],
      new Map([["new@a.com", lc2("in_production", "smtp", PREWARMED)]]),
      new Map([["new@a.com", new Map([["2026-09-13", 14], ["2026-09-14", 1]])]]),
      new Map(),
      DAY_AFTER_IMPORT,
    );
    expect(summary.dailyCapacity).toBe(50);
  });

  it("rejoins the ramp once we have watched it for the maturity window", () => {
    const summary = computeCapacitySummary(
      [acct("new@a.com")],
      new Map([["new@a.com", lc2("in_production", "smtp", PREWARMED)]]),
      new Map([["new@a.com", new Map([["2026-10-20", 10], ["2026-10-21", 10]])]]),
      new Map(),
      LONG_AFTER_IMPORT,
    );
    expect(summary.dailyCapacity).toBe(15);
  });

  it("agrees with capForAccount for the same account — one rule, two readers", () => {
    // These two surfaces derive the same quantity; a disagreement is the staff
    // Audit page contradicting the selector about one mailbox. Subtract them.
    const account = acct("new@a.com");
    const volume = new Map([["new@a.com", new Map([["2026-09-13", 14], ["2026-09-14", 1]])]]);
    for (const asOf of [DAY_AFTER_IMPORT, LONG_AFTER_IMPORT]) {
      const summary = computeCapacitySummary(
        [account],
        new Map([["new@a.com", lc2("in_production", "smtp", PREWARMED)]]),
        volume,
        new Map(),
        asOf,
      );
      const selector = capForAccount(
        { daily_limit: 50, vendorPrewarmedAt: PREWARMED, timestamp_created: IMPORTED },
        1,
        "smtp",
        asOf,
      );
      expect(summary.dailyCapacity).toBe(selector);
    }
  });

  it("takes an alias group's OLDEST import, so a newer sibling cannot extend the exemption", () => {
    // One relay login, two aliases: the mailbox has been ours since the older
    // one arrived, whatever date the second carries.
    const summary = computeCapacitySummary(
      [
        acct("old@a.com", { timestamp_created: "2026-08-01T00:00:00Z" }),
        acct("new@a.com", { timestamp_created: "2026-10-20T00:00:00Z" }),
      ],
      new Map([
        ["old@a.com", lc2("in_production", "smtp", PREWARMED)],
        ["new@a.com", lc2("in_production", "smtp", PREWARMED)],
      ]),
      new Map([["old@a.com", new Map([["2026-10-20", 10], ["2026-10-21", 10]])]]),
      new Map([
        ["old@a.com", "relay@a.com"],
        ["new@a.com", "relay@a.com"],
      ]),
      LONG_AFTER_IMPORT,
    );
    // Watched since 08-01 ⇒ past the window ⇒ ramped on the mailbox's own volume.
    expect(summary.dailyCapacity).toBe(15);
  });

  it("still ramps a mailbox we did NOT buy pre-warmed", () => {
    const summary = computeCapacitySummary(
      [acct("own@a.com")],
      new Map([["own@a.com", lc2("in_production", "smtp", null)]]),
      new Map(),
      new Map(),
      DAY_AFTER_IMPORT,
    );
    expect(summary.dailyCapacity).toBe(5);
  });
});
