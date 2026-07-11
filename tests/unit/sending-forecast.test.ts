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
    const s = computeCapacitySummary(accounts, lifecycle);
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
    );
    expect(s.dailyCapacity).toBe(15);
    expect(s.healthyAccountCount).toBe(2);
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
