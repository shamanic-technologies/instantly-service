import { describe, it, expect } from "vitest";
import type { Account } from "../../src/lib/instantly-client";
import type { LifecycleView } from "../../src/lib/account-lifecycle-sync";
import {
  computeCapacitySummary,
  projectDailySchedule,
  scheduleLead,
  STEP_GAP_CALENDAR_DAYS,
  type PendingLead,
} from "../../src/lib/sending-forecast";

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

  it("never-contacted lead: first pending step fires ~today, next +GAP", () => {
    const lead: PendingLead = {
      provisionedSteps: [1, 2],
      lastSentStep: null,
      lastSentAt: null,
    };
    const dates = scheduleLead(lead, wed).map(key);
    expect(dates[0]).toBe("2026-07-01"); // step 1 today (Wed)
    // +3 calendar days from Wed = Sat 07-04 → snaps forward to Mon 07-06
    expect(dates[1]).toBe("2026-07-06");
  });

  it("contacted lead: next step is GAP business-days after the last sent step", () => {
    const lead: PendingLead = {
      provisionedSteps: [2],
      lastSentStep: 1,
      lastSentAt: new Date("2026-07-01T09:00:00.000Z"), // Wed
    };
    // +3 cal days = Sat → snaps to Mon 07-06
    expect(scheduleLead(lead, wed).map(key)).toEqual(["2026-07-06"]);
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

  it("weekend landings always snap forward to a weekday", () => {
    const lead: PendingLead = {
      provisionedSteps: [1, 2, 3, 4],
      lastSentStep: null,
      lastSentAt: null,
    };
    for (const d of scheduleLead(lead, wed)) {
      const dow = d.getUTCDay();
      expect(dow).not.toBe(0); // never Sunday
      expect(dow).not.toBe(6); // never Saturday
    }
  });

  it("GAP constant is the documented 3 calendar days", () => {
    expect(STEP_GAP_CALENDAR_DAYS).toBe(3);
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
      }, // +3 → Mon 07-06
    ];
    const days = projectDailySchedule(leads, wed);
    expect(days[0]).toEqual({ date: "2026-07-01", scheduledCount: 2 });
    expect(days).toContainEqual({ date: "2026-07-06", scheduledCount: 1 });
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
      // step 1 today (Wed 07-01), step 2 +3 → Sat 07-04 snaps to Mon 07-06.
      const leads: PendingLead[] = [
        { provisionedSteps: [1, 2], lastSentStep: null, lastSentAt: null },
      ];
      const days = projectDailySchedule(leads, wed);
      expect(days.map((d) => d.date)).toEqual([
        "2026-07-01",
        "2026-07-02",
        "2026-07-03",
        "2026-07-04",
        "2026-07-05",
        "2026-07-06",
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
      // 07-01 and 07-06 carry sends; the days between are real zero bars.
      expect(days).toContainEqual({ date: "2026-07-01", scheduledCount: 1 });
      expect(days).toContainEqual({ date: "2026-07-02", scheduledCount: 0 });
      expect(days).toContainEqual({ date: "2026-07-03", scheduledCount: 0 });
      expect(days).toContainEqual({ date: "2026-07-06", scheduledCount: 1 });
    });

    it("includes weekend days inside the range with count 0", () => {
      const leads: PendingLead[] = [
        { provisionedSteps: [1, 2], lastSentStep: null, lastSentAt: null },
      ];
      const days = projectDailySchedule(leads, wed);
      // Sat 07-04 + Sun 07-05 present as zero bars (projection snaps off them).
      const sat = days.find((d) => d.date === "2026-07-04");
      const sun = days.find((d) => d.date === "2026-07-05");
      expect(sat).toEqual({ date: "2026-07-04", scheduledCount: 0 });
      expect(sun).toEqual({ date: "2026-07-05", scheduledCount: 0 });
    });

    it("zero-fills a leading gap when the first scheduled day is after asOf", () => {
      // Contacted lead, next step +3 → Mon 07-06; nothing lands on asOf (07-01).
      const leads: PendingLead[] = [
        {
          provisionedSteps: [2],
          lastSentStep: 1,
          lastSentAt: new Date("2026-07-01T09:00:00.000Z"),
        },
      ];
      const days = projectDailySchedule(leads, wed);
      expect(days[0]).toEqual({ date: "2026-07-01", scheduledCount: 0 });
      expect(days[days.length - 1]).toEqual({ date: "2026-07-06", scheduledCount: 1 });
      // every day from asOf through the send is present, all zero except the last
      expect(days.map((d) => d.date)).toEqual([
        "2026-07-01",
        "2026-07-02",
        "2026-07-03",
        "2026-07-04",
        "2026-07-05",
        "2026-07-06",
      ]);
    });

    it("empty input still returns [] (chart shows its empty state)", () => {
      expect(projectDailySchedule([], wed)).toEqual([]);
    });
  });
});
