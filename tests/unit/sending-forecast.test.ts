import { describe, it, expect } from "vitest";
import type { Account } from "../../src/lib/instantly-client";
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

describe("computeCapacitySummary", () => {
  it("sums daily_limit over ONLY healthy accounts; counts totals + blocked", () => {
    const accounts: Account[] = [
      acct({ email: "a@good.com", daily_limit: 30 }), // healthy
      acct({ email: "b@good.com", daily_limit: 20 }), // healthy
      acct({ email: "c@good.com", stat_warmup_score: 80, daily_limit: 50 }), // under-warmed → excluded
      acct({ email: "d@good.com", status: 0, daily_limit: 50 }), // inactive → excluded
      acct({ email: "e@distribute.you", daily_limit: 40 }), // blocked domain → excluded
      acct({ email: "f@arcadiaquest.org", daily_limit: 40 }), // blocked domain → excluded
    ];
    const s = computeCapacitySummary(accounts);
    expect(s.dailyCapacity).toBe(50); // 30 + 20 only
    expect(s.healthyAccountCount).toBe(2);
    expect(s.totalAccountCount).toBe(6);
    expect(s.blockedDomainCount).toBe(2);
  });

  it("healthy account missing daily_limit contributes 0 (no fabricated number)", () => {
    const s = computeCapacitySummary([
      acct({ email: "a@good.com", daily_limit: undefined }),
      acct({ email: "b@good.com", daily_limit: 15 }),
    ]);
    expect(s.dailyCapacity).toBe(15);
    expect(s.healthyAccountCount).toBe(2);
  });

  it("empty fleet → all zeros (real number, blocked ≤ total)", () => {
    const s = computeCapacitySummary([]);
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
});
