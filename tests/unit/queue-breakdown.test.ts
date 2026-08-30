import { describe, it, expect } from "vitest";
import {
  classifyQueuedStep,
  projectStepDate,
  aggregateQueueBreakdown,
  aggregateQueueCapacity,
  type QueuedSequenceInput,
} from "../../src/lib/queue-breakdown";
import { STEP_GAP_CALENDAR_DAYS } from "../../src/lib/sending-forecast";

const asOf = new Date("2026-07-11T12:00:00.000Z"); // a fixed "today" (UTC)
const DAY = 86_400_000;

/** Build a queued-sequence input, defaulting the fields a case doesn't set. */
function seq(over: Partial<QueuedSequenceInput> & { account: string }): QueuedSequenceInput {
  return {
    lastSentStep: null,
    lastSentAt: null,
    provisionedSteps: [],
    stepDelays: null,
    ...over,
  };
}

describe("projectStepDate — chains real per-step delays across every remaining step", () => {
  it("immediate next step = lastSentAt + steps[k-1].delay", () => {
    const s = seq({
      account: "a",
      lastSentStep: 1,
      lastSentAt: new Date("2026-07-01T00:00:00.000Z"),
      stepDelays: [3, 7], // step0.delay=3 (1→2), step1.delay=7 (2→3)
    });
    // step 2: hop 1→2 = steps[0].delay = 3 days.
    expect(projectStepDate(s, 2).toISOString().slice(0, 10)).toBe("2026-07-04");
  });

  it("a step two hops out SUMS both nominal gaps (compounding lower bound)", () => {
    const s = seq({
      account: "a",
      lastSentStep: 1,
      lastSentAt: new Date("2026-07-01T00:00:00.000Z"),
      stepDelays: [3, 7],
    });
    // step 3: hop 1→2 (3) + hop 2→3 (7) = 10 days off lastSentAt.
    expect(projectStepDate(s, 3).toISOString().slice(0, 10)).toBe("2026-07-11");
  });

  it("falls back to STEP_GAP_CALENDAR_DAYS per hop when a delay is missing", () => {
    const s = seq({
      account: "a",
      lastSentStep: 1,
      lastSentAt: new Date("2026-07-01T00:00:00.000Z"),
      stepDelays: null, // no config → each hop uses the canonical gap
    });
    // step 3: two hops × STEP_GAP_CALENDAR_DAYS.
    const expected = new Date("2026-07-01T00:00:00.000Z").getTime() + 2 * STEP_GAP_CALENDAR_DAYS * DAY;
    expect(projectStepDate(s, 3).getTime()).toBe(expected);
  });
});

describe("classifyQueuedStep", () => {
  it("firstUnsent for any step of a never-contacted sequence (no anchor)", () => {
    const s = seq({ account: "a", provisionedSteps: [1, 2, 3] });
    expect(classifyQueuedStep(s, 1, asOf)).toBe("firstUnsent");
    expect(classifyQueuedStep(s, 3, asOf)).toBe("firstUnsent");
  });

  it("nextToday when the projected step lands on today's UTC date", () => {
    const s = seq({
      account: "a",
      lastSentStep: 1,
      lastSentAt: new Date(asOf.getTime() - 3 * DAY),
      stepDelays: [3],
    });
    expect(classifyQueuedStep(s, 2, asOf)).toBe("nextToday");
  });

  it("nextToday when the projected step is OVERDUE (already in the past)", () => {
    const s = seq({
      account: "a",
      lastSentStep: 2,
      lastSentAt: new Date(asOf.getTime() - 10 * DAY),
      stepDelays: [3, 3],
    });
    // hop 2→3 = steps[1].delay = 3 → projected 7 days ago → overdue → today bucket.
    expect(classifyQueuedStep(s, 3, asOf)).toBe("nextToday");
  });

  it("nextTomorrow when the projected step lands on tomorrow's UTC date", () => {
    const s = seq({
      account: "a",
      lastSentStep: 1,
      lastSentAt: new Date(asOf.getTime()),
      stepDelays: [1],
    });
    expect(classifyQueuedStep(s, 2, asOf)).toBe("nextTomorrow");
  });

  it("nextLater when the projected step is after tomorrow", () => {
    const s = seq({
      account: "a",
      lastSentStep: 1,
      lastSentAt: new Date(asOf.getTime()),
      stepDelays: [7],
    });
    expect(classifyQueuedStep(s, 2, asOf)).toBe("nextLater");
  });
});

describe("aggregateQueueBreakdown — per-STEP partition", () => {
  it("the four buckets sum to queued STEPS (not sequences); sequences kept separate", () => {
    const rows: QueuedSequenceInput[] = [
      // never-contacted, 2 un-sent steps → both firstUnsent
      seq({ account: "a", provisionedSteps: [1, 2] }),
      // contacted at step 1, three remaining steps chained off 3/7/7 delays.
      // lastSentAt = 3d ago; step2 = +3 → today; step3 = +10 → later; step4 = +17 → later.
      seq({
        account: "a",
        lastSentStep: 1,
        lastSentAt: new Date(asOf.getTime() - 3 * DAY),
        provisionedSteps: [2, 3, 4],
        stepDelays: [3, 7, 7],
      }),
    ];
    const a = aggregateQueueBreakdown(rows, asOf).get("a")!;

    // 2 sequences, 5 total un-sent steps.
    expect(a.sequences).toBe(2);
    expect(a.steps).toBe(5);
    // 2 firstUnsent + 1 today (step2) + 0 tomorrow + 2 later (steps 3,4).
    expect(a).toEqual({
      sequences: 2,
      steps: 5,
      firstUnsent: 2,
      // ONE never-contacted sequence → one first email due, even though it
      // carries 2 un-sent steps. This is what send selection counts for today.
      firstUnsentSequences: 1,
      nextToday: 1,
      nextTomorrow: 0,
      nextOverdue: 0,
      nextLater: 2,
    });
    // The load-bearing invariant: buckets partition STEPS, not sequences.
    expect(a.firstUnsent + a.nextToday + a.nextTomorrow + a.nextLater).toBe(a.steps);
    expect(a.steps).not.toBe(a.sequences);
  });

  it("partitions every account independently; invariant holds for each", () => {
    const rows: QueuedSequenceInput[] = [
      seq({ account: "a", provisionedSteps: [1] }), // firstUnsent
      seq({
        account: "b",
        lastSentStep: 1,
        lastSentAt: new Date(asOf.getTime()),
        provisionedSteps: [2, 3],
        stepDelays: [1, 9], // step2 → tomorrow; step3 → +10 → later
      }),
    ];
    const map = aggregateQueueBreakdown(rows, asOf);

    const a = map.get("a")!;
    expect(a).toEqual({ sequences: 1, steps: 1, firstUnsent: 1, firstUnsentSequences: 1, nextToday: 0, nextOverdue: 0, nextTomorrow: 0, nextLater: 0 });

    const b = map.get("b")!;
    expect(b).toEqual({ sequences: 1, steps: 2, firstUnsent: 0, firstUnsentSequences: 0, nextToday: 0, nextOverdue: 0, nextTomorrow: 1, nextLater: 1 });
    expect(b.firstUnsent + b.nextToday + b.nextTomorrow + b.nextLater).toBe(b.steps);
  });

  it("skips rows with no account (unattributable — never fabricated)", () => {
    const rows: QueuedSequenceInput[] = [seq({ account: "", provisionedSteps: [1] })];
    expect(aggregateQueueBreakdown(rows, asOf).size).toBe(0);
  });
});

describe("aggregateQueueCapacity — books every step on the day it will really leave", () => {
  // A weekday "today" (Monday 2026-08-31, 09:00 in Chicago), so nothing snaps
  // unless the projection genuinely lands on a weekend.
  const weekday = new Date("2026-08-31T14:00:00.000Z");
  const CHICAGO = "America/Chicago";

  it("projects a NEVER-CONTACTED sequence's followups onto FUTURE days, not today", () => {
    // This is the whole point of the divergence from the ops breakdown: the
    // breakdown refuses to date these (it has no send anchor), but selection has
    // just decided this lead sends imminently, so its D+3 / D+10 are real days
    // the mailbox owes and must be booked.
    const rows: QueuedSequenceInput[] = [
      seq({
        account: "a",
        provisionedSteps: [1, 2, 3],
        stepDelays: [3, 7],
        timezone: CHICAGO,
      }),
    ];
    expect(aggregateQueueCapacity(rows, weekday).get("a")).toEqual({
      byDay: { "2026-08-31": 1, "2026-09-03": 1, "2026-09-10": 1 },
    });
  });

  it("books a CONTACTED sequence's remaining steps off its real last send", () => {
    const rows: QueuedSequenceInput[] = [
      // Sent step 1 three days ago; delays [3,7] → step 2 today, step 3 in a week.
      seq({
        account: "a",
        lastSentStep: 1,
        lastSentAt: new Date(weekday.getTime() - 3 * DAY),
        provisionedSteps: [2, 3],
        stepDelays: [3, 7],
        timezone: CHICAGO,
      }),
    ];
    expect(aggregateQueueCapacity(rows, weekday).get("a")).toEqual({
      byDay: { "2026-08-31": 1, "2026-09-07": 1 },
    });
  });

  it("books an OVERDUE step on today, never in the past", () => {
    const rows: QueuedSequenceInput[] = [
      // Nominally due a week ago and never dispatched — it competes for TODAY's
      // capacity, which is what the breakdown's today-or-overdue bucket says too.
      seq({
        account: "a",
        lastSentStep: 1,
        lastSentAt: new Date(weekday.getTime() - 10 * DAY),
        provisionedSteps: [2],
        stepDelays: [3],
        timezone: CHICAGO,
      }),
    ];
    expect(aggregateQueueCapacity(rows, weekday).get("a")).toEqual({
      byDay: { "2026-08-31": 1 },
    });
  });

  it("books a New Zealand lead's local MONDAY on the mailbox's SUNDAY", () => {
    // Sunday 2026-08-30 06:00Z is Sunday evening in Auckland, so the next open
    // window is Monday 08:00 local = Sunday 20:00 UTC. The mailbox spends its
    // SUNDAY on this send; counting it on Monday would over-book Monday and hand
    // out a Sunday slot nothing consumes.
    const sunday = new Date("2026-08-30T06:00:00.000Z");
    const rows: QueuedSequenceInput[] = [
      seq({ account: "a", provisionedSteps: [1], timezone: "Pacific/Auckland" }),
      seq({ account: "b", provisionedSteps: [1], timezone: "America/Chicago" }),
    ];
    const map = aggregateQueueCapacity(rows, sunday);
    expect(map.get("a")).toEqual({ byDay: { "2026-08-30": 1 } });
    // The US lead's own window does not open until Monday local, which is Monday
    // UTC — same step, different day, purely because of where the prospect is.
    expect(map.get("b")).toEqual({ byDay: { "2026-08-31": 1 } });
  });

  it("falls back to the fleet default timezone rather than guessing", () => {
    const withNull = aggregateQueueCapacity(
      [seq({ account: "a", provisionedSteps: [1, 2], stepDelays: [3], timezone: null })],
      weekday,
    );
    const withDefault = aggregateQueueCapacity(
      [seq({ account: "a", provisionedSteps: [1, 2], stepDelays: [3], timezone: CHICAGO })],
      weekday,
    );
    expect(withNull.get("a")).toEqual(withDefault.get("a"));
  });

  it("drops a step booked past the horizon — an unprojected day reads as room", () => {
    const rows: QueuedSequenceInput[] = [
      seq({
        account: "a",
        provisionedSteps: [1, 2],
        stepDelays: [90], // far beyond CAPACITY_HORIZON_DAYS
        timezone: CHICAGO,
      }),
    ];
    expect(aggregateQueueCapacity(rows, weekday).get("a")).toEqual({
      byDay: { "2026-08-31": 1 },
    });
  });

  it("sums several sequences onto the same day", () => {
    const rows: QueuedSequenceInput[] = [
      seq({ account: "a", provisionedSteps: [1], timezone: CHICAGO }),
      seq({ account: "a", provisionedSteps: [1], timezone: CHICAGO }),
      seq({ account: "b", provisionedSteps: [1], timezone: CHICAGO }),
    ];
    const map = aggregateQueueCapacity(rows, weekday);
    expect(map.get("a")).toEqual({ byDay: { "2026-08-31": 2 } });
    expect(map.get("b")).toEqual({ byDay: { "2026-08-31": 1 } });
  });

  it("skips rows with no account (unattributable — never fabricated)", () => {
    const rows: QueuedSequenceInput[] = [seq({ account: "", provisionedSteps: [1] })];
    expect(aggregateQueueCapacity(rows, asOf).size).toBe(0);
  });

  // This runs on the SEND path (behind a 60s cache email-gateway abandons after
  // ~10s, then retries), over the whole fleet's queue — ~6,000 sequences in prod.
  // The first version took ~9 SECONDS there, because `canonicalIanaTimezone`
  // builds a fresh Intl.DateTimeFormat on every primary zone name. The bound is
  // ~20x the measured 86ms, so it is not a flaky timing assertion — it fails
  // only if per-call Intl construction comes back.
  it("stays fast at fleet scale — a per-call Intl construction would blow this", () => {
    const zones = [
      "America/Chicago",
      "America/Detroit",
      "America/Dawson",
      "Europe/Belgrade",
      "Asia/Kolkata",
      "Pacific/Auckland",
    ];
    const rows: QueuedSequenceInput[] = Array.from({ length: 6000 }, (_, i) =>
      seq({
        account: `acct${i % 25}@x.com`,
        lastSentStep: i % 3 === 0 ? null : 1,
        lastSentAt: i % 3 === 0 ? null : new Date(weekday.getTime() - 3 * DAY),
        provisionedSteps: i % 3 === 0 ? [1, 2, 3] : [2, 3],
        stepDelays: [3, 7],
        timezone: zones[i % zones.length],
      }),
    );
    const started = Date.now();
    expect(aggregateQueueCapacity(rows, weekday).size).toBe(25);
    expect(Date.now() - started).toBeLessThan(2000);
  });
});

describe("nextOverdue — the BACKLOG subset of nextToday", () => {
  it("counts only steps nominally due on a STRICTLY EARLIER UTC day", () => {
    const rows: QueuedSequenceInput[] = [
      // lastSentAt 10 days ago with 3/3/3 delays: step2 = -7d, step3 = -4d,
      // step4 = -1d → all three overdue. step5 = +2d → later.
      seq({
        account: "a",
        lastSentStep: 1,
        lastSentAt: new Date(asOf.getTime() - 10 * DAY),
        provisionedSteps: [2, 3, 4, 5],
        stepDelays: [3, 3, 3, 3],
      }),
    ];
    const a = aggregateQueueBreakdown(rows, asOf).get("a")!;
    expect(a.nextToday).toBe(3);
    expect(a.nextOverdue).toBe(3);
    expect(a.nextLater).toBe(1);
  });

  it("does NOT count a step due EXACTLY today — due is not the same as behind", () => {
    const rows: QueuedSequenceInput[] = [
      seq({
        account: "a",
        lastSentStep: 1,
        lastSentAt: new Date(asOf.getTime() - 3 * DAY),
        provisionedSteps: [2],
        stepDelays: [3],
      }),
    ];
    const a = aggregateQueueBreakdown(rows, asOf).get("a")!;
    expect(a.nextToday).toBe(1);
    expect(a.nextOverdue).toBe(0);
  });

  it("never counts a never-contacted sequence as overdue (no anchor to be late against)", () => {
    const rows: QueuedSequenceInput[] = [
      seq({ account: "a", provisionedSteps: [1, 2, 3] }),
    ];
    const a = aggregateQueueBreakdown(rows, asOf).get("a")!;
    expect(a.firstUnsent).toBe(3);
    expect(a.nextOverdue).toBe(0);
  });

  it("is a SUBSET counter — it never breaks the four-way step partition", () => {
    const rows: QueuedSequenceInput[] = [
      seq({ account: "a", provisionedSteps: [1, 2] }),
      seq({
        account: "a",
        lastSentStep: 1,
        lastSentAt: new Date(asOf.getTime() - 12 * DAY),
        provisionedSteps: [2, 3, 4],
        stepDelays: [3, 3, 20],
      }),
    ];
    const a = aggregateQueueBreakdown(rows, asOf).get("a")!;
    expect(a.firstUnsent + a.nextToday + a.nextTomorrow + a.nextLater).toBe(a.steps);
    expect(a.nextOverdue).toBeLessThanOrEqual(a.nextToday);
    expect(a.nextOverdue).toBeGreaterThan(0);
  });
});
