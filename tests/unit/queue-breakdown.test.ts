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
      nextToday: 1,
      nextTomorrow: 0,
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
    expect(a).toEqual({ sequences: 1, steps: 1, firstUnsent: 1, nextToday: 0, nextTomorrow: 0, nextLater: 0 });

    const b = map.get("b")!;
    expect(b).toEqual({ sequences: 1, steps: 2, firstUnsent: 0, nextToday: 0, nextTomorrow: 1, nextLater: 1 });
    expect(b.firstUnsent + b.nextToday + b.nextTomorrow + b.nextLater).toBe(b.steps);
  });

  it("skips rows with no account (unattributable — never fabricated)", () => {
    const rows: QueuedSequenceInput[] = [seq({ account: "", provisionedSteps: [1] })];
    expect(aggregateQueueBreakdown(rows, asOf).size).toBe(0);
  });
});

describe("aggregateQueueCapacity — send-selection buckets", () => {
  it("counts never-contacted SEQUENCES once (q0first), steps by day, totalQueue = all steps", () => {
    const rows: QueuedSequenceInput[] = [
      // never-contacted, 3 un-sent steps → q0first 1 (NOT 3), totalQueue +3.
      seq({ account: "a", provisionedSteps: [1, 2, 3] }),
      // contacted 3d ago at step 1; steps 2,3; delays [3,7]:
      // step2 = +3 → today (q0next), step3 = +10 → later. totalQueue +2.
      seq({
        account: "a",
        lastSentStep: 1,
        lastSentAt: new Date(asOf.getTime() - 3 * 86_400_000),
        provisionedSteps: [2, 3],
        stepDelays: [3, 7],
      }),
      // contacted today at step 1; step 2 queued; delay [1] → tomorrow. totalQueue +1.
      seq({
        account: "b",
        lastSentStep: 1,
        lastSentAt: new Date(asOf.getTime()),
        provisionedSteps: [2],
        stepDelays: [1],
      }),
    ];
    const map = aggregateQueueCapacity(rows, asOf);
    expect(map.get("a")).toEqual({ q0first: 1, q0next: 1, q1next: 0, totalQueue: 5 });
    expect(map.get("b")).toEqual({ q0first: 0, q0next: 0, q1next: 1, totalQueue: 1 });
  });

  it("skips rows with no account (unattributable — never fabricated)", () => {
    const rows: QueuedSequenceInput[] = [seq({ account: "", provisionedSteps: [1] })];
    expect(aggregateQueueCapacity(rows, asOf).size).toBe(0);
  });
});
