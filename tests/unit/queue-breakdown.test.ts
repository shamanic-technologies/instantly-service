import { describe, it, expect } from "vitest";
import {
  classifyQueuedSequence,
  aggregateQueueBreakdown,
  type QueuedSequenceInput,
} from "../../src/lib/queue-breakdown";
import { STEP_GAP_CALENDAR_DAYS } from "../../src/lib/sending-forecast";

const asOf = new Date("2026-07-11T12:00:00.000Z"); // a fixed "today" (UTC)
const DAY = 86_400_000;

describe("classifyQueuedSequence", () => {
  it("firstUnsent when nothing has been sent (lastSentStep/lastSentAt null)", () => {
    expect(
      classifyQueuedSequence(
        { account: "a", lastSentStep: null, lastSentAt: null, nextDelayDays: 3 },
        asOf,
      ),
    ).toBe("firstUnsent");
  });

  it("nextToday when the projected next step lands on today's UTC date", () => {
    // sent 3 days ago, delay 3 → projected today.
    const lastSentAt = new Date(asOf.getTime() - 3 * DAY);
    expect(
      classifyQueuedSequence({ account: "a", lastSentStep: 1, lastSentAt, nextDelayDays: 3 }, asOf),
    ).toBe("nextToday");
  });

  it("nextToday when the projected date is OVERDUE (already in the past)", () => {
    // sent 10 days ago, delay 3 → projected 7 days ago → due/overdue → today bucket.
    const lastSentAt = new Date(asOf.getTime() - 10 * DAY);
    expect(
      classifyQueuedSequence({ account: "a", lastSentStep: 2, lastSentAt, nextDelayDays: 3 }, asOf),
    ).toBe("nextToday");
  });

  it("nextTomorrow when the projected next step lands on tomorrow's UTC date", () => {
    // sent today, delay 1 → projected tomorrow.
    const lastSentAt = new Date(asOf.getTime());
    expect(
      classifyQueuedSequence({ account: "a", lastSentStep: 1, lastSentAt, nextDelayDays: 1 }, asOf),
    ).toBe("nextTomorrow");
  });

  it("nextLater when the projected next step is after tomorrow", () => {
    const lastSentAt = new Date(asOf.getTime());
    expect(
      classifyQueuedSequence({ account: "a", lastSentStep: 1, lastSentAt, nextDelayDays: 7 }, asOf),
    ).toBe("nextLater");
  });

  it("falls back to STEP_GAP_CALENDAR_DAYS when nextDelayDays is null (config missing)", () => {
    // sent today, delay unknown → uses the canonical gap (3) → later than tomorrow.
    const lastSentAt = new Date(asOf.getTime());
    const bucket = classifyQueuedSequence(
      { account: "a", lastSentStep: 1, lastSentAt, nextDelayDays: null },
      asOf,
    );
    // Sanity: with the fallback gap > 1, a just-sent sequence is not today/tomorrow.
    expect(STEP_GAP_CALENDAR_DAYS).toBeGreaterThan(1);
    expect(bucket).toBe("nextLater");
  });
});

describe("aggregateQueueBreakdown", () => {
  it("partitions each account's sequences — Qtotal == sum of the four buckets", () => {
    const rows: QueuedSequenceInput[] = [
      { account: "a", lastSentStep: null, lastSentAt: null, nextDelayDays: null }, // firstUnsent
      { account: "a", lastSentStep: 1, lastSentAt: new Date(asOf.getTime() - 3 * DAY), nextDelayDays: 3 }, // nextToday
      { account: "a", lastSentStep: 1, lastSentAt: new Date(asOf.getTime()), nextDelayDays: 1 }, // nextTomorrow
      { account: "a", lastSentStep: 1, lastSentAt: new Date(asOf.getTime()), nextDelayDays: 9 }, // nextLater
      { account: "b", lastSentStep: null, lastSentAt: null, nextDelayDays: null }, // firstUnsent (b)
    ];
    const map = aggregateQueueBreakdown(rows, asOf);

    const a = map.get("a")!;
    expect(a).toEqual({ sequences: 4, firstUnsent: 1, nextToday: 1, nextTomorrow: 1, nextLater: 1 });
    expect(a.firstUnsent + a.nextToday + a.nextTomorrow + a.nextLater).toBe(a.sequences);

    const b = map.get("b")!;
    expect(b).toEqual({ sequences: 1, firstUnsent: 1, nextToday: 0, nextTomorrow: 0, nextLater: 0 });
  });

  it("skips rows with no account (unattributable — never fabricated)", () => {
    const rows: QueuedSequenceInput[] = [
      { account: "", lastSentStep: null, lastSentAt: null, nextDelayDays: null },
    ];
    expect(aggregateQueueBreakdown(rows, asOf).size).toBe(0);
  });
});
