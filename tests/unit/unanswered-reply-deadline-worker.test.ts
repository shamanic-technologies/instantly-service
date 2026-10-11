import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const mockSweep = vi.fn();
vi.mock("../../src/lib/unanswered-reply-deadline", () => ({
  askClientsAboutUnansweredReplies: (...args: unknown[]) => mockSweep(...args),
}));

const { startUnansweredReplyDeadlineWorker, UNANSWERED_REPLY_DEADLINE_FIRST_TICK_MS, UNANSWERED_REPLY_DEADLINE_INTERVAL_MS } =
  await import("../../src/lib/unanswered-reply-deadline-worker");

beforeEach(() => vi.useFakeTimers());
afterEach(() => vi.useRealTimers());

describe("unanswered-reply deadline worker", () => {
  it("first pass ~60 s after boot, then every 15 minutes, survives a failed pass", async () => {
    mockSweep.mockRejectedValueOnce(new Error("db down")).mockResolvedValue({ unanswered: 0, asked: 0 });
    startUnansweredReplyDeadlineWorker();
    expect(mockSweep).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(UNANSWERED_REPLY_DEADLINE_FIRST_TICK_MS);
    expect(mockSweep).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(UNANSWERED_REPLY_DEADLINE_INTERVAL_MS);
    expect(mockSweep).toHaveBeenCalledTimes(2);
  });
});
