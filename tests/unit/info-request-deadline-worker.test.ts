import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const mockSweep = vi.fn();
vi.mock("../../src/lib/info-request-deadline", () => ({
  alertOverdueInfoRequests: (...args: unknown[]) => mockSweep(...args),
}));

const { startInfoRequestDeadlineWorker, INFO_REQUEST_DEADLINE_FIRST_TICK_MS, INFO_REQUEST_DEADLINE_INTERVAL_MS } =
  await import("../../src/lib/info-request-deadline-worker");

beforeEach(() => vi.useFakeTimers());
afterEach(() => vi.useRealTimers());

describe("info-request deadline worker", () => {
  it("first pass ~60 s after boot, then every 15 minutes, survives a failed pass", async () => {
    mockSweep.mockRejectedValueOnce(new Error("db down")).mockResolvedValue({ overdue: 0, alerted: 0 });
    startInfoRequestDeadlineWorker();
    expect(mockSweep).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(INFO_REQUEST_DEADLINE_FIRST_TICK_MS);
    expect(mockSweep).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(INFO_REQUEST_DEADLINE_INTERVAL_MS);
    expect(mockSweep).toHaveBeenCalledTimes(2);
  });
});
