import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const mockProcessRow = vi.fn();
const mockSelectOneStuckRow = vi.fn();

vi.mock("../../src/lib/retry-stuck", () => ({
  processRow: (...args: unknown[]) => mockProcessRow(...args),
  selectOneStuckRow: (...args: unknown[]) => mockSelectOneStuckRow(...args),
}));

import {
  startRetryStuckWorker,
  stopRetryStuckWorker,
  isRetryStuckWorkerRunning,
  RETRY_STUCK_IDLE_SLEEP_MS,
} from "../../src/lib/retry-stuck-worker";

describe("retry-stuck worker — continuous loop", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    mockProcessRow.mockReset();
    mockSelectOneStuckRow.mockReset();
    mockProcessRow.mockResolvedValue({ kind: "redispatched" });
    mockSelectOneStuckRow.mockResolvedValue(null);
  });

  afterEach(async () => {
    stopRetryStuckWorker();
    // Let any in-flight loop iteration observe shouldStop=true.
    await vi.runAllTimersAsync().catch(() => {});
    vi.useRealTimers();
  });

  it("processes rows back-to-back without waiting between them", async () => {
    let i = 0;
    mockSelectOneStuckRow.mockImplementation(async () => {
      i++;
      if (i <= 3) return { id: `row-${i}` };
      return null;
    });

    startRetryStuckWorker();

    // Drain microtasks — no timer involved between rows.
    for (let k = 0; k < 50; k++) {
      await Promise.resolve();
    }

    expect(mockProcessRow).toHaveBeenCalledTimes(3);
    expect(mockProcessRow.mock.calls[0][0].id).toBe("row-1");
    expect(mockProcessRow.mock.calls[1][0].id).toBe("row-2");
    expect(mockProcessRow.mock.calls[2][0].id).toBe("row-3");
  });

  it("sleeps RETRY_STUCK_IDLE_SLEEP_MS when SELECT returns null, then re-checks", async () => {
    mockSelectOneStuckRow
      .mockResolvedValueOnce(null)
      .mockResolvedValueOnce({ id: "row-1" })
      .mockResolvedValue(null);

    startRetryStuckWorker();

    // First select resolves to null — loop enters sleep.
    for (let k = 0; k < 10; k++) await Promise.resolve();
    expect(mockProcessRow).not.toHaveBeenCalled();

    // Advance past the idle sleep → loop wakes, sees row-1, processes it.
    await vi.advanceTimersByTimeAsync(RETRY_STUCK_IDLE_SLEEP_MS + 1);
    for (let k = 0; k < 10; k++) await Promise.resolve();
    expect(mockProcessRow).toHaveBeenCalledTimes(1);
    expect(mockProcessRow.mock.calls[0][0].id).toBe("row-1");
  });

  it("isRetryStuckWorkerRunning reflects start/stop state", async () => {
    expect(isRetryStuckWorkerRunning()).toBe(false);
    startRetryStuckWorker();
    expect(isRetryStuckWorkerRunning()).toBe(true);

    stopRetryStuckWorker();
    // Let the loop iteration observe shouldStop.
    await vi.advanceTimersByTimeAsync(RETRY_STUCK_IDLE_SLEEP_MS + 1);
    for (let k = 0; k < 10; k++) await Promise.resolve();
    expect(isRetryStuckWorkerRunning()).toBe(false);
  });

  it("calling start twice is a no-op (no parallel loops)", async () => {
    mockSelectOneStuckRow.mockResolvedValue({ id: "row-1" });
    mockProcessRow.mockImplementation(async () => {
      // Slow processor so we can observe whether two loops fired in parallel.
      await new Promise((r) => setTimeout(r, 100));
      return { kind: "redispatched" };
    });

    startRetryStuckWorker();
    startRetryStuckWorker();

    // Drain to land in the awaited processRow call.
    for (let k = 0; k < 5; k++) await Promise.resolve();
    await vi.advanceTimersByTimeAsync(50);

    // Only one in-flight call (not two).
    expect(mockProcessRow).toHaveBeenCalledTimes(1);
  });

  it("loop keeps going when selectOneStuckRow throws — sleeps then retries", async () => {
    const errSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    mockSelectOneStuckRow
      .mockRejectedValueOnce(new Error("DB blip"))
      .mockResolvedValueOnce({ id: "row-1" })
      .mockResolvedValue(null);

    startRetryStuckWorker();

    for (let k = 0; k < 10; k++) await Promise.resolve();
    expect(errSpy).toHaveBeenCalled();
    expect(mockProcessRow).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(RETRY_STUCK_IDLE_SLEEP_MS + 1);
    for (let k = 0; k < 10; k++) await Promise.resolve();

    expect(mockProcessRow).toHaveBeenCalledTimes(1);
    errSpy.mockRestore();
  });

  it("RETRY_STUCK_IDLE_SLEEP_MS defaults to 60_000 ms (1 minute)", () => {
    expect(RETRY_STUCK_IDLE_SLEEP_MS).toBe(60 * 1000);
  });
});
