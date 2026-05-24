import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const mockRunRetryStuck = vi.fn();

vi.mock("../../src/lib/retry-stuck", () => ({
  runRetryStuck: (...args: unknown[]) => mockRunRetryStuck(...args),
}));

import {
  startRetryStuckWorker,
  stopRetryStuckWorker,
  RETRY_STUCK_TICK_INTERVAL_MS,
} from "../../src/lib/retry-stuck-worker";

describe("RETRY_STUCK_TICK_INTERVAL_MS default", () => {
  it("defaults to 15 minutes (longer than the observed ~13min worst-case tick duration)", () => {
    // The worker uses setInterval, so the interval must exceed the worst-case
    // tick wall-clock to avoid adjacent ticks short-circuiting on the
    // advisory lock. Prod observation 2026-05-24: tick duration 764s ≈ 12.7min.
    expect(RETRY_STUCK_TICK_INTERVAL_MS).toBe(15 * 60 * 1000);
  });
});

describe("retry-stuck worker", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    mockRunRetryStuck.mockReset();
    mockRunRetryStuck.mockResolvedValue({
      scanned: 0,
      redispatched: 0,
      failed: 0,
      skippedNoKey: 0,
      durationMs: 1,
    });
  });

  afterEach(() => {
    stopRetryStuckWorker();
    vi.useRealTimers();
  });

  it("does not fire runRetryStuck immediately on start (port-bind first)", async () => {
    startRetryStuckWorker();

    // Advance microtasks but no clock progression.
    await Promise.resolve();
    expect(mockRunRetryStuck).not.toHaveBeenCalled();
  });

  it("fires runRetryStuck once per tick interval", async () => {
    startRetryStuckWorker();

    await vi.advanceTimersByTimeAsync(RETRY_STUCK_TICK_INTERVAL_MS);
    expect(mockRunRetryStuck).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(RETRY_STUCK_TICK_INTERVAL_MS);
    expect(mockRunRetryStuck).toHaveBeenCalledTimes(2);

    await vi.advanceTimersByTimeAsync(RETRY_STUCK_TICK_INTERVAL_MS);
    expect(mockRunRetryStuck).toHaveBeenCalledTimes(3);
  });

  it("stopRetryStuckWorker halts further ticks", async () => {
    startRetryStuckWorker();

    await vi.advanceTimersByTimeAsync(RETRY_STUCK_TICK_INTERVAL_MS);
    expect(mockRunRetryStuck).toHaveBeenCalledTimes(1);

    stopRetryStuckWorker();

    await vi.advanceTimersByTimeAsync(RETRY_STUCK_TICK_INTERVAL_MS * 3);
    expect(mockRunRetryStuck).toHaveBeenCalledTimes(1);
  });

  it("a tick that throws does not crash the loop — next tick still fires", async () => {
    mockRunRetryStuck
      .mockRejectedValueOnce(new Error("boom"))
      .mockResolvedValueOnce({
        scanned: 1,
        redispatched: 1,
        failed: 0,
        skippedNoKey: 0,
        durationMs: 5,
      });
    const errSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    startRetryStuckWorker();

    await vi.advanceTimersByTimeAsync(RETRY_STUCK_TICK_INTERVAL_MS);
    // Let the rejection propagate through microtasks.
    await Promise.resolve();
    await Promise.resolve();

    await vi.advanceTimersByTimeAsync(RETRY_STUCK_TICK_INTERVAL_MS);

    expect(mockRunRetryStuck).toHaveBeenCalledTimes(2);
    expect(errSpy).toHaveBeenCalled();
    errSpy.mockRestore();
  });

  it("startRetryStuckWorker is idempotent (calling twice does not double tick)", async () => {
    startRetryStuckWorker();
    startRetryStuckWorker();

    await vi.advanceTimersByTimeAsync(RETRY_STUCK_TICK_INTERVAL_MS);
    expect(mockRunRetryStuck).toHaveBeenCalledTimes(1);
  });
});
