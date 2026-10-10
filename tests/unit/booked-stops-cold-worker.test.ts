import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const mockSweep = vi.fn();
vi.mock("../../src/lib/booked-stops-cold", () => ({
  stopQueuedSequencesOfBookedPeople: (...args: unknown[]) => mockSweep(...args),
}));

const { startBookedStopsWorker, BOOKED_STOPS_FIRST_TICK_MS, BOOKED_STOPS_INTERVAL_MS } = await import(
  "../../src/lib/booked-stops-cold-worker"
);

beforeEach(() => {
  vi.useFakeTimers();
  mockSweep.mockResolvedValue({ summary: {}, notYetStopped: new Set() });
});

afterEach(() => {
  vi.useRealTimers();
});

describe("booked-stops-cold worker", () => {
  // Deploys restart the container more often than the 10-min dispatch interval fires.
  it("runs a first pass ~60 s after boot, then every 2 minutes, and survives a failed pass", async () => {
    expect(BOOKED_STOPS_FIRST_TICK_MS).toBeLessThanOrEqual(60_000);
    startBookedStopsWorker();
    expect(mockSweep).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(BOOKED_STOPS_FIRST_TICK_MS);
    expect(mockSweep).toHaveBeenCalledTimes(1);

    mockSweep.mockRejectedValueOnce(new Error("lead-service 503"));
    await vi.advanceTimersByTimeAsync(BOOKED_STOPS_INTERVAL_MS - BOOKED_STOPS_FIRST_TICK_MS);
    expect(mockSweep).toHaveBeenCalledTimes(2);

    await vi.advanceTimersByTimeAsync(BOOKED_STOPS_INTERVAL_MS);
    expect(mockSweep).toHaveBeenCalledTimes(3);
  });
});
