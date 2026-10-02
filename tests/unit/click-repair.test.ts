import { describe, it, expect, vi, beforeEach } from "vitest";

const calls: string[] = [];
const mockBackfill = vi.fn();
const mockReactivate = vi.fn();

vi.mock("../../src/lib/self-send/click-promotion", () => ({ promotePendingClicks: vi.fn() }));
vi.mock("../../src/lib/self-send/click-scanner-backfill", () => ({
  backfillScannerClicks: (...args: unknown[]) => mockBackfill(...args),
}));
vi.mock("../../src/lib/self-send/reactivate-scanner-paused", () => ({
  reactivateScannerPausedSequences: (...args: unknown[]) => mockReactivate(...args),
}));

const { runClickRepair, CLICK_REPAIR_INTERVAL_MS } = await import(
  "../../src/lib/self-send/click-promotion-worker"
);

beforeEach(() => {
  vi.resetAllMocks();
  calls.length = 0;
  mockBackfill.mockImplementation(async () => {
    calls.push("backfill");
    return { scannerHits: 1, leadsDemoted: 1, reasons: {} };
  });
  mockReactivate.mockImplementation(async () => {
    calls.push("reactivate");
    return { reactivated: 1 };
  });
});

describe("click repair loop", () => {
  it("re-judges clicks FIRST, then resumes what a now-scanner click stopped, both for real", async () => {
    await runClickRepair();
    expect(calls).toEqual(["backfill", "reactivate"]);
    expect(mockBackfill).toHaveBeenCalledWith({ dryRun: false });
    expect(mockReactivate).toHaveBeenCalledWith({ dryRun: false });
  });

  it("does not resume anything when the re-judging failed", async () => {
    mockBackfill.mockRejectedValueOnce(new Error("db down"));
    await expect(runClickRepair()).rejects.toThrow("db down");
    expect(mockReactivate).not.toHaveBeenCalled();
  });

  it("runs every 6 hours by default", () => {
    expect(CLICK_REPAIR_INTERVAL_MS).toBe(6 * 60 * 60 * 1000);
  });
});
