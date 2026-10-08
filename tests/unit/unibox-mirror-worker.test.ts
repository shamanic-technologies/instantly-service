import { describe, it, expect, vi, beforeEach } from "vitest";

const mockBackfillEmails = vi.fn();
vi.mock("../../src/lib/emails-backfill", () => ({
  backfillEmails: (...a: unknown[]) => mockBackfillEmails(...a),
}));

const mockResolvePlatformKey = vi.fn();
vi.mock("../../src/lib/key-client", () => ({
  resolvePlatformInstantlyApiKey: (...a: unknown[]) => mockResolvePlatformKey(...a),
}));

import { runUniboxMirrorTick, UNIBOX_MIRROR_MAX_PAGES } from "../../src/lib/unibox-mirror-worker";

beforeEach(() => {
  vi.resetAllMocks();
  mockResolvePlatformKey.mockResolvedValue("platform-key");
  mockBackfillEmails.mockResolvedValue({ pages: 1, emailsStored: 0 });
});

describe("unibox-mirror tick", () => {
  it("walks the Unibox newest-first on the platform key, bounded, stopping at the known frontier", async () => {
    await runUniboxMirrorTick();

    expect(mockBackfillEmails).toHaveBeenCalledWith("platform-key", {
      maxPages: UNIBOX_MIRROR_MAX_PAGES,
      stopAtKnownPage: true,
    });
  });

  it("never throws: a failed tick is logged and the next one runs", async () => {
    mockBackfillEmails.mockRejectedValueOnce(new Error("instantly 500"));
    await expect(runUniboxMirrorTick()).resolves.toBeUndefined();

    await runUniboxMirrorTick();
    expect(mockBackfillEmails).toHaveBeenCalledTimes(2);
  });

  it("does not stack a second walk on one still in flight", async () => {
    let release: () => void = () => {};
    mockBackfillEmails.mockReturnValueOnce(new Promise((r) => { release = () => r({}); }));

    const first = runUniboxMirrorTick();
    await Promise.resolve();
    await Promise.resolve();
    await runUniboxMirrorTick();
    release();
    await first;

    expect(mockBackfillEmails).toHaveBeenCalledTimes(1);
  });
});

describe("unibox-mirror worker start", () => {
  it("runs a first tick shortly after boot, not only after a full interval (deploys reset the interval)", async () => {
    vi.useFakeTimers();
    const { startUniboxMirrorWorker, stopUniboxMirrorWorker, UNIBOX_MIRROR_BOOT_DELAY_MS } =
      await import("../../src/lib/unibox-mirror-worker");
    startUniboxMirrorWorker();
    await vi.advanceTimersByTimeAsync(UNIBOX_MIRROR_BOOT_DELAY_MS + 10);
    expect(mockBackfillEmails).toHaveBeenCalledTimes(1);
    stopUniboxMirrorWorker();
    vi.useRealTimers();
  });
});
