import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import {
  statsCacheKey,
  getCachedStats,
  setCachedStats,
  clearStatsCache,
  STATS_CACHE_TTL_MS,
  getOrSetCachedStats,
  deleteCachedStatsWhere,
  STATS_REFRESH_AFTER_MS,
} from "../../src/lib/stats-cache";

describe("stats-cache", () => {
  beforeEach(() => clearStatsCache());
  afterEach(() => vi.useRealTimers());

  it("returns the set value within TTL (hit)", () => {
    setCachedStats("k", { a: 1 });
    expect(getCachedStats("k")).toEqual({ a: 1 });
  });

  it("returns undefined for an unknown key (miss)", () => {
    expect(getCachedStats("nope")).toBeUndefined();
  });

  it("expires entries after the TTL (miss)", () => {
    vi.useFakeTimers();
    setCachedStats("k", { a: 1 }, 1000);
    vi.advanceTimersByTime(1001);
    expect(getCachedStats("k")).toBeUndefined();
  });

  it("clearStatsCache empties the store", () => {
    setCachedStats("k", { a: 1 });
    clearStatsCache();
    expect(getCachedStats("k")).toBeUndefined();
  });

  it("builds a deterministic key regardless of param order", () => {
    const a = statsCacheKey("stats:org1", { brandId: "b", campaignId: "c" });
    const b = statsCacheKey("stats:org1", { campaignId: "c", brandId: "b" });
    expect(a).toBe(b);
  });

  it("omits undefined params from the key (so they don't collide with set values)", () => {
    const withUndef = statsCacheKey("p", { brandId: "b", campaignId: undefined });
    const without = statsCacheKey("p", { brandId: "b" });
    expect(withUndef).toBe(without);
  });

  it("different params produce different keys", () => {
    expect(statsCacheKey("p", { brandId: "x" })).not.toBe(statsCacheKey("p", { brandId: "y" }));
  });

  it("exposes a 60s default TTL", () => {
    expect(STATS_CACHE_TTL_MS).toBe(60_000);
  });

  describe("refresh-ahead (opt-in refreshAfterMs)", () => {
    const flush = () => new Promise((r) => setImmediate(r));

    it("answers a stale-but-unexpired hit from memory and reloads in the background", async () => {
      vi.useFakeTimers({ toFake: ["Date"] });
      const opts = { refreshAfterMs: 500 };
      expect(await getOrSetCachedStats("k", async () => 1, 1000, opts)).toBe(1);
      vi.advanceTimersByTime(600);
      let release!: (v: number) => void;
      const reload = vi.fn(() => new Promise<number>((r) => { release = r; }));
      // Served at once from memory while ONE background reload runs.
      expect(await getOrSetCachedStats("k", reload, 1000, opts)).toBe(1);
      expect(await getOrSetCachedStats("k", reload, 1000, opts)).toBe(1);
      expect(reload).toHaveBeenCalledTimes(1);
      release(2);
      await flush();
      expect(await getOrSetCachedStats("k", reload, 1000, opts)).toBe(2);
      expect(reload).toHaveBeenCalledTimes(1);
    });

    it("never serves past the TTL: a failed background reload lets the entry expire and the next caller gets the error", async () => {
      vi.useFakeTimers({ toFake: ["Date"] });
      const errSpy = vi.spyOn(console, "error").mockImplementation(() => {});
      const opts = { refreshAfterMs: 500 };
      await getOrSetCachedStats("k", async () => 1, 1000, opts);
      vi.advanceTimersByTime(600);
      const failing = vi.fn(async () => { throw new Error("db down"); });
      expect(await getOrSetCachedStats("k", failing, 1000, opts)).toBe(1);
      await flush();
      expect(errSpy).toHaveBeenCalled();
      vi.advanceTimersByTime(500);
      await expect(getOrSetCachedStats("k", failing, 1000, opts)).rejects.toThrow("db down");
      errSpy.mockRestore();
    });

    it("does not refresh ahead without the option (default behaviour unchanged)", async () => {
      vi.useFakeTimers({ toFake: ["Date"] });
      const loader = vi.fn(async () => 1);
      await getOrSetCachedStats("k", loader, 1000);
      vi.advanceTimersByTime(900);
      await getOrSetCachedStats("k", loader, 1000);
      expect(loader).toHaveBeenCalledTimes(1);
    });

    it("a load in flight when its key is invalidated does not store its (pre-write) value", async () => {
      let release!: (v: number) => void;
      const slow = getOrSetCachedStats("k", () => new Promise<number>((r) => { release = r; }));
      deleteCachedStatsWhere((key) => key === "k");
      release(1);
      expect(await slow).toBe(1);
      expect(getCachedStats("k")).toBeUndefined();
    });

    it("refreshes late in the TTL (45 s of 60 s), not half-way: the refresh point is the recompute period of a polled key", () => {
      expect(STATS_REFRESH_AFTER_MS).toBe(45_000);
      expect(STATS_CACHE_TTL_MS).toBe(60_000);
      // The reload still gets a lead time well above the 4-5 s observed load.
      expect(STATS_CACHE_TTL_MS - STATS_REFRESH_AFTER_MS).toBeGreaterThanOrEqual(15_000);
    });
  });
});
