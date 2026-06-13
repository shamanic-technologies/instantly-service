import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import {
  statsCacheKey,
  getCachedStats,
  setCachedStats,
  clearStatsCache,
  STATS_CACHE_TTL_MS,
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
});
