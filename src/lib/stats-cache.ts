/**
 * Tiny in-process TTL cache for the analytics stats endpoints.
 *
 * Why this exists: GET /stats (authed `/orgs/stats`) and GET /public/stats
 * live-aggregate over the silver event log on EVERY request. Warm, a single
 * call is fast (~150ms), but the gateway fans out bursts of identical calls
 * (leaderboard / landing / dashboard polling) against a 0.25-1 CU Neon compute.
 * The concurrent re-aggregation saturates the compute and requests queue past
 * the caller's ~10s AbortSignal timeout. The no-filter /public/stats total is
 * byte-identical for every caller, so a short TTL collapses a flood of
 * identical requests down to ~1 aggregation per window.
 *
 * Deliberately in-memory (per replica), not a DB/materialized table: zero
 * migration, zero new persistent state, and a stale window bounded by the TTL.
 * The doctrine in CLAUDE.md (db/schema.ts) is "no analytics_snapshots cache
 * unless live aggregation is provably too slow" — this is the lightest possible
 * cache that still attacks the saturation flood, short of reintroducing a
 * materialized table.
 */

const DEFAULT_TTL_MS = 60_000;

interface CacheEntry {
  expiresAt: number;
  value: unknown;
}

const store = new Map<string, CacheEntry>();
const inFlight = new Map<string, Promise<unknown>>();

/** Default TTL (ms) applied when a caller does not pass one explicitly. */
export const STATS_CACHE_TTL_MS = DEFAULT_TTL_MS;

/**
 * Build a deterministic cache key from a prefix + the validated query object.
 * Keys are sorted so `{a,b}` and `{b,a}` collide intentionally (same query).
 */
export function statsCacheKey(prefix: string, params: Record<string, unknown>): string {
  const sorted = Object.keys(params)
    .filter((k) => params[k] !== undefined)
    .sort()
    .map((k) => `${k}=${String(params[k])}`)
    .join("&");
  return `${prefix}|${sorted}`;
}

/** Return the cached value if present and unexpired, else undefined. */
export function getCachedStats<T>(key: string): T | undefined {
  const entry = store.get(key);
  if (!entry) return undefined;
  if (entry.expiresAt <= Date.now()) {
    store.delete(key);
    return undefined;
  }
  return entry.value as T;
}

/** Store a value under key with the given TTL (defaults to STATS_CACHE_TTL_MS). */
export function setCachedStats(key: string, value: unknown, ttlMs: number = DEFAULT_TTL_MS): void {
  store.set(key, { expiresAt: Date.now() + ttlMs, value });
}

/**
 * Return a cached value or share one in-flight loader for this key. This avoids
 * a burst of identical requests all missing the cache and re-running the same
 * expensive stats aggregation before the first response has stored the value.
 */
export async function getOrSetCachedStats<T>(
  key: string,
  loader: () => Promise<T>,
  ttlMs: number = DEFAULT_TTL_MS,
): Promise<T> {
  const cached = getCachedStats<T>(key);
  if (cached !== undefined) return cached;

  const existing = inFlight.get(key);
  if (existing) return existing as Promise<T>;

  const pending = loader()
    .then((value) => {
      setCachedStats(key, value, ttlMs);
      return value;
    })
    .finally(() => {
      inFlight.delete(key);
    });
  inFlight.set(key, pending);
  return pending;
}

/** Drop all cached entries. Used by tests for isolation. */
export function clearStatsCache(): void {
  store.clear();
  inFlight.clear();
}
