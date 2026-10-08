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
  storedAt: number;
  expiresAt: number;
  value: unknown;
}

const store = new Map<string, CacheEntry>();
const inFlight = new Map<string, Promise<unknown>>();

/** Default TTL (ms) applied when a caller does not pass one explicitly. */
export const STATS_CACHE_TTL_MS = DEFAULT_TTL_MS;

/**
 * Lead time the background reload gets before the entry expires. A fleet
 * /public/stats reload measured 4-5 s on the saturated box (2026-10-08), so 15 s
 * is three times the worst observed load.
 */
export const STATS_REFRESH_LEAD_MS = 15_000;

/**
 * Refresh-ahead point for the polled GET /stats reads: TTL minus the lead time
 * (45 s), so the background reload still lands before the entry expires and a
 * page polling every 5 s never meets a cold key.
 *
 * ⚠️ NOT half the TTL (v0.83.15 shipped TTL/2): every hit at or past this age
 * starts a reload, so for a key polled faster than this point the recompute
 * period IS this point. TTL/2 recomputed every steadily polled key every ~30 s,
 * twice the pre-refresh-ahead rate, on a Postgres already burning most of the
 * box. Readers gain nothing from the earlier reload: no answer is ever older
 * than the 60 s TTL either way.
 */
export const STATS_REFRESH_AFTER_MS = DEFAULT_TTL_MS - STATS_REFRESH_LEAD_MS;

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
  const now = Date.now();
  store.set(key, { storedAt: now, expiresAt: now + ttlMs, value });
}

/**
 * Return a cached value or share one in-flight loader for this key. This avoids
 * a burst of identical requests all missing the cache and re-running the same
 * expensive stats aggregation before the first response has stored the value.
 *
 * `refreshAfterMs` (opt-in, < ttlMs) turns on REFRESH-AHEAD: a hit on an entry
 * older than that still answers from memory at once, and starts ONE background
 * reload that replaces the entry when it lands. A page polling the key therefore
 * never waits on the aggregation after its first read, and no answer is ever
 * older than `ttlMs` (the entry still expires on schedule; if the background
 * reload fails it is logged, the entry expires, and the next caller runs the
 * loader inline and gets the error). Without it, every TTL boundary made the
 * next poll wait the full aggregation (fleet /public/stats: 4-5 s, 2026-10-08).
 */
export async function getOrSetCachedStats<T>(
  key: string,
  loader: () => Promise<T>,
  ttlMs: number = DEFAULT_TTL_MS,
  options: { refreshAfterMs?: number } = {},
): Promise<T> {
  const entry = store.get(key);
  if (entry && entry.expiresAt > Date.now()) {
    const { refreshAfterMs } = options;
    if (
      refreshAfterMs !== undefined &&
      Date.now() - entry.storedAt >= refreshAfterMs &&
      !inFlight.has(key)
    ) {
      startLoad(key, loader, ttlMs).catch((error: any) => {
        console.error(
          `[instantly-service] stats cache background refresh failed for "${key}" (cached value kept until it expires): ${error?.cause?.message ?? error?.message ?? error}`,
        );
      });
    }
    return entry.value as T;
  }
  if (entry) store.delete(key);

  const existing = inFlight.get(key);
  if (existing) return existing as Promise<T>;

  return startLoad(key, loader, ttlMs);
}

function startLoad<T>(key: string, loader: () => Promise<T>, ttlMs: number): Promise<T> {
  const pending: Promise<T> = loader()
    .then((value) => {
      // A `deleteCachedStatsWhere` that ran while this load was in flight
      // dropped it from `inFlight`: the value may predate the write that
      // invalidated the key, so it is returned to its waiters but not stored.
      if (inFlight.get(key) === pending) setCachedStats(key, value, ttlMs);
      return value;
    })
    .finally(() => {
      if (inFlight.get(key) === pending) inFlight.delete(key);
    });
  inFlight.set(key, pending);
  return pending;
}

/**
 * Drop every cached entry (and in-flight loader) whose key matches. Returns how
 * many keys were dropped. Used when a write makes a cached answer wrong before
 * its TTL runs out — see `invalidateOrgStatusCache` in evidence-changed.ts.
 */
export function deleteCachedStatsWhere(predicate: (key: string) => boolean): number {
  let dropped = 0;
  for (const key of [...store.keys()]) {
    if (predicate(key)) {
      store.delete(key);
      dropped += 1;
    }
  }
  for (const key of [...inFlight.keys()]) {
    if (predicate(key)) inFlight.delete(key);
  }
  return dropped;
}

/** Drop all cached entries. Used by tests for isolation. */
export function clearStatsCache(): void {
  store.clear();
  inFlight.clear();
}
