/**
 * Per-ADDRESS cache of `POST /orgs/status` answers (BOX-DB-LOAD-1027).
 *
 * Why: lead-service reads status in batches of 100 addresses, ~6 distinct batches a second, and the
 * same address comes back in a DIFFERENT batch within the minute (2026-10-27 capture: 59,319 address
 * lookups a minute, 34,763 distinct). The batch-keyed cache (`stats-cache.ts`) only hits an
 * identical batch, so each address was aggregated again: the status statements were 697 DB minutes
 * a day, the brand breakdown alone 635k calls.
 *
 * Exact: every status statement keys on the address (`lead_email = ANY(...)`, grouped per address,
 * refusals per address), so one address's answer does not depend on which batch it arrived in. An
 * entry is keyed on the whole scope (org, brand, campaign, refusal brands) plus the address, lives
 * the same 60 s as the batch cache, and is dropped by `invalidateOrgStatusCache` exactly when the
 * batch answers naming it are. A load that overlapped an invalidation of an address does not store
 * that address (its answer may predate the write).
 *
 * Bounded: past {@link STATUS_EMAIL_CACHE_MAX} entries the oldest-written go first (an unbounded
 * stats store once took the process to its heap limit, SFC-MONEY-SLOW-1016).
 */

export const STATUS_EMAIL_CACHE_TTL_MS = 60_000;
export const STATUS_EMAIL_CACHE_MAX = 40_000;
const SWEEP_INTERVAL_MS = 30_000;

export interface StatusScope {
  orgId: string;
  brandId: string;
  campaignId: string;
  refusalBrandIds: string;
}

interface Entry {
  orgId: string;
  email: string;
  /** When the load that computed it began: an invalidation at or after it makes the entry stale. */
  computedFrom: number;
  expiresAt: number;
  value: unknown;
}

const store = new Map<string, Entry>();
/** org|lowercased address -> when it was last invalidated (kept one TTL, then swept). */
const invalidatedAt = new Map<string, number>();
let lastSweepAt = 0;

function entryKey(scope: StatusScope, email: string): string {
  return JSON.stringify([scope.orgId, scope.brandId, scope.campaignId, scope.refusalBrandIds, email]);
}

function invalidationKey(orgId: string, email: string): string {
  return `${orgId}|${email.trim().toLowerCase()}`;
}

function sweep(now: number): void {
  if (now - lastSweepAt < SWEEP_INTERVAL_MS) return;
  lastSweepAt = now;
  for (const [key, entry] of store) if (entry.expiresAt <= now) store.delete(key);
  for (const [key, at] of invalidatedAt) if (at <= now - STATUS_EMAIL_CACHE_TTL_MS) invalidatedAt.delete(key);
}

/** The cached answer for this address in this scope, or undefined. */
export function getStatusEmailResult(scope: StatusScope, email: string): unknown {
  const key = entryKey(scope, email);
  const entry = store.get(key);
  if (!entry) return undefined;
  const invalidated = invalidatedAt.get(invalidationKey(entry.orgId, entry.email));
  if (entry.expiresAt <= Date.now() || (invalidated !== undefined && invalidated >= entry.computedFrom)) {
    store.delete(key);
    return undefined;
  }
  return entry.value;
}

/**
 * Store freshly computed answers. `loadStartedAt` is when the load that computed them began: an
 * address invalidated at or after it is skipped.
 */
export function setStatusEmailResults(
  scope: StatusScope,
  results: ReadonlyMap<string, unknown>,
  loadStartedAt: number,
): void {
  const now = Date.now();
  sweep(now);
  for (const [email, value] of results) {
    const invalidated = invalidatedAt.get(invalidationKey(scope.orgId, email));
    if (invalidated !== undefined && invalidated >= loadStartedAt) continue;
    const key = entryKey(scope, email);
    store.delete(key);
    store.set(key, {
      orgId: scope.orgId,
      email,
      computedFrom: loadStartedAt,
      expiresAt: now + STATUS_EMAIL_CACHE_TTL_MS,
      value,
    });
  }
  for (const oldest of store.keys()) {
    if (store.size <= STATUS_EMAIL_CACHE_MAX) break;
    store.delete(oldest);
  }
}

/**
 * Make this org's cached answers for these addresses stale (case-insensitive): recorded per
 * address, checked on read, so the cost is the number of addresses, never the store size (this runs
 * on every evidence change).
 */
export function invalidateStatusEmailResults(orgId: string, emails: ReadonlySet<string>): void {
  const now = Date.now();
  for (const email of emails) invalidatedAt.set(invalidationKey(orgId, email), now);
}

export function statusEmailCacheSize(): number {
  return store.size;
}

/** Tests only. */
export function clearStatusEmailCache(): void {
  store.clear();
  invalidatedAt.clear();
  lastSweepAt = 0;
}
