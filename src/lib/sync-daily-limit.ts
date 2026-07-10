/**
 * One-time (idempotent, resumable) sweep to align EVERY currently-`in_production`
 * account's Instantly campaign `daily_limit` to `IN_PRODUCTION_DAILY_LIMIT`.
 *
 * Why this exists: `reconcileLifecycle` PATCHes `daily_limit` ONLY on a state
 * FLIP into `in_production` (see account-lifecycle-sync). So when the constant is
 * bumped (40 → 50), accounts ALREADY in_production keep their old Instantly cap
 * until they happen to re-flip — the new value never reaches them. This sweep
 * closes that gap: it re-PATCHes the target onto the whole live-send pool.
 *
 * Properties:
 *   - idempotent — only PATCHes accounts whose silver `daily_limit` differs from
 *                  the target; a re-run after a full sweep no-ops (all skipped).
 *   - resumable  — re-reads live silver state each run; already-aligned accounts
 *                  drop out of the next selection.
 *   - in-cluster — resolves the platform Instantly key via key-service
 *                  (`*.railway.internal`), so it MUST run inside Railway (the
 *                  `/internal/audit/daily-limit-sync` endpoint), NOT a laptop shell.
 *
 * Fail-loud per account: a PATCH error is counted under `failed` and the sweep
 * continues (a re-run retries it) — no silent swallow of the overall op.
 */
import { IN_PRODUCTION_DAILY_LIMIT } from "./account-lifecycle";
import { fetchInProductionAccounts } from "./account-lifecycle-sync";
import { setDailyLimit, type Account } from "./instantly-client";

export interface DailyLimitSyncSummary {
  /** in_production accounts read from silver. */
  accountsRead: number;
  /** Already at the target (skipped, no PATCH). */
  skipped: number;
  /** PATCHed to the target this run. */
  patched: number;
  /** PATCH threw — left for the next run. */
  failed: number;
}

/**
 * Pure: emails of the accounts whose `daily_limit` is not already `target`
 * (including a null/absent limit, which must be set). Deterministic order.
 */
export function selectAccountsNeedingDailyLimit(
  accounts: Account[],
  target: number,
): string[] {
  return accounts
    .filter((a) => a.daily_limit !== target)
    .map((a) => a.email)
    .filter(Boolean);
}

/**
 * IO glue: read the in_production pool from silver, PATCH each account whose
 * campaign `daily_limit` differs from `IN_PRODUCTION_DAILY_LIMIT` to that value.
 * `limit` bounds the batch (account count); omit to sweep all.
 */
export async function syncInProductionDailyLimit(
  apiKey: string,
  limit?: number,
): Promise<DailyLimitSyncSummary> {
  const target = IN_PRODUCTION_DAILY_LIMIT;
  const accounts = await fetchInProductionAccounts();
  const needing = selectAccountsNeedingDailyLimit(accounts, target);
  const batch = limit && limit > 0 ? needing.slice(0, limit) : needing;

  let patched = 0;
  let failed = 0;
  for (const email of batch) {
    try {
      await setDailyLimit(apiKey, email, target);
      patched += 1;
    } catch (error: unknown) {
      failed += 1;
      const message = error instanceof Error ? error.message : String(error);
      console.error(`[daily-limit-sync] PATCH failed email=${email}: ${message}`);
    }
  }

  return {
    accountsRead: accounts.length,
    skipped: accounts.length - needing.length,
    patched,
    failed,
  };
}
