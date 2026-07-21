/**
 * IDEMPOTENT ENFORCEMENT of each account's warmup + campaign daily_limit to the
 * target for its CURRENT lifecycle state — on EVERY run, not only on a flip.
 *
 * Why this exists (the bug it fixes):
 *   `reconcileLifecycle` PATCHes warmup + daily_limit ONLY on a state FLIP
 *   (`if (status === currentStatus) continue`). It never re-asserts the target
 *   while the status is UNCHANGED. Two ways that bit prod:
 *     - in_recovery accounts that were already in_recovery before the recovery
 *       targets became 20/30 never re-flipped → stuck at old 45/5 (or 45/50).
 *     - an in_production account whose values Instantly RESET on its own
 *       reactivation (e.g. after a 550 throttle auto-clears) drifted back to
 *       50/10 and reconcile never re-imposed 45/5 (magnolia@saviolabsco.com).
 *   The only prior remediation sweep (`sync-daily-limit`) covered ONLY
 *   `daily_limit` AND ONLY `in_production` — no warmup, no recovery. This sweep
 *   generalizes it: it enforces BOTH fields for BOTH `in_production` and
 *   `in_recovery`, so it SUPERSEDES `sync-daily-limit` on the cron path.
 *
 * Reads the LIVE, FULL account list (`listAccounts`, paginated) — NOT silver —
 * because the LIST row carries the actual Instantly values to compare against
 * (`daily_limit` + the abbreviated `warmup.limit`), and silver stores no
 * `warmup.limit` column. Lifecycle STATUS is read from silver
 * (`fetchLifecycleByEmail`). Same rationale as `sync-slow-ramp`.
 *
 * `deactivated_by_instantly` / `deactivated_by_user` are LEFT UNTOUCHED
 * (their targets are null — an off account keeps draining its already-loaded
 * queue at whatever cap it had), matching `dailyLimitForStatus` /
 * `warmupDailyForStatus`.
 *
 * Properties:
 *   - idempotent — only PATCHes a field whose live value differs from the target;
 *                  a re-run after a full sweep no-ops (all aligned).
 *   - resumable  — re-reads live state each run; aligned accounts drop out.
 *   - in-cluster — resolves the platform Instantly key via key-service
 *                  (`*.railway.internal`), so it MUST run inside Railway (the
 *                  `/internal/audit/lifecycle-limits-sync` endpoint).
 *
 * Fail-loud per account: a PATCH error is counted under `failed` and the sweep
 * continues (a re-run retries it) — no silent swallow. Warmup is PATCHed BEFORE
 * daily (mirrors reconcile's ordering); a warmup failure skips that account's
 * daily PATCH this run (next run heals).
 */
import {
  listAccounts,
  setWarmupDailyLimit,
  setDailyLimit,
  type Account,
} from "./instantly-client";
import { fetchLifecycleByEmail, type LifecycleView } from "./account-lifecycle-sync";
import {
  warmupDailyForStatus,
  dailyLimitForStatus,
  type LifecycleStatus,
} from "./account-lifecycle";

/** A per-account patch plan: which fields drift from the lifecycle target. */
export interface LifecycleLimitPatch {
  email: string;
  /** Target warmup daily volume to PATCH, or null if already aligned. */
  warmup: number | null;
  /** Target campaign daily_limit to PATCH, or null if already aligned. */
  daily: number | null;
}

export interface LifecycleLimitsSyncSummary {
  /** Accounts read from the live Instantly list (all lifecycle statuses). */
  accountsRead: number;
  /** Accounts that received at least one PATCH this run. */
  accountsPatched: number;
  /** Warmup PATCHes issued. */
  warmupPatched: number;
  /** daily_limit PATCHes issued. */
  dailyPatched: number;
  /** Accounts whose PATCH threw — left for the next run. */
  failed: number;
}

/**
 * Pure: for each account whose silver lifecycle is `in_production` or
 * `in_recovery`, compute which of {warmup.limit, daily_limit} drift from that
 * state's target. Returns only accounts with at least one drifting field, in
 * input order; empty emails filtered out. Accounts in any other state (or with
 * an unknown/absent lifecycle) are skipped — their targets are null.
 */
export function selectLifecycleLimitPatches(
  accounts: Account[],
  lifecycleByEmail: Map<string, LifecycleView>,
): LifecycleLimitPatch[] {
  const patches: LifecycleLimitPatch[] = [];
  for (const account of accounts) {
    if (!account.email) continue;
    const status = lifecycleByEmail.get(account.email)?.status as
      | LifecycleStatus
      | null
      | undefined;
    if (status !== "in_production" && status !== "in_recovery") continue;

    const targetWarmup = warmupDailyForStatus(status); // 5 | 30 (never null here)
    const targetDaily = dailyLimitForStatus(status); // 45 | 20 (never null here)

    const currentWarmup = account.warmup?.limit;
    const currentDaily = account.daily_limit;

    const warmup = targetWarmup !== null && currentWarmup !== targetWarmup ? targetWarmup : null;
    const daily = targetDaily !== null && currentDaily !== targetDaily ? targetDaily : null;

    if (warmup !== null || daily !== null) {
      patches.push({ email: account.email, warmup, daily });
    }
  }
  return patches;
}

/**
 * IO glue: read the FULL live account list + the silver lifecycle projection,
 * then PATCH each drifting field to its lifecycle target. `limit` bounds the
 * batch (account count); omit to sweep all.
 */
export async function syncLifecycleLimits(
  apiKey: string,
  limit?: number,
): Promise<LifecycleLimitsSyncSummary> {
  const [accounts, lifecycleByEmail] = await Promise.all([
    listAccounts(apiKey),
    fetchLifecycleByEmail(),
  ]);
  const patches = selectLifecycleLimitPatches(accounts, lifecycleByEmail);
  const batch = limit && limit > 0 ? patches.slice(0, limit) : patches;

  let accountsPatched = 0;
  let warmupPatched = 0;
  let dailyPatched = 0;
  let failed = 0;

  for (const patch of batch) {
    try {
      // Warmup FIRST (mirrors reconcile). A warmup throw aborts this account's
      // daily PATCH for this run — next run heals the remaining field.
      if (patch.warmup !== null) {
        await setWarmupDailyLimit(apiKey, patch.email, patch.warmup);
        warmupPatched += 1;
      }
      if (patch.daily !== null) {
        await setDailyLimit(apiKey, patch.email, patch.daily);
        dailyPatched += 1;
      }
      accountsPatched += 1;
    } catch (error: unknown) {
      failed += 1;
      const message = error instanceof Error ? error.message : String(error);
      console.error(`[lifecycle-limits-sync] PATCH failed email=${patch.email}: ${message}`);
    }
  }

  return {
    accountsRead: accounts.length,
    accountsPatched,
    warmupPatched,
    dailyPatched,
    failed,
  };
}
