/**
 * One-time (idempotent, resumable) sweep to turn OFF Instantly's "Campaign slow
 * ramp" (`enable_slow_ramp`) on EVERY account, regardless of lifecycle status.
 *
 * Why this exists: our fleet is pre-warmed / DFY-aged, so a slow ramp only
 * throttles live sends — the invariant is `enable_slow_ramp == false` on EVERY
 * account (in_production, in_recovery, deactivated_by_user, deactivated_by_
 * instantly — ALL of them). DFY-order setup PATCHes it false on new orders, but
 * nothing enforced it fleet-wide or kept legacy / Primeforge / re-enabled
 * accounts off. This sweep closes that gap across the WHOLE live account list.
 *
 * Reads the LIVE, FULL account list (`listAccounts`, paginated) — NOT silver —
 * because (a) the invariant spans every lifecycle status (silver's in_production
 * pool would miss the rest) and (b) silver stores no slow-ramp column. The
 * `enable_slow_ramp` flag is read straight off each account row.
 *
 * Properties:
 *   - idempotent — only PATCHes accounts whose `enable_slow_ramp` is not already
 *                  false; a re-run after a full sweep no-ops (all skipped). If
 *                  the LIST response omits the field, an account reads as absent
 *                  → PATCHed false each run (idempotent in EFFECT, no skip).
 *   - resumable  — re-reads the live account list each run; already-off accounts
 *                  drop out of the next selection.
 *   - in-cluster — resolves the platform Instantly key via key-service
 *                  (`*.railway.internal`), so it MUST run inside Railway (the
 *                  `/internal/audit/slow-ramp-sync` endpoint), NOT a laptop shell.
 *
 * Fail-loud per account: a PATCH error is counted under `failed` and the sweep
 * continues (a re-run retries it) — no silent swallow of the overall op.
 */
import { listAccounts, setSlowRamp, type Account } from "./instantly-client";

export interface SlowRampSyncSummary {
  /** Accounts read from the live Instantly list (all lifecycle statuses). */
  accountsRead: number;
  /** Already `enable_slow_ramp: false` (skipped, no PATCH). */
  skipped: number;
  /** PATCHed to `enable_slow_ramp: false` this run. */
  patched: number;
  /** PATCH threw — left for the next run. */
  failed: number;
}

/**
 * Pure: emails of the accounts whose `enable_slow_ramp` is not already false —
 * i.e. `true` OR absent (fail-safe: an unknown flag is treated as needing the
 * off-PATCH). Deterministic order, empty emails filtered out.
 */
export function selectAccountsNeedingSlowRampOff(accounts: Account[]): string[] {
  return accounts
    .filter((a) => a.enable_slow_ramp !== false)
    .map((a) => a.email)
    .filter(Boolean);
}

/**
 * IO glue: read the FULL live account list, PATCH `enable_slow_ramp: false` on
 * each account that isn't already off. `limit` bounds the batch (account count);
 * omit to sweep all.
 */
export async function syncSlowRampOff(
  apiKey: string,
  limit?: number,
): Promise<SlowRampSyncSummary> {
  const accounts = await listAccounts(apiKey);
  const needing = selectAccountsNeedingSlowRampOff(accounts);
  const batch = limit && limit > 0 ? needing.slice(0, limit) : needing;

  let patched = 0;
  let failed = 0;
  for (const email of batch) {
    try {
      await setSlowRamp(apiKey, email, false);
      patched += 1;
    } catch (error: unknown) {
      failed += 1;
      const message = error instanceof Error ? error.message : String(error);
      console.error(`[slow-ramp-sync] PATCH failed email=${email}: ${message}`);
    }
  }

  return {
    accountsRead: accounts.length,
    skipped: accounts.length - needing.length,
    patched,
    failed,
  };
}
