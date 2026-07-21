/**
 * Reactivate accounts Instantly DEACTIVATED that are healthy again.
 *
 * The lifecycle model deliberately does NOT fight Instantly: an account with
 * `instantlyStatus <= 0` derives to `deactivated_by_instantly` and we leave it
 * off. But some of those accounts are Health 100 + 100% inbox — Instantly most
 * often disabled them on a transient Gmail `550-5.4.5 Daily user sending limit
 * exceeded` throttle (CLAUDE.md 2026-07-14). Once the throttle clears Instantly
 * usually auto-resumes them, but some stay stuck deactivated. This sweep nudges
 * those healthy-again accounts back into the pool via `POST /accounts/{email}/resume`;
 * the next accounts-sync then observes `status > 0` and reconcile flips them to
 * `in_production`.
 *
 * ⚠️ THROTTLE-NUDGE GUARD (load-bearing). "Repeated resume nudges make the 550
 * throttle WORSE" (CLAUDE.md). So a candidate MUST be:
 *   1. lifecycle `deactivated_by_instantly` (silver),
 *   2. Health (stat_warmup_score) == 100,
 *   3. delivery FULL — 100% inbox across every ESP of its latest placement test,
 *   4. deactivated for >= `REACTIVATE_MIN_DEACTIVATED_MS` (24h) — the throttle
 *      has almost certainly cleared, and this gives NATURAL BACKOFF: if a resume
 *      re-deactivates the account, its `lifecycle_updated_at` resets, so it
 *      won't be nudged again for another 24h (no tight retry loop).
 * Criterion 3 excludes low-inbox accounts (a 35%/43%/57% account stays off).
 *
 * Reading each account's live `status_message.responseCode` to skip an account
 * STILL under a live 550 (vs the 24h-age proxy) is a documented follow-up — it
 * costs one single-account GET per candidate; the 24h gate + natural backoff
 * covers the common case without it.
 *
 * Fail-loud per account; a resume error is counted under `failed` and the sweep
 * continues. In-cluster only (platform key via key-service).
 */
import { listAccounts, resumeAccount, type Account } from "./instantly-client";
import {
  fetchLifecycleByEmail,
  fetchLatestDeliveryByAccount,
  type LifecycleView,
  type AccountDelivery,
} from "./account-lifecycle-sync";
import { FULL_SCORE } from "./account-lifecycle";

/** Minimum time an account must have been deactivated before a resume nudge. */
export const REACTIVATE_MIN_DEACTIVATED_MS = 24 * 60 * 60 * 1000; // 24h

/**
 * Kill-switch, default OFF (mirrors PLACEMENT_TESTS_ENABLED / DELETE_FINISHED_
 * CONTACTS_ENABLED): reactivation nudges Instantly and can interact with the 550
 * throttle, so it stays disarmed until explicitly enabled. Exactly `"true"` =
 * ON; anything else (incl. unset) = OFF.
 */
export function isReactivateAccountsEnabled(): boolean {
  return process.env.REACTIVATE_ACCOUNTS_ENABLED === "true";
}

export interface ReactivateSummary {
  /** Accounts read from the live Instantly list. */
  accountsRead: number;
  /** Accounts that passed every gate (candidates for resume). */
  eligible: number;
  /** Accounts resumed this run. */
  reactivated: number;
  /** Resume calls that threw — left for the next run. */
  failed: number;
}

/**
 * Pure: emails of accounts eligible for a resume nudge (all 4 gates). `nowMs` is
 * the reference time; `minDeactivatedMs` the age gate (default 24h). Input order,
 * empty emails filtered out.
 */
export function selectReactivatable(
  accounts: Account[],
  lifecycleByEmail: Map<string, LifecycleView>,
  deliveryByEmail: Map<string, AccountDelivery>,
  nowMs: number,
  minDeactivatedMs: number = REACTIVATE_MIN_DEACTIVATED_MS,
): string[] {
  const eligible: string[] = [];
  for (const account of accounts) {
    if (!account.email) continue;
    const lifecycle = lifecycleByEmail.get(account.email);
    if (lifecycle?.status !== "deactivated_by_instantly") continue;

    // 2. Health == 100.
    if ((account.stat_warmup_score ?? 0) !== FULL_SCORE) continue;

    // 3. delivery FULL (100% inbox across every ESP of the latest test).
    if (!deliveryByEmail.get(account.email)?.full) continue;

    // 4. deactivated for >= minDeactivatedMs (throttle likely cleared + backoff).
    const updatedAt = lifecycle.updatedAt ? new Date(lifecycle.updatedAt).getTime() : NaN;
    if (!Number.isFinite(updatedAt) || nowMs - updatedAt < minDeactivatedMs) continue;

    eligible.push(account.email);
  }
  return eligible;
}

/**
 * IO glue: read the live account list + silver lifecycle + latest placement
 * delivery, select the eligible accounts, and `resume` each. `nowMs` is the
 * reference time; `limit` bounds the batch. Fail-loud per account.
 */
export async function reactivateEligibleAccounts(
  apiKey: string,
  nowMs: number,
  limit?: number,
): Promise<ReactivateSummary> {
  const [accounts, lifecycleByEmail, deliveryByEmail] = await Promise.all([
    listAccounts(apiKey),
    fetchLifecycleByEmail(),
    fetchLatestDeliveryByAccount(),
  ]);
  const eligible = selectReactivatable(accounts, lifecycleByEmail, deliveryByEmail, nowMs);
  const batch = limit && limit > 0 ? eligible.slice(0, limit) : eligible;

  let reactivated = 0;
  let failed = 0;
  for (const email of batch) {
    try {
      await resumeAccount(apiKey, email);
      reactivated += 1;
    } catch (error: unknown) {
      failed += 1;
      const message = error instanceof Error ? error.message : String(error);
      console.error(`[reactivate-accounts] resume failed email=${email}: ${message}`);
    }
  }

  return {
    accountsRead: accounts.length,
    eligible: eligible.length,
    reactivated,
    failed,
  };
}
