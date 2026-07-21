/**
 * Reactivate accounts Instantly DEACTIVATED that are healthy again — REASON-AWARE.
 *
 * The lifecycle model deliberately does NOT fight Instantly: an account with
 * `instantlyStatus <= 0` derives to `deactivated_by_instantly` and we leave it
 * off. Kevin wants healthy-again accounts (Health 100 + 100% inbox) brought back
 * automatically. But prod validation (2026-07-21) showed EVERY such account is
 * one of exactly two Instantly-deactivation reasons, and a blind `POST
 * /accounts/{email}/resume` is WRONG for both:
 *   - `status -3` = Gmail `550-5.4.5 Daily user sending limit exceeded` throttle.
 *     It clears BY ITSELF after ~24h of reduced sending and Instantly
 *     auto-reactivates. Nudging it with resume makes the throttle WORSE
 *     (CLAUDE.md 2026-07-14). DO NOT resume.
 *   - `status -1` = broken Google↔Instantly OAuth ("needs review"). A resume
 *     flips it to 1 only momentarily then it reverts; the durable fix is a
 *     manual Primeforge → Instantly re-export (UI, login creds — no API). Resume
 *     is useless. DO NOT resume.
 * Both carry `autofix_failed: true` — Instantly's own auto-recovery already gave
 * up, so our resume won't help either.
 *
 * So this sweep is REASON-AWARE: it resumes ONLY a genuinely-resumable pause —
 * NOT a `550` throttle, NOT an OAuth `-1`, NOT an `autofix_failed` account. In
 * prod today that resumes ZERO (every deactivation is `-3`/`-1`), which is the
 * correct safe outcome; it future-proofs the case where a genuinely-paused,
 * healthy account appears.
 *
 * The reason lives in the account's `status_message` ({responseCode, response})
 * which is present ONLY on the single-account GET (`getAccountRaw`), NOT the LIST
 * — so the cheap base gates run on the LIST first (incl. `status` not in
 * {-1,-3}, which needs no extra IO), and only the few survivors pay a
 * single-account GET to read the precise reason.
 *
 * Fail-loud per account; in-cluster only (platform key via key-service).
 */
import {
  listAccounts,
  getAccountRaw,
  resumeAccount,
  type Account,
} from "./instantly-client";
import {
  fetchLifecycleByEmail,
  fetchLatestDeliveryByAccount,
  type LifecycleView,
  type AccountDelivery,
} from "./account-lifecycle-sync";
import { FULL_SCORE } from "./account-lifecycle";

/** Minimum time an account must have been deactivated before a resume nudge. */
export const REACTIVATE_MIN_DEACTIVATED_MS = 24 * 60 * 60 * 1000; // 24h

/** Instantly account status codes that are NEVER resumable via the API. */
export const OAUTH_NEEDS_REVIEW_STATUS = -1; // broken OAuth — needs Primeforge re-export
export const THROTTLE_550_STATUS = -3; // Gmail 550 daily-limit — self-heals, do NOT nudge

/**
 * Kill-switch, default OFF (mirrors PLACEMENT_TESTS_ENABLED / DELETE_FINISHED_
 * CONTACTS_ENABLED). Exactly `"true"` = ON; anything else (incl. unset) = OFF.
 */
export function isReactivateAccountsEnabled(): boolean {
  return process.env.REACTIVATE_ACCOUNTS_ENABLED === "true";
}

/**
 * Pure: BASE candidates for a resume nudge, from the LIST + silver + delivery.
 * Gates (all required):
 *   1. lifecycle `deactivated_by_instantly` (silver),
 *   2. Health `stat_warmup_score == 100`,
 *   3. delivery FULL (100% inbox every ESP of the latest placement test),
 *   4. deactivated for >= `minDeactivatedMs` (throttle likely cleared + natural
 *      backoff — a re-deactivation resets `lifecycle_updated_at`),
 *   5. `status` NOT in {-1 OAuth, -3 550-throttle} — the two never-resumable
 *      reasons, cheaply excluded from the LIST (no extra IO).
 * The precise reason (status_message) is re-checked per survivor in
 * `isResumableAccountDetail` after a single-account GET.
 */
export function selectReactivationCandidates(
  accounts: Account[],
  lifecycleByEmail: Map<string, LifecycleView>,
  deliveryByEmail: Map<string, AccountDelivery>,
  nowMs: number,
  minDeactivatedMs: number = REACTIVATE_MIN_DEACTIVATED_MS,
): string[] {
  const candidates: string[] = [];
  for (const account of accounts) {
    if (!account.email) continue;
    const lifecycle = lifecycleByEmail.get(account.email);
    if (lifecycle?.status !== "deactivated_by_instantly") continue;
    if ((account.stat_warmup_score ?? 0) !== FULL_SCORE) continue;
    if (!deliveryByEmail.get(account.email)?.full) continue;

    const updatedAt = lifecycle.updatedAt ? new Date(lifecycle.updatedAt).getTime() : NaN;
    if (!Number.isFinite(updatedAt) || nowMs - updatedAt < minDeactivatedMs) continue;

    // Exclude the two never-resumable reasons cheaply from the LIST status.
    if (account.status === OAUTH_NEEDS_REVIEW_STATUS || account.status === THROTTLE_550_STATUS) {
      continue;
    }
    candidates.push(account.email);
  }
  return candidates;
}

/**
 * Pure: is a candidate's FULL single-account detail genuinely resumable? False
 * when the reason is a Gmail 550 throttle, an OAuth `-1`, or Instantly's own
 * auto-fix already failed. A missing `status_message` with a clean status is
 * treated as resumable (a plain pause). `raw` is the untouched Instantly account
 * object (`getAccountRaw`).
 */
export function isResumableAccountDetail(raw: Record<string, unknown>): boolean {
  const status = typeof raw.status === "number" ? raw.status : undefined;
  if (status === OAUTH_NEEDS_REVIEW_STATUS || status === THROTTLE_550_STATUS) return false;
  if (raw.autofix_failed === true) return false;

  const sm = raw.status_message;
  if (sm && typeof sm === "object") {
    const responseCode = (sm as { responseCode?: unknown }).responseCode;
    if (responseCode === 550) return false;
    const response = (sm as { response?: unknown }).response;
    if (typeof response === "string" && /5\.4\.5|daily user sending limit|550[-\s]?5\.4\.5/i.test(response)) {
      return false;
    }
  }
  return true;
}

export interface ReactivateSummary {
  /** Accounts read from the live Instantly list. */
  accountsRead: number;
  /** Passed the base LIST gates (before the per-account reason check). */
  candidates: number;
  /** Accounts resumed this run. */
  reactivated: number;
  /** Candidates skipped by the reason check (550 throttle / OAuth / autofix_failed). */
  skippedNotResumable: number;
  /** Resume / detail-fetch that threw — left for the next run. */
  failed: number;
}

/**
 * IO glue: read the live account list + silver lifecycle + latest delivery, pick
 * base candidates, then for each fetch the single-account detail to confirm the
 * reason is genuinely resumable before `resume`. `nowMs` is the reference time;
 * `limit` bounds the candidate batch. Fail-loud per account.
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
  const candidates = selectReactivationCandidates(
    accounts,
    lifecycleByEmail,
    deliveryByEmail,
    nowMs,
  );
  const batch = limit && limit > 0 ? candidates.slice(0, limit) : candidates;

  let reactivated = 0;
  let skippedNotResumable = 0;
  let failed = 0;
  for (const email of batch) {
    try {
      const raw = await getAccountRaw(apiKey, email);
      if (!isResumableAccountDetail(raw)) {
        skippedNotResumable += 1;
        continue;
      }
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
    candidates: candidates.length,
    reactivated,
    skippedNotResumable,
    failed,
  };
}
