/**
 * Reactivate accounts Instantly DEACTIVATED whose failure was TRANSIENT.
 *
 * The lifecycle model deliberately does NOT fight Instantly: an account with
 * `instantlyStatus <= 0` derives to `deactivated_by_instantly` and we leave it
 * off. This sweep brings back the ones Instantly turned off for a reason that
 * has since passed.
 *
 * THE GATE IS THE SMTP RESPONSE CLASS, NOT THE INSTANTLY `status` CODE.
 * An earlier version excluded `status -1` (broken OAuth) and `status -3`
 * (assumed = Gmail `550-5.4.5 Daily user sending limit exceeded`) outright, plus
 * anything with `autofix_failed: true`. Prod audit 2026-07-28 disproved every
 * premise of that gate:
 *   - ZERO of the 12 live `-3` accounts was a Gmail 550. All 12 were
 *     `provider_code 1` (the legacy Gandi/Mailforge IMAP relay fleet) — not
 *     Google mailboxes at all, so a Gmail per-user throttle is impossible for
 *     them by construction.
 *   - 9/12 were `450 4.1.2 <recipient>: Recipient address rejected: Domain not
 *     found` — the PROSPECT's domain is dead. Instantly deactivated OUR sender
 *     over a bad lead in our list. 2/12 were `450 4.7.1 … Too many mail per day
 *     for sasl <user>` — the Gandi relay's own cap. Every one a 4xx TRANSIENT
 *     reply, not a single 5xx.
 *   - "`-3` clears itself in ~24h" was false too: two had been down 3-6 days.
 *   - `autofix_failed` is a STALE field, not a verdict — it stayed `true` on
 *     three accounts throughout a resume that WORKED, then cleared to null after.
 * So `status`-code and `autofix_failed` exclusions blocked 12 healthy-enough
 * mailboxes indefinitely while blocking nothing real.
 *
 * `isResumableAccountDetail` now classifies by the SMTP reply itself: a
 * PERMANENT 5xx rejection (which is what a real Gmail `550-5.4.5` throttle looks
 * like) is never resumed; a TRANSIENT 4xx is. A `-1` OAuth account has no
 * recorded failure at all (`status_message: null`) so it IS retried — harmless,
 * because a still-broken OAuth simply reverts to `-1`, and the 24h gate below
 * gives it natural backoff (a re-deactivation resets `lifecycle_updated_at`).
 *
 * NO health / delivery gate. Resume only sets Instantly `status: 1`;
 * `deriveLifecycle` then decides what the account BECOMES, and an account below
 * the bar (health >= 95, delivery >= 90% inbox on every gated ESP) lands
 * `in_recovery` — which takes ZERO new sends. Gating this
 * sweep on health+delivery gated the same thing twice AND created a deadlock: a
 * `deactivated_by_instantly` account is outside `fetchTestablePoolEmails`
 * (`in_recovery` + `in_production` only), so it could never be placement-tested,
 * never earn delivery-at-bar, and never be reactivated. Dropping the gate breaks
 * the loop — the resumed account lands `in_recovery`, which IS testable.
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
  type LifecycleView,
} from "./account-lifecycle-sync";

/** Minimum time an account must have been deactivated before a resume nudge. */
export const REACTIVATE_MIN_DEACTIVATED_MS = 24 * 60 * 60 * 1000; // 24h

/**
 * A PERMANENT (5xx) SMTP rejection in the raw response text — either the reply
 * code at the start of the line (`550 …` / `550-…`) or an RFC 3463 enhanced
 * status code whose class is 5 (`5.4.5`, `5.7.1`). Class 4 (`450`, `4.1.2`,
 * `4.7.1`) is TRANSIENT and stays resumable.
 */
const PERMANENT_SMTP_RESPONSE = /(?:^|\s)5\d{2}[\s-]|\b5\.\d{1,3}\.\d{1,3}\b/;

/**
 * Kill-switch, default OFF (mirrors PLACEMENT_TESTS_ENABLED / DELETE_FINISHED_
 * CONTACTS_ENABLED). Exactly `"true"` = ON; anything else (incl. unset) = OFF.
 */
export function isReactivateAccountsEnabled(): boolean {
  return process.env.REACTIVATE_ACCOUNTS_ENABLED === "true";
}

/**
 * Pure: BASE candidates for a resume nudge, from the LIST + silver lifecycle.
 * Gates (both required):
 *   1. lifecycle `deactivated_by_instantly` (silver),
 *   2. deactivated for >= `minDeactivatedMs` (lets a transient block clear, and
 *      gives natural backoff — a re-deactivation resets `lifecycle_updated_at`,
 *      so an account that keeps failing is retried at most daily).
 * The account must still exist in the live Instantly LIST. There is deliberately
 * NO health / delivery / `status`-code gate here — see the file header: the
 * lifecycle derivation is the real safety gate (a below-bar account resumes into
 * `in_recovery`, which sends nothing), and gating on delivery deadlocked the
 * account out of ever being placement-tested. The genuine block (a PERMANENT
 * SMTP rejection) is checked per survivor in `isResumableAccountDetail`.
 */
export function selectReactivationCandidates(
  accounts: Account[],
  lifecycleByEmail: Map<string, LifecycleView>,
  nowMs: number,
  minDeactivatedMs: number = REACTIVATE_MIN_DEACTIVATED_MS,
): string[] {
  const candidates: string[] = [];
  for (const account of accounts) {
    if (!account.email) continue;
    const lifecycle = lifecycleByEmail.get(account.email);
    if (lifecycle?.status !== "deactivated_by_instantly") continue;

    const updatedAt = lifecycle.updatedAt ? new Date(lifecycle.updatedAt).getTime() : NaN;
    if (!Number.isFinite(updatedAt) || nowMs - updatedAt < minDeactivatedMs) continue;

    candidates.push(account.email);
  }
  return candidates;
}

/**
 * Pure: is a candidate's FULL single-account detail genuinely resumable?
 *
 * The verdict comes from the SMTP reply Instantly recorded in `status_message`
 * ({responseCode, response, e_message}), which is present ONLY on the
 * single-account GET (`getAccountRaw`), not the LIST:
 *   - PERMANENT 5xx (a real Gmail `550-5.4.5 Daily user sending limit exceeded`,
 *     a `5.7.1` block) → NOT resumable. Resuming would nudge a live throttle.
 *   - TRANSIENT 4xx (`450 4.1.2 Domain not found` — a dead PROSPECT domain;
 *     `450 4.7.1 Too many mail per day for sasl` — the relay's own cap) →
 *     resumable. The condition has passed or was never ours.
 *   - No `status_message` at all (e.g. an OAuth `-1` "needs review", or a plain
 *     pause) → resumable. A still-broken OAuth just reverts to `-1`, and the 24h
 *     age gate keeps that from churning.
 *
 * Deliberately does NOT read `status` or `autofix_failed` — both proved to be
 * false blockers in prod (see the file header). `raw` is the untouched Instantly
 * account object.
 */
export function isResumableAccountDetail(raw: Record<string, unknown>): boolean {
  const sm = raw.status_message;
  if (!sm || typeof sm !== "object") return true;

  const responseCode = (sm as { responseCode?: unknown }).responseCode;
  if (typeof responseCode === "number" && responseCode >= 500 && responseCode < 600) return false;

  for (const key of ["response", "e_message"] as const) {
    const text = (sm as Record<string, unknown>)[key];
    if (typeof text === "string" && PERMANENT_SMTP_RESPONSE.test(text)) return false;
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
  /** Candidates skipped by the reason check (a PERMANENT 5xx SMTP rejection). */
  skippedNotResumable: number;
  /** Resume / detail-fetch that threw — left for the next run. */
  failed: number;
}

/**
 * IO glue: read the live account list + silver lifecycle, pick base candidates,
 * then for each fetch the single-account detail to confirm the recorded SMTP
 * failure is transient before `resume`. `nowMs` is the reference time; `limit`
 * bounds the candidate batch. Fail-loud per account.
 */
export async function reactivateEligibleAccounts(
  apiKey: string,
  nowMs: number,
  limit?: number,
): Promise<ReactivateSummary> {
  const [accounts, lifecycleByEmail] = await Promise.all([
    listAccounts(apiKey),
    fetchLifecycleByEmail(),
  ]);
  const candidates = selectReactivationCandidates(accounts, lifecycleByEmail, nowMs);
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
