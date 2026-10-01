/**
 * IDEMPOTENT ENFORCEMENT, on EVERY run (not only on a flip), of each account's:
 *   - warmup.limit + campaign daily_limit → the target for its CURRENT lifecycle
 *     state (in_production / in_recovery); and
 *   - enable_slow_ramp → its AGE target (fresh < MATURE_AGE_DAYS → on, mature →
 *     off), INDEPENDENT of lifecycle state. This is the enforcement home for the
 *     age→slow-ramp rule: an account crossing the ~4-week line does NOT flip
 *     lifecycle state, so reconcile (flip-only) never turns its ramp off — this
 *     hourly sweep does. A fresh Google mailbox at full volume trips 550-5.4.5;
 *     slow ramp grows its volume gently until its Gmail send quota builds.
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
import { fetchRecentDailyVolume, sustainedFor } from "./recent-send-volume";
import {
  listAccounts,
  setWarmupDailyLimit,
  setDailyLimit,
  setSlowRamp,
  type Account,
} from "./instantly-client";
import { fetchLifecycleByEmail, type LifecycleView } from "./account-lifecycle-sync";
import { loadMailboxLogins } from "./self-send/mailbox-credentials";
import {
  warmupDailyForStatus,
  dailyLimitForStatus,
  slowRampForAge,
  rampCapForVolume,
  isInstantlyEnforced,
  IN_PRODUCTION_DAILY_LIMIT,
  type LifecycleStatus,
} from "./account-lifecycle";

/** A per-account patch plan: which fields drift from the lifecycle target. */
export interface LifecycleLimitPatch {
  email: string;
  /** Target warmup daily volume to PATCH, or null if already aligned. */
  warmup: number | null;
  /** Target campaign daily_limit to PATCH, or null if already aligned. */
  daily: number | null;
  /** Target `enable_slow_ramp` (age-driven), or null if aligned / age unknown. */
  slowRamp: boolean | null;
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
  /** enable_slow_ramp PATCHes issued (age-driven). */
  slowRampPatched: number;
  /** Accounts whose PATCH threw — left for the next run. */
  failed: number;
}

/**
 * Pure: compute the per-account drift patch.
 *   - warmup.limit is enforced on EVERY account in a limited state, an smtp
 *     one Instantly disabled included (it warms with its held value on resume).
 *     daily_limit is enforced on the `instantly` transport, AND on an `smtp`
 *     account Instantly still holds ACTIVE (our own cap reads its
 *     `daily_limit`) — not on a DISABLED smtp account. Enforced ONLY when the silver lifecycle is
 *     `in_production` or `in_recovery` (their targets are non-null); any other
 *     state (or unknown lifecycle) leaves both untouched. The daily_limit target
 *     is additionally capped by the VOLUME ramp (`rampCapForVolume`), so a quiet mailbox
 *     is held to what Gmail will actually accept from it rather than the state's
 *     full 45/20 — this is the Instantly-side twin of the send-selection cap.
 *   - enable_slow_ramp is AGE-driven and INDEPENDENT of lifecycle state: a fresh
 *     account (< MATURE_AGE_DAYS) targets `true` (ramp gently — a fresh Google
 *     mailbox at full volume trips 550-5.4.5), a mature one targets `false`, and
 *     an undatable account (no `timestamp_created`) is skipped (`null`). This is
 *     the enforcement home for the age→slow-ramp rule: reconcile only flips on a
 *     STATE change, but an account crossing the 4-week line does NOT flip state,
 *     so the hourly sweep is what turns its ramp off.
 *   - ⚠️ warmup.limit is budgeted PER REAL MAILBOX (relay login), not per address
 *     — see {@link warmupTargetsByLogin}. Instantly applies `warmup.limit` to
 *     each ALIAS, so N aliases on one login at the per-state 30 warm 30×N/day
 *     through ONE mailbox's quota. `mailboxLoginByEmail` (address → login, from
 *     `loadMailboxLogins`) is what groups them; an address absent from it is its
 *     own login, i.e. behaves exactly as a single-alias mailbox.
 * Returns only accounts with at least one drifting field, in input order; empty
 * emails filtered out.
 */
export function selectLifecycleLimitPatches(
  accounts: Account[],
  lifecycleByEmail: Map<string, LifecycleView>,
  asOf: Date = new Date(),
  recentSustainedByEmail: ReadonlyMap<string, number> = new Map(),
  mailboxLoginByEmail: ReadonlyMap<string, string> = new Map(),
): LifecycleLimitPatch[] {
  const warmupByLogin = warmupTargetsByLogin(accounts, lifecycleByEmail, mailboxLoginByEmail);
  const patches: LifecycleLimitPatch[] = [];
  for (const account of accounts) {
    if (!account.email) continue;
    const view = lifecycleByEmail.get(account.email);
    const status = view?.status as LifecycleStatus | null | undefined;
    // An account we dispatch OURSELVES (`smtp`) is not sent through Instantly's
    // campaigns, so `enable_slow_ramp` (a campaign setting) is meaningless there
    // and is never touched.
    const instantlyEnforced = isInstantlyEnforced(view?.sendTransport ?? "instantly");
    // ⚠️ But warmup.limit + daily_limit ARE still ours to enforce on an smtp
    // account that Instantly holds ACTIVE (`status > 0`), for two reasons:
    //   1. Instantly's warmup POOL still dispatches `warmup.limit`/day FROM the
    //      mailbox — invisible to `fetchRecentDailyVolume`, on top of our own
    //      warmup mesh — i.e. it spends the exact Gmail quota our cap guards.
    //   2. Our own selector's cap (`capForAccount`) reads `daily_limit` from
    //      silver, which mirrors Instantly's value. A value frozen at a flip
    //      that happened on smtp (reconcile skips smtp) caps the mailbox there.
    // Measured 2026-09-29: 16 smtp in_production mailboxes sat at the recovery
    // 20/30 and sent ~18/day while their peers at 50/0 sent ~45.
    // An smtp account Instantly DISABLED (`status <= 0`) keeps its daily_limit
    // untouched (not our pipe, nothing reads it), but its WARMUP is still
    // enforced: Instantly accepts the PATCH on a disabled account (verified
    // 2026-10-01 on a `-1` alias), and the account warms with whatever it holds
    // the moment Instantly resumes it (growthagency.diy/.email flap -3 <-> 1;
    // skipping them left 14 logins holding 30 per alias, 120-150 per mailbox).
    const enforceDaily = instantlyEnforced || (account.status ?? 0) > 0;
    const limited = status === "in_production" || status === "in_recovery";

    let warmup: number | null = null;
    let daily: number | null = null;
    if (limited) {
      // The login's per-alias share, not the per-state figure — see
      // `warmupTargetsByLogin`. Always defined here: this account is in a
      // limited state, which is exactly what that map is keyed over.
      const targetWarmup =
        warmupByLogin.get(loginOf(account.email, mailboxLoginByEmail)) ??
        warmupDailyForStatus(status);
      const stateDaily = enforceDaily ? dailyLimitForStatus(status) : null; // 50 | 20
      // ⚠️ NO volume ramp here. Instantly is the pipe for these mailboxes, and
      // our volume figure is blind to its warmup pool — see
      // `rampAppliesToTransport`. Applying the ramp wrote a floor of 5 onto
      // 103-day-old mailboxes scoring 92-100% inbox, and re-wrote it every hour.
      // Instantly throttles its own with `enable_slow_ramp`, set by age below.
      const targetDaily = stateDaily;
      const currentWarmup = account.warmup?.limit;
      const currentDaily = account.daily_limit;
      warmup = targetWarmup !== null && currentWarmup !== targetWarmup ? targetWarmup : null;
      daily = targetDaily !== null && currentDaily !== targetDaily ? targetDaily : null;
    }

    // Age-driven slow ramp — every account, every state (but only where
    // Instantly's campaigns are the pipe, i.e. never on smtp; see above).
    const targetSlowRamp = instantlyEnforced
      ? slowRampForAge(account.timestamp_created, asOf)
      : null;
    const slowRamp =
      targetSlowRamp !== null && account.enable_slow_ramp !== targetSlowRamp
        ? targetSlowRamp
        : null;

    if (warmup !== null || daily !== null || slowRamp !== null) {
      patches.push({ email: account.email, warmup, daily, slowRamp });
    }
  }
  return patches;
}

/** The real mailbox an address spends the quota of; itself when unknown. */
function loginOf(email: string, mailboxLoginByEmail: ReadonlyMap<string, string>): string {
  const address = email.trim().toLowerCase();
  return mailboxLoginByEmail.get(address) ?? address;
}

/**
 * Pure: the Instantly `warmup.limit` each ALIAS of a login gets, keyed by login.
 * SCOPE: this sweep only (the hourly `lifecycle-limits-sync`); reconcile's flip
 * PATCH still writes the per-state figure and this sweep corrects it next hour.
 *
 * Instantly sets warmup PER ADDRESS, so its pool warms `limit` from EACH alias —
 * all through the one relay login the provider meters (measured 2026-09-28:
 * growthagency.email's 4 aliases at 30 each sent ~117 warmup/day through one
 * mailbox). The per-state targets (50/0 production, 20/30 recovery) are sized so
 * a MAILBOX totals 50/day; so is the warmup:
 *   - ANY in_production alias on the login → 0 on every alias. Production
 *     self-warms with real volume and the login's 50 goes to outreach; Instantly
 *     warming its recovery siblings would spend that same quota invisibly to our
 *     selector. The production check reads every alias our lifecycle calls
 *     in_production, including an smtp one Instantly has disabled — we still
 *     dispatch outreach from it ourselves.
 *   - otherwise (all aliases in_recovery) → floor(30 / N) per alias, N = EVERY
 *     limited-state alias on the login, Instantly-disabled ones included (they
 *     are patched too and warm again the moment Instantly resumes them), so the
 *     login's total is ≤ 30 whichever aliases are up. Measured 2026-10-01:
 *     counting only the active ones, an alias flapping -3 ↔ 1 swung the split
 *     6 ↔ 7 and growthagency.diy/.email warmed 34.
 * A single-alias login gets exactly the per-state figure, as before.
 * Aliases in deactivated_* states are untouched by this sweep and not counted.
 */
export function warmupTargetsByLogin(
  accounts: Account[],
  lifecycleByEmail: Map<string, LifecycleView>,
  mailboxLoginByEmail: ReadonlyMap<string, string>,
): Map<string, number> {
  const enforcedCount = new Map<string, number>();
  const hasProduction = new Set<string>();
  const perStateTarget = new Map<string, number>();
  for (const account of accounts) {
    if (!account.email) continue;
    const view = lifecycleByEmail.get(account.email);
    const status = view?.status;
    const login = loginOf(account.email, mailboxLoginByEmail);
    if (status === "in_production") hasProduction.add(login);
    if (status !== "in_production" && status !== "in_recovery") continue;
    enforcedCount.set(login, (enforcedCount.get(login) ?? 0) + 1);
    const target = warmupDailyForStatus(status) ?? 0;
    // A login mixing states takes the smaller per-state figure (production's 0).
    perStateTarget.set(login, Math.min(perStateTarget.get(login) ?? target, target));
  }
  const byLogin = new Map<string, number>();
  for (const [login, count] of enforcedCount) {
    const total = hasProduction.has(login) ? 0 : (perStateTarget.get(login) ?? 0);
    byLogin.set(login, Math.floor(total / count));
  }
  return byLogin;
}

/**
 * IO glue: read the FULL live account list + the silver lifecycle projection,
 * then PATCH each drifting field to its lifecycle target. `limit` bounds the
 * batch (account count); omit to sweep all. `asOf` is optional for deterministic
 * tests (the age-driven targets would otherwise move with the wall clock).
 */
export async function syncLifecycleLimits(
  apiKey: string,
  limit?: number,
  asOf: Date = new Date(),
): Promise<LifecycleLimitsSyncSummary> {
  // The address → real-mailbox map, from the same live loader the dispatcher
  // uses. Fails LOUD: without it the sweep would warm each alias at the full
  // per-state figure again — the multiplication this map exists to prevent.
  const [accounts, lifecycleByEmail, volume, mailboxLoginByEmail] = await Promise.all([
    listAccounts(apiKey),
    fetchLifecycleByEmail(),
    fetchRecentDailyVolume(),
    loadMailboxLogins({ method: "POST", path: "/internal/audit/lifecycle-limits-sync" }),
  ]);
  // daily_limit stays per ADDRESS (the selector folds aliases for outreach);
  // warmup is split per LOGIN via `mailboxLoginByEmail`.
  const patches = selectLifecycleLimitPatches(
    accounts,
    lifecycleByEmail,
    asOf,
    new Map(accounts.filter((a) => a.email).map((a) => [a.email as string, sustainedFor(volume, a.email as string)])),
    mailboxLoginByEmail,
  );
  const batch = limit && limit > 0 ? patches.slice(0, limit) : patches;

  let accountsPatched = 0;
  let warmupPatched = 0;
  let dailyPatched = 0;
  let slowRampPatched = 0;
  let failed = 0;

  for (const patch of batch) {
    try {
      // Warmup FIRST (mirrors reconcile). A warmup throw aborts this account's
      // remaining PATCHes for this run — next run heals the rest.
      if (patch.warmup !== null) {
        await setWarmupDailyLimit(apiKey, patch.email, patch.warmup);
        warmupPatched += 1;
      }
      if (patch.daily !== null) {
        await setDailyLimit(apiKey, patch.email, patch.daily);
        dailyPatched += 1;
      }
      if (patch.slowRamp !== null) {
        await setSlowRamp(apiKey, patch.email, patch.slowRamp);
        slowRampPatched += 1;
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
    slowRampPatched,
    failed,
  };
}
