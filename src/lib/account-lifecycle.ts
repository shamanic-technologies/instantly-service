/**
 * Per-account LIFECYCLE — pure derivation (no IO). Replaces the manual
 * "rest an account" blacklist with an auto-driven, health-derived state machine.
 *
 * IO glue (snapshot accounts, read placement delivery, reconcile, PATCH warmup)
 * lives in lib/account-lifecycle-sync.ts. This file is only the pure logic + the
 * constants, so `deriveLifecycle` can be unit-tested exhaustively.
 *
 * ── The model (LOCKED) — four states, first match wins ───────────────────────
 *   domain ∈ domain_policy                          → deactivated_by_user
 *   instantlyStatus <= 0                            → deactivated_by_instantly
 *   healthScore < BAR **and not already in prod**   → in_recovery
 *   delivery below BAR, or its evidence stale       → in_recovery
 *   otherwise                                       → in_production
 *
 * - `healthScore` = Instantly `stat_warmup_score` (0-100), gated at 90. The bar
 *   is ASYMMETRIC: it gates ENTRY into production, never continued membership,
 *   because a production account no longer warms and its score therefore decays
 *   to 0. See {@link deriveLifecycle} for why that is the point, not a leak.
 * - `deliveryAtBar` POOLS every (account, ESP) row of the account's latest
 *   placement test into one score (see {@link isDeliveryAtBar}), gated at 90.
 *   Delivery UNKNOWN (never tested) is passed as `null` and treated as below bar,
 *   so an untested account defaults to in_recovery. Since delivery is the only
 *   demotion path, its evidence also expires — see
 *   {@link DELIVERY_EVIDENCE_MAX_AGE_DAYS}.
 */

export type LifecycleStatus =
  | "in_production"
  | "in_recovery"
  | "deactivated_by_instantly"
  | "deactivated_by_user";

/**
 * Snapshot reason recorded on the lifecycle event. `reactivated` is NOT produced
 * by `deriveLifecycle` (it depends on the PRIOR state) — the reconcile glue
 * overrides the reason to `reactivated` when an account leaves
 * deactivated_by_instantly. Every other reason is a pure function of the inputs.
 */
export type LifecycleReason =
  | "brand_domain"
  | "deactivated_by_instantly"
  | "health_below_bar"
  | "delivery_below_bar"
  | "delivery_evidence_stale"
  | "passed"
  | "reactivated";

export interface DeriveLifecycleInput {
  /** Instantly account.status (numeric; <= 0 ⇒ Instantly disabled the account). */
  instantlyStatus: number;
  /** The account's email domain (part after `@`). */
  domain: string;
  /** Instantly stat_warmup_score (0-100). */
  healthScore: number;
  /**
   * True ⇔ the account's latest placement test inboxed at
   * >= PRODUCTION_DELIVERY_PCT_BAR POOLED across every ESP
   * (see {@link isDeliveryAtBar}).
   * `null` = never placement-tested → treated as below bar.
   */
  deliveryAtBar: boolean | null;
  /**
   * When the account's latest placement test RAN. A passing but ancient result
   * is not evidence — see {@link DELIVERY_EVIDENCE_MAX_AGE_DAYS}.
   * `null` = never tested (already covered by `deliveryAtBar: null`).
   */
  deliveryTestedAt: Date | string | null;
  /**
   * The account's CURRENT lifecycle status, or `null` if never classified.
   *
   * Load-bearing: the health bar is asymmetric (entry-only), so the target state
   * is NOT a pure function of the health/delivery scores alone — see
   * {@link deriveLifecycle}.
   */
  currentStatus: LifecycleStatus | null;
  /** Set of brand/product domains from instantly_domain_policy. */
  domainPolicy: ReadonlySet<string>;
  /** Clock, for the delivery-evidence freshness check. */
  asOf: Date;
}

export interface Lifecycle {
  status: LifecycleStatus;
  reason: LifecycleReason;
}

/**
 * The production bars — BOTH 90, both a single score over a single sample:
 *   - health: Instantly `stat_warmup_score` >= 90
 *   - delivery: >= 90% inbox POOLED across every ESP of the latest placement test
 *     (`Σinbox / Σseeds`), the same number the ops dashboard displays.
 *
 * Delivery was briefly gated PER ESP (every leg had to clear the bar) with a
 * 5-seed floor to stop Instantly's 1-3 seed "other" bucket from vetoing a good
 * account. That whole apparatus is gone: one test, one score, one bar.
 *
 * Both bars were 100 until 2026-07-28, then 95, then 90 (2026-08-12). The 95 →
 * 90 move was driven by a QUANTIZATION artifact, not by a desire for a looser
 * gate: a placement test seeds ~38-40 mailboxes, so the achievable pooled scores
 * bracketing 95 are 36/38 = 94.7 and 37/38 = 97.4 — there is NOTHING between
 * 94.9 and 97.4. A "95" bar therefore meant, in practice, "at most ONE seed in
 * spam", and 7 accounts sat blocked at 94.7-94.9 (2 spam seeds) while 4 accounts
 * already IN production sat at exactly 95.0. Dropping to 90 admits that cohort
 * without reaching the next real cluster down (92.3, then 89.7).
 *
 * ⚠️ Pooling blends a strong Outlook leg into a weaker Gmail one, so the bar and
 * the pooled form are a PAIR — re-measure before moving either. Measured against
 * the live fleet 2026-08-12, at a 90 bar:
 *   - 37 accounts qualify (28 under the 95 bar); the worst GMAIL leg admitted is
 *     88.9%, against a 92.3% worst leg under 95.
 *   - the ~187 Gmail-spam shared-IP accounts this gate exists to catch stay
 *     excluded — the best of them pools to 89.7%.
 *   - residual: the margin under the bar is now thin (89.7 vs 90), so test noise
 *     will oscillate accounts across it week to week, and a seed split skewed
 *     toward Outlook (max observed share 83%) could in theory pass a bad Gmail
 *     leg. No account admitted today has such a split (all are 29-40% Outlook).
 *
 * Do NOT lower `PRODUCTION_DELIVERY_PCT_BAR` further without re-running that
 * Gmail-leg distribution over the accounts it would newly admit.
 */
export const PRODUCTION_HEALTH_BAR = 90;
export const PRODUCTION_DELIVERY_PCT_BAR = 90;

/**
 * Warmup daily send volume pushed to Instantly per target lifecycle state.
 * Paired with the campaign daily_limit below so EVERY state's total daily send
 * (campaign + warmup) stays at 50 — under Gmail's per-user daily sending limit,
 * which throttled the fleet with a `550-5.4.5 Daily user sending limit exceeded`
 * when it ran at 60/day (2026-07-19 incident).
 *
 * in_production warms at ZERO (2026-08-12, was 5): the fleet is capacity-bound,
 * and those 5 slots are worth more as real sends. Warmup only exists to keep a
 * mailbox's reputation alive while it is NOT carrying campaign traffic — an
 * account sending 50 real emails a day is warming itself.
 *
 * ⚠️ This is only safe BECAUSE the health bar is entry-only. Instantly's warmup
 * score is a rolling 7-day window over warmup activity, so with no warmup it
 * RESETS TO 0 — it does not hold its last value (verified: a prod account whose
 * warmup volume stopped went 100 → 44 → 0 in 48h with warmup still enabled).
 * Under a symmetric bar, every in_production account would demote itself within
 * a week. See {@link deriveLifecycle}.
 */
export const IN_PRODUCTION_WARMUP_DAILY = 0; // self-warming via real volume (50 + 0 = 50)
export const RECOVERY_WARMUP_DAILY = 30; // recover reputation → warm harder, send less (20 + 30 = 50)

/**
 * Campaign daily max-send pushed to Instantly on a flip INTO in_production (50)
 * or in_recovery (20). Paired with the warmup volume above so the total stays 50.
 * deactivated_* states leave the campaign `daily_limit` untouched (null) so an
 * off account keeps draining its already-loaded queue at whatever limit it had.
 */
export const IN_PRODUCTION_DAILY_LIMIT = 50;
export const RECOVERY_DAILY_LIMIT = 20;

/**
 * How long a placement result stays usable as evidence, in days.
 *
 * With the health bar entry-only, the weekly placement test is the ONLY signal
 * that can demote a production account — and `isDeliveryAtBar` reads the LATEST
 * test without regard for its age, so a passing-but-ancient result would pin an
 * account in production forever if the placement cron silently stopped
 * producing. (It has: the `run`/`sync` chaining bug left results up to 7 days
 * stale.) Past this age the evidence is treated as unknown → in_recovery.
 *
 * 16 days = two consecutive missed Saturdays. It CANNOT fire in normal
 * operation: tests run exactly 7 days apart and ingestion lags the run by 40min
 * to 3 days (the test itself takes hours; `sync` is daily), so the freshest
 * evidence legitimately reaches ~10 days old just before a new cycle lands.
 * A 7-day cap would demote the whole fleet every Sunday morning.
 */
export const DELIVERY_EVIDENCE_MAX_AGE_DAYS = 16;

/**
 * True ⇔ the placement evidence is recent enough to still be believed.
 * `null`/absent `testedAt` ⇒ false (no evidence is not fresh evidence).
 */
export function isDeliveryEvidenceFresh(
  testedAt: Date | string | null | undefined,
  asOf: Date,
): boolean {
  if (testedAt === null || testedAt === undefined) return false;
  const t = testedAt instanceof Date ? testedAt : new Date(testedAt);
  const ms = t.getTime();
  if (Number.isNaN(ms)) return false;
  return asOf.getTime() - ms <= DELIVERY_EVIDENCE_MAX_AGE_DAYS * 24 * 60 * 60 * 1000;
}

/**
 * Pure lifecycle derivation. First match wins (order is load-bearing — a domain
 * in the policy is deactivated_by_user even if Instantly-disabled or under-warmed).
 *
 * ⚠️ THE HEALTH BAR IS ASYMMETRIC — it gates ENTRY into production, never
 * CONTINUED MEMBERSHIP. An account already `in_production` is never demoted for
 * a low warmup score; only delivery can put it back into recovery. This is not a
 * leniency, it is what makes `IN_PRODUCTION_WARMUP_DAILY = 0` possible: Instantly
 * computes the warmup score over a rolling 7-day window of warmup activity, so an
 * account that stops warming has its score reset to 0 within a week. Under a
 * symmetric bar the fleet would demote itself every 7 days, permanently.
 *
 * The trade is deliberate: the warmup score is a PROXY (does the warmup pool see
 * us?), the placement test is the REAL measurement (does Gmail see us?), and the
 * test does not depend on warmup — it sends its own seeds from the mailbox. So a
 * production account keeps being graded weekly on the signal that matters, and
 * the proxy is only used to prove a recovering account is ready to come back.
 *
 * Consequence: delivery is the sole demotion path, so its evidence must not be
 * allowed to go stale unnoticed — hence {@link DELIVERY_EVIDENCE_MAX_AGE_DAYS}.
 */
export function deriveLifecycle(input: DeriveLifecycleInput): Lifecycle {
  const {
    instantlyStatus,
    domain,
    healthScore,
    deliveryAtBar,
    deliveryTestedAt,
    currentStatus,
    domainPolicy,
    asOf,
  } = input;

  if (domainPolicy.has(domain)) {
    return { status: "deactivated_by_user", reason: "brand_domain" };
  }
  if (instantlyStatus <= 0) {
    return {
      status: "deactivated_by_instantly",
      reason: "deactivated_by_instantly",
    };
  }
  // Health gates ENTRY only: an account already in production keeps its place
  // regardless of warmup score (it no longer warms — see the note above).
  if (healthScore < PRODUCTION_HEALTH_BAR && currentStatus !== "in_production") {
    return { status: "in_recovery", reason: "health_below_bar" };
  }
  // Delivery demotes from anywhere. Never tested → below bar.
  if (deliveryAtBar !== true) {
    return { status: "in_recovery", reason: "delivery_below_bar" };
  }
  // A passing result we can no longer vouch for is not a pass.
  if (!isDeliveryEvidenceFresh(deliveryTestedAt, asOf)) {
    return { status: "in_recovery", reason: "delivery_evidence_stale" };
  }
  return { status: "in_production", reason: "passed" };
}

/**
 * Warmup daily volume to PATCH into Instantly when an account flips INTO a state.
 * `null` ⇒ do NOT touch warmup (deactivated_by_instantly — the account is off).
 */
export function warmupDailyForStatus(status: LifecycleStatus): number | null {
  switch (status) {
    case "in_production":
      return IN_PRODUCTION_WARMUP_DAILY;
    case "in_recovery":
    case "deactivated_by_user":
      return RECOVERY_WARMUP_DAILY;
    case "deactivated_by_instantly":
      return null;
  }
}

/**
 * Campaign daily max-send to PATCH into Instantly on a flip. in_production opens
 * the tap to 50, in_recovery caps it to 20 (paired with more warmup so the total
 * stays 50); deactivated_* states return `null` = do NOT touch the campaign
 * daily_limit, so an off account keeps draining its already-loaded queue.
 */
export function dailyLimitForStatus(status: LifecycleStatus): number | null {
  switch (status) {
    case "in_production":
      return IN_PRODUCTION_DAILY_LIMIT;
    case "in_recovery":
      return RECOVERY_DAILY_LIMIT;
    case "deactivated_by_user":
    case "deactivated_by_instantly":
      return null;
  }
}

/** Domain part of an email (lowercased), or "" when there is no `@domain`. */
export function emailDomain(email: string): string {
  return (email.split("@")[1] ?? "").toLowerCase();
}

/**
 * Account AGE gate. An account is "mature" once it is at least this old; a
 * FRESHER account (a new Google/Workspace mailbox) has a much lower REAL Gmail
 * per-user send quota during its first weeks — INDEPENDENT of inbox placement —
 * so pushing full campaign volume onto it day-one trips `550-5.4.5 Daily user
 * sending limit exceeded`. Age is NOT a lifecycle state (a fresh account stays
 * `in_production` if it passes health+delivery); it only (a) SCALES DOWN the daily
 * assignment cap send selection uses for it (see `rampCapForAge`) and (b) keeps
 * Instantly's slow ramp ON so its volume grows gently. 28 days = the ~4-week ramp
 * window Gmail needs to build per-user send trust.
 */
export const MATURE_AGE_DAYS = 28;
const MATURE_AGE_MS = MATURE_AGE_DAYS * 24 * 60 * 60 * 1000;

/** Account age in ms at `asOf`, or null when the created timestamp is unknown /
 * unparseable (→ callers treat unknown as mature, never trapping an undatable account). */
function accountAgeMs(
  timestampCreated: string | Date | null | undefined,
  asOf: Date,
): number | null {
  if (!timestampCreated) return null;
  const created =
    timestampCreated instanceof Date ? timestampCreated.getTime() : Date.parse(timestampCreated);
  if (Number.isNaN(created)) return null;
  return asOf.getTime() - created;
}

/**
 * Floor for a fresh account's daily assignment cap. Even a day-old mailbox takes
 * a few leads a day — that IS the ramp. Zero would starve it (and starvation is
 * not a ramp: an idle mailbox builds no Gmail send trust at all).
 */
export const RAMP_FLOOR_PER_DAY = 5;

/**
 * The account's DAILY ASSIGNMENT CAP — how many emails send-selection is willing
 * to put on this account today. Mature (or undatable) → its full Instantly
 * `daily_limit`. Fresh → that limit scaled LINEARLY by age over the
 * {@link MATURE_AGE_DAYS} window, floored at {@link RAMP_FLOOR_PER_DAY} and never
 * above the account's own limit.
 *
 * This REPLACED the former mature-before-fresh tier ordering in send selection.
 * That ordering made "fresh" a PRIORITY class (fresh took volume only as overflow,
 * once no mature account had room today) — and since the mature pool's headroom
 * exceeded fleet volume, the overflow branch never ran and every fresh account got
 * ZERO sends for weeks (prod 2026-07-29→31: 845/845 campaigns to mature accounts,
 * 5 accounts never assigned a single campaign in their life). Age is now a CAP,
 * not a priority: a fresh account always gets a proportional share, bounded by
 * what Gmail will accept from a young mailbox.
 */
export function rampCapForAge(
  timestampCreated: string | Date | null | undefined,
  dailyLimit: number,
  asOf: Date,
): number {
  const age = accountAgeMs(timestampCreated, asOf);
  if (age === null || age >= MATURE_AGE_MS) return dailyLimit;
  const ageDays = age / (24 * 60 * 60 * 1000);
  const ramped = Math.round((dailyLimit * ageDays) / MATURE_AGE_DAYS);
  return Math.min(dailyLimit, Math.max(RAMP_FLOOR_PER_DAY, ramped));
}

/**
 * Target `enable_slow_ramp` by age: fresh → `true` (ramp volume gently), mature →
 * `false` (full volume, no throttle needed). Unknown created date → `null` = do
 * NOT touch (avoid flipping an account we cannot date until the timestamp backfills).
 */
export function slowRampForAge(
  timestampCreated: string | Date | null | undefined,
  asOf: Date,
): boolean | null {
  const age = accountAgeMs(timestampCreated, asOf);
  if (age === null) return null;
  return age < MATURE_AGE_MS;
}

/**
 * The delivery bar: the account's latest placement test must inbox at
 * >= {@link PRODUCTION_DELIVERY_PCT_BAR}% POOLED across every ESP —
 * `Σinbox / Σseeds`. `espRows` is one row per (account, ESP) of that test.
 *
 * ONE score, no per-leg gating and no seed floor: sample size is deliberately not
 * a gate, so a small test still grades (see {@link PRODUCTION_DELIVERY_PCT_BAR}
 * for the fleet measurement behind the bar's value). This is the same number
 * `summarizeEspRows` displays, so the ops row and the lifecycle reason cannot
 * contradict each other.
 *
 * No test at all, or every row 0 seeds → false (delivery unknown → recovery).
 * Never fabricates a pass.
 */
export function isDeliveryAtBar(
  espRows: ReadonlyArray<{ inboxCount: number; seedTotal: number }>,
): boolean {
  const seedTotal = espRows.reduce((sum, r) => sum + r.seedTotal, 0);
  if (seedTotal === 0) return false;
  const inboxCount = espRows.reduce((sum, r) => sum + r.inboxCount, 0);
  // Integer comparison (inbox * 100 >= seed * bar) — no float rounding.
  return inboxCount * 100 >= seedTotal * PRODUCTION_DELIVERY_PCT_BAR;
}
