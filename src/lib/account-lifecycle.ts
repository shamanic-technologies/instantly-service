/**
 * Per-account LIFECYCLE — pure derivation (no IO). Replaces the manual
 * "rest an account" blacklist with an auto-driven, health-derived state machine.
 *
 * IO glue (snapshot accounts, read placement delivery, reconcile, PATCH warmup)
 * lives in lib/account-lifecycle-sync.ts. This file is only the pure logic + the
 * constants, so `deriveLifecycle` can be unit-tested exhaustively.
 *
 * ── The model (LOCKED) — four states, first match wins ───────────────────────
 *   domain ∈ domain_policy                     → deactivated_by_user
 *   instantlyStatus <= 0                       → deactivated_by_instantly
 *   healthScore < BAR OR delivery below BAR    → in_recovery
 *   healthScore >= BAR AND delivery at BAR     → in_production
 *
 * - `healthScore` = Instantly `stat_warmup_score` (0-100), gated at 95.
 * - `deliveryAtBar` is computed PER ESP (see {@link isDeliveryAtBar}): every
 *   (account, ESP) silver row of the account's latest placement test must inbox
 *   at >= 90%. Deliberately NOT a blended average — Gmail-spam vs Outlook-fine is
 *   the whole deliverability finding, so a 60% Gmail hidden behind a 100% Outlook
 *   must NOT promote. Delivery UNKNOWN (never tested) is passed as `null` and
 *   treated as below bar, so an untested account defaults to in_recovery.
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
   * True ⇔ every ESP of the account's latest placement test inboxed at
   * >= PRODUCTION_DELIVERY_PCT_BAR (see {@link isDeliveryAtBar}).
   * `null` = never placement-tested → treated as below bar.
   */
  deliveryAtBar: boolean | null;
  /** Set of brand/product domains from instantly_domain_policy. */
  domainPolicy: ReadonlySet<string>;
}

export interface Lifecycle {
  status: LifecycleStatus;
  reason: LifecycleReason;
}

/**
 * The production bars — BOTH 95, both a single score over a single sample:
 *   - health: Instantly `stat_warmup_score` >= 95
 *   - delivery: >= 95% inbox POOLED across every ESP of the latest placement test
 *     (`Σinbox / Σseeds`), the same number the ops dashboard displays.
 *
 * Delivery was briefly gated PER ESP (every leg had to clear the bar) with a
 * 5-seed floor to stop Instantly's 1-3 seed "other" bucket from vetoing a good
 * account. That whole apparatus is gone: one test, one score, one bar. Measured
 * against the live fleet 2026-07-28, pooling costs nothing the per-ESP form
 * bought — the 198 Gmail-spam accounts this gate exists to catch top out at a
 * pooled 83.3%, so a 95% pooled bar still excludes every one, and the worst Gmail
 * leg that can hide behind a passing pooled score is 90.9%.
 *
 * ⚠️ The 95 is what makes pooling safe, so the two are a pair. At a pooled 90 bar
 * the worst hideable Gmail leg drops to ~80% — genuine Gmail distrust passing as
 * a good account. Do NOT lower `PRODUCTION_DELIVERY_PCT_BAR` without re-measuring
 * the Gmail-leg distribution of the accounts it would newly admit.
 */
export const PRODUCTION_HEALTH_BAR = 95;
export const PRODUCTION_DELIVERY_PCT_BAR = 95;

/**
 * Warmup daily send volume pushed to Instantly per target lifecycle state.
 * Paired with the campaign daily_limit below so EVERY state's total daily send
 * (campaign + warmup) stays at 50 — under Gmail's per-user daily sending limit,
 * which throttled the fleet with a `550-5.4.5 Daily user sending limit exceeded`
 * when it ran at 60/day (2026-07-19 incident).
 */
export const IN_PRODUCTION_WARMUP_DAILY = 5; // fully warmed → mostly campaign send (45 + 5 = 50)
export const RECOVERY_WARMUP_DAILY = 30; // recover reputation → warm harder, send less (20 + 30 = 50)

/**
 * Campaign daily max-send pushed to Instantly on a flip INTO in_production (45)
 * or in_recovery (20). Paired with the warmup volume above so the total stays 50.
 * deactivated_* states leave the campaign `daily_limit` untouched (null) so an
 * off account keeps draining its already-loaded queue at whatever limit it had.
 */
export const IN_PRODUCTION_DAILY_LIMIT = 45;
export const RECOVERY_DAILY_LIMIT = 20;

/**
 * Pure lifecycle derivation. First match wins (order is load-bearing — a domain
 * in the policy is deactivated_by_user even if Instantly-disabled or under-warmed).
 */
export function deriveLifecycle(input: DeriveLifecycleInput): Lifecycle {
  const { instantlyStatus, domain, healthScore, deliveryAtBar, domainPolicy } = input;

  if (domainPolicy.has(domain)) {
    return { status: "deactivated_by_user", reason: "brand_domain" };
  }
  if (instantlyStatus <= 0) {
    return {
      status: "deactivated_by_instantly",
      reason: "deactivated_by_instantly",
    };
  }
  // deliveryAtBar === null (never tested) is treated as below-bar → in_recovery.
  if (healthScore < PRODUCTION_HEALTH_BAR || deliveryAtBar !== true) {
    // Health is checked first for the reason label; if health is fine but
    // delivery is not (incl. never-tested), the block is delivery.
    const reason: LifecycleReason =
      healthScore < PRODUCTION_HEALTH_BAR ? "health_below_bar" : "delivery_below_bar";
    return { status: "in_recovery", reason };
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
 * the tap to 45, in_recovery caps it to 20 (paired with more warmup so the total
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
 * for the fleet measurement that makes pooling at 95 safe). This is the same
 * number `summarizeEspRows` displays, so the ops row and the lifecycle reason
 * cannot contradict each other.
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
