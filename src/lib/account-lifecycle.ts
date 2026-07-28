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
 * The production bars (lowered from 100/100 on 2026-07-28, then delivery 95 → 90
 * on the same day once prod data showed what the ESP grain does to the denominator):
 *   - health: Instantly `stat_warmup_score` >= 95
 *   - delivery: >= 90% inbox on EVERY ESP of the latest placement test
 *
 * Why the two bars differ. Health is one score over the whole warmup pool, so 95
 * there is a large-sample bar. Delivery is applied PER ESP, and an ESP leg of a
 * placement test only seeds ~25-30 mailboxes — so 2 spam seeds (unavoidable
 * seed-level noise) already reads as 92%. At a 95 per-ESP bar, prod had 174
 * accounts at health >= 95 held out of production, 9 of which were Gmail 91-94% /
 * Outlook 100% — genuinely fine mailboxes blocked by 2 seeds. 90 clears exactly
 * those 9 and NOTHING else: the legacy shared-IP fleet this gate exists to catch
 * sits at <10% inbox on Gmail (136 accounts), so the separation is untouched.
 * Do NOT raise it back without re-measuring the in_production count against prod.
 */
export const PRODUCTION_HEALTH_BAR = 95;
export const PRODUCTION_DELIVERY_PCT_BAR = 90;

/**
 * Minimum seeds an (account, ESP) leg must carry to be gated on. Instantly emits
 * an "other" ESP bucket (`recipient_esp: 999`) with 1-3 seeds; at that size a
 * single spam seed reads as 0-50% and would veto an account whose real Gmail and
 * Outlook legs both pass. Legs below this are excluded from the gate (they stay
 * visible in the placement breakdown). An account whose every leg is below it has
 * no gradable delivery signal → below bar, same as never-tested.
 */
export const MIN_GATED_ESP_SEEDS = 5;

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
 * `in_production` if it passes health+delivery); it only (a) de-prioritizes the
 * account in send selection — picked last, taking overflow only once every mature
 * account is filled for the day — and (b) keeps Instantly's slow ramp ON so its
 * volume grows gently. 28 days = the ~4-week ramp window Gmail needs to build
 * per-user send trust.
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
 * True ⇔ the account is younger than {@link MATURE_AGE_DAYS}. Unknown/unparseable
 * created date → false (treated as MATURE — never de-prioritize or trap an account
 * we cannot date; once the timestamp backfills, a genuinely fresh one gates).
 */
export function isAccountFresh(
  timestampCreated: string | Date | null | undefined,
  asOf: Date,
): boolean {
  const age = accountAgeMs(timestampCreated, asOf);
  return age !== null && age < MATURE_AGE_MS;
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

/** True ⇔ this (account, ESP) leg carries enough seeds to be gated on. */
export function isGatedEspRow(row: { seedTotal: number }): boolean {
  return row.seedTotal >= MIN_GATED_ESP_SEEDS;
}

/**
 * The delivery bar: >= {@link PRODUCTION_DELIVERY_PCT_BAR}% inbox on EVERY gated
 * ESP leg of the account's latest placement test. `espRows` is one row per
 * (account, ESP) of that test; legs under {@link MIN_GATED_ESP_SEEDS} seeds are
 * excluded (see that constant).
 *
 * PER-ESP, NOT blended — deliberately. Gmail-spam vs Outlook-fine is the entire
 * deliverability finding, and a blended average hides it: Gmail 60% + Outlook 100%
 * on a 2:1 seed split blends to ~73% and, on any bar a blend could pass, would
 * promote an account Gmail distrusts. Do NOT collapse it back to sums.
 *
 * No gradable leg (never tested, or every leg under the seed floor) → false
 * (delivery unknown → recovery). Never fabricates a pass.
 */
export function isDeliveryAtBar(
  espRows: ReadonlyArray<{ inboxCount: number; seedTotal: number }>,
): boolean {
  const gated = espRows.filter(isGatedEspRow);
  if (gated.length === 0) return false;
  // Integer comparison (inbox * 100 >= seed * bar) — no float rounding.
  return gated.every((r) => r.inboxCount * 100 >= r.seedTotal * PRODUCTION_DELIVERY_PCT_BAR);
}
