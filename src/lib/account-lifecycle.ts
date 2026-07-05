/**
 * Per-account LIFECYCLE — pure derivation (no IO). Replaces the manual
 * "rest an account" blacklist with an auto-driven, health-derived state machine.
 *
 * IO glue (snapshot accounts, read placement delivery, reconcile, PATCH warmup)
 * lives in lib/account-lifecycle-sync.ts. This file is only the pure logic + the
 * constants, so `deriveLifecycle` can be unit-tested exhaustively.
 *
 * ── The model (LOCKED) — four states, first match wins ───────────────────────
 *   domain ∈ domain_policy               → deactivated_by_user
 *   instantlyStatus <= 0                 → deactivated_by_instantly
 *   healthScore < 100 OR delivery < 100  → in_recovery
 *   healthScore == 100 AND delivery == 100 → in_production
 *
 * - `healthScore` = Instantly `stat_warmup_score` (0-100).
 * - `delivery === 100` means LITERALLY 100% inbox across ALL ESP recipients of the
 *   account's latest placement test (every (account, ESP) silver row: inbox == seed,
 *   spam 0, missing 0). Delivery UNKNOWN (never tested) → treated as < 100, so an
 *   untested account defaults to in_recovery. `delivery` is passed as `null` when
 *   unknown.
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
  | "health_below_100"
  | "delivery_below_100"
  | "passed"
  | "reactivated";

export interface DeriveLifecycleInput {
  /** Instantly account.status (numeric; <= 0 ⇒ Instantly disabled the account). */
  instantlyStatus: number;
  /** The account's email domain (part after `@`). */
  domain: string;
  /** Instantly stat_warmup_score (0-100). */
  healthScore: number;
  /** 100 = literal 100% inbox across all ESPs of the latest test; null = never tested. */
  delivery: number | null;
  /** Set of brand/product domains from instantly_domain_policy. */
  domainPolicy: ReadonlySet<string>;
}

export interface Lifecycle {
  status: LifecycleStatus;
  reason: LifecycleReason;
}

/** Fully-warmed health score AND full inbox placement — the production bar. */
export const FULL_SCORE = 100;

/** Warmup daily send volume pushed to Instantly per target lifecycle state. */
export const IN_PRODUCTION_WARMUP_DAILY = 10; // fully warmed → maintenance volume
export const RECOVERY_WARMUP_DAILY = 50; // recover reputation → warm harder

/**
 * Pure lifecycle derivation. First match wins (order is load-bearing — a domain
 * in the policy is deactivated_by_user even if Instantly-disabled or under-warmed).
 */
export function deriveLifecycle(input: DeriveLifecycleInput): Lifecycle {
  const { instantlyStatus, domain, healthScore, delivery, domainPolicy } = input;

  if (domainPolicy.has(domain)) {
    return { status: "deactivated_by_user", reason: "brand_domain" };
  }
  if (instantlyStatus <= 0) {
    return {
      status: "deactivated_by_instantly",
      reason: "deactivated_by_instantly",
    };
  }
  // delivery === null (never tested) is treated as below-100 → in_recovery.
  const deliveryFull = delivery === FULL_SCORE;
  if (healthScore < FULL_SCORE || !deliveryFull) {
    // Health is checked first for the reason label; if health is fine but
    // delivery is not (incl. never-tested), the block is delivery.
    const reason: LifecycleReason =
      healthScore < FULL_SCORE ? "health_below_100" : "delivery_below_100";
    return { status: "in_recovery", reason };
  }
  return { status: "in_production", reason: "passed" };
}

/**
 * Warmup daily volume to PATCH into Instantly when an account flips INTO a state.
 * `null` ⇒ do NOT touch warmup (deactivated_by_instantly — the account is off).
 * The campaign `daily_limit` (max send) is NEVER touched here (queue keeps draining).
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

/** Domain part of an email (lowercased), or "" when there is no `@domain`. */
export function emailDomain(email: string): string {
  return (email.split("@")[1] ?? "").toLowerCase();
}

/**
 * The exact "delivery == 100%" rule: LITERALLY 100% inbox across ALL ESP
 * recipients of the account's latest placement test. `espRows` is one row per
 * (account, ESP) of that test. True ⇔ every ESP row is inbox == seed (spam 0,
 * missing 0) AND the test had at least one seed.
 *
 * Implemented on the blended sums: because inbox_i ≤ seed_i for every ESP i,
 * Σinbox == Σseed forces per-ESP equality — so the summed check is equivalent to
 * "100% on every ESP". No rows (never tested) → false (delivery unknown → recovery).
 */
export function isDeliveryFull(
  espRows: ReadonlyArray<{ inboxCount: number; seedTotal: number }>,
): boolean {
  if (espRows.length === 0) return false;
  const seed = espRows.reduce((s, r) => s + r.seedTotal, 0);
  const inbox = espRows.reduce((s, r) => s + r.inboxCount, 0);
  return seed > 0 && inbox === seed;
}
