import { describe, it, expect } from "vitest";
import {
  deriveLifecycle,
  warmupDailyForStatus,
  dailyLimitForStatus,
  emailDomain,
  isDeliveryFull,
  IN_PRODUCTION_WARMUP_DAILY,
  RECOVERY_WARMUP_DAILY,
  IN_PRODUCTION_DAILY_LIMIT,
  type DeriveLifecycleInput,
} from "../../src/lib/account-lifecycle";

const POLICY = new Set(["distribute.you", "growthagency.dev", "arcadiaquest.org"]);

function input(overrides: Partial<DeriveLifecycleInput> = {}): DeriveLifecycleInput {
  return {
    instantlyStatus: 1,
    domain: "dfy-prewarmed.com",
    healthScore: 100,
    delivery: 100,
    domainPolicy: POLICY,
    ...overrides,
  };
}

describe("deriveLifecycle — four branches, first match wins", () => {
  it("domain in policy → deactivated_by_user (wins over every other signal)", () => {
    // Even Instantly-disabled + under-warmed + no delivery, a brand domain is user-deactivated.
    expect(
      deriveLifecycle(
        input({ domain: "distribute.you", instantlyStatus: 0, healthScore: 10, delivery: null }),
      ),
    ).toEqual({ status: "deactivated_by_user", reason: "brand_domain" });
    expect(deriveLifecycle(input({ domain: "growthagency.dev" }))).toEqual({
      status: "deactivated_by_user",
      reason: "brand_domain",
    });
  });

  it("instantlyStatus <= 0 → deactivated_by_instantly (non-brand domain)", () => {
    expect(deriveLifecycle(input({ instantlyStatus: 0 }))).toEqual({
      status: "deactivated_by_instantly",
      reason: "deactivated_by_instantly",
    });
    expect(deriveLifecycle(input({ instantlyStatus: -1 }))).toEqual({
      status: "deactivated_by_instantly",
      reason: "deactivated_by_instantly",
    });
  });

  it("healthScore < 100 → in_recovery (reason health_below_100)", () => {
    expect(deriveLifecycle(input({ healthScore: 99, delivery: 100 }))).toEqual({
      status: "in_recovery",
      reason: "health_below_100",
    });
  });

  it("delivery < 100 (health fine) → in_recovery (reason delivery_below_100)", () => {
    expect(deriveLifecycle(input({ healthScore: 100, delivery: 40 }))).toEqual({
      status: "in_recovery",
      reason: "delivery_below_100",
    });
  });

  it("delivery UNKNOWN (never tested, null) → in_recovery (delivery_below_100)", () => {
    expect(deriveLifecycle(input({ healthScore: 100, delivery: null }))).toEqual({
      status: "in_recovery",
      reason: "delivery_below_100",
    });
  });

  it("health < 100 label wins over delivery when both fail", () => {
    expect(deriveLifecycle(input({ healthScore: 50, delivery: null }))).toEqual({
      status: "in_recovery",
      reason: "health_below_100",
    });
  });

  it("healthScore == 100 AND delivery == 100 → in_production (passed)", () => {
    expect(deriveLifecycle(input({ healthScore: 100, delivery: 100 }))).toEqual({
      status: "in_production",
      reason: "passed",
    });
  });

  it("delivery just below (99) is NOT production", () => {
    expect(deriveLifecycle(input({ delivery: 99 })).status).toBe("in_recovery");
  });
});

describe("isDeliveryFull — exact 100% across ALL ESPs", () => {
  it("true only when every ESP row is inbox == seed and seed > 0", () => {
    expect(isDeliveryFull([{ inboxCount: 88, seedTotal: 88 }, { inboxCount: 10, seedTotal: 10 }])).toBe(true);
  });
  it("false when ANY ESP is short of 100%", () => {
    expect(isDeliveryFull([{ inboxCount: 87, seedTotal: 88 }, { inboxCount: 10, seedTotal: 10 }])).toBe(false);
  });
  it("false when one ESP is fully missing (0 inbox)", () => {
    expect(isDeliveryFull([{ inboxCount: 88, seedTotal: 88 }, { inboxCount: 0, seedTotal: 10 }])).toBe(false);
  });
  it("false when never tested (no rows)", () => {
    expect(isDeliveryFull([])).toBe(false);
  });
  it("false when seed total is zero", () => {
    expect(isDeliveryFull([{ inboxCount: 0, seedTotal: 0 }])).toBe(false);
  });
});

describe("warmupDailyForStatus", () => {
  it("in_production → 10/day", () => {
    expect(warmupDailyForStatus("in_production")).toBe(IN_PRODUCTION_WARMUP_DAILY);
    expect(IN_PRODUCTION_WARMUP_DAILY).toBe(10);
  });
  it("in_recovery and deactivated_by_user → 50/day", () => {
    expect(warmupDailyForStatus("in_recovery")).toBe(RECOVERY_WARMUP_DAILY);
    expect(warmupDailyForStatus("deactivated_by_user")).toBe(RECOVERY_WARMUP_DAILY);
    expect(RECOVERY_WARMUP_DAILY).toBe(50);
  });
  it("deactivated_by_instantly → null (do NOT touch warmup)", () => {
    expect(warmupDailyForStatus("deactivated_by_instantly")).toBeNull();
  });
});

describe("dailyLimitForStatus", () => {
  it("in_production → 40 (opens the campaign daily max-send)", () => {
    expect(dailyLimitForStatus("in_production")).toBe(IN_PRODUCTION_DAILY_LIMIT);
    expect(IN_PRODUCTION_DAILY_LIMIT).toBe(40);
  });
  it("every other state → null (leave daily_limit untouched, queue drains)", () => {
    expect(dailyLimitForStatus("in_recovery")).toBeNull();
    expect(dailyLimitForStatus("deactivated_by_user")).toBeNull();
    expect(dailyLimitForStatus("deactivated_by_instantly")).toBeNull();
  });
});

describe("emailDomain", () => {
  it("lowercases the part after @", () => {
    expect(emailDomain("Amy.Moore@DFY-Prewarmed.COM")).toBe("dfy-prewarmed.com");
  });
  it("returns empty string when there is no domain", () => {
    expect(emailDomain("no-at-sign")).toBe("");
  });
});
