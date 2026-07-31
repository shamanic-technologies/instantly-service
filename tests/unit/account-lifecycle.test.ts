import { describe, it, expect } from "vitest";
import {
  deriveLifecycle,
  warmupDailyForStatus,
  dailyLimitForStatus,
  emailDomain,
  isDeliveryAtBar,
  PRODUCTION_HEALTH_BAR,
  PRODUCTION_DELIVERY_PCT_BAR,
  IN_PRODUCTION_WARMUP_DAILY,
  RECOVERY_WARMUP_DAILY,
  IN_PRODUCTION_DAILY_LIMIT,
  RECOVERY_DAILY_LIMIT,
  rampCapForAge,
  RAMP_FLOOR_PER_DAY,
  slowRampForAge,
  MATURE_AGE_DAYS,
  type DeriveLifecycleInput,
} from "../../src/lib/account-lifecycle";

const POLICY = new Set(["distribute.you", "growthagency.dev", "arcadiaquest.org"]);

function input(overrides: Partial<DeriveLifecycleInput> = {}): DeriveLifecycleInput {
  return {
    instantlyStatus: 1,
    domain: "dfy-prewarmed.com",
    healthScore: 100,
    deliveryAtBar: true,
    domainPolicy: POLICY,
    ...overrides,
  };
}

describe("deriveLifecycle — four branches, first match wins", () => {
  it("domain in policy → deactivated_by_user (wins over every other signal)", () => {
    // Even Instantly-disabled + under-warmed + no delivery, a brand domain is user-deactivated.
    expect(
      deriveLifecycle(
        input({
          domain: "distribute.you",
          instantlyStatus: 0,
          healthScore: 10,
          deliveryAtBar: null,
        }),
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

  it("both bars are 95 — health, and delivery pooled across every ESP", () => {
    expect(PRODUCTION_HEALTH_BAR).toBe(95);
    expect(PRODUCTION_DELIVERY_PCT_BAR).toBe(95);
  });

  it("healthScore exactly at the bar (95) + delivery at bar → in_production", () => {
    expect(deriveLifecycle(input({ healthScore: 95, deliveryAtBar: true }))).toEqual({
      status: "in_production",
      reason: "passed",
    });
  });

  it("healthScore below the bar (94) → in_recovery (reason health_below_bar)", () => {
    expect(deriveLifecycle(input({ healthScore: 94, deliveryAtBar: true }))).toEqual({
      status: "in_recovery",
      reason: "health_below_bar",
    });
  });

  it("health 97/98/99 now PASSES (was in_recovery under the 100 bar)", () => {
    for (const healthScore of [97, 98, 99]) {
      expect(deriveLifecycle(input({ healthScore, deliveryAtBar: true })).status).toBe(
        "in_production",
      );
    }
  });

  it("delivery below bar (health fine) → in_recovery (reason delivery_below_bar)", () => {
    expect(deriveLifecycle(input({ healthScore: 100, deliveryAtBar: false }))).toEqual({
      status: "in_recovery",
      reason: "delivery_below_bar",
    });
  });

  it("delivery UNKNOWN (never tested, null) → in_recovery (delivery_below_bar)", () => {
    expect(deriveLifecycle(input({ healthScore: 100, deliveryAtBar: null }))).toEqual({
      status: "in_recovery",
      reason: "delivery_below_bar",
    });
  });

  it("health below bar label wins over delivery when both fail", () => {
    expect(deriveLifecycle(input({ healthScore: 50, deliveryAtBar: null }))).toEqual({
      status: "in_recovery",
      reason: "health_below_bar",
    });
  });

  it("healthScore == 100 AND delivery at bar → in_production (passed)", () => {
    expect(deriveLifecycle(input({ healthScore: 100, deliveryAtBar: true }))).toEqual({
      status: "in_production",
      reason: "passed",
    });
  });
});

describe("isDeliveryAtBar — >= 95% inbox POOLED across every ESP", () => {
  it("true when the pooled score clears the bar", () => {
    expect(
      isDeliveryAtBar([
        { inboxCount: 88, seedTotal: 88 },
        { inboxCount: 10, seedTotal: 10 },
      ]),
    ).toBe(true);
  });

  it("true at EXACTLY 95% pooled", () => {
    // 57/60 = 95%.
    expect(
      isDeliveryAtBar([
        { inboxCount: 37, seedTotal: 40 },
        { inboxCount: 20, seedTotal: 20 },
      ]),
    ).toBe(true);
  });

  it("false just under: 2 spam seeds out of 25 Gmail with a clean Outlook leg", () => {
    // 36/38 = 94.7%. Pooling does not round up to the bar.
    expect(
      isDeliveryAtBar([
        { inboxCount: 23, seedTotal: 25 },
        { inboxCount: 13, seedTotal: 13 },
      ]),
    ).toBe(false);
  });

  // The pooled form is only safe BECAUSE the bar is 95: the worst Gmail leg that
  // can hide behind a passing pooled score is ~91%, and every Gmail-spam account
  // in prod tops out at a pooled 83.3%. These two cases pin that.
  it("Gmail 85% + Outlook 100% pools to 92.5% → FALSE", () => {
    expect(
      isDeliveryAtBar([
        { inboxCount: 17, seedTotal: 20 },
        { inboxCount: 20, seedTotal: 20 },
      ]),
    ).toBe(false);
  });

  it("Gmail-dead legacy fleet (2% inbox) stays FALSE — the separation the gate exists for", () => {
    // 21/70 = 30%, nowhere near 95 even with a perfect Outlook leg.
    expect(
      isDeliveryAtBar([
        { inboxCount: 1, seedTotal: 50 },
        { inboxCount: 20, seedTotal: 20 },
      ]),
    ).toBe(false);
  });

  it("false when one ESP is fully missing (0 inbox)", () => {
    expect(
      isDeliveryAtBar([
        { inboxCount: 88, seedTotal: 88 },
        { inboxCount: 0, seedTotal: 10 },
      ]),
    ).toBe(false);
  });

  it("a tiny 'other' leg counts like any other — no seed floor", () => {
    // Gmail 28/30 + Outlook 12/12 + a 1-of-2 bucket → 41/44 = 93.2% → false.
    expect(
      isDeliveryAtBar([
        { inboxCount: 28, seedTotal: 30 },
        { inboxCount: 12, seedTotal: 12 },
        { inboxCount: 1, seedTotal: 2 },
      ]),
    ).toBe(false);
  });

  it("grades a tiny all-inbox test as passing (sample size is not a gate)", () => {
    // Prod: emily@fuseconnectio.com, 2 Gmail + 3 Outlook seeds, all inbox. The old
    // seed floor called this ungradable and trapped the account in in_recovery.
    expect(
      isDeliveryAtBar([
        { inboxCount: 2, seedTotal: 2 },
        { inboxCount: 3, seedTotal: 3 },
      ]),
    ).toBe(true);
  });

  it("false when never tested (no rows)", () => {
    expect(isDeliveryAtBar([])).toBe(false);
  });

  it("false when an ESP row seeded zero times", () => {
    expect(isDeliveryAtBar([{ inboxCount: 0, seedTotal: 0 }])).toBe(false);
  });
});

describe("warmupDailyForStatus", () => {
  it("in_production → 5/day", () => {
    expect(warmupDailyForStatus("in_production")).toBe(IN_PRODUCTION_WARMUP_DAILY);
    expect(IN_PRODUCTION_WARMUP_DAILY).toBe(5);
  });
  it("in_recovery and deactivated_by_user → 30/day", () => {
    expect(warmupDailyForStatus("in_recovery")).toBe(RECOVERY_WARMUP_DAILY);
    expect(warmupDailyForStatus("deactivated_by_user")).toBe(RECOVERY_WARMUP_DAILY);
    expect(RECOVERY_WARMUP_DAILY).toBe(30);
  });
  it("deactivated_by_instantly → null (do NOT touch warmup)", () => {
    expect(warmupDailyForStatus("deactivated_by_instantly")).toBeNull();
  });
});

describe("dailyLimitForStatus", () => {
  it("in_production → 45 (opens the campaign daily max-send)", () => {
    expect(dailyLimitForStatus("in_production")).toBe(IN_PRODUCTION_DAILY_LIMIT);
    expect(IN_PRODUCTION_DAILY_LIMIT).toBe(45);
  });
  it("in_recovery → 20 (caps campaign send, paired with more warmup)", () => {
    expect(dailyLimitForStatus("in_recovery")).toBe(RECOVERY_DAILY_LIMIT);
    expect(RECOVERY_DAILY_LIMIT).toBe(20);
  });
  it("deactivated_* → null (leave daily_limit untouched, queue drains)", () => {
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

describe("account age gate — rampCapForAge / slowRampForAge", () => {
  const asOf = new Date("2026-07-22T00:00:00Z");
  const daysAgo = (n: number) =>
    new Date(asOf.getTime() - n * 24 * 60 * 60 * 1000).toISOString();

  it(`rampCapForAge: at/after ${MATURE_AGE_DAYS}d the cap is the full daily_limit`, () => {
    expect(rampCapForAge(daysAgo(MATURE_AGE_DAYS), 45, asOf)).toBe(45);
    expect(rampCapForAge(daysAgo(MATURE_AGE_DAYS + 60), 45, asOf)).toBe(45);
  });

  it("rampCapForAge: a fresh account ramps LINEARLY with age", () => {
    expect(rampCapForAge(daysAgo(14), 45, asOf)).toBe(23); // 45 * 14/28
    expect(rampCapForAge(daysAgo(21), 45, asOf)).toBe(34); // 45 * 21/28
    expect(rampCapForAge(daysAgo(24), 45, asOf)).toBe(39); // 45 * 24/28
  });

  it(`rampCapForAge: never below the ${RAMP_FLOOR_PER_DAY}/day floor (idle ≠ ramp)`, () => {
    expect(rampCapForAge(daysAgo(1), 45, asOf)).toBe(RAMP_FLOOR_PER_DAY);
    expect(rampCapForAge(daysAgo(3), 45, asOf)).toBe(RAMP_FLOOR_PER_DAY); // round(4.8)=5
  });

  it("rampCapForAge: the floor never EXCEEDS the account's own daily_limit", () => {
    expect(rampCapForAge(daysAgo(1), 4, asOf)).toBe(4);
    expect(rampCapForAge(daysAgo(1), 0, asOf)).toBe(0);
  });

  it("rampCapForAge: unknown/unparseable created date → full limit (never trap)", () => {
    expect(rampCapForAge(null, 45, asOf)).toBe(45);
    expect(rampCapForAge(undefined, 45, asOf)).toBe(45);
    expect(rampCapForAge("not-a-date", 45, asOf)).toBe(45);
  });

  it("rampCapForAge: accepts a Date instance", () => {
    expect(rampCapForAge(new Date(daysAgo(14)), 45, asOf)).toBe(23);
  });

  it("slowRampForAge: fresh → true, mature → false, unknown → null", () => {
    expect(slowRampForAge(daysAgo(3), asOf)).toBe(true);
    expect(slowRampForAge(daysAgo(MATURE_AGE_DAYS + 1), asOf)).toBe(false);
    expect(slowRampForAge(null, asOf)).toBeNull();
    expect(slowRampForAge("bad", asOf)).toBeNull();
  });
});
