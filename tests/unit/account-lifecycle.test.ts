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
  isDeliveryEvidenceFresh,
  DELIVERY_EVIDENCE_MAX_AGE_DAYS,
  type DeriveLifecycleInput,
} from "../../src/lib/account-lifecycle";

const POLICY = new Set(["distribute.you", "growthagency.dev", "arcadiaquest.org"]);

const NOW = new Date("2026-08-12T12:00:00.000Z");
/** Two days old — comfortably inside DELIVERY_EVIDENCE_MAX_AGE_DAYS. */
const FRESH_TEST = new Date("2026-08-10T12:00:00.000Z").toISOString();

function input(overrides: Partial<DeriveLifecycleInput> = {}): DeriveLifecycleInput {
  return {
    instantlyStatus: 1,
    domain: "dfy-prewarmed.com",
    healthScore: 100,
    deliveryAtBar: true,
    deliveryTestedAt: FRESH_TEST,
    currentStatus: null,
    domainPolicy: POLICY,
    asOf: NOW,
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

  it("both bars are 90 — health, and delivery pooled across every ESP", () => {
    expect(PRODUCTION_HEALTH_BAR).toBe(90);
    expect(PRODUCTION_DELIVERY_PCT_BAR).toBe(90);
  });

  it("healthScore exactly at the bar (90) + delivery at bar → in_production", () => {
    expect(deriveLifecycle(input({ healthScore: 90, deliveryAtBar: true }))).toEqual({
      status: "in_production",
      reason: "passed",
    });
  });

  it("healthScore below the bar (89) → in_recovery (reason health_below_bar)", () => {
    expect(deriveLifecycle(input({ healthScore: 89, deliveryAtBar: true }))).toEqual({
      status: "in_recovery",
      reason: "health_below_bar",
    });
  });

  it("health 90/94/99 all PASS (94 was in_recovery under the 95 bar)", () => {
    for (const healthScore of [90, 94, 99]) {
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

// The health bar gates ENTRY only. This is what makes warmup-at-0 possible:
// Instantly resets the warmup score to 0 after 7 days without warmup activity,
// so under a symmetric bar every in_production account would demote itself.
describe("deriveLifecycle — health is an ENTRY bar, not a membership bar", () => {
  it("in_production with a warmup score of 0 STAYS in_production", () => {
    expect(
      deriveLifecycle(input({ healthScore: 0, currentStatus: "in_production" })),
    ).toEqual({ status: "in_production", reason: "passed" });
  });

  it("in_recovery with the same score of 0 is NOT promoted — entry still needs health", () => {
    expect(
      deriveLifecycle(input({ healthScore: 0, currentStatus: "in_recovery" })),
    ).toEqual({ status: "in_recovery", reason: "health_below_bar" });
  });

  it("a never-classified account (null) is treated as entering — health applies", () => {
    expect(deriveLifecycle(input({ healthScore: 0, currentStatus: null }))).toEqual({
      status: "in_recovery",
      reason: "health_below_bar",
    });
  });

  it("coming back from deactivated_by_instantly needs health again", () => {
    expect(
      deriveLifecycle(
        input({ healthScore: 0, currentStatus: "deactivated_by_instantly" }),
      ),
    ).toEqual({ status: "in_recovery", reason: "health_below_bar" });
  });

  it("delivery still demotes from in_production — it is the ONLY demotion path", () => {
    expect(
      deriveLifecycle(
        input({ healthScore: 100, deliveryAtBar: false, currentStatus: "in_production" }),
      ),
    ).toEqual({ status: "in_recovery", reason: "delivery_below_bar" });
  });

  it("a brand domain still wins over in_production membership", () => {
    expect(
      deriveLifecycle(
        input({ domain: "distribute.you", healthScore: 0, currentStatus: "in_production" }),
      ),
    ).toEqual({ status: "deactivated_by_user", reason: "brand_domain" });
  });

  it("Instantly disabling the account still wins over in_production membership", () => {
    expect(
      deriveLifecycle(
        input({ instantlyStatus: -3, healthScore: 0, currentStatus: "in_production" }),
      ),
    ).toEqual({
      status: "deactivated_by_instantly",
      reason: "deactivated_by_instantly",
    });
  });
});

// Delivery being the sole demotion path means its evidence must not go stale
// unnoticed: `isDeliveryAtBar` reads the LATEST test with no regard for its age.
describe("deriveLifecycle — delivery evidence expires", () => {
  const daysAgo = (n: number) =>
    new Date(NOW.getTime() - n * 24 * 60 * 60 * 1000).toISOString();

  it("DELIVERY_EVIDENCE_MAX_AGE_DAYS is 16 — two missed Saturdays", () => {
    expect(DELIVERY_EVIDENCE_MAX_AGE_DAYS).toBe(16);
  });

  it("a passing test older than the cap demotes, even from in_production", () => {
    expect(
      deriveLifecycle(
        input({ deliveryTestedAt: daysAgo(17), currentStatus: "in_production" }),
      ),
    ).toEqual({ status: "in_recovery", reason: "delivery_evidence_stale" });
  });

  it("exactly at the cap still counts as evidence", () => {
    expect(
      deriveLifecycle(
        input({ deliveryTestedAt: daysAgo(16), currentStatus: "in_production" }),
      ),
    ).toEqual({ status: "in_production", reason: "passed" });
  });

  it("10 days — the normal steady-state peak — does NOT demote", () => {
    // Tests run 7 days apart and ingestion lags the run by up to ~3 days, so the
    // freshest evidence legitimately reaches ~10 days old before a cycle lands.
    // A tighter cap would demote the whole fleet every Sunday morning.
    expect(
      deriveLifecycle(
        input({ deliveryTestedAt: daysAgo(10), currentStatus: "in_production" }),
      ),
    ).toEqual({ status: "in_production", reason: "passed" });
  });

  it("a FAILING test is delivery_below_bar regardless of age (not stale)", () => {
    expect(
      deriveLifecycle(
        input({
          deliveryAtBar: false,
          deliveryTestedAt: daysAgo(40),
          currentStatus: "in_production",
        }),
      ),
    ).toEqual({ status: "in_recovery", reason: "delivery_below_bar" });
  });

  it("never tested → delivery_below_bar, not delivery_evidence_stale", () => {
    expect(
      deriveLifecycle(
        input({
          deliveryAtBar: null,
          deliveryTestedAt: null,
          currentStatus: "in_production",
        }),
      ),
    ).toEqual({ status: "in_recovery", reason: "delivery_below_bar" });
  });
});

describe("isDeliveryEvidenceFresh", () => {
  it("null / undefined / unparseable → false (no evidence is not fresh evidence)", () => {
    expect(isDeliveryEvidenceFresh(null, NOW)).toBe(false);
    expect(isDeliveryEvidenceFresh(undefined, NOW)).toBe(false);
    expect(isDeliveryEvidenceFresh("not-a-date", NOW)).toBe(false);
  });

  it("accepts a Date and an ISO string alike", () => {
    const d = new Date(NOW.getTime() - 24 * 60 * 60 * 1000);
    expect(isDeliveryEvidenceFresh(d, NOW)).toBe(true);
    expect(isDeliveryEvidenceFresh(d.toISOString(), NOW)).toBe(true);
  });

  it("a future timestamp is fresh (clock skew must never demote)", () => {
    expect(
      isDeliveryEvidenceFresh(new Date(NOW.getTime() + 60_000), NOW),
    ).toBe(true);
  });
});

describe("isDeliveryAtBar — >= 90% inbox POOLED across every ESP", () => {
  it("true when the pooled score clears the bar", () => {
    expect(
      isDeliveryAtBar([
        { inboxCount: 88, seedTotal: 88 },
        { inboxCount: 10, seedTotal: 10 },
      ]),
    ).toBe(true);
  });

  it("true at EXACTLY 90% pooled", () => {
    // 54/60 = 90%.
    expect(
      isDeliveryAtBar([
        { inboxCount: 34, seedTotal: 40 },
        { inboxCount: 20, seedTotal: 20 },
      ]),
    ).toBe(true);
  });

  it("false just under: 4 spam seeds out of 25 Gmail with a clean Outlook leg", () => {
    // 34/38 = 89.5%. Pooling does not round up to the bar. This is the shape of
    // the best account the 90 bar still excludes in prod (89.7%).
    expect(
      isDeliveryAtBar([
        { inboxCount: 21, seedTotal: 25 },
        { inboxCount: 13, seedTotal: 13 },
      ]),
    ).toBe(false);
  });

  // What the 90 bar does and does not stop. Measured against the live fleet
  // 2026-08-12: the worst GMAIL leg it admits is 88.9%, and the shared-IP
  // Gmail-spam fleet it exists to catch tops out at a pooled 89.7%.
  it("Gmail 70% + Outlook 100% pools to 85% → FALSE", () => {
    expect(
      isDeliveryAtBar([
        { inboxCount: 14, seedTotal: 20 },
        { inboxCount: 20, seedTotal: 20 },
      ]),
    ).toBe(false);
  });

  it("worst leg the bar lets through: Gmail 87.5% + a perfect Outlook leg → TRUE", () => {
    // 21/24 Gmail + 14/14 Outlook = 35/38 = 92.1%. Pooling blends the weak leg
    // up; this is the residual the bar's value accepts, pinned so a future
    // change to either the bar or the pooling shows up here.
    expect(
      isDeliveryAtBar([
        { inboxCount: 21, seedTotal: 24 },
        { inboxCount: 14, seedTotal: 14 },
      ]),
    ).toBe(true);
  });

  it("Gmail-dead legacy fleet (2% inbox) stays FALSE — the separation the gate exists for", () => {
    // 21/70 = 30%, nowhere near 90 even with a perfect Outlook leg.
    expect(
      isDeliveryAtBar([
        { inboxCount: 1, seedTotal: 50 },
        { inboxCount: 20, seedTotal: 20 },
      ]),
    ).toBe(false);
  });

  it("false when one ESP is fully missing (0 inbox)", () => {
    // 88/108 = 81.5% — a perfect leg does not carry a fully-dead one over the bar.
    expect(
      isDeliveryAtBar([
        { inboxCount: 88, seedTotal: 88 },
        { inboxCount: 0, seedTotal: 20 },
      ]),
    ).toBe(false);
  });

  it("a tiny 'other' leg counts like any other — no seed floor", () => {
    // Gmail 25/30 + Outlook 12/12 + a 1-of-2 bucket → 38/44 = 86.4% → false.
    expect(
      isDeliveryAtBar([
        { inboxCount: 25, seedTotal: 30 },
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
  it("in_production → 0/day (self-warming via real volume; 50 + 0 = 50)", () => {
    expect(warmupDailyForStatus("in_production")).toBe(IN_PRODUCTION_WARMUP_DAILY);
    expect(IN_PRODUCTION_WARMUP_DAILY).toBe(0);
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
  it("in_production → 50 (opens the campaign daily max-send)", () => {
    expect(dailyLimitForStatus("in_production")).toBe(IN_PRODUCTION_DAILY_LIMIT);
    expect(IN_PRODUCTION_DAILY_LIMIT).toBe(50);
  });

  it("every state still totals <= 50/day (campaign + warmup), the Gmail cap", () => {
    for (const status of ["in_production", "in_recovery"] as const) {
      const total =
        (dailyLimitForStatus(status) ?? 0) + (warmupDailyForStatus(status) ?? 0);
      expect(total).toBe(50);
    }
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
