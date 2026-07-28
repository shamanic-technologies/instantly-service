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
  isAccountFresh,
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

  it("health bar is 95; the PER-ESP delivery bar is 90 (smaller denominator)", () => {
    expect(PRODUCTION_HEALTH_BAR).toBe(95);
    expect(PRODUCTION_DELIVERY_PCT_BAR).toBe(90);
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

describe("isDeliveryAtBar — >= 90% inbox on EVERY gated ESP (never blended)", () => {
  it("true when every ESP row is at or above the bar", () => {
    expect(
      isDeliveryAtBar([
        { inboxCount: 88, seedTotal: 88 },
        { inboxCount: 10, seedTotal: 10 },
      ]),
    ).toBe(true);
  });

  it("true at EXACTLY 90% on each ESP", () => {
    expect(
      isDeliveryAtBar([
        { inboxCount: 18, seedTotal: 20 },
        { inboxCount: 36, seedTotal: 40 },
      ]),
    ).toBe(true);
  });

  it("true at 92% — 2 spam seeds out of 25 Gmail, the real prod case the 95 bar blocked", () => {
    // Prod 2026-07-28: nine health-100 accounts sat in_recovery on exactly this shape
    // (Gmail 20-30 seeds with 2 spam, Outlook clean). Seed noise, not a delivery failure.
    expect(
      isDeliveryAtBar([
        { inboxCount: 23, seedTotal: 25 }, // Gmail 92%
        { inboxCount: 13, seedTotal: 13 }, // Outlook 100%
      ]),
    ).toBe(true);
  });

  it("PER-ESP, NOT blended: Gmail 85% + Outlook 100% blends to 92.5% but is FALSE", () => {
    // Blended: (17 + 20) / (20 + 20) = 92.5% — would wrongly pass a blended check at 90.
    expect(
      isDeliveryAtBar([
        { inboxCount: 17, seedTotal: 20 }, // Gmail 85%
        { inboxCount: 20, seedTotal: 20 }, // Outlook 100%
      ]),
    ).toBe(false);
  });

  it("Gmail-dead legacy fleet (2% inbox) stays FALSE — the separation the gate exists for", () => {
    expect(
      isDeliveryAtBar([
        { inboxCount: 1, seedTotal: 50 }, // Gmail 2% (shared-IP legacy)
        { inboxCount: 20, seedTotal: 20 }, // Outlook 100%
      ]),
    ).toBe(false);
  });

  it("false when ANY gated ESP is below the bar", () => {
    expect(
      isDeliveryAtBar([
        { inboxCount: 35, seedTotal: 40 }, // 87.5%
        { inboxCount: 10, seedTotal: 10 },
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

  it("IGNORES an under-seeded leg: a failing 1-of-2 'other' ESP does not veto", () => {
    // Instantly's recipient_esp 999 bucket seeds 1-3 mailboxes; one spam there reads
    // as 50% and would sink an account whose Gmail and Outlook legs both pass.
    expect(
      isDeliveryAtBar([
        { inboxCount: 28, seedTotal: 30 }, // Gmail 93%
        { inboxCount: 12, seedTotal: 12 }, // Outlook 100%
        { inboxCount: 1, seedTotal: 2 }, // other 50%, under the seed floor
      ]),
    ).toBe(true);
  });

  it("false when EVERY leg is under the seed floor (no gradable signal)", () => {
    expect(
      isDeliveryAtBar([
        { inboxCount: 2, seedTotal: 2 },
        { inboxCount: 3, seedTotal: 3 },
      ]),
    ).toBe(false);
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

describe("account age gate — isAccountFresh / slowRampForAge", () => {
  const asOf = new Date("2026-07-22T00:00:00Z");
  const daysAgo = (n: number) =>
    new Date(asOf.getTime() - n * 24 * 60 * 60 * 1000).toISOString();

  it(`isAccountFresh: younger than ${MATURE_AGE_DAYS}d is fresh`, () => {
    expect(isAccountFresh(daysAgo(1), asOf)).toBe(true);
    expect(isAccountFresh(daysAgo(MATURE_AGE_DAYS - 1), asOf)).toBe(true);
  });
  it(`isAccountFresh: exactly/older than ${MATURE_AGE_DAYS}d is mature`, () => {
    expect(isAccountFresh(daysAgo(MATURE_AGE_DAYS), asOf)).toBe(false);
    expect(isAccountFresh(daysAgo(MATURE_AGE_DAYS + 30), asOf)).toBe(false);
  });
  it("isAccountFresh: unknown/unparseable created date → mature (never trap)", () => {
    expect(isAccountFresh(null, asOf)).toBe(false);
    expect(isAccountFresh(undefined, asOf)).toBe(false);
    expect(isAccountFresh("not-a-date", asOf)).toBe(false);
  });
  it("isAccountFresh: accepts a Date instance", () => {
    expect(isAccountFresh(new Date(daysAgo(2)), asOf)).toBe(true);
  });

  it("slowRampForAge: fresh → true, mature → false, unknown → null", () => {
    expect(slowRampForAge(daysAgo(3), asOf)).toBe(true);
    expect(slowRampForAge(daysAgo(MATURE_AGE_DAYS + 1), asOf)).toBe(false);
    expect(slowRampForAge(null, asOf)).toBeNull();
    expect(slowRampForAge("bad", asOf)).toBeNull();
  });
});
