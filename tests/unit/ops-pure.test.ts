import { describe, it, expect } from "vitest";
import {
  DELIVERY_EVIDENCE_MAX_AGE_DAYS,
  IN_PRODUCTION_DAILY_LIMIT,
  PRODUCTION_DELIVERY_PCT_BAR,
  PRODUCTION_HEALTH_BAR,
  RAMP_FLOOR_PER_DAY,
  RECOVERY_DAILY_LIMIT,
  RECOVERY_WARMUP_DAILY,
  rampCapForVolume,
} from "../../src/lib/account-lifecycle";
import { SEED_TEST_INTERVAL_DAYS } from "../../src/lib/seed-placement/due";
import { WARMUP_PARTNERS_PER_DAY } from "../../src/lib/warmup/plan";
import { lifecycleRules } from "../../src/lib/ops/lifecycle-rules";
import {
  estimatePaidToDate,
  evidenceExpiresAt,
  nextNonSendingDay,
  poolDelivery,
  projectNextSeedTest,
  rampProjection,
  ratePerMille,
} from "../../src/lib/ops/pure";
import { decodeCursor, encodeCursor } from "../../src/lib/ops/reads";

describe("lifecycleRules — the served rules ARE the decision's constants", () => {
  it("pins every number to its source constant", () => {
    const r = lifecycleRules();
    expect(r.bars).toEqual({
      healthEntryBar: PRODUCTION_HEALTH_BAR,
      deliveryPctBar: PRODUCTION_DELIVERY_PCT_BAR,
      deliveryEvidenceMaxAgeDays: DELIVERY_EVIDENCE_MAX_AGE_DAYS,
    });
    expect(r.states.find((s) => s.status === "in_production")).toMatchObject({ newSends: true, campaignDailyLimit: IN_PRODUCTION_DAILY_LIMIT });
    expect(r.states.find((s) => s.status === "in_recovery")).toMatchObject({ newSends: false, campaignDailyLimit: RECOVERY_DAILY_LIMIT, warmupDaily: RECOVERY_WARMUP_DAILY });
    expect(r.ramp.floorPerDay).toBe(RAMP_FLOOR_PER_DAY);
    expect(r.ramp.ceiling).toBe(IN_PRODUCTION_DAILY_LIMIT);
    expect(r.placement.seedTestIntervalDays).toBe(SEED_TEST_INTERVAL_DAYS);
    expect(r.warmup.partnersPerDay).toBe(WARMUP_PARTNERS_PER_DAY);
  });

  it("states the rule order with in_production LAST (first match wins)", () => {
    const order = lifecycleRules().order.map((o) => o.leadsTo);
    expect(order[0]).toBe("deactivated_by_user");
    expect(order[order.length - 1]).toBe("in_production");
    expect(new Set(lifecycleRules().states.map((s) => s.status)).size).toBe(4);
  });
});

describe("rampProjection — the cap trajectory from the ramp's own arithmetic", () => {
  it("starts at today's cap and climbs to the ceiling, day by day", () => {
    const asOf = new Date("2026-09-14T00:00:00Z");
    const p = rampProjection(0, IN_PRODUCTION_DAILY_LIMIT, asOf);
    expect(p[0]).toEqual({ date: "2026-09-14", cap: rampCapForVolume(0, IN_PRODUCTION_DAILY_LIMIT) });
    expect(p[p.length - 1].cap).toBe(IN_PRODUCTION_DAILY_LIMIT);
    expect(p.map((x) => x.cap)).toEqual([...p.map((x) => x.cap)].sort((a, b) => a - b));
    expect(p.length).toBeLessThanOrEqual(30);
  });

  it("a mailbox already at the ceiling projects one point", () => {
    expect(rampProjection(50, 50, new Date("2026-09-14T00:00:00Z"))).toHaveLength(1);
  });
});

describe("evidenceExpiresAt / projectNextSeedTest / nextNonSendingDay", () => {
  it("expiry is testedAt + the evidence max age; null when never tested", () => {
    expect(evidenceExpiresAt("2026-09-01T00:00:00Z")).toBe(
      new Date(Date.UTC(2026, 8, 1 + DELIVERY_EVIDENCE_MAX_AGE_DAYS)).toISOString(),
    );
    expect(evidenceExpiresAt(null)).toBeNull();
  });

  it("the next non-sending day is the coming Saturday, or today on a weekend", () => {
    expect(nextNonSendingDay(new Date("2026-09-16T10:00:00Z"))).toBe("2026-09-19"); // Wed → Sat
    expect(nextNonSendingDay(new Date("2026-09-20T10:00:00Z"))).toBe("2026-09-20"); // Sun
  });

  it("never tested → due today; recently tested → expected on the weekend after the interval elapses", () => {
    const asOf = new Date("2026-09-16T10:00:00Z");
    expect(projectNextSeedTest(null, asOf)).toMatchObject({ due: true, reason: "no_previous_test", expectedAt: "2026-09-16" });
    const recent = projectNextSeedTest(new Date("2026-09-14T00:00:00Z"), asOf);
    expect(recent.due).toBe(false);
    expect(recent.reason).toBe("recently_tested");
    expect(recent.expectedAt).toBe("2026-09-20"); // interval elapses Sun 20th, a non-sending day
  });
});

describe("poolDelivery / estimatePaidToDate / ratePerMille — never fabricate", () => {
  it("pools Σinbox/Σseeds, ignores unmeasured rows, keeps the newest test", () => {
    const p = poolDelivery([
      { inboxCount: 36, seedTotal: 38, testedAt: "2026-09-01T00:00:00Z" },
      { inboxCount: 0, seedTotal: 0, testedAt: "2026-09-10T00:00:00Z" },
      { inboxCount: 20, seedTotal: 40, testedAt: "2026-09-05T00:00:00Z" },
    ]);
    expect(p).toEqual({ inboxCount: 56, seedTotal: 78, inboxPct: 71.8, testedAt: "2026-09-05T00:00:00.000Z", measuredAccounts: 2 });
    expect(poolDelivery([]).inboxPct).toBeNull();
  });

  it("an estimate is labelled, counts the current month, and refuses a start in the future", () => {
    const asOf = new Date("2026-09-18T00:00:00Z");
    expect(estimatePaidToDate(450, "USD", "2026-07-07T00:00:00Z", asOf)).toEqual({ cents: 1350, currency: "USD", source: "estimate", since: "2026-07-07T00:00:00.000Z", months: 3 });
    expect(estimatePaidToDate(450, "USD", null, asOf)).toBeNull();
    expect(estimatePaidToDate(450, "USD", "2027-01-01T00:00:00Z", asOf)).toBeNull();
  });

  it("a rate over an empty base is null, not 0", () => {
    expect(ratePerMille(3, 0)).toBeNull();
    expect(ratePerMille(3, 1000)).toBe(3);
  });
});

describe("cursor — opaque, round-trips, refuses garbage", () => {
  it("round-trips", () => {
    const c = encodeCursor("2026-09-18T00:00:00.000Z", "self:abc|with|pipes");
    expect(decodeCursor(c)).toEqual({ lastAt: "2026-09-18T00:00:00.000Z", id: "self:abc|with|pipes" });
  });
  it("rejects a cursor whose timestamp is not a date", () => {
    expect(decodeCursor(Buffer.from("nope|x").toString("base64url"))).toBeNull();
  });
});
