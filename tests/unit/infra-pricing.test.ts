import { describe, it, expect } from "vitest";

import {
  classifyWaste,
  costPerEmailCents,
  indexRates,
  monthlyCostForDomain,
  splitDomainCost,
  summarizePlanSpend,
  summarizeSpend,
  type InventoryDomain,
  type PriceRate,
} from "../../src/lib/infra-pricing";

function domain(overrides: Partial<InventoryDomain> = {}): InventoryDomain {
  return {
    provider: "mailforge",
    domain: "example.com",
    role: "mailbox",
    status: "active",
    expiresAt: null,
    autorenew: null,
    deletionScheduled: false,
    cancelledAt: null,
    absentSince: null,
    priceCents: null,
    priceCurrency: null,
    mailboxCount: 0,
    instantlyAccountCount: 0,
    inProductionCount: 0,
    sentLast30d: 0,
    ...overrides,
  };
}

function rate(overrides: Partial<PriceRate> = {}): PriceRate {
  return {
    provider: "instantly-dfy",
    scope: "domain-year",
    item: "",
    unitCents: 1500,
    currency: "USD",
    source: "rate-card",
    note: null,
    ...overrides,
  };
}

const DFY_RATES = indexRates([
  rate(),
  rate({ scope: "mailbox-month", unitCents: 1000 }),
]);

describe("monthlyCostForDomain", () => {
  it("divides a vendor-reported yearly price by 12 and marks it api-sourced", () => {
    const cost = monthlyCostForDomain(
      domain({ priceCents: 1400, priceCurrency: "USD" }),
      indexRates([]),
    );

    expect(cost).toEqual({ cents: 117, currency: "USD", source: "api" });
  });

  it("prefers the vendor's own price over the rate card", () => {
    const cost = monthlyCostForDomain(
      domain({ provider: "instantly-dfy", priceCents: 2400, priceCurrency: "USD" }),
      DFY_RATES,
    );

    expect(cost?.cents).toBe(200);
    expect(cost?.source).toBe("api");
  });

  it("adds the per-mailbox monthly rate, counted on VENDOR mailboxes", () => {
    const cost = monthlyCostForDomain(
      domain({ provider: "instantly-dfy", mailboxCount: 5, instantlyAccountCount: 40 }),
      DFY_RATES,
    );

    // $15/yr → 125¢/mo, plus 5 × $10/mo.
    expect(cost).toEqual({ cents: 5125, currency: "USD", source: "rate-card" });
  });

  it("returns null when nothing prices the domain — never a placeholder", () => {
    expect(monthlyCostForDomain(domain({ provider: "primeforge" }), DFY_RATES)).toBeNull();
  });

  it("returns null for a cancelled domain — we stopped paying for it", () => {
    const cost = monthlyCostForDomain(
      domain({ provider: "instantly-dfy", cancelledAt: new Date("2026-05-02T00:00:00Z") }),
      DFY_RATES,
    );

    expect(cost).toBeNull();
  });

  it("refuses to blend two currencies inside one domain", () => {
    const mixed = indexRates([
      rate({ provider: "weird", scope: "domain-year", unitCents: 1200, currency: "EUR" }),
      rate({ provider: "weird", scope: "mailbox-month", unitCents: 500, currency: "USD" }),
    ]);

    expect(monthlyCostForDomain(domain({ provider: "weird", mailboxCount: 2 }), mixed)).toBeNull();
  });
});

describe("costPerEmailCents", () => {
  it("divides the monthly cost by the trailing send count", () => {
    const perEmail = costPerEmailCents({ cents: 5125, currency: "USD", source: "rate-card" }, 1000);
    expect(perEmail).toEqual({ cents: 5.125, currency: "USD" });
  });

  it("returns null for a silent domain rather than dividing by zero", () => {
    expect(costPerEmailCents({ cents: 5125, currency: "USD", source: "rate-card" }, 0)).toBeNull();
  });

  it("returns null when the domain has no cost", () => {
    expect(costPerEmailCents(null, 500)).toBeNull();
  });
});

describe("classifyWaste", () => {
  const asOf = new Date("2026-08-02T00:00:00Z");

  it("flags a domain we pay for with no sending account", () => {
    const findings = classifyWaste([domain({ instantlyAccountCount: 0 })], indexRates([]), asOf);

    expect(findings).toHaveLength(1);
    expect(findings[0].reason).toBe("paid_no_sending_accounts");
  });

  it("does not flag a domain that is actually sending", () => {
    const findings = classifyWaste(
      [domain({ instantlyAccountCount: 5, sentLast30d: 900 })],
      indexRates([]),
      asOf,
    );

    expect(findings).toEqual([]);
  });

  it("flags a cancelled order once, without also calling it idle", () => {
    const findings = classifyWaste(
      [domain({ domain: "arcadiaquest.org", cancelledAt: new Date("2026-05-02T00:00:00Z") })],
      indexRates([]),
      asOf,
    );

    expect(findings).toHaveLength(1);
    expect(findings[0].reason).toBe("cancelled_by_vendor");
  });

  it("flags a domain the vendor has scheduled for deletion", () => {
    const findings = classifyWaste([domain({ deletionScheduled: true })], indexRates([]), asOf);
    expect(findings.map((f) => f.reason)).toEqual(["deletion_scheduled"]);
  });

  it("flags an imminent expiry and says when autorenew is off", () => {
    const findings = classifyWaste(
      [
        domain({
          instantlyAccountCount: 3,
          expiresAt: new Date("2026-08-20T00:00:00Z"),
          autorenew: false,
        }),
      ],
      indexRates([]),
      asOf,
    );

    expect(findings).toHaveLength(1);
    expect(findings[0].reason).toBe("expiring_within_30d");
    expect(findings[0].detail).toContain("autorenew OFF");
  });

  it("ignores a row the vendor stopped reporting — it is gone, not wasteful", () => {
    const findings = classifyWaste(
      [domain({ absentSince: new Date("2026-07-01T00:00:00Z") })],
      indexRates([]),
      asOf,
    );

    expect(findings).toEqual([]);
  });

  it("never proposes an action — the finding carries evidence only", () => {
    const findings = classifyWaste([domain({ deletionScheduled: true })], indexRates([]), asOf);
    expect(Object.keys(findings[0]).sort()).toEqual(
      ["currency", "detail", "domain", "expiresAt", "monthlyCostCents", "provider", "reason"].sort(),
    );
  });
});

describe("summarizeSpend", () => {
  it("totals per currency and never blends them", () => {
    const summary = summarizeSpend(
      [
        domain({ provider: "gandi", priceCents: 3838, priceCurrency: "EUR" }),
        domain({ domain: "b.com", provider: "mailforge", priceCents: 1400, priceCurrency: "USD" }),
      ],
      indexRates([]),
    );

    expect(summary.monthlyByCurrency).toEqual([
      { currency: "EUR", cents: 320 },
      { currency: "USD", cents: 117 },
    ]);
  });

  it("names a vendor it cannot price instead of dropping it silently", () => {
    const summary = summarizeSpend(
      [domain({ provider: "primeforge" }), domain({ domain: "b.com", provider: "primeforge" })],
      DFY_RATES,
    );

    expect(summary.unpricedProviders).toEqual(["primeforge"]);
    expect(summary.unpricedDomainCount).toBe(2);
    expect(summary.byProvider).toEqual([]);
  });

  it("does not count a cancelled domain as unpriced — we simply stopped paying", () => {
    const summary = summarizeSpend(
      [domain({ provider: "instantly-dfy", cancelledAt: new Date("2026-05-02T00:00:00Z") })],
      DFY_RATES,
    );

    expect(summary.unpricedProviders).toEqual([]);
    expect(summary.unpricedDomainCount).toBe(0);
  });

  it("marks a provider mixed when its rows come from both an API and the rate card", () => {
    const summary = summarizeSpend(
      [
        domain({ provider: "instantly-dfy", priceCents: 1200, priceCurrency: "USD" }),
        domain({ domain: "b.com", provider: "instantly-dfy" }),
      ],
      DFY_RATES,
    );

    expect(summary.byProvider[0].source).toBe("mixed");
  });

  it("skips rows a vendor stopped reporting", () => {
    const summary = summarizeSpend(
      [domain({ provider: "instantly-dfy", absentSince: new Date("2026-07-01T00:00:00Z") })],
      DFY_RATES,
    );

    expect(summary.byProvider).toEqual([]);
    expect(summary.unpricedDomainCount).toBe(0);
  });
});

describe("summarizePlanSpend", () => {
  it("returns only plan-scoped rates, biggest first", () => {
    const plans = summarizePlanSpend([
      rate({ provider: "instantly", scope: "plan-month", item: "inbox-placement", unitCents: 4700 }),
      rate({ provider: "instantly", scope: "plan-month", item: "hypergrowth", unitCents: 9700 }),
      rate({ scope: "domain-year", unitCents: 1500 }),
    ]);

    expect(plans.map((p) => p.item)).toEqual(["hypergrowth", "inbox-placement"]);
  });
});

describe("splitDomainCost", () => {
  it("puts a mailbox subscription in recurring — cancelling stops it immediately", () => {
    const split = splitDomainCost(
      domain({ provider: "instantly-dfy", mailboxCount: 5, expiresAt: new Date("2027-01-01T00:00:00Z") }),
      DFY_RATES,
    );

    expect(split.recurringMonthlyCents).toBe(5000);
    expect(split.currency).toBe("USD");
  });

  it("puts the registration in renewal, with the date it actually falls due", () => {
    const split = splitDomainCost(
      domain({ provider: "gandi", priceCents: 3838, priceCurrency: "EUR", expiresAt: new Date("2027-02-03T00:00:00Z") }),
      indexRates([]),
    );

    // Gandi mailboxes are free, so nothing stops billing by deleting today.
    expect(split.recurringMonthlyCents).toBeNull();
    expect(split.renewalCents).toBe(3838);
    expect(split.renewalAt?.toISOString()).toBe("2027-02-03T00:00:00.000Z");
  });

  it("keeps the renewal figure yearly — it is not a monthly number", () => {
    const split = splitDomainCost(
      domain({ provider: "gandi", priceCents: 3838, priceCurrency: "EUR" }),
      indexRates([]),
    );

    // The blended monthly read divides by 12; this one must not.
    expect(split.renewalCents).toBe(3838);
  });

  it("reports both halves when a vendor charges for the domain AND the mailboxes", () => {
    const split = splitDomainCost(
      domain({ provider: "instantly-dfy", mailboxCount: 5, expiresAt: new Date("2027-01-01T00:00:00Z") }),
      DFY_RATES,
    );

    expect(split.renewalCents).toBe(1500);
    expect(split.recurringMonthlyCents).toBe(5000);
  });

  it("saves nothing on a cancelled domain — it already bills nothing", () => {
    const split = splitDomainCost(
      domain({ provider: "instantly-dfy", mailboxCount: 5, cancelledAt: new Date("2026-05-02T00:00:00Z") }),
      DFY_RATES,
    );

    expect(split).toEqual({
      recurringMonthlyCents: null,
      renewalCents: null,
      renewalAt: null,
      currency: null,
    });
  });

  it("reports nothing at all for an unpriced vendor", () => {
    expect(splitDomainCost(domain({ provider: "primeforge" }), DFY_RATES)).toEqual({
      recurringMonthlyCents: null,
      renewalCents: null,
      renewalAt: null,
      currency: null,
    });
  });

  it("has no renewal date when the vendor reports no expiry", () => {
    const split = splitDomainCost(domain({ provider: "instantly-dfy", mailboxCount: 1 }), DFY_RATES);
    expect(split.renewalCents).toBe(1500);
    expect(split.renewalAt).toBeNull();
  });

  it("refuses to split across two currencies", () => {
    const mixed = indexRates([
      rate({ provider: "weird", scope: "mailbox-month", unitCents: 500, currency: "USD" }),
    ]);

    const split = splitDomainCost(
      domain({ provider: "weird", priceCents: 1200, priceCurrency: "EUR", mailboxCount: 2 }),
      mixed,
    );

    expect(split.currency).toBeNull();
    expect(split.recurringMonthlyCents).toBeNull();
  });
});
