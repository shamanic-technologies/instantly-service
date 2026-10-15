import { afterEach, describe, expect, it, vi } from "vitest";
import {
  creditRefusalBody,
  fundingRefusalCause,
  fundingRefusalCauseFromText,
} from "../../src/lib/funding-refusal";

const CAP = {
  salesFunnelId: "sf-1",
  salesFunnelCampaignId: "sfc-1",
  amountCents: 1000,
  period: "day",
  consumedCents: 1000,
  reservedCents: 0,
};

describe("fundingRefusalCause — billing's named cause, verbatim", () => {
  it("no reason = a balance shortfall = no cause", () => {
    expect(fundingRefusalCause({ sufficient: false, balance_cents: 0, required_cents: 4 })).toBeNull();
  });

  it("reason + sales_funnel_cap are carried untouched", () => {
    expect(
      fundingRefusalCause({ sufficient: false, reason: "sales_funnel_max_budget_reached", sales_funnel_cap: CAP }),
    ).toEqual({ reason: "sales_funnel_max_budget_reached", sales_funnel_cap: CAP });
  });
});

describe("creditRefusalBody", () => {
  const base = { error: "Insufficient credits", code: "insufficient_credits", balance_cents: 2, required_cents: 18 };

  it("no cause → exactly the base body", () => {
    expect(creditRefusalBody(base, null)).toBe(base);
  });

  it("unreadable cap is named too, code kept", () => {
    const body = creditRefusalBody(base, { reason: "sales_funnel_max_budget_unreadable", sales_funnel_cap: CAP });
    expect(body).toMatchObject({
      error: "Sales funnel max budget unreadable",
      code: "insufficient_credits",
      reason: "sales_funnel_max_budget_unreadable",
      sales_funnel_cap: CAP,
    });
  });
});

describe("fundingRefusalCauseFromText — a sibling's (apollo-service) relayed refusal", () => {
  it("a JSON body carrying reason + sales_funnel_cap is relayed verbatim", () => {
    const text = JSON.stringify({ error: "Sales funnel max budget reached", reason: "sales_funnel_max_budget_reached", sales_funnel_cap: CAP });
    expect(fundingRefusalCauseFromText(text)).toEqual({ reason: "sales_funnel_max_budget_reached", sales_funnel_cap: CAP });
  });

  it("prose naming the reason still names it", () => {
    expect(fundingRefusalCauseFromText("verification failed: sales_funnel_max_budget_reached")).toEqual({
      reason: "sales_funnel_max_budget_reached",
    });
  });

  it("a plain insufficient-balance answer has no cause", () => {
    expect(fundingRefusalCauseFromText('{"error":"insufficient balance (balance=0¢, required=4¢)"}')).toBeNull();
  });
});

describe("verifyEmailAddresses — apollo's 402 with billing's cause reaches the caller", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.unstubAllEnvs();
    vi.resetModules();
  });

  async function load() {
    vi.stubEnv("APOLLO_SERVICE_URL", "http://apollo");
    vi.stubEnv("APOLLO_SERVICE_API_KEY", "k");
    vi.resetModules();
    return import("../../src/lib/apollo-client");
  }

  const identity = { orgId: "org-1", userId: "u-1", runId: "run-1" };

  it("reason + sales_funnel_cap are kept on the error (insufficientCredits stays true)", async () => {
    const { verifyEmailAddresses, EmailVerifierError } = await load();
    const body = { error: "Sales funnel max budget reached", reason: "sales_funnel_max_budget_reached", sales_funnel_cap: CAP };
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify(body), { status: 402 })));

    const err = await verifyEmailAddresses(["a@x.com"], identity, "t").catch((e: unknown) => e);
    expect(err).toBeInstanceOf(EmailVerifierError);
    expect((err as InstanceType<typeof EmailVerifierError>).insufficientCredits).toBe(true);
    expect((err as InstanceType<typeof EmailVerifierError>).fundingCause).toEqual({
      reason: "sales_funnel_max_budget_reached",
      sales_funnel_cap: CAP,
    });
  });

  it("a plain insufficient balance stays a credit refusal with no cause", async () => {
    const { verifyEmailAddresses, EmailVerifierError } = await load();
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(new Response('{"error":"insufficient balance (balance=0¢, required=4¢)"}', { status: 502 })),
    );
    const err = (await verifyEmailAddresses(["a@x.com"], identity, "t").catch((e: unknown) => e)) as InstanceType<
      typeof EmailVerifierError
    >;
    expect(err.insufficientCredits).toBe(true);
    expect(err.fundingCause).toBeNull();
  });
});
