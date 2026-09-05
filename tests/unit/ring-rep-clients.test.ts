import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const ORIGINAL_ENV = { ...process.env };

beforeEach(() => {
  vi.resetModules();
  process.env.BRAND_SERVICE_URL = "https://brand.example";
  process.env.BRAND_SERVICE_API_KEY = "brand-key";
  process.env.APOLLO_SERVICE_URL = "https://apollo.example";
  process.env.APOLLO_SERVICE_API_KEY = "apollo-key";
  process.env.TWILIO_SERVICE_URL = "https://twilio.example";
  process.env.TWILIO_SERVICE_API_KEY = "twilio-key";
  process.env.LEAD_SERVICE_URL = "https://lead.example";
  process.env.LEAD_SERVICE_API_KEY = "lead-key";
});

afterEach(() => {
  process.env = { ...ORIGINAL_ENV };
  vi.unstubAllGlobals();
});

function stubFetch(response: {
  ok?: boolean;
  status?: number;
  json?: unknown;
  text?: string;
}) {
  const mock = vi.fn().mockResolvedValue({
    ok: response.ok ?? true,
    status: response.status ?? 200,
    json: async () => response.json,
    text: async () => response.text ?? "",
  });
  vi.stubGlobal("fetch", mock);
  return mock;
}

describe("getSalesRepPhone", () => {
  it("reads the deployed per-brand path with the org identity", async () => {
    const fetchMock = stubFetch({ json: { salesRepPhone: "+15559990000" } });
    const { getSalesRepPhone } = await import("../../src/lib/brand-client");

    await expect(getSalesRepPhone("brand-1", "org-1")).resolves.toBe("+15559990000");

    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("https://brand.example/orgs/brands/brand-1/sales-rep-phone");
    expect(init.headers["x-org-id"]).toBe("org-1");
    expect(init.headers["x-api-key"]).toBe("brand-key");
  });

  it("nobody-to-ring is null, not an error — including a brand this org cannot see", async () => {
    stubFetch({ json: { salesRepPhone: null } });
    const { getSalesRepPhone } = await import("../../src/lib/brand-client");
    await expect(getSalesRepPhone("brand-1", "org-1")).resolves.toBeNull();

    vi.resetModules();
    stubFetch({ ok: false, status: 404, text: "not found" });
    const again = await import("../../src/lib/brand-client");
    await expect(again.getSalesRepPhone("brand-1", "org-1")).resolves.toBeNull();
  });

  it("FAILS LOUD on anything else — an unreachable brand-service is not 'no number'", async () => {
    stubFetch({ ok: false, status: 500, text: "boom" });
    const { getSalesRepPhone } = await import("../../src/lib/brand-client");
    await expect(getSalesRepPhone("brand-1", "org-1")).rejects.toThrow(/500/);
  });
});

describe("phone reveal client", () => {
  const REVEAL = {
    revealId: "rev-1",
    apolloPersonId: "p-1",
    status: "pending",
    mobilePhone: null,
    dncStatus: null,
    doNotCall: false,
    phoneNumbers: [],
    failureReason: null,
    creditsConsumed: null,
    requestedAt: null,
    completedAt: null,
  };

  it("POSTs the reveal with the full identity apollo-service requires", async () => {
    const fetchMock = stubFetch({ status: 202, json: REVEAL });
    const { requestPhoneReveal } = await import("../../src/lib/apollo-client");

    await requestPhoneReveal("p-1", {
      orgId: "org-1",
      userId: "user-1",
      runId: "run-1",
      brandId: "brand-1",
      campaignId: "camp-1",
    });

    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("https://apollo.example/people/p-1/phone-reveal");
    expect(init.method).toBe("POST");
    expect(init.headers["x-org-id"]).toBe("org-1");
    expect(init.headers["x-user-id"]).toBe("user-1");
    // A real run id: apollo-service declares the reveal's cost against it.
    expect(init.headers["x-run-id"]).toBe("run-1");
    expect(init.headers["x-brand-id"]).toBe("brand-1");
  });

  it("GETs the same path to read the delivered result", async () => {
    const fetchMock = stubFetch({ json: { ...REVEAL, status: "found" } });
    const { readPhoneReveal } = await import("../../src/lib/apollo-client");

    const out = await readPhoneReveal("p-1", {
      orgId: "org-1",
      userId: "user-1",
      runId: "run-1",
    });

    expect(out.status).toBe("found");
    expect(fetchMock.mock.calls[0][0]).toBe("https://apollo.example/people/p-1/phone-reveal");
    expect(fetchMock.mock.calls[0][1].method).toBeUndefined();
  });

  it("fails loud on a non-2xx", async () => {
    stubFetch({ ok: false, status: 402, text: "credits" });
    const { requestPhoneReveal } = await import("../../src/lib/apollo-client");
    await expect(
      requestPhoneReveal("p-1", { orgId: "o", userId: "u", runId: "r" }),
    ).rejects.toThrow(/402/);
  });
});

describe("placeCall", () => {
  it("POSTs /calls with the org identity and the body twilio-service parses", async () => {
    const fetchMock = stubFetch({
      json: { success: true, callId: "c-1", costName: "voice-us", connectOffered: true },
    });
    const { placeCall } = await import("../../src/lib/twilio-client");

    await placeCall({
      orgId: "org-1",
      userId: "user-1",
      to: "+15559990000",
      reply: { name: "Dana", message: "interested" },
      connectTo: "+15551230000",
      brandId: "brand-1",
    });

    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("https://twilio.example/calls");
    expect(init.headers["x-org-id"]).toBe("org-1");
    expect(init.headers["x-user-id"]).toBe("user-1");
    const body = JSON.parse(init.body);
    // Identity travels as headers, never in the body.
    expect(body.orgId).toBeUndefined();
    expect(body.userId).toBeUndefined();
    expect(body).toMatchObject({
      to: "+15559990000",
      connectTo: "+15551230000",
      reply: { name: "Dana", message: "interested" },
    });
  });

  it("fails loud on a non-2xx — a call that was never placed must not read as one that was", async () => {
    stubFetch({ ok: false, status: 502, text: "twilio refused" });
    const { placeCall } = await import("../../src/lib/twilio-client");
    await expect(
      placeCall({
        orgId: "o",
        userId: "u",
        to: "+1555",
        reply: { name: "n", message: "m" },
      }),
    ).rejects.toThrow(/502/);
  });
});

describe("findLeadOnCampaignByEmail", () => {
  const ROW = {
    id: "lc-1",
    email: "Prospect@Example.com",
    apolloPersonId: "apollo-1",
    lead: { name: "Dana Reid", organization: { name: "Acme" } },
  };

  it("narrows with the campaign-scoped search and matches the address EXACTLY", async () => {
    const fetchMock = stubFetch({ json: { leads: [ROW] } });
    const { findLeadOnCampaignByEmail } = await import("../../src/lib/lead-client");

    const out = await findLeadOnCampaignByEmail({
      orgId: "org-1",
      campaignId: "camp-1",
      email: "prospect@example.com",
    });

    expect(out).toEqual({
      id: "lc-1",
      email: "Prospect@Example.com",
      apolloPersonId: "apollo-1",
      name: "Dana Reid",
      company: "Acme",
    });

    const url = new URL(fetchMock.mock.calls[0][0]);
    expect(url.pathname).toBe("/orgs/leads");
    expect(url.searchParams.get("view")).toBe("basic");
    expect(url.searchParams.get("campaignId")).toBe("camp-1");
    expect(url.searchParams.get("q")).toBe("prospect@example.com");
    expect(url.searchParams.get("status")).toBe("all");
  });

  it("refuses rather than guesses: a substring search matching two people resolves to nobody", async () => {
    stubFetch({
      json: {
        leads: [ROW, { ...ROW, id: "lc-2", email: "PROSPECT@example.com" }],
      },
    });
    const { findLeadOnCampaignByEmail } = await import("../../src/lib/lead-client");
    await expect(
      findLeadOnCampaignByEmail({
        orgId: "org-1",
        campaignId: "camp-1",
        email: "prospect@example.com",
      }),
    ).resolves.toBeNull();
  });

  it("drops rows the search matched on some other field", async () => {
    stubFetch({ json: { leads: [{ ...ROW, email: "someone-else@example.com" }] } });
    const { findLeadOnCampaignByEmail } = await import("../../src/lib/lead-client");
    await expect(
      findLeadOnCampaignByEmail({
        orgId: "org-1",
        campaignId: "camp-1",
        email: "prospect@example.com",
      }),
    ).resolves.toBeNull();
  });

  it("falls back to first + last name when no display name is held", async () => {
    stubFetch({
      json: {
        leads: [
          {
            ...ROW,
            lead: { firstName: "Dana", lastName: "Reid", organization: null },
          },
        ],
      },
    });
    const { findLeadOnCampaignByEmail } = await import("../../src/lib/lead-client");
    const out = await findLeadOnCampaignByEmail({
      orgId: "org-1",
      campaignId: "camp-1",
      email: "prospect@example.com",
    });
    expect(out?.name).toBe("Dana Reid");
    expect(out?.company).toBeNull();
  });

  it("fails loud on a non-2xx — unreachable is not 'nobody at that address'", async () => {
    stubFetch({ ok: false, status: 500, text: "boom" });
    const { findLeadOnCampaignByEmail } = await import("../../src/lib/lead-client");
    await expect(
      findLeadOnCampaignByEmail({ orgId: "o", campaignId: "c", email: "a@b.com" }),
    ).rejects.toThrow(/500/);
  });
});
