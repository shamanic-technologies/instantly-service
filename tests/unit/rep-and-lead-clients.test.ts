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
  process.env.CAMPAIGN_SERVICE_URL = "https://campaign.example";
  process.env.CAMPAIGN_SERVICE_API_KEY = "campaign-key";
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
      // Absent on this row: null, never guessed from another field.
      firstName: null,
      lastName: null,
      title: null,
      city: null,
      state: null,
      country: null,
    });

    const url = new URL(fetchMock.mock.calls[0][0]);
    expect(url.pathname).toBe("/orgs/leads");
    expect(url.searchParams.get("view")).toBe("basic");
    expect(url.searchParams.get("campaignId")).toBe("camp-1");
    expect(url.searchParams.get("q")).toBe("prospect@example.com");
    expect(url.searchParams.get("status")).toBe("all");
  });

  it("reads the identity the ?view=basic projection already carries", async () => {
    stubFetch({
      json: {
        leads: [
          {
            ...ROW,
            lead: {
              name: "Colleen Morley",
              firstName: "  Colleen ",
              lastName: "Morley",
              currentTitle: "Doctor of Chiropractic",
              city: "Boston",
              state: "Massachusetts",
              country: "United States",
              organization: { name: "Spine & Sports Injury Center" },
            },
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

    expect(out).toMatchObject({
      firstName: "Colleen",
      lastName: "Morley",
      title: "Doctor of Chiropractic",
      city: "Boston",
      state: "Massachusetts",
      country: "United States",
      company: "Spine & Sports Injury Center",
    });
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

