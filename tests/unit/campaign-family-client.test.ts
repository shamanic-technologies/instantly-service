import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const ORIGINAL_ENV = { ...process.env };

const mockFetch = vi.fn();
vi.stubGlobal("fetch", mockFetch);

function jsonResponse(status: number, body: unknown) {
  return {
    ok: status >= 200 && status < 300,
    status,
    json: async () => body,
    text: async () => JSON.stringify(body),
  };
}

const IDENTITY = {
  orgId: "org-1",
  brandId: "brand-1",
  brandIds: ["brand-1"],
  funnelKey: "sales_meetings_from_conversation",
  acquisitionChannel: "cold_email",
};

beforeEach(() => {
  vi.resetModules();
  vi.clearAllMocks();
  process.env.CAMPAIGN_SERVICE_URL = "https://campaign.test";
  process.env.CAMPAIGN_SERVICE_API_KEY = "campaign-key";
});

afterEach(() => {
  process.env = { ...ORIGINAL_ENV };
});

async function load() {
  return (await import("../../src/lib/campaign-client")).getCampaignFamily;
}

describe("getCampaignFamily", () => {
  it("returns every stored row sharing the campaign's identity", async () => {
    mockFetch
      .mockResolvedValueOnce(jsonResponse(200, { campaign: { ...IDENTITY } }))
      .mockResolvedValueOnce(
        jsonResponse(200, {
          campaigns: [
            { id: "camp-1", ...IDENTITY, workflowSlug: "lithium" },
            { id: "camp-2", ...IDENTITY, workflowSlug: "permafrost" },
            { id: "other-funnel", ...IDENTITY, funnelKey: "website_purchases" },
          ],
        }),
      );

    const getCampaignFamily = await load();
    expect(await getCampaignFamily("camp-1", "org-1")).toEqual(["camp-1", "camp-2"]);

    // The brand read is NOT narrowed by featureSlug — the feature is no part of
    // the identity, so narrowing could drop a sibling stating another one.
    const url = String(mockFetch.mock.calls[1][0]);
    expect(url).toContain("/campaigns?brandId=brand-1");
    expect(url).not.toContain("featureSlug");
    expect(mockFetch.mock.calls[1][1].headers["x-org-id"]).toBe("org-1");
  });

  it("a campaign campaign-service does not know is a family of one — no brand read", async () => {
    mockFetch.mockResolvedValueOnce(jsonResponse(404, { error: "not found" }));

    const getCampaignFamily = await load();
    expect(await getCampaignFamily("camp-1", "org-1")).toEqual(["camp-1"]);
    expect(mockFetch).toHaveBeenCalledTimes(1);
  });

  it("a campaign stating too little to be pooled is a family of one", async () => {
    mockFetch.mockResolvedValueOnce(
      jsonResponse(200, { campaign: { ...IDENTITY, acquisitionChannel: null } }),
    );

    const getCampaignFamily = await load();
    expect(await getCampaignFamily("camp-1", "org-1")).toEqual(["camp-1"]);
    expect(mockFetch).toHaveBeenCalledTimes(1);
  });

  it("FAILS LOUD when campaign-service is unreachable — never a silent family of one", async () => {
    mockFetch
      .mockResolvedValueOnce(jsonResponse(200, { campaign: { ...IDENTITY } }))
      .mockResolvedValueOnce(jsonResponse(503, { error: "down" }));

    const getCampaignFamily = await load();
    await expect(getCampaignFamily("camp-1", "org-1")).rejects.toThrow(/503/);
  });

  it("fails loud when the brand list comes back without a campaigns array", async () => {
    mockFetch
      .mockResolvedValueOnce(jsonResponse(200, { campaign: { ...IDENTITY } }))
      .mockResolvedValueOnce(jsonResponse(200, {}));

    const getCampaignFamily = await load();
    await expect(getCampaignFamily("camp-1", "org-1")).rejects.toThrow(
      /no campaigns array/,
    );
  });

  it("trusts the DIRECT read for the asked row's own identity", async () => {
    // The brand list carries a stale/partial copy of the asked row; the direct
    // read is what was asked for, so it wins.
    mockFetch
      .mockResolvedValueOnce(jsonResponse(200, { campaign: { ...IDENTITY } }))
      .mockResolvedValueOnce(
        jsonResponse(200, {
          campaigns: [
            { id: "camp-1", ...IDENTITY, acquisitionChannel: null },
            { id: "camp-2", ...IDENTITY },
          ],
        }),
      );

    const getCampaignFamily = await load();
    expect(await getCampaignFamily("camp-1", "org-1")).toEqual(["camp-1", "camp-2"]);
  });
});
