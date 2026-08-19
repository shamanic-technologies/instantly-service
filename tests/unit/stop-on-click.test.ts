import { describe, it, expect, vi, beforeEach } from "vitest";

const mockGetCampaignFunnelKey = vi.fn();
const mockResolveInstantlyApiKey = vi.fn();
const mockUpdateCampaignStatus = vi.fn();

vi.mock("../../src/lib/campaign-client", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  getCampaignFunnelKey: (...args: unknown[]) => mockGetCampaignFunnelKey(...args),
}));

vi.mock("../../src/lib/key-client", () => ({
  resolveInstantlyApiKey: (...args: unknown[]) => mockResolveInstantlyApiKey(...args),
}));

vi.mock("../../src/lib/instantly-client", () => ({
  updateCampaignStatus: (...args: unknown[]) => mockUpdateCampaignStatus(...args),
}));

const { maybeStopOnClickForFunnel } = await import("../../src/lib/stop-on-click");
const { funnelStopsOnClick } = await import("../../src/lib/campaign-client");

const CAMPAIGN = {
  instantlyCampaignId: "inst-camp-1",
  campaignId: "camp-1",
  orgId: "org-1",
};

beforeEach(() => {
  vi.resetAllMocks();
  mockResolveInstantlyApiKey.mockResolvedValue({ key: "k", keySource: "platform" });
  mockUpdateCampaignStatus.mockResolvedValue({});
});

// ─── funnelStopsOnClick (pure) ────────────────────────────────────────────────

describe("funnelStopsOnClick", () => {
  // The key encodes the chain the campaign runs, and its prefix is the leg that
  // opens it. `visit_*` means the conversion starts on the site, which is exactly
  // when a click means the prospect is already there.
  it.each([["visit_form"], ["visit_signup"]])("stops on a visit-first funnel (%s)", (key) => {
    expect(funnelStopsOnClick(key)).toBe(true);
  });

  // This funnel's conversion starts with a REPLY — a click says nothing about
  // whether the sequence should continue.
  it("does NOT stop on reply_meeting", () => {
    expect(funnelStopsOnClick("reply_meeting")).toBe(false);
  });

  // campaign-service's own rule: a funnel is a fact, never a guess. Pausing a
  // live sequence on an unknown is the wrong direction to be wrong in.
  it.each([[null], [undefined], [""], ["unknown_funnel"]])(
    "does NOT stop on %p",
    (key) => {
      expect(funnelStopsOnClick(key as string | null | undefined)).toBe(false);
    },
  );
});

// ─── maybeStopOnClickForFunnel ────────────────────────────────────────────────

describe("maybeStopOnClickForFunnel", () => {
  it("pauses the Instantly campaign on a visit-first funnel", async () => {
    mockGetCampaignFunnelKey.mockResolvedValue("visit_signup");

    await maybeStopOnClickForFunnel(CAMPAIGN, "lead@x.com");

    expect(mockGetCampaignFunnelKey).toHaveBeenCalledWith("camp-1", "org-1");
    expect(mockUpdateCampaignStatus).toHaveBeenCalledWith("k", "inst-camp-1", "paused");
  });

  it("leaves a reply-first funnel running", async () => {
    mockGetCampaignFunnelKey.mockResolvedValue("reply_meeting");

    await maybeStopOnClickForFunnel(CAMPAIGN, "lead@x.com");

    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
  });

  it("leaves an unknown funnel running", async () => {
    mockGetCampaignFunnelKey.mockResolvedValue(null);

    await maybeStopOnClickForFunnel(CAMPAIGN, "lead@x.com");

    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
  });

  // The funnel is a property of the CAMPAIGN. Reading it anywhere else answers a
  // different question — two campaigns of one brand can run different funnels.
  it("reads the caller campaign id, never the Instantly one", async () => {
    mockGetCampaignFunnelKey.mockResolvedValue("visit_form");

    await maybeStopOnClickForFunnel(CAMPAIGN, "lead@x.com");

    const [campaignId] = mockGetCampaignFunnelKey.mock.calls[0]!;
    expect(campaignId).toBe("camp-1");
    expect(campaignId).not.toBe("inst-camp-1");
  });

  // A platform send belongs to no caller campaign, so it runs no funnel.
  it("no-ops on a platform send (campaignId null) without calling out", async () => {
    await maybeStopOnClickForFunnel({ ...CAMPAIGN, campaignId: null }, "lead@x.com");

    expect(mockGetCampaignFunnelKey).not.toHaveBeenCalled();
    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
  });

  it("no-ops without an org", async () => {
    await maybeStopOnClickForFunnel({ ...CAMPAIGN, orgId: null }, "lead@x.com");

    expect(mockGetCampaignFunnelKey).not.toHaveBeenCalled();
  });

  // Never throw into promoteEvent — a 5xx there makes Instantly auto-pause the
  // webhook, which would cost far more than a missed pause.
  it("swallows a campaign-service failure and lets the sequence continue", async () => {
    mockGetCampaignFunnelKey.mockRejectedValue(new Error("campaign-service down"));

    await expect(
      maybeStopOnClickForFunnel(CAMPAIGN, "lead@x.com"),
    ).resolves.toBeUndefined();
    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
  });

  it("swallows an Instantly pause failure", async () => {
    mockGetCampaignFunnelKey.mockResolvedValue("visit_signup");
    mockUpdateCampaignStatus.mockRejectedValue(new Error("instantly 500"));

    await expect(
      maybeStopOnClickForFunnel(CAMPAIGN, "lead@x.com"),
    ).resolves.toBeUndefined();
  });
});
