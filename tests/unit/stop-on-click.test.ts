import { describe, it, expect, vi, beforeEach } from "vitest";

const mockGetCampaignLeg = vi.fn();
const mockGetChannelCatalogue = vi.fn();
const mockResolveInstantlyApiKey = vi.fn();
const mockUpdateCampaignStatus = vi.fn();

vi.mock("../../src/lib/campaign-client", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  getCampaignLeg: (...args: unknown[]) => mockGetCampaignLeg(...args),
}));

vi.mock("../../src/lib/leg-catalogue", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  getChannelCatalogue: (...args: unknown[]) => mockGetChannelCatalogue(...args),
}));

vi.mock("../../src/lib/key-client", () => ({
  resolveInstantlyApiKey: (...args: unknown[]) => mockResolveInstantlyApiKey(...args),
}));

vi.mock("../../src/lib/instantly-client", () => ({
  updateCampaignStatus: (...args: unknown[]) => mockUpdateCampaignStatus(...args),
}));

const { maybeStopOnClickForLeg } = await import("../../src/lib/stop-on-click");
const { legArrivalStep, WEBSITE_VISIT_STEP_KEY } = await import("../../src/lib/leg-catalogue");

/** The catalogue as features-service served it in prod (2026-09-26), trimmed to the legs. */
const CATALOGUE = [
  {
    slug: "sales-cold-email-outreach",
    stepTransitions: [
      { legKey: "start_to_conversation", from: null, to: { key: "conversation" } },
      { legKey: "start_to_website_visit", from: null, to: { key: "website_visit" } },
    ],
  },
  {
    slug: "pr-cold-email-outreach",
    stepTransitions: [{ legKey: "start_to_website_visit", from: null, to: { key: "website_visit" } }],
  },
  {
    slug: "ai-meeting-booking",
    stepTransitions: [
      { legKey: "conversation_to_meeting_booked", from: { key: "conversation" }, to: { key: "meeting_booked" } },
    ],
  },
];

const CAMPAIGN = {
  instantlyCampaignId: "inst-camp-1",
  campaignId: "camp-1",
  orgId: "org-1",
  userId: null,
  runId: null,
};

const VISIT_LEG = { legKey: "start_to_website_visit", featureSlug: "sales-cold-email-outreach" };
const REPLY_LEG = { legKey: "start_to_conversation", featureSlug: "sales-cold-email-outreach" };

beforeEach(() => {
  vi.resetAllMocks();
  mockGetChannelCatalogue.mockResolvedValue(CATALOGUE);
  mockResolveInstantlyApiKey.mockResolvedValue({ key: "k", keySource: "platform" });
  mockUpdateCampaignStatus.mockResolvedValue({});
});

// ─── legArrivalStep (pure) ────────────────────────────────────────────────────

describe("legArrivalStep", () => {
  it("reads the arrival step of the leg on the campaign's own channel", () => {
    expect(legArrivalStep(CATALOGUE, "sales-cold-email-outreach", "start_to_website_visit")).toBe(WEBSITE_VISIT_STEP_KEY);
    expect(legArrivalStep(CATALOGUE, "sales-cold-email-outreach", "start_to_conversation")).toBe("conversation");
  });

  // A channel the catalogue does not list (or that is silent about the leg) falls back to every
  // channel stating it — but only when they AGREE on where the leg lands.
  it("falls back to the other channels when they all agree", () => {
    expect(legArrivalStep(CATALOGUE, "feedback-request-cold-email-outreach", "start_to_website_visit")).toBe("website_visit");
    expect(legArrivalStep(CATALOGUE, null, "start_to_conversation")).toBe("conversation");
  });

  it("refuses to pick when channels disagree about the same leg", () => {
    const split = [
      { slug: "a", stepTransitions: [{ legKey: "x", from: null, to: { key: "website_visit" } }] },
      { slug: "b", stepTransitions: [{ legKey: "x", from: null, to: { key: "conversation" } }] },
    ];
    expect(legArrivalStep(split, "c", "x")).toBeNull();
  });

  it("is null for a leg the catalogue does not know", () => {
    expect(legArrivalStep(CATALOGUE, "sales-cold-email-outreach", "start_to_signup_v2")).toBeNull();
  });
});

// ─── maybeStopOnClickForLeg ───────────────────────────────────────────────────

describe("maybeStopOnClickForLeg", () => {
  it("pauses the Instantly campaign when the leg lands on a website visit", async () => {
    mockGetCampaignLeg.mockResolvedValue(VISIT_LEG);

    await maybeStopOnClickForLeg(CAMPAIGN, "lead@x.com");

    expect(mockGetCampaignLeg).toHaveBeenCalledWith("camp-1", "org-1");
    expect(mockUpdateCampaignStatus).toHaveBeenCalledWith("k", "inst-camp-1", "paused");
  });

  it("leaves a leg landing on a conversation running", async () => {
    mockGetCampaignLeg.mockResolvedValue(REPLY_LEG);

    await maybeStopOnClickForLeg(CAMPAIGN, "lead@x.com");

    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
  });

  it("leaves a campaign bought for no leg running, without reading the catalogue or warning", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    mockGetCampaignLeg.mockResolvedValue({ legKey: null, featureSlug: "sales-cold-email-outreach" });

    await maybeStopOnClickForLeg(CAMPAIGN, "lead@x.com");

    expect(mockGetChannelCatalogue).not.toHaveBeenCalled();
    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
    expect(warn).not.toHaveBeenCalled();
    warn.mockRestore();
  });

  it("leaves an absent campaign running", async () => {
    mockGetCampaignLeg.mockResolvedValue(null);

    await maybeStopOnClickForLeg(CAMPAIGN, "lead@x.com");

    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
  });

  // The leg is a property of the CAMPAIGN — two campaigns of one brand can be bought for
  // different legs.
  it("reads the caller campaign id, never the Instantly one", async () => {
    mockGetCampaignLeg.mockResolvedValue(VISIT_LEG);

    await maybeStopOnClickForLeg(CAMPAIGN, "lead@x.com");

    const [campaignId] = mockGetCampaignLeg.mock.calls[0]!;
    expect(campaignId).toBe("camp-1");
  });

  it("no-ops on a platform send (campaignId null) without calling out", async () => {
    await maybeStopOnClickForLeg({ ...CAMPAIGN, campaignId: null }, "lead@x.com");

    expect(mockGetCampaignLeg).not.toHaveBeenCalled();
    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
  });

  it("no-ops without an org", async () => {
    await maybeStopOnClickForLeg({ ...CAMPAIGN, orgId: null }, "lead@x.com");

    expect(mockGetCampaignLeg).not.toHaveBeenCalled();
  });

  // Never throw into promoteEvent — a 5xx there makes Instantly auto-pause the webhook.
  it("swallows a campaign-service failure and lets the sequence continue", async () => {
    mockGetCampaignLeg.mockRejectedValue(new Error("campaign-service down"));

    await expect(maybeStopOnClickForLeg(CAMPAIGN, "lead@x.com")).resolves.toBeUndefined();
    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
  });

  // No fallback when the catalogue cannot be read: the sequence continues, loudly.
  it("swallows a leg-catalogue failure without pausing", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    mockGetCampaignLeg.mockResolvedValue(VISIT_LEG);
    mockGetChannelCatalogue.mockRejectedValue(new Error("features-service down"));

    await expect(maybeStopOnClickForLeg(CAMPAIGN, "lead@x.com")).resolves.toBeUndefined();
    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
    expect(warn).toHaveBeenCalledWith(expect.stringContaining("features-service down"));
    warn.mockRestore();
  });

  // A renamed leg vocabulary must be LOUD — an unknown leg still does not stop (we never guess).
  it("warns on a leg the catalogue does not know, and does not stop", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    mockGetCampaignLeg.mockResolvedValue({ legKey: "start_to_signup_v2", featureSlug: "sales-cold-email-outreach" });

    await maybeStopOnClickForLeg(CAMPAIGN, "lead@x.com");

    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
    expect(warn).toHaveBeenCalledWith(expect.stringContaining("start_to_signup_v2"));
    warn.mockRestore();
  });

  it("swallows an Instantly pause failure", async () => {
    mockGetCampaignLeg.mockResolvedValue(VISIT_LEG);
    mockUpdateCampaignStatus.mockRejectedValue(new Error("instantly 500"));

    await expect(maybeStopOnClickForLeg(CAMPAIGN, "lead@x.com")).resolves.toBeUndefined();
  });
});
