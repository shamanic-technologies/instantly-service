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
const { funnelStopsOnClick, toFunnelKey, isUnrecognisedFunnelKey, SALES_FUNNEL_KEYS, LEGACY_FUNNEL_KEYS } =
  await import("../../src/lib/campaign-client");

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
  // The three funnels whose conversion happens on the website: a click means the prospect is
  // already there, so more cold email only distracts. Canonical spelling — what campaign-service
  // stores TODAY, and what the retired `visit_` prefix test matched none of.
  it.each([["form_magnet"], ["website_purchases"], ["sales_meetings_from_website"]])(
    "stops on a visit-led funnel (%s)",
    (key) => {
      expect(funnelStopsOnClick(key)).toBe(true);
    },
  );

  // Pre-rename spellings still sit on campaign rows written before campaign-service migration
  // 0043. Nothing may regress for them.
  it.each([["visit_form"], ["visit_signup"], ["visit_meeting"]])(
    "stops on the pre-rename spelling (%s)",
    (key) => {
      expect(funnelStopsOnClick(key)).toBe(true);
    },
  );

  // This funnel's conversion starts with a REPLY — a click says nothing about whether the sequence
  // should continue. Both spellings.
  it.each([["sales_meetings_from_conversation"], ["reply_meeting"]])(
    "does NOT stop on a reply-led funnel (%s)",
    (key) => {
      expect(funnelStopsOnClick(key)).toBe(false);
    },
  );

  // campaign-service's own rule: a funnel is a fact, never a guess. Pausing a live sequence on an
  // unknown is the wrong direction to be wrong in.
  it.each([[null], [undefined], [""], ["unknown_funnel"]])("does NOT stop on %p", (key) => {
    expect(funnelStopsOnClick(key as string | null | undefined)).toBe(false);
  });
});

// ─── Vocabulary regression cover ──────────────────────────────────────────────

/**
 * This is the guard the outage needed and did not have.
 *
 * `SALES_FUNNEL_KEYS` + `LEGACY_FUNNEL_KEYS` here MIRROR campaign-service's
 * `src/lib/sales-funnel-vocabulary.ts`. If that vocabulary changes and this copy does not, the
 * fleet goes silently blind again — so pin both catalogues literally. A failure here means "go read
 * campaign-service's vocabulary and decide what a click on the new funnel means", not "update the
 * expected array".
 */
describe("sales-funnel vocabulary (mirrors campaign-service)", () => {
  it("recognises exactly the four funnels campaign-service stores", () => {
    expect([...SALES_FUNNEL_KEYS].sort()).toEqual(
      [
        "form_magnet",
        "sales_meetings_from_conversation",
        "sales_meetings_from_website",
        "website_purchases",
      ].sort(),
    );
  });

  it("accepts exactly the four pre-rename spellings, mapped to their canonical funnel", () => {
    expect(LEGACY_FUNNEL_KEYS).toEqual({
      reply_meeting: "sales_meetings_from_conversation",
      visit_meeting: "sales_meetings_from_website",
      visit_signup: "website_purchases",
      visit_form: "form_magnet",
    });
  });

  // Every canonical key must be a fact this service already holds an opinion about — the gate is an
  // exhaustive Record, so a fifth funnel is a compile error rather than a silent `false`. This
  // asserts the runtime half: no canonical key falls through to "unrecognised".
  it("holds a stop/no-stop opinion for every canonical funnel", () => {
    for (const key of SALES_FUNNEL_KEYS) {
      expect(toFunnelKey(key)).toBe(key);
      expect(isUnrecognisedFunnelKey(key)).toBe(false);
      expect(typeof funnelStopsOnClick(key)).toBe("boolean");
    }
  });

  // The rename signal itself: a token from neither catalogue is flagged, which is what the caller
  // logs. An absent funnel is NOT flagged — nobody renamed anything, the campaign just stated none.
  it("flags an unknown token, and only an unknown token", () => {
    expect(isUnrecognisedFunnelKey("visit_purchase_v2")).toBe(true);
    expect(isUnrecognisedFunnelKey(null)).toBe(false);
    expect(isUnrecognisedFunnelKey(undefined)).toBe(false);
    expect(isUnrecognisedFunnelKey("")).toBe(false);
    expect(isUnrecognisedFunnelKey("form_magnet")).toBe(false);
    expect(isUnrecognisedFunnelKey("visit_form")).toBe(false);
  });
});

// ─── maybeStopOnClickForFunnel ────────────────────────────────────────────────

describe("maybeStopOnClickForFunnel", () => {
  it("pauses the Instantly campaign on a visit-first funnel", async () => {
    mockGetCampaignFunnelKey.mockResolvedValue("website_purchases");

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

  // A renamed vocabulary is the exact way this side effect went silent for weeks with nothing in
  // the logs to see. An unknown token still does not stop (we never guess), but it must be LOUD.
  it("warns on an unrecognised funnel key, and still does not stop", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    mockGetCampaignFunnelKey.mockResolvedValue("visit_purchase_v2");

    await maybeStopOnClickForFunnel(CAMPAIGN, "lead@x.com");

    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
    expect(warn).toHaveBeenCalledWith(expect.stringContaining("visit_purchase_v2"));
    warn.mockRestore();
  });

  // A campaign that stated no funnel is an ordinary absence, not a rename. Warning on it would
  // drown the signal that matters in the common case.
  it("does NOT warn when the campaign simply states no funnel", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    mockGetCampaignFunnelKey.mockResolvedValue(null);

    await maybeStopOnClickForFunnel(CAMPAIGN, "lead@x.com");

    expect(warn).not.toHaveBeenCalled();
    warn.mockRestore();
  });

  it("swallows an Instantly pause failure", async () => {
    mockGetCampaignFunnelKey.mockResolvedValue("website_purchases");
    mockUpdateCampaignStatus.mockRejectedValue(new Error("instantly 500"));

    await expect(
      maybeStopOnClickForFunnel(CAMPAIGN, "lead@x.com"),
    ).resolves.toBeUndefined();
  });
});
