import { describe, it, expect, vi, beforeEach } from "vitest";

const mockGetCampaignTriggerScope = vi.fn();
const mockTriggerCampaignForStep = vi.fn();

vi.mock("../../src/lib/campaign-client", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  getCampaignTriggerScope: (...args: unknown[]) => mockGetCampaignTriggerScope(...args),
  triggerCampaignForStep: (...args: unknown[]) => mockTriggerCampaignForStep(...args),
}));

const {
  maybeTriggerSalesInterestCampaign,
  isSalesInterestQualification,
  SALES_INTEREST_STEP_KEY,
} = await import("../../src/lib/trigger-sales-interest-campaign");
const { POSITIVE_REPLY_KINDS, REPLY_KIND_CLASSIFICATION, REPLY_KINDS } = await import(
  "../../src/lib/reply-kind"
);

const CAMPAIGN = {
  instantlyCampaignId: "inst-camp-1",
  campaignId: "camp-1",
  orgId: "org-1",
};

const SCOPE = {
  brandId: "brand-1",
  offerId: "offer-1",
  funnelKey: "sales_meetings_from_conversation",
};

const EMPTY_OUTCOME = {
  funnelKey: SCOPE.funnelKey,
  step: SALES_INTEREST_STEP_KEY,
  legKeys: [],
  triggered: [],
  skipped: [],
};

beforeEach(() => {
  vi.resetAllMocks();
  mockGetCampaignTriggerScope.mockResolvedValue(SCOPE);
  mockTriggerCampaignForStep.mockResolvedValue(EMPTY_OUTCOME);
});

// ─── which replies count as a sales interest (pure) ───────────────────────────

describe("isSalesInterestQualification", () => {
  // The step is labelled "Sales interest": a buyer answers and a conversation
  // opens. That is exactly what the coarse map reports as `positive`, and it is
  // the same signal the fleet already prices as the brand's sales interest.
  it.each([["lead_interested"], ["lead_info_requested"], ["lead_meeting_requested"]])(
    "fires on %s",
    (kind) => {
      expect(isSalesInterestQualification(kind)).toBe(true);
    },
  );

  // ⚠️ The divergence from POSITIVE_REPLY_KINDS is the point. "Not me, but talk
  // to X" is worth a human's eyes (it forwards) and is NOT this person opening a
  // conversation — firing a funded meeting-booking campaign at someone who just
  // said they are the wrong person is the mistake the map exists to prevent.
  it("does NOT fire on lead_referral, even though it forwards to the agency inbox", () => {
    expect(isSalesInterestQualification("lead_referral")).toBe(false);
    expect(POSITIVE_REPLY_KINDS).toContain("lead_referral");
  });

  it("keys on the classification map rather than a second hand-written list", () => {
    for (const kind of REPLY_KINDS) {
      expect(isSalesInterestQualification(kind)).toBe(
        REPLY_KIND_CLASSIFICATION[kind] === "positive",
      );
    }
  });

  it.each([["lead_not_interested"], ["auto_reply_received"], ["email_sent"], ["nonsense"]])(
    "does not fire on %s",
    (eventType) => {
      expect(isSalesInterestQualification(eventType)).toBe(false);
    },
  );
});

// ─── the ask ─────────────────────────────────────────────────────────────────

describe("maybeTriggerSalesInterestCampaign", () => {
  it("asks campaign-service to run the campaign for the leg out of the step", async () => {
    await maybeTriggerSalesInterestCampaign(CAMPAIGN, "lead@x.com", "lead_interested");

    expect(mockGetCampaignTriggerScope).toHaveBeenCalledWith("camp-1", "org-1");
    expect(mockTriggerCampaignForStep).toHaveBeenCalledWith({
      orgId: "org-1",
      brandId: "brand-1",
      offerId: "offer-1",
      funnelKey: "sales_meetings_from_conversation",
      // features-service's step key, carried verbatim — never parsed into legs here.
      step: "conversation",
    });
  });

  it("carries features-service's published step key", () => {
    expect(SALES_INTEREST_STEP_KEY).toBe("conversation");
  });

  it("asks nothing on a reply that is not a sales interest", async () => {
    await maybeTriggerSalesInterestCampaign(CAMPAIGN, "lead@x.com", "lead_not_interested");
    expect(mockGetCampaignTriggerScope).not.toHaveBeenCalled();
    expect(mockTriggerCampaignForStep).not.toHaveBeenCalled();
  });

  // A platform send belongs to no caller campaign, so it is on no funnel and no
  // offer — there is nothing to name.
  it("asks nothing for a platform send (no caller campaign)", async () => {
    await maybeTriggerSalesInterestCampaign(
      { ...CAMPAIGN, campaignId: null },
      "lead@x.com",
      "lead_interested",
    );
    expect(mockGetCampaignTriggerScope).not.toHaveBeenCalled();
    expect(mockTriggerCampaignForStep).not.toHaveBeenCalled();
  });

  it("asks nothing when the campaign is not org-scoped", async () => {
    await maybeTriggerSalesInterestCampaign(
      { ...CAMPAIGN, orgId: null },
      "lead@x.com",
      "lead_interested",
    );
    expect(mockGetCampaignTriggerScope).not.toHaveBeenCalled();
  });

  // Nothing is inferred: a campaign stating no offer (or no funnel, or no brand)
  // cannot have a leg resolved for it, so naming a scope would be a guess.
  it.each([
    ["brandId", { ...SCOPE, brandId: null }],
    ["offerId", { ...SCOPE, offerId: null }],
    ["funnelKey", { ...SCOPE, funnelKey: null }],
    ["campaign", null],
  ])("asks nothing when the scope states no %s", async (_label, scope) => {
    mockGetCampaignTriggerScope.mockResolvedValue(scope);
    await maybeTriggerSalesInterestCampaign(CAMPAIGN, "lead@x.com", "lead_interested");
    expect(mockTriggerCampaignForStep).not.toHaveBeenCalled();
  });
});

// ─── a brand with no responsible campaign, and a trigger that fails ──────────

describe("maybeTriggerSalesInterestCampaign — quiet answers and loud failures", () => {
  // Most brands buy one leg of one funnel, so "nobody bought the leg out of this
  // step" is the COMMON answer and it is an ordinary 200, not an error.
  it("treats an empty leg set as the ordinary answer it is", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    await expect(
      maybeTriggerSalesInterestCampaign(CAMPAIGN, "lead@x.com", "lead_interested"),
    ).resolves.toBeUndefined();
    expect(warn).not.toHaveBeenCalled();
    warn.mockRestore();
  });

  it("treats a NAMED skip as an ordinary answer, not a failure", async () => {
    mockTriggerCampaignForStep.mockResolvedValue({
      ...EMPTY_OUTCOME,
      legKeys: ["conversation_to_meeting_booked"],
      skipped: [
        {
          campaignId: "camp-2",
          legKey: "conversation_to_meeting_booked",
          reason: "unfunded",
          detail: "the customer funds nothing for it",
        },
      ],
    });
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    await maybeTriggerSalesInterestCampaign(CAMPAIGN, "lead@x.com", "lead_interested");
    expect(warn).not.toHaveBeenCalled();
    warn.mockRestore();
  });

  // Qualifying the reply is the primary job. A trigger failure must never change
  // its outcome — and must never throw into the webhook (Instantly auto-pauses a
  // webhook that keeps failing).
  it("never throws when campaign-service fails, and says so loudly", async () => {
    mockTriggerCampaignForStep.mockRejectedValue(new Error("502 catalogue unreadable"));
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});

    await expect(
      maybeTriggerSalesInterestCampaign(CAMPAIGN, "lead@x.com", "lead_interested"),
    ).resolves.toBeUndefined();

    expect(warn).toHaveBeenCalledTimes(1);
    expect(warn.mock.calls[0][0]).toContain("502 catalogue unreadable");
    warn.mockRestore();
  });

  it("never throws when the scope read fails", async () => {
    mockGetCampaignTriggerScope.mockRejectedValue(new Error("campaign-service down"));
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});

    await expect(
      maybeTriggerSalesInterestCampaign(CAMPAIGN, "lead@x.com", "lead_interested"),
    ).resolves.toBeUndefined();

    expect(warn).toHaveBeenCalledTimes(1);
    expect(mockTriggerCampaignForStep).not.toHaveBeenCalled();
    warn.mockRestore();
  });
});
