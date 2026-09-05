import { describe, it, expect, vi, beforeEach } from "vitest";

const mockScheduleFollowupByEmail = vi.fn();

vi.mock("../../src/lib/lead-client", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  scheduleFollowupByEmail: (...args: unknown[]) => mockScheduleFollowupByEmail(...args),
}));

const { maybeEnqueueFollowupOnInterest } = await import(
  "../../src/lib/enqueue-followup-on-interest"
);
const { isSalesInterestQualification } = await import(
  "../../src/lib/trigger-sales-interest-campaign"
);
const { REPLY_KINDS, POSITIVE_REPLY_KINDS } = await import("../../src/lib/reply-kind");

const CAMPAIGN = {
  instantlyCampaignId: "inst-camp-1",
  campaignId: "camp-1",
  orgId: "org-1",
};

const NOW = new Date("2026-09-05T10:00:00.000Z");

const RESULT = {
  followup: {
    id: "row-1",
    leadId: "lead-1",
    campaignId: "camp-1",
    dueAt: NOW.toISOString(),
    claimedAt: null,
    followupCount: 0,
    lastActionAt: null,
    stoppedReason: null,
  },
  leadId: "lead-1",
  email: "prospect@example.com",
};

beforeEach(() => {
  vi.resetAllMocks();
  mockScheduleFollowupByEmail.mockResolvedValue(RESULT);
});

describe("maybeEnqueueFollowupOnInterest", () => {
  it("enters the person into the queue, owed an answer NOW", async () => {
    await maybeEnqueueFollowupOnInterest(
      CAMPAIGN,
      "prospect@example.com",
      "lead_interested",
      NOW,
    );

    expect(mockScheduleFollowupByEmail).toHaveBeenCalledTimes(1);
    expect(mockScheduleFollowupByEmail).toHaveBeenCalledWith({
      orgId: "org-1",
      // The CALLER campaign — campaign-service's own row id, the scope the claim
      // will name. Never the Instantly campaign id.
      campaignId: "camp-1",
      email: "prospect@example.com",
      dueAt: NOW.toISOString(),
    });
  });

  // The gate is the sibling's gate, not a second hand-written list: a queue fed by
  // a wider set than the campaign that drains it holds rows nothing ever claims.
  it("fires on EXACTLY the kinds that trigger the campaign, for every reply kind", async () => {
    for (const kind of REPLY_KINDS) {
      mockScheduleFollowupByEmail.mockClear();
      await maybeEnqueueFollowupOnInterest(CAMPAIGN, "prospect@example.com", kind, NOW);
      expect(mockScheduleFollowupByEmail.mock.calls.length > 0).toBe(
        isSalesInterestQualification(kind),
      );
    }
  });

  // ⚠️ The divergence from the forward-to-the-agency-inbox set is deliberate:
  // "not me, but talk to X" is worth a human's eyes and is NOT this person opening
  // a conversation, so nobody is owed an answer on it.
  it("does NOT enqueue a referral, even though it forwards to the agency inbox", async () => {
    await maybeEnqueueFollowupOnInterest(CAMPAIGN, "prospect@example.com", "lead_referral", NOW);
    expect(mockScheduleFollowupByEmail).not.toHaveBeenCalled();
    expect(POSITIVE_REPLY_KINDS).toContain("lead_referral");
  });

  it("no-ops on a non-reply event", async () => {
    await maybeEnqueueFollowupOnInterest(CAMPAIGN, "prospect@example.com", "email_sent", NOW);
    expect(mockScheduleFollowupByEmail).not.toHaveBeenCalled();
  });

  // A platform send belongs to no caller campaign, so there is no campaign for the
  // debt to be owed on and no worker that would ever claim it.
  it("no-ops on a platform send (no caller campaign)", async () => {
    await maybeEnqueueFollowupOnInterest(
      { ...CAMPAIGN, campaignId: null },
      "prospect@example.com",
      "lead_interested",
      NOW,
    );
    expect(mockScheduleFollowupByEmail).not.toHaveBeenCalled();
  });

  it("no-ops when the send is org-less", async () => {
    await maybeEnqueueFollowupOnInterest(
      { ...CAMPAIGN, orgId: null },
      "prospect@example.com",
      "lead_interested",
      NOW,
    );
    expect(mockScheduleFollowupByEmail).not.toHaveBeenCalled();
  });

  // The qualification is the primary job. A failed enqueue must never change its
  // outcome — and must never be silent, or we believe a debt was recorded.
  it("swallows a refusal and WARNS with its reason", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    mockScheduleFollowupByEmail.mockRejectedValue(
      new Error("lead-service POST ... failed: 404 - {\"code\":\"lead_not_found\"}"),
    );

    await expect(
      maybeEnqueueFollowupOnInterest(CAMPAIGN, "prospect@example.com", "lead_interested", NOW),
    ).resolves.toBeUndefined();

    expect(warn).toHaveBeenCalledTimes(1);
    const message = String(warn.mock.calls[0]?.[0]);
    expect(message).toContain("followup-enqueue: FAILED");
    expect(message).toContain("lead_not_found");
    expect(message).toContain("prospect@example.com");
    warn.mockRestore();
  });
});
