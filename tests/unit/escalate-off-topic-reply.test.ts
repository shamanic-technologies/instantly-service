/**
 * A reply about something other than the offer is handed to a person.
 *
 * Prod 2026-09-28: jakub@marktize.com replied "can you explain?" on a
 * partnership thread. Filed `lead_neutral`, it reached no queue and no human.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";


const mockHandThreadToHuman = vi.fn();
vi.mock("../../src/lib/escalate-reply", () => ({
  handThreadToHuman: (...a: unknown[]) => mockHandThreadToHuman(...a),
}));

const mockScope = vi.fn();
const mockResponders = vi.fn();
vi.mock("../../src/lib/campaign-client", () => ({
  getCampaignTriggerScope: (...a: unknown[]) => mockScope(...a),
  findOngoingResponderCampaigns: (...a: unknown[]) => mockResponders(...a),
}));

import { maybeEscalateOffTopicReply } from "../../src/lib/escalate-off-topic-reply";
import { isSalesInterestQualification } from "../../src/lib/trigger-sales-interest-campaign";
import {
  REPLY_KIND_CLASSIFICATION,
  isOffTopicReplyKind,
  isSequenceStoppingReplyKind,
  isDisqualifyingReplyKind,
  POSITIVE_REPLY_KINDS,
} from "../../src/lib/reply-kind";

const JAKUB = {
  instantlyCampaignId: "self:61642f1a-7171-4d9f-be7b-ef3c96f1bbde",
  campaignId: "f7b1b610-4fa1-4b54-8fec-f7be124dc32b",
  orgId: "91e76989-71ba-420d-ba73-bb3961430aa7",
  userId: "cfe148ed-e3d8-40a2-8920-f8c040a81934",
  runId: "de46c4a6-3027-4934-9d19-8bbcf98e1af9",
  brandIds: ["75d7e3e8-6926-4f85-a557-976895400666"],
};

beforeEach(() => {
  vi.resetAllMocks();
  mockHandThreadToHuman.mockResolvedValue({
    instantlyCampaignId: JAKUB.instantlyCampaignId,
    leadEmail: "jakub@marktize.com",
    threadMessages: 4,
    followupsStopped: true,
  });
  mockScope.mockResolvedValue({ brandId: JAKUB.brandIds[0], offerId: "offer-1" });
  mockResponders.mockResolvedValue([]);
});

describe("the lead_off_topic reply kind", () => {
  it("reports neutral, is NOT sales interest, and is not forwarded as a positive", () => {
    expect(REPLY_KIND_CLASSIFICATION.lead_off_topic).toBe("neutral");
    expect(isSalesInterestQualification("lead_off_topic")).toBe(false);
    expect((POSITIVE_REPLY_KINDS as readonly string[]).includes("lead_off_topic")).toBe(false);
    expect(isOffTopicReplyKind("lead_off_topic")).toBe(true);
    expect(isOffTopicReplyKind("lead_interested")).toBe(false);
  });

  it("is a human reply (stops the sequence) and disqualifies nobody", () => {
    expect(isSequenceStoppingReplyKind("lead_off_topic")).toBe(true);
    expect(isDisqualifyingReplyKind("lead_off_topic")).toBe(false);
  });
});

describe("maybeEscalateOffTopicReply", () => {
  // REPLY-NOTIFS-1013: no AI answers bought = the client reads it in the
  // Unibox; nobody is emailed, staff never.
  it("stops Jakub's ladder and emails NOBODY when the brand pays for no AI answers", async () => {
    await maybeEscalateOffTopicReply(JAKUB, "jakub@marktize.com", "lead_off_topic");

    expect(mockHandThreadToHuman).toHaveBeenCalledTimes(1);
    const [input] = mockHandThreadToHuman.mock.calls[0];
    expect(input).toMatchObject({
      instantlyCampaignId: JAKUB.instantlyCampaignId,
      campaignId: JAKUB.campaignId,
      leadEmail: "jakub@marktize.com",
      brandId: JAKUB.brandIds[0],
      orgId: JAKUB.orgId,
      userId: JAKUB.userId,
      runId: JAKUB.runId,
      notifyTeam: null,
    });
    expect(mockScope).toHaveBeenCalledWith(JAKUB.campaignId, JAKUB.orgId);
  });

  it("tells the brand's team when an AI responder runs on the offer", async () => {
    mockResponders.mockResolvedValueOnce([{ id: "responder-1" }]);
    await maybeEscalateOffTopicReply(JAKUB, "jakub@marktize.com", "lead_off_topic");
    expect(mockHandThreadToHuman.mock.calls[0][0].notifyTeam).toBe("off_topic");
  });

  it("emails nobody (and still stops the ladder) when the responder read fails", async () => {
    mockScope.mockRejectedValueOnce(new Error("campaign-service down"));
    await maybeEscalateOffTopicReply(JAKUB, "jakub@marktize.com", "lead_off_topic");
    expect(mockHandThreadToHuman.mock.calls[0][0].notifyTeam).toBeNull();
  });

  it("does NOTHING on a normal interested reply", async () => {
    for (const kind of ["lead_interested", "lead_info_requested", "lead_neutral", "reply_received"]) {
      await maybeEscalateOffTopicReply(JAKUB, "jakub@marktize.com", kind);
    }
    expect(mockHandThreadToHuman).not.toHaveBeenCalled();
    expect(mockScope).not.toHaveBeenCalled();
  });

  it("never escalates on a made-up identity", async () => {
    await maybeEscalateOffTopicReply({ ...JAKUB, runId: null }, "jakub@marktize.com", "lead_off_topic");
    expect(mockHandThreadToHuman).not.toHaveBeenCalled();
  });

  it("is fail-soft: a failed hand-over never throws into promotion", async () => {
    mockHandThreadToHuman.mockRejectedValueOnce(new Error("postmark down"));
    await expect(
      maybeEscalateOffTopicReply(JAKUB, "jakub@marktize.com", "lead_off_topic"),
    ).resolves.toBeUndefined();
  });

});

describe("a referral is handed to a person too", () => {
  const ELENA = {
    instantlyCampaignId: "fbde0b33-337d-4e83-8305-d9acf63da7e0",
    campaignId: "38ba8069-3d50-4ae7-b37a-54409071e260",
    orgId: "8bfec2f4-6184-40b7-b1aa-c8933d506a87",
    userId: "u-1",
    runId: "r-1",
    brandIds: ["f2408cfb-4f02-4910-acec-e61fc8edb9cf"],
  };

  it("escalates Elena's referral with its own reason, and forwards nothing as a positive", async () => {
    mockScope.mockResolvedValueOnce({ brandId: ELENA.brandIds[0], offerId: "offer-2" });
    mockResponders.mockResolvedValueOnce([{ id: "responder-2" }]);

    await maybeEscalateOffTopicReply(ELENA, "elena.staeheli@biopartner.ch", "lead_referral");

    const [input] = mockHandThreadToHuman.mock.calls[0];
    expect(input.notifyTeam).toBe("referral");
    expect(input.stopReason).toContain("referred us to someone else");
    expect(input.brandId).toBe("f2408cfb-4f02-4910-acec-e61fc8edb9cf");
  });
});
