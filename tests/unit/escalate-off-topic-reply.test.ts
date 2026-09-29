/**
 * A reply about something other than the offer is handed to a person.
 *
 * Prod 2026-09-28: jakub@marktize.com replied "can you explain?" on a
 * partnership thread. Filed `lead_neutral`, it reached no queue and no human.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";

const mockDbExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: { execute: (...a: unknown[]) => mockDbExecute(...a) },
}));

const mockHandThreadToHuman = vi.fn();
vi.mock("../../src/lib/escalate-reply", () => ({
  handThreadToHuman: (...a: unknown[]) => mockHandThreadToHuman(...a),
}));

const mockFetchLatestMirroredInbound = vi.fn();
vi.mock("../../src/lib/reply-opt-out", () => ({
  fetchLatestMirroredInbound: (...a: unknown[]) => mockFetchLatestMirroredInbound(...a),
}));

import {
  maybeEscalateOffTopicReply,
  offTopicQuestion,
  WORDS_UNAVAILABLE,
} from "../../src/lib/escalate-off-topic-reply";
import { isSalesInterestQualification } from "../../src/lib/trigger-sales-interest-campaign";
import {
  REPLY_KIND_CLASSIFICATION,
  isOffTopicReplyKind,
  isSequenceStoppingReplyKind,
  isDisqualifyingReplyKind,
  POSITIVE_REPLY_KINDS,
} from "../../src/lib/reply-kind";

function pgResult<T>(rows: T[]) {
  return { command: "SELECT", rowCount: rows.length, oid: null, fields: [], rows };
}

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
  it("hands Jakub's reply to a person, their words first, on the campaign's own identity", async () => {
    mockDbExecute.mockResolvedValueOnce(
      pgResult([{ text: "can you explain?\n\nOn Mon, Sep 28, 2026 at 4:05 PM Kevin Lourd wrote:\n> Totally understand" }]),
    );

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
    });
    expect(input.question).toContain("They wrote: can you explain?");
    expect(input.question).not.toContain("Totally understand");
  });

  it("reads the Instantly mirror on an Instantly-transport thread", async () => {
    mockFetchLatestMirroredInbound.mockResolvedValueOnce({ instantlyEmailId: "e1", text: "We are hiring?", subject: null });
    await maybeEscalateOffTopicReply(
      { ...JAKUB, instantlyCampaignId: "e3c917a5-83fe-460f-a5da-49125ad0fa0b" },
      "x@y.com",
      "lead_off_topic",
    );
    expect(mockFetchLatestMirroredInbound).toHaveBeenCalledWith("e3c917a5-83fe-460f-a5da-49125ad0fa0b");
    expect(mockHandThreadToHuman.mock.calls[0][0].question).toContain("They wrote: We are hiring?");
  });

  it("does NOTHING on a normal interested reply", async () => {
    for (const kind of ["lead_interested", "lead_info_requested", "lead_neutral", "reply_received"]) {
      await maybeEscalateOffTopicReply(JAKUB, "jakub@marktize.com", kind);
    }
    expect(mockHandThreadToHuman).not.toHaveBeenCalled();
    expect(mockDbExecute).not.toHaveBeenCalled();
  });

  it("never escalates on a made-up identity", async () => {
    await maybeEscalateOffTopicReply({ ...JAKUB, runId: null }, "jakub@marktize.com", "lead_off_topic");
    expect(mockHandThreadToHuman).not.toHaveBeenCalled();
  });

  it("is fail-soft: a failed hand-over never throws into promotion", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([]));
    mockHandThreadToHuman.mockRejectedValueOnce(new Error("postmark down"));
    await expect(
      maybeEscalateOffTopicReply(JAKUB, "jakub@marktize.com", "lead_off_topic"),
    ).resolves.toBeUndefined();
  });

  it("says so when their words cannot be read, rather than inventing them", () => {
    expect(offTopicQuestion(null)).toContain(WORDS_UNAVAILABLE);
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
    mockFetchLatestMirroredInbound.mockResolvedValueOnce({
      instantlyEmailId: "e1",
      text: "Hoi Joshua, danke für deine Anfrage. Solltet ihr daran interessiert sein, eure Bio-Flohsamenschalen über uns zu vertreiben, darfst du dich gerne direkt an sortimente@biopartner.ch wenden.",
      subject: null,
    });

    await maybeEscalateOffTopicReply(ELENA, "elena.staeheli@biopartner.ch", "lead_referral");

    const [input] = mockHandThreadToHuman.mock.calls[0];
    expect(input.question).toContain("point you at someone else");
    expect(input.question).toContain("sortimente@biopartner.ch");
    expect(input.stopReason).toContain("referred us to someone else");
    expect(input.brandId).toBe("f2408cfb-4f02-4910-acec-e61fc8edb9cf");
  });
});
