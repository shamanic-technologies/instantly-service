import { beforeEach, describe, expect, it, vi } from "vitest";

const mockAdopted = vi.fn();
vi.mock("../../src/db", () => ({
  db: {
    select: () => ({ from: () => ({ where: () => ({ limit: () => mockAdopted() }) }) }),
  },
}));
vi.mock("../../src/db/schema", () => ({
  instantlyEvents: { eventType: "event_type", campaignId: "campaign_id", leadEmail: "lead_email", timestamp: "timestamp", inferred: "inferred" },
}));

const mockFetchInbound = vi.fn();
vi.mock("../../src/lib/reply-opt-out", () => ({
  fetchLatestMirroredInbound: (...a: unknown[]) => mockFetchInbound(...a),
}));
const mockMirror = vi.fn();
vi.mock("../../src/lib/mirror-emails", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  maybeMirrorCampaignEmails: (...a: unknown[]) => mockMirror(...a),
}));
const mockQualify = vi.fn();
vi.mock("../../src/lib/self-send/qualify-reply", () => ({
  qualifyReply: (...a: unknown[]) => mockQualify(...a),
}));

import { REPLY_KIND_CLASSIFICATION } from "../../src/lib/reply-kind";
import {
  REFINED_INTEREST_KINDS,
  isRefinableInterest,
  refineInstantlyInterest,
  refinedKind,
} from "../../src/lib/refine-interest-kind";

const campaign = { instantlyCampaignId: "20c5d046-4696-4ec5-986d-95879748bdd5", orgId: "org-1", userId: "u-1" };
const TS = new Date("2026-10-01T17:25:03.334Z");
const interested = { eventType: "lead_interested", source: "webhook", leadEmail: "drjoe@chirohealthspa.com", timestamp: TS };
const DR_JOE = { instantlyEmailId: "em-1", text: "Send me more information on how it works.", subject: "Re: Dinner with Docs" };

describe("refinedKind (pure)", () => {
  it("takes a finer POSITIVE kind over Instantly's plain interest", () => {
    expect(refinedKind("lead_interested", "lead_info_requested")).toBe("lead_info_requested");
    expect(refinedKind("lead_interested", "lead_meeting_requested")).toBe("lead_meeting_requested");
  });
  it("never lets the classifier overrule Instantly outside the positive kinds", () => {
    for (const k of ["lead_not_interested", "lead_neutral", "lead_opt_out_requested", "lead_referral", "lead_off_topic", null]) {
      expect(refinedKind("lead_interested", k)).toBe("lead_interested");
    }
  });
  it("every refined kind is still a POSITIVE reply (no stat moves)", () => {
    for (const k of REFINED_INTEREST_KINDS) expect(REPLY_KIND_CLASSIFICATION[k]).toBe("positive");
    expect(REPLY_KIND_CLASSIFICATION.lead_interested).toBe("positive");
  });
});

describe("isRefinableInterest (pure)", () => {
  it("only Instantly's own verdicts on an Instantly-held sequence", () => {
    expect(isRefinableInterest(interested, campaign.instantlyCampaignId)).toBe(true);
    expect(isRefinableInterest({ ...interested, source: "poll_leads" }, campaign.instantlyCampaignId)).toBe(true);
    expect(isRefinableInterest({ ...interested, source: "manual" }, campaign.instantlyCampaignId)).toBe(false);
    expect(isRefinableInterest({ ...interested, source: "self_send" }, campaign.instantlyCampaignId)).toBe(false);
    expect(isRefinableInterest({ ...interested, inferred: true }, campaign.instantlyCampaignId)).toBe(false);
    expect(isRefinableInterest({ ...interested, eventType: "lead_not_interested" }, campaign.instantlyCampaignId)).toBe(false);
    expect(isRefinableInterest(interested, "self:abc")).toBe(false);
    expect(isRefinableInterest({ ...interested, leadEmail: null }, campaign.instantlyCampaignId)).toBe(false);
  });
});

describe("refineInstantlyInterest", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockAdopted.mockResolvedValue([]);
    mockFetchInbound.mockResolvedValue(DR_JOE);
    mockMirror.mockResolvedValue(undefined);
  });

  it("Dr. Joe: Instantly's 'interested' is recorded as the info request it is", async () => {
    mockQualify.mockResolvedValue("lead_info_requested");
    const out = await refineInstantlyInterest(campaign, interested);
    expect(out.eventType).toBe("lead_info_requested");
    expect(out.reading).toEqual({ qualification: "lead_info_requested", inbound: DR_JOE });
    expect(mockQualify).toHaveBeenCalledWith(DR_JOE.text, expect.objectContaining({ source: "interest_refinement", subject: DR_JOE.subject }));
  });

  it("a redelivery adopts the kind already recorded and asks no model", async () => {
    mockAdopted.mockResolvedValue([{ eventType: "lead_info_requested" }]);
    const out = await refineInstantlyInterest(campaign, interested);
    expect(out.eventType).toBe("lead_info_requested");
    expect(mockQualify).not.toHaveBeenCalled();
  });

  it("a re-poll of an interest recorded BEFORE this existed keeps lead_interested (no second event, no second answer)", async () => {
    mockAdopted.mockResolvedValue([{ eventType: "lead_interested" }]);
    const out = await refineInstantlyInterest(campaign, { ...interested, source: "poll_leads" });
    expect(out.eventType).toBe("lead_interested");
    expect(mockQualify).not.toHaveBeenCalled();
  });

  it("mirrors first when the reply is not readable yet", async () => {
    mockFetchInbound.mockResolvedValueOnce(null).mockResolvedValueOnce(DR_JOE);
    mockQualify.mockResolvedValue("lead_meeting_requested");
    const out = await refineInstantlyInterest(campaign, interested);
    expect(mockMirror).toHaveBeenCalledTimes(1);
    expect(out.eventType).toBe("lead_meeting_requested");
  });

  it("keeps Instantly's verdict when nothing can be read, or the read fails", async () => {
    mockFetchInbound.mockResolvedValue(null);
    expect((await refineInstantlyInterest(campaign, interested)).eventType).toBe("lead_interested");
    mockFetchInbound.mockResolvedValue(DR_JOE);
    mockQualify.mockRejectedValue(new Error("chat-service 502"));
    await expect(refineInstantlyInterest(campaign, interested)).resolves.toEqual({ eventType: "lead_interested", reading: null });
  });

  it("does nothing on any other event", async () => {
    const out = await refineInstantlyInterest(campaign, { ...interested, eventType: "reply_received" });
    expect(out).toEqual({ eventType: "reply_received", reading: null });
    expect(mockAdopted).not.toHaveBeenCalled();
  });
});
