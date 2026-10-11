import { describe, it, expect, vi, beforeEach } from "vitest";

const mockExecute = vi.fn();
const mockResponderRuns = vi.fn();
const mockForward = vi.fn();
const mockReplyText = vi.fn();

vi.mock("../../src/db", () => ({ db: { execute: (...a: unknown[]) => mockExecute(...a) } }));
vi.mock("../../src/lib/ask-client-to-answer", () => ({
  responderRuns: (...a: unknown[]) => mockResponderRuns(...a),
}));
vi.mock("../../src/lib/forward-positive-reply", () => ({
  sendThreadForward: (...a: unknown[]) => mockForward(...a),
}));
const mockHandThread = vi.fn();
vi.mock("../../src/lib/hand-followups-to-human", () => ({
  handThreadFollowupsToHuman: (...a: unknown[]) => mockHandThread(...a),
}));
vi.mock("../../src/lib/escalate-off-topic-reply", () => ({
  fetchLatestReplyText: (...a: unknown[]) => mockReplyText(...a),
  WORDS_UNAVAILABLE: "(unavailable)",
}));

import { maybeAlertUnansweredInfoRequest } from "../../src/lib/alert-unanswered-info-request";

const campaign = {
  instantlyCampaignId: "ic-1",
  campaignId: "c-1",
  orgId: "org-1",
  userId: "u-1",
  runId: "r-1",
  brandIds: ["a179bbd9-8eed-4dba-9338-78125922b0c6"],
};

// The real driver shape: node-postgres returns { rows }.
const claimed = { rows: [{ instantly_campaign_id: "ic-1" }] };
const notClaimed = { rows: [] };

describe("info request nobody will answer → staff alert", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockExecute.mockResolvedValue(claimed);
    mockForward.mockResolvedValue(3);
    mockReplyText.mockResolvedValue("Please send me the specs. Do you have a video?\n\nOn Mon, Kevin wrote:\n> hi");
  });

  it("no responder on the offer (Shockwavecenters replay): alerts staff, client NOT copied, their words quoted", async () => {
    mockResponderRuns.mockResolvedValue(false);
    const out = await maybeAlertUnansweredInfoRequest(campaign, "steve@kaizen.com", "lead_info_requested");
    expect(out.sent).toBe(true);
    expect(mockForward).toHaveBeenCalledTimes(1);
    const [, lead, template] = mockForward.mock.calls[0];
    expect(lead).toBe("steve@kaizen.com");
    expect(template.eventType).toBe("info-request-unanswered");
    expect(template.copySalesRep).toBe(false);
    expect(template.metadata.question).toContain("No AI responder runs");
    expect(template.metadata.question).toContain("They wrote: Please send me the specs. Do you have a video?");
    expect(template.metadata.question).not.toContain("Kevin wrote");
    // Staff own it now: the thread is HANDED in lead-service, on the caller campaign.
    expect(mockHandThread).toHaveBeenCalledWith(
      expect.objectContaining({ orgId: "org-1", campaignId: "c-1", leadEmail: "steve@kaizen.com" }),
    );
  });

  it("an active responder: nothing sent, no claim taken (behaves as today)", async () => {
    mockResponderRuns.mockResolvedValue(true);
    const out = await maybeAlertUnansweredInfoRequest(campaign, "pam@buffalo.com", "lead_info_requested");
    expect(out).toEqual({ sent: false, reason: "responder_running" });
    expect(mockForward).not.toHaveBeenCalled();
    expect(mockExecute).not.toHaveBeenCalled();
    expect(mockHandThread).not.toHaveBeenCalled();
  });

  it("responder unreadable: still alerts (never silence), says why", async () => {
    mockResponderRuns.mockRejectedValue(new Error("campaign-service 503"));
    const out = await maybeAlertUnansweredInfoRequest(campaign, "pam@buffalo.com", "lead_info_requested");
    expect(out.sent).toBe(true);
    expect(mockForward.mock.calls[0][2].metadata.question).toContain("could not read whether an AI responder runs");
  });

  it("thread scope unknown: still alerts", async () => {
    mockResponderRuns.mockResolvedValue(null);
    const out = await maybeAlertUnansweredInfoRequest(campaign, "pam@buffalo.com", "lead_info_requested");
    expect(out.sent).toBe(true);
    expect(mockForward.mock.calls[0][2].metadata.question).toContain("could not tell which brand offer");
  });

  it("once per thread: an already-claimed thread sends nothing", async () => {
    mockResponderRuns.mockResolvedValue(false);
    mockExecute.mockResolvedValue(notClaimed);
    const out = await maybeAlertUnansweredInfoRequest(campaign, "pam@buffalo.com", "lead_info_requested");
    expect(out).toEqual({ sent: false, reason: "already_alerted" });
    expect(mockForward).not.toHaveBeenCalled();
  });

  it("send failure releases the claim so a later event retries", async () => {
    mockResponderRuns.mockResolvedValue(false);
    mockForward.mockRejectedValue(new Error("postmark down"));
    const out = await maybeAlertUnansweredInfoRequest(campaign, "pam@buffalo.com", "lead_info_requested");
    expect(out).toEqual({ sent: false, reason: "send_failed" });
    expect(mockExecute).toHaveBeenCalledTimes(2);
  });

  it("overdue mode (deadline sweep): never asks the responder status, says nobody answered", async () => {
    const out = await maybeAlertUnansweredInfoRequest(campaign, "pam@buffalo.com", "lead_info_requested", {
      overdueSince: "on Oct 5",
    });
    expect(out.sent).toBe(true);
    expect(mockResponderRuns).not.toHaveBeenCalled();
    expect(mockForward.mock.calls[0][2].metadata.question).toContain("asked a question on Oct 5 and nobody has answered it yet");
  });

  it("any other kind is a no-op", async () => {
    const out = await maybeAlertUnansweredInfoRequest(campaign, "pam@buffalo.com", "lead_interested");
    expect(out).toEqual({ sent: false, reason: "not_info_request" });
    expect(mockResponderRuns).not.toHaveBeenCalled();
  });

  it("a row without run/user is logged, never alerted on a made-up identity", async () => {
    const out = await maybeAlertUnansweredInfoRequest({ ...campaign, runId: null }, "pam@buffalo.com", "lead_info_requested");
    expect(out).toEqual({ sent: false, reason: "no_identity" });
    expect(mockForward).not.toHaveBeenCalled();
  });
});
