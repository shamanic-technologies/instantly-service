import { describe, it, expect, vi, beforeEach } from "vitest";

const mockLoadCampaign = vi.fn();
vi.mock("../../src/lib/reply-to-lead", () => ({
  loadCampaign: (...a: unknown[]) => mockLoadCampaign(...a),
}));

const mockSendThreadForward = vi.fn();
vi.mock("../../src/lib/forward-positive-reply", () => ({
  sendThreadForward: (...a: unknown[]) => mockSendThreadForward(...a),
}));

const mockFindLead = vi.fn();
const mockStopFollowups = vi.fn();
vi.mock("../../src/lib/lead-client", () => ({
  findLeadOnCampaignByEmail: (...a: unknown[]) => mockFindLead(...a),
  stopFollowups: (...a: unknown[]) => mockStopFollowups(...a),
}));

import { escalateReply, EscalateReplyError } from "../../src/lib/escalate-reply";

const CAMPAIGN = {
  campaignId: "camp-1",
  instantlyCampaignId: "ic-1",
  leadEmail: "alice@media.com",
  accountEmail: "amy@boostdistribute.com",
  sendTransport: "instantly",
  createdAt: null,
  timezone: null,
  brandId: "brand-1",
};

const INPUT = {
  orgId: "org-1",
  userId: "user-1",
  runId: "run-1",
  campaignId: "camp-1",
  leadEmail: "Alice@Media.com",
  question: "What does it cost for 5 seats?",
};

beforeEach(() => {
  vi.resetAllMocks();
  mockLoadCampaign.mockResolvedValue(CAMPAIGN);
  mockSendThreadForward.mockResolvedValue(4);
  mockFindLead.mockResolvedValue({ id: "lead-row-1", email: "alice@media.com" });
  mockStopFollowups.mockResolvedValue(undefined);
});

describe("handing a thread to a human", () => {
  it("forwards the caller's run identity on the agency-inbox send", async () => {
    await escalateReply(INPUT);

    const [campaign] = mockSendThreadForward.mock.calls[0];
    expect(campaign).toMatchObject({
      orgId: "org-1",
      userId: "user-1",
      runId: "run-1",
    });
  });

  it("tells a person AND stops the ladder", async () => {
    const result = await escalateReply(INPUT);

    expect(mockSendThreadForward).toHaveBeenCalledTimes(1);
    expect(mockStopFollowups).toHaveBeenCalledTimes(1);
    expect(result).toEqual({
      instantlyCampaignId: "ic-1",
      leadEmail: "alice@media.com",
      threadMessages: 4,
      followupsStopped: true,
    });
  });

  it("sends the prospect NOTHING", async () => {
    // The escalation exists because we have nothing to say to them. Its whole
    // job is a notification and a stop — anything reaching the prospect here
    // would be the deflection the feature exists to prevent.
    await escalateReply(INPUT);

    const [, , template] = mockSendThreadForward.mock.calls[0];
    expect((template as { eventType: string }).eventType).toBe("reply-escalation");
  });

  it("carries the question, because a bare give-up is not actionable", async () => {
    await escalateReply(INPUT);

    const [, , template] = mockSendThreadForward.mock.calls[0];
    expect((template as { metadata: Record<string, string> }).metadata).toEqual({
      leadEmail: "alice@media.com",
      question: "What does it cost for 5 seats?",
    });
  });

  it("states the question as the reason the ladder stopped", async () => {
    // lead-service requires a non-empty reason and keeps it, so the stop stays
    // auditable months later.
    await escalateReply(INPUT);

    expect(mockStopFollowups).toHaveBeenCalledWith({
      orgId: "org-1",
      leadRowId: "lead-row-1",
      reason: "The automated responder could not answer: What does it cost for 5 seats?",
    });
  });

  it("forwards BEFORE it stops", async () => {
    // A failure between the two must leave a human informed on a thread still
    // scheduled — visible and undoable. The reverse stops the ladder silently
    // and the prospect never hears from anyone again.
    const order: string[] = [];
    mockSendThreadForward.mockImplementation(async () => {
      order.push("forward");
      return 4;
    });
    mockStopFollowups.mockImplementation(async () => {
      order.push("stop");
    });

    await escalateReply(INPUT);
    expect(order).toEqual(["forward", "stop"]);
  });

  it("fails loud when the human cannot be reached, and stops nothing", async () => {
    // An escalation that emptied the schedule and told nobody is strictly worse
    // than none: the ladder was at least still talking to them.
    mockSendThreadForward.mockRejectedValue(new Error("postmark refused"));

    await expect(escalateReply(INPUT)).rejects.toThrow("postmark refused");
    expect(mockStopFollowups).not.toHaveBeenCalled();
  });

  it("still tells a human when lead-service holds no row for them", async () => {
    // A real state (a platform send, a lead registered elsewhere), not a
    // failure — and the notification is the half that must not be missed.
    mockFindLead.mockResolvedValue(null);

    const result = await escalateReply(INPUT);

    expect(result.followupsStopped).toBe(false);
    expect(mockSendThreadForward).toHaveBeenCalledTimes(1);
    expect(mockStopFollowups).not.toHaveBeenCalled();
  });

  it("refuses a campaign this org does not hold", async () => {
    mockLoadCampaign.mockResolvedValue(null);

    await expect(escalateReply(INPUT)).rejects.toMatchObject({
      code: "campaign_not_found",
      status: 404,
    });
    expect(mockSendThreadForward).not.toHaveBeenCalled();
  });

  it("refuses an empty question under its OWN code, not a borrowed one", async () => {
    // Reporting this as `campaign_not_found` would send a caller looking for a
    // campaign that is perfectly fine.
    await expect(
      escalateReply({ ...INPUT, question: "   " }),
    ).rejects.toMatchObject({ code: "question_required", status: 400 });

    expect(mockLoadCampaign).not.toHaveBeenCalled();
    expect(mockSendThreadForward).not.toHaveBeenCalled();
  });

  it("names its refusals as its own class", async () => {
    mockLoadCampaign.mockResolvedValue(null);
    await expect(escalateReply(INPUT)).rejects.toBeInstanceOf(EscalateReplyError);
  });
});
