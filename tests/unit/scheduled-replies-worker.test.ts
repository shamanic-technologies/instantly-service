import { describe, it, expect, vi, beforeEach } from "vitest";

const loadPending = vi.fn();
const markSent = vi.fn();
const markFailed = vi.fn();
const markSuperseded = vi.fn();
vi.mock("../../src/lib/scheduled-replies", async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  loadPendingScheduledReplies: () => loadPending(),
  markScheduledReplySent: (...a: unknown[]) => markSent(...a),
  markScheduledReplyFailed: (...a: unknown[]) => markFailed(...a),
  markScheduledReplySuperseded: (...a: unknown[]) => markSuperseded(...a),
}));
const replyToLead = vi.fn();
vi.mock("../../src/lib/reply-to-lead", () => ({
  replyToLead: (...a: unknown[]) => replyToLead(...a),
}));
const scheduleFollowup = vi.fn();
vi.mock("../../src/lib/lead-client", () => ({
  scheduleFollowupByEmail: (...a: unknown[]) => scheduleFollowup(...a),
}));
const triggerLeg = vi.fn();
vi.mock("../../src/lib/trigger-sales-interest-campaign", () => ({
  triggerSalesInterestLeg: (...a: unknown[]) => triggerLeg(...a),
}));

import { dispatchScheduledReplies } from "../../src/lib/scheduled-replies-worker";
import type { ScheduledReply } from "../../src/lib/scheduled-replies";

const ASOF = new Date("2026-09-25T07:00:00.000Z");

function row(over: Partial<ScheduledReply> = {}): ScheduledReply {
  return {
    id: "46cb9b17",
    orgId: "org-1",
    userId: "user-1",
    campaignId: "camp-1",
    instantlyCampaignId: "self:abc",
    leadEmail: "joanie@aimforwellness.com",
    bodyHtml: "<p>Would tomorrow, September 25th at 9:00 AM work?</p>",
    sentBy: "automation",
    timezone: "America/New_York",
    scheduledFor: new Date("2026-09-25T12:00:00.000Z"),
    createdAt: new Date("2026-09-24T21:43:52.000Z"),
    attempts: 0,
    ...over,
  };
}

beforeEach(() => {
  vi.resetAllMocks();
});

describe("dispatchScheduledReplies — what is left of the waiting room", () => {
  it("does NOT send a stale automated draft: it re-queues the lead for a fresh answer", async () => {
    loadPending.mockResolvedValue([row()]);
    scheduleFollowup.mockResolvedValue({});

    const summary = await dispatchScheduledReplies(ASOF);

    expect(replyToLead).not.toHaveBeenCalled();
    expect(scheduleFollowup).toHaveBeenCalledWith({
      orgId: "org-1",
      campaignId: "camp-1",
      email: "joanie@aimforwellness.com",
      dueAt: ASOF.toISOString(),
    });
    expect(triggerLeg).toHaveBeenCalledTimes(1);
    expect(markSuperseded).toHaveBeenCalledWith("46cb9b17");
    expect(summary).toMatchObject({ pending: 1, due: 0, redrafted: 1, sent: 0, failed: 0 });
  });

  it("re-queues BEFORE superseding: a lead-service failure keeps the row pending", async () => {
    loadPending.mockResolvedValue([row()]);
    scheduleFollowup.mockRejectedValue(new Error("lead-service 503"));
    markFailed.mockResolvedValue(undefined);

    const summary = await dispatchScheduledReplies(ASOF);

    expect(markSuperseded).not.toHaveBeenCalled();
    expect(markFailed).toHaveBeenCalledWith("46cb9b17", 0, expect.any(Error));
    expect(summary.failed).toBe(1);
  });

  it("sends a fresh draft now, at any hour, with no window re-check", async () => {
    loadPending.mockResolvedValue([row({ createdAt: new Date(ASOF.getTime() - 60_000) })]);
    replyToLead.mockResolvedValue({ status: "sent" });

    const summary = await dispatchScheduledReplies(ASOF);

    expect(replyToLead).toHaveBeenCalledTimes(1);
    expect(replyToLead.mock.calls[0]).toHaveLength(1);
    expect(markSent).toHaveBeenCalledWith("46cb9b17");
    expect(scheduleFollowup).not.toHaveBeenCalled();
    expect(summary).toMatchObject({ due: 1, sent: 1 });
  });

  it("replays who asked, so the takeover gate reads true on a human's row", async () => {
    loadPending.mockResolvedValue([row({ sentBy: "human" })]);
    replyToLead.mockResolvedValue({ status: "sent" });

    await dispatchScheduledReplies(ASOF);

    expect(replyToLead.mock.calls[0]![0]).toMatchObject({ sentBy: "human" });
  });
});
