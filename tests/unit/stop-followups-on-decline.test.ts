import { describe, it, expect, vi, beforeEach } from "vitest";

const mockFind = vi.fn();
const mockRead = vi.fn();
const mockStop = vi.fn();

vi.mock("../../src/lib/lead-client", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  findLeadOnCampaignByEmail: (...a: unknown[]) => mockFind(...a),
  readFollowupState: (...a: unknown[]) => mockRead(...a),
  stopFollowups: (...a: unknown[]) => mockStop(...a),
}));

const { maybeStopFollowupsOnDecline, isFollowupStoppingEvent } = await import(
  "../../src/lib/stop-followups-on-decline"
);
const { isSalesInterestQualification } = await import(
  "../../src/lib/trigger-sales-interest-campaign"
);
const { REPLY_KINDS } = await import("../../src/lib/reply-kind");

const CAMPAIGN = { instantlyCampaignId: "inst-1", campaignId: "camp-1", orgId: "org-1" };
const PENDING = {
  id: "row-1", leadId: "lead-1", campaignId: "camp-1",
  dueAt: "2026-09-28T05:00:00.000Z", claimedAt: null,
  followupCount: 1, lastActionAt: null, stoppedReason: null,
};

beforeEach(() => {
  vi.resetAllMocks();
  mockFind.mockResolvedValue({ id: "row-1", email: "p@x.com" });
  mockRead.mockResolvedValue(PENDING);
  mockStop.mockResolvedValue(undefined);
});

describe("maybeStopFollowupsOnDecline", () => {
  it("stops a pending follow-up when an interested person then declines", async () => {
    await maybeStopFollowupsOnDecline(CAMPAIGN, "p@x.com", "lead_not_interested");
    expect(mockFind).toHaveBeenCalledWith({ orgId: "org-1", campaignId: "camp-1", email: "p@x.com" });
    expect(mockStop).toHaveBeenCalledWith({
      orgId: "org-1", leadRowId: "row-1", reason: "reply:lead_not_interested",
    });
  });

  it("stops on an unsubscribe and on an opt-out request", async () => {
    await maybeStopFollowupsOnDecline(CAMPAIGN, "p@x.com", "lead_unsubscribed");
    await maybeStopFollowupsOnDecline(CAMPAIGN, "p@x.com", "lead_opt_out_requested");
    expect(mockStop).toHaveBeenCalledTimes(2);
  });

  it("stops a CLAIMED schedule too (a worker holding it must not answer)", async () => {
    mockRead.mockResolvedValue({ ...PENDING, dueAt: null, claimedAt: "2026-09-28T05:50:00.000Z" });
    await maybeStopFollowupsOnDecline(CAMPAIGN, "p@x.com", "lead_wrong_person");
    expect(mockStop).toHaveBeenCalledTimes(1);
  });

  it("does NOT stop a schedule that holds nothing (no false timeline event)", async () => {
    mockRead.mockResolvedValue({ ...PENDING, dueAt: null, claimedAt: null });
    await maybeStopFollowupsOnDecline(CAMPAIGN, "p@x.com", "lead_not_interested");
    expect(mockStop).not.toHaveBeenCalled();
  });

  it("never fires on a buying signal, a neutral reply, or an autoresponder", async () => {
    for (const kind of REPLY_KINDS) {
      if (isSalesInterestQualification(kind)) expect(isFollowupStoppingEvent(kind)).toBe(false);
    }
    for (const k of ["lead_neutral", "lead_out_of_office", "auto_reply_received", "email_sent", "reply_received"]) {
      expect(isFollowupStoppingEvent(k)).toBe(false);
    }
    await maybeStopFollowupsOnDecline(CAMPAIGN, "p@x.com", "lead_interested");
    expect(mockFind).not.toHaveBeenCalled();
  });

  it("stops on every negative kind and on a referral", () => {
    for (const k of ["lead_not_interested", "lead_wrong_person", "lead_changed_job", "lead_opt_out_requested", "lead_referral", "lead_unsubscribed"]) {
      expect(isFollowupStoppingEvent(k)).toBe(true);
    }
  });

  it("no-ops on a platform send", async () => {
    await maybeStopFollowupsOnDecline({ ...CAMPAIGN, campaignId: null }, "p@x.com", "lead_not_interested");
    await maybeStopFollowupsOnDecline({ ...CAMPAIGN, orgId: null }, "p@x.com", "lead_not_interested");
    expect(mockFind).not.toHaveBeenCalled();
  });

  it("never throws when lead-service fails, and says so", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    mockStop.mockRejectedValue(new Error("lead-service 503"));
    await expect(
      maybeStopFollowupsOnDecline(CAMPAIGN, "p@x.com", "lead_not_interested"),
    ).resolves.toBeUndefined();
    expect(warn.mock.calls.some((c) => String(c[0]).includes("lead-service 503"))).toBe(true);
    mockFind.mockRejectedValue(new Error("down"));
    await expect(
      maybeStopFollowupsOnDecline(CAMPAIGN, "p@x.com", "lead_not_interested"),
    ).resolves.toBeUndefined();
    warn.mockRestore();
  });

  it("warns and stops nothing when no single row matches", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    mockFind.mockResolvedValue(null);
    await maybeStopFollowupsOnDecline(CAMPAIGN, "p@x.com", "lead_not_interested");
    expect(mockStop).not.toHaveBeenCalled();
    expect(warn).toHaveBeenCalled();
    warn.mockRestore();
  });
});
