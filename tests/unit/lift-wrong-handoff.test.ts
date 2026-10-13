import { describe, it, expect, vi, beforeEach } from "vitest";

const mockFindHumanTakeover = vi.fn();
vi.mock("../../src/lib/human-takeover", () => ({
  findHumanTakeover: (...a: unknown[]) => mockFindHumanTakeover(...a),
}));
const mockFindLead = vi.fn();
const mockWithdraw = vi.fn();
vi.mock("../../src/lib/lead-client", () => ({
  findLeadOnCampaignByEmail: (...a: unknown[]) => mockFindLead(...a),
  withdrawHandoff: (...a: unknown[]) => mockWithdraw(...a),
}));

import { liftWrongEscalationHandoff, WITHDRAWAL_REASON } from "../../src/lib/lift-wrong-handoff";
import {
  OFF_TOPIC_HANDOFF_REASON,
  REFERRAL_HANDOFF_REASON,
} from "../../src/lib/escalation-handoff-reasons";

const CAMPAIGN = { instantlyCampaignId: "self:abc", campaignId: "camp-1", orgId: "org-1" };

beforeEach(() => {
  vi.resetAllMocks();
  mockFindHumanTakeover.mockResolvedValue(null);
  mockFindLead.mockResolvedValue({ id: "row-1" });
});

/**
 * prod 2026-10-13, DefiniteJobs: Sam's "yes, a free listing sounds good, can we
 * have a quick call?" was wrongly called off topic and handed to a person; once
 * re-judged a meeting request, the responder found nothing due.
 */
describe("liftWrongEscalationHandoff", () => {
  it("withdraws the off-topic hand-over WE stated, naming it by its exact reason", async () => {
    mockWithdraw.mockResolvedValueOnce(true);
    expect(await liftWrongEscalationHandoff(CAMPAIGN, "sam@carter.co")).toBe("lifted");
    expect(mockFindLead).toHaveBeenCalledWith({ orgId: "org-1", campaignId: "camp-1", email: "sam@carter.co" });
    expect(mockWithdraw).toHaveBeenCalledTimes(1);
    expect(mockWithdraw).toHaveBeenCalledWith({
      orgId: "org-1",
      leadRowId: "row-1",
      handedReason: OFF_TOPIC_HANDOFF_REASON,
      reason: WITHDRAWAL_REASON,
    });
  });

  it("tries the referral hand-over when the off-topic one is not on the row", async () => {
    mockWithdraw.mockResolvedValueOnce(false).mockResolvedValueOnce(true);
    expect(await liftWrongEscalationHandoff(CAMPAIGN, "sam@carter.co")).toBe("lifted");
    expect(mockWithdraw.mock.calls[1][0].handedReason).toBe(REFERRAL_HANDOFF_REASON);
  });

  it("lifts nothing it did not state: lead-service refuses every other hand-over", async () => {
    mockWithdraw.mockResolvedValue(false);
    expect(await liftWrongEscalationHandoff(CAMPAIGN, "sam@carter.co")).toBe("nothing_to_lift");
  });

  it("NEVER lifts when a person answered on the thread: it is theirs", async () => {
    mockFindHumanTakeover.mockResolvedValue({ at: "2026-10-13T21:00:00Z", source: "dispatched" });
    expect(await liftWrongEscalationHandoff(CAMPAIGN, "sam@carter.co")).toBe("person_answered");
    expect(mockWithdraw).not.toHaveBeenCalled();
  });

  it("does nothing on a platform send (no org or caller campaign)", async () => {
    expect(await liftWrongEscalationHandoff({ ...CAMPAIGN, campaignId: null }, "x@y.z")).toBe("no_lead_row");
    expect(mockFindHumanTakeover).not.toHaveBeenCalled();
  });

  it("fails loud when lead-service cannot be told", async () => {
    mockWithdraw.mockRejectedValue(new Error("lead-service 502"));
    await expect(liftWrongEscalationHandoff(CAMPAIGN, "x@y.z")).rejects.toThrow("lead-service 502");
  });
});
