import { describe, it, expect, vi, beforeEach } from "vitest";

const mockFindLead = vi.fn();
const mockHand = vi.fn();
vi.mock("../../src/lib/lead-client", () => ({
  findLeadOnCampaignByEmail: (...a: unknown[]) => mockFindLead(...a),
  handFollowupsToHuman: (...a: unknown[]) => mockHand(...a),
}));

import { handThreadFollowupsToHuman } from "../../src/lib/hand-followups-to-human";

const INPUT = {
  orgId: "org-1",
  campaignId: "3922c8e1-3405-46af-8a56-1eef3f221b19",
  leadEmail: "drdoug@prohealthdoc.com",
  reason: "Client asked to answer",
  tag: "test",
};

describe("handThreadFollowupsToHuman — the thread is a person's, lead-service is told", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockFindLead.mockResolvedValue({ id: "row-1" });
    mockHand.mockResolvedValue(undefined);
  });

  it("finds the row on the caller campaign and states `handed` with the reason", async () => {
    expect(await handThreadFollowupsToHuman(INPUT)).toBe(true);
    expect(mockFindLead).toHaveBeenCalledWith({ orgId: "org-1", campaignId: INPUT.campaignId, email: INPUT.leadEmail });
    expect(mockHand).toHaveBeenCalledWith({ orgId: "org-1", leadRowId: "row-1", reason: "Client asked to answer" });
  });

  it("no row on the campaign: nothing stated, false", async () => {
    mockFindLead.mockResolvedValue(null);
    expect(await handThreadFollowupsToHuman(INPUT)).toBe(false);
    expect(mockHand).not.toHaveBeenCalled();
  });

  it("no campaign or org: nothing asked, false", async () => {
    expect(await handThreadFollowupsToHuman({ ...INPUT, campaignId: null })).toBe(false);
    expect(await handThreadFollowupsToHuman({ ...INPUT, orgId: null })).toBe(false);
    expect(mockFindLead).not.toHaveBeenCalled();
  });

  it("never throws: a lead-service failure is logged and reported false", async () => {
    mockHand.mockRejectedValue(new Error("lead-service 400"));
    expect(await handThreadFollowupsToHuman(INPUT)).toBe(false);
  });
});
