import { describe, it, expect, vi, beforeEach } from "vitest";

const mockCelebrate = vi.fn();
const mockAsk = vi.fn();

vi.mock("../../src/lib/celebrate-positive-reply", () => ({
  celebrateOnce: (...a: unknown[]) => mockCelebrate(...a),
}));
vi.mock("../../src/lib/ask-client-to-answer", () => ({
  maybeAskClientToAnswer: (...a: unknown[]) => mockAsk(...a),
}));

import { maybeForwardPositiveReply } from "../../src/lib/forward-positive-reply";

const campaign = {
  instantlyCampaignId: "ic-1",
  campaignId: "c-1",
  orgId: "org-1",
  userId: "u-1",
  runId: "r-1",
  brandIds: ["b-1"],
};

describe("celebration then 'answer it yourself'", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockAsk.mockResolvedValue({ sent: false, reason: "responder_running" });
  });

  it("asks the client only after a celebration was SENT", async () => {
    mockCelebrate.mockResolvedValue(true);
    await maybeForwardPositiveReply(campaign, "lead@x.com", "lead_interested", { background: false });
    expect(mockCelebrate).toHaveBeenCalledTimes(1);
    expect(mockAsk).toHaveBeenCalledWith(campaign, "lead@x.com");
  });

  it("no celebration sent (info request, claim taken): nothing asked", async () => {
    mockCelebrate.mockResolvedValue(false);
    await maybeForwardPositiveReply(campaign, "lead@x.com", "lead_info_requested", { background: false });
    expect(mockAsk).not.toHaveBeenCalled();
  });
});
