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

describe("celebration, then 'answer it yourself' when NO responder took the reply", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockAsk.mockResolvedValue({ sent: true });
  });

  it("interested + nobody took it: celebration first, then the client is asked", async () => {
    const order: string[] = [];
    mockCelebrate.mockImplementation(async () => {
      order.push("celebrate");
      return true;
    });
    mockAsk.mockImplementation(async () => {
      order.push("ask");
      return { sent: true };
    });
    await maybeForwardPositiveReply(campaign, "lead@x.com", "lead_interested", { background: false, responder: "nobody" });
    expect(order).toEqual(["celebrate", "ask"]);
    expect(mockAsk).toHaveBeenCalledWith(campaign, "lead@x.com", { because: "no_responder" });
  });

  it("question (info request, never celebrated) + nobody took it: the client is asked", async () => {
    mockCelebrate.mockResolvedValue(false);
    await maybeForwardPositiveReply(campaign, "lead@x.com", "lead_info_requested", {
      background: false,
      responder: "nobody",
    });
    expect(mockAsk).toHaveBeenCalledWith(campaign, "lead@x.com", { because: "no_responder" });
  });

  it("a responder took it: the client is NOT asked (behaves as today)", async () => {
    mockCelebrate.mockResolvedValue(true);
    await maybeForwardPositiveReply(campaign, "lead@x.com", "lead_info_requested", {
      background: false,
      responder: "answers",
    });
    expect(mockAsk).not.toHaveBeenCalled();
  });

  it("verdict unknown (trigger unreadable): nothing asked now, the 24 h deadline decides", async () => {
    mockCelebrate.mockResolvedValue(true);
    await maybeForwardPositiveReply(campaign, "lead@x.com", "lead_interested", {
      background: false,
      responder: "unknown",
    });
    expect(mockAsk).not.toHaveBeenCalled();
  });
});
