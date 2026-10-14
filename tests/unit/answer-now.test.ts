import { describe, it, expect, vi, beforeEach } from "vitest";

// FORM-ANSWER-LATENCY-1014: which sends leave at once is campaign-service's word
// for the campaign (reactive = it runs because a person did something), never a
// form, channel or client branch.

const mockExecute = vi.fn();
const mockGetCampaignKind = vi.fn();
const mockDispatchAnswerNow = vi.fn();

vi.mock("../../src/db", () => ({ db: { execute: (...a: unknown[]) => mockExecute(...a) } }));
vi.mock("../../src/lib/campaign-client", () => ({
  getCampaignKind: (...a: unknown[]) => mockGetCampaignKind(...a),
}));
vi.mock("../../src/lib/self-send/dispatch-worker", () => ({
  dispatchAnswerNow: (...a: unknown[]) => mockDispatchAnswerNow(...a),
}));

const { sendAnswerNowIfOwed } = await import("../../src/lib/self-send/answer-now");

const params = { orgId: "org-1", campaignId: "camp-1", instantlyCampaignId: "self:1" };

beforeEach(() => {
  vi.resetAllMocks();
  process.env.SELF_SEND_DISPATCH_ENABLED = "true";
  mockExecute.mockResolvedValue({ rows: [] });
  mockDispatchAnswerNow.mockResolvedValue({ sent: 1 });
});

describe("sendAnswerNowIfOwed", () => {
  it("stamps and sends at once a sequence of a REACTIVE campaign", async () => {
    mockGetCampaignKind.mockResolvedValue("reactive");
    const outcome = await sendAnswerNowIfOwed(params);
    expect(outcome.answersPerson).toBe(true);
    expect(mockGetCampaignKind).toHaveBeenCalledWith("org-1", "camp-1");
    expect(JSON.stringify(mockExecute.mock.calls[0]![0])).toContain("answersPerson");
    expect(mockDispatchAnswerNow).toHaveBeenCalledWith("self:1");
  });

  it("leaves a PROACTIVE (cold) campaign's sequence to the sweep: no stamp, no send", async () => {
    mockGetCampaignKind.mockResolvedValue("proactive");
    const outcome = await sendAnswerNowIfOwed(params);
    expect(outcome.answersPerson).toBe(false);
    expect(mockExecute).not.toHaveBeenCalled();
    expect(mockDispatchAnswerNow).not.toHaveBeenCalled();
  });

  it("does nothing when campaign-service cannot say (null kind)", async () => {
    mockGetCampaignKind.mockResolvedValue(null);
    expect((await sendAnswerNowIfOwed(params)).answersPerson).toBe(false);
    expect(mockDispatchAnswerNow).not.toHaveBeenCalled();
  });

  it("does nothing for a send with no campaign (platform send)", async () => {
    expect((await sendAnswerNowIfOwed({ ...params, campaignId: null })).answersPerson).toBe(false);
    expect(mockGetCampaignKind).not.toHaveBeenCalled();
  });

  it("stamps but does not send while the dispatch kill switch is off", async () => {
    process.env.SELF_SEND_DISPATCH_ENABLED = "false";
    mockGetCampaignKind.mockResolvedValue("reactive");
    const outcome = await sendAnswerNowIfOwed(params);
    expect(outcome).toEqual({ answersPerson: true, summary: null });
    expect(mockDispatchAnswerNow).not.toHaveBeenCalled();
  });

  it("throws when campaign-service cannot answer (the caller logs it; the sweep sends later)", async () => {
    mockGetCampaignKind.mockRejectedValue(new Error("campaign-service down"));
    await expect(sendAnswerNowIfOwed(params)).rejects.toThrow("campaign-service down");
  });
});
