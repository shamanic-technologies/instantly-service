import { describe, it, expect, vi, beforeEach } from "vitest";

const mockExecute = vi.fn();
vi.mock("../../src/db", () => ({ db: { execute: (...a: unknown[]) => mockExecute(...a) } }));

const mockGate = vi.fn();
vi.mock("../../src/lib/email-verification-gate", () => ({
  gateAddresses: (...a: unknown[]) => mockGate(...a),
}));

const mockStopSelfSend = vi.fn();
vi.mock("../../src/lib/self-send/stop-sequence", () => ({
  stopSelfSendSequence: (...a: unknown[]) => mockStopSelfSend(...a),
}));

const mockPause = vi.fn();
vi.mock("../../src/lib/instantly-client", () => ({
  updateCampaignStatus: (...a: unknown[]) => mockPause(...a),
}));
vi.mock("../../src/lib/key-client", () => ({
  resolveInstantlyApiKey: async () => ({ key: "k", keySource: "platform" }),
}));

import { verifyQueuedSequences } from "../../src/lib/queued-verification";

const CALLER = { method: "POST", path: "/internal/self-send/dispatch" };

function seq(id: string, email: string, extra: Record<string, unknown> = {}) {
  return {
    instantlyCampaignId: id,
    campaignId: "camp-1",
    orgId: "org-1",
    userId: "user-1",
    runId: `run-${id}`,
    leadEmail: email,
    brandIds: ["brand-1"],
    ...extra,
  };
}

function decision(email: string, verdict: string | null, accepted: boolean, reason: string) {
  return new Map([[email, { email, verdict, accepted, reason, verificationId: null, verifiedAt: null, reused: null }]]);
}

describe("verifyQueuedSequences — the backlog is verified before it dispatches", () => {
  beforeEach(() => {
    mockExecute.mockReset();
    mockGate.mockReset();
    mockStopSelfSend.mockReset().mockResolvedValue(undefined);
    mockPause.mockReset().mockResolvedValue({});
  });

  it("clears accepted, stops+refunds refused, holds no-verdict", async () => {
    mockExecute.mockResolvedValue({
      rows: [
        seq("self:1", "ok@x.com"),
        seq("self:2", "bad@x.com"),
        seq("self:3", "down@x.com"),
        seq("inst-4", "trap@x.com"),
      ],
    });
    mockGate.mockImplementation(async (emails: string[]) => {
      const e = emails[0];
      if (e === "ok@x.com") return decision(e, "catch_all", true, "verdict_catch_all");
      if (e === "bad@x.com") return decision(e, "invalid", false, "verdict_invalid");
      if (e === "trap@x.com") return decision(e, "risky", false, "verdict_risky");
      return decision(e, null, false, "verifier_failed");
    });

    const { summary, notCleared } = await verifyQueuedSequences(CALLER);

    expect(summary).toMatchObject({ unverified: 4, verified: 4, accepted: 1, stopped: 2, held: 1, deferred: 0 });
    expect([...notCleared].sort()).toEqual(["inst-4", "self:2", "self:3"]);
    // billed on the sequence's own run, to its org
    expect(mockGate.mock.calls[0][1]).toMatchObject({ orgId: "org-1", userId: "user-1", runId: "run-self:1" });
    expect(mockGate.mock.calls[0][2]).toBe("queued");
    // self-send refused: refunded + stopped; Instantly refused: paused FIRST
    expect(mockStopSelfSend).toHaveBeenCalledTimes(2);
    expect(mockPause).toHaveBeenCalledWith("k", "inst-4", "paused");
  });

  it("holds a sequence with no run/user to bill, without calling the verifier", async () => {
    mockExecute.mockResolvedValue({ rows: [seq("self:1", "a@x.com", { runId: null })] });
    const { summary, notCleared } = await verifyQueuedSequences(CALLER);
    expect(mockGate).not.toHaveBeenCalled();
    expect(summary.held).toBe(1);
    expect(notCleared.has("self:1")).toBe(true);
  });

  it("a stop that fails keeps the sequence held", async () => {
    mockExecute.mockResolvedValue({ rows: [seq("self:1", "bad@x.com")] });
    mockGate.mockResolvedValue(decision("bad@x.com", "invalid", false, "verdict_invalid"));
    mockStopSelfSend.mockRejectedValue(new Error("runs down"));
    const { summary, notCleared } = await verifyQueuedSequences(CALLER);
    expect(summary.held).toBe(1);
    expect(notCleared.has("self:1")).toBe(true);
  });

  it("beyond the tick's limit: held, verified on a later tick", async () => {
    mockExecute.mockResolvedValue({ rows: [seq("self:1", "a@x.com"), seq("self:2", "b@x.com")] });
    mockGate.mockImplementation(async (emails: string[]) => decision(emails[0], "valid", true, "verdict_valid"));
    const { summary, notCleared } = await verifyQueuedSequences(CALLER, 1);
    expect(summary).toMatchObject({ deferred: 1, accepted: 1 });
    expect([...notCleared]).toEqual(["self:2"]);
  });

  it("FAILS LOUD when the queue cannot be read", async () => {
    mockExecute.mockRejectedValue(new Error("db down"));
    await expect(verifyQueuedSequences(CALLER)).rejects.toThrow(/db down/);
  });
});
