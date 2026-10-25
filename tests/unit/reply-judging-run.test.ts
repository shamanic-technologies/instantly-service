import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const execute = vi.fn();
vi.mock("../../src/db", () => ({ db: { execute: (...a: unknown[]) => execute(...a) } }));
const createRun = vi.fn();
const updateRun = vi.fn();
vi.mock("../../src/lib/runs-client", () => ({
  createRun: (...a: unknown[]) => createRun(...a),
  updateRun: (...a: unknown[]) => updateRun(...a),
}));

import {
  NO_PIPE,
  REPLY_JUDGING_TASK,
  pipeOfRow,
  replyJudgingRunFor,
  replyJudgingRunForSequence,
  resetReplyJudgingRunMemo,
  utcDay,
} from "../../src/lib/reply-judging-run";
import { platformComplete, platformJudgment, SUPPORT_FUNCTIONS } from "../../src/lib/chat-client";

const pgResult = (rows: unknown[]) => ({ rows, rowCount: rows.length, command: "SELECT", fields: [] });
const ORG = "06ffd332-0000-4000-8000-000000000001";
const RUN = "4c8b7a1e-1111-4222-8333-444455556666";
const AS_OF = new Date("2026-10-23T23:59:00Z");
const PIPE = { campaignId: "camp-1", workflowSlug: "cold-email-v3", brandIds: ["brand-1", "brand-2"], featureSlug: "sales-cold-email" };
const params = (call: unknown[]) => JSON.stringify(call[0]);

describe("replyJudgingRunFor: one reply-judging run per (org, UTC day, pipe) (LEDGER-RESIDUAL-1025)", () => {
  beforeEach(() => {
    execute.mockReset();
    createRun.mockReset().mockResolvedValue({ id: RUN });
    updateRun.mockReset().mockResolvedValue({});
    resetReplyJudgingRunMemo();
  });

  it("keys on the UTC day", () => {
    expect(utcDay(AS_OF)).toBe("2026-10-23");
    expect(utcDay(new Date("2026-10-24T00:00:01Z"))).toBe("2026-10-24");
  });

  it("creates the day's run of the pipe once, TAGGED with the pipe (a no-positive day is that pipe's loss), closes it and stores it", async () => {
    execute.mockResolvedValueOnce(pgResult([])).mockResolvedValueOnce(pgResult([{ run_id: RUN }]));
    expect(await replyJudgingRunFor(ORG, PIPE, AS_OF)).toBe(RUN);
    expect(createRun.mock.calls[0][0]).toEqual({ serviceName: "instantly-service", taskName: REPLY_JUDGING_TASK });
    expect(createRun.mock.calls[0][1]).toMatchObject({
      orgId: ORG,
      tracking: { campaignId: "camp-1", workflowSlug: "cold-email-v3", brandId: "brand-1,brand-2", featureSlug: "sales-cold-email" },
    });
    expect(updateRun).toHaveBeenCalledWith(RUN, "completed", expect.objectContaining({ orgId: ORG }));
    expect(params(execute.mock.calls[1])).toContain("camp-1");
    expect(params(execute.mock.calls[1])).toContain("cold-email-v3");
    // memoized: the next call that day on that pipe reads nothing
    expect(await replyJudgingRunFor(ORG, PIPE, AS_OF)).toBe(RUN);
    expect(execute).toHaveBeenCalledTimes(2);
  });

  it("another pipe of the same org and day is another run", async () => {
    execute.mockResolvedValue(pgResult([]));
    createRun.mockResolvedValueOnce({ id: "run-a" }).mockResolvedValueOnce({ id: "run-b" });
    execute
      .mockResolvedValueOnce(pgResult([]))
      .mockResolvedValueOnce(pgResult([{ run_id: "run-a" }]))
      .mockResolvedValueOnce(pgResult([]))
      .mockResolvedValueOnce(pgResult([{ run_id: "run-b" }]));
    expect(await replyJudgingRunFor(ORG, PIPE, AS_OF)).toBe("run-a");
    expect(await replyJudgingRunFor(ORG, { ...PIPE, workflowSlug: "cold-email-v4" }, AS_OF)).toBe("run-b");
  });

  it("no pipe recorded (or half of one): the org's pipe-less run, created with no tags", async () => {
    execute.mockResolvedValueOnce(pgResult([])).mockResolvedValueOnce(pgResult([{ run_id: RUN }]));
    expect(await replyJudgingRunFor(ORG, { ...NO_PIPE, campaignId: "camp-1" }, AS_OF)).toBe(RUN);
    expect(createRun.mock.calls[0][1]).not.toHaveProperty("tracking");
  });

  it("reuses a stored run: no new run", async () => {
    execute.mockResolvedValueOnce(pgResult([{ run_id: RUN }]));
    expect(await replyJudgingRunFor(ORG, PIPE, AS_OF)).toBe(RUN);
    expect(createRun).not.toHaveBeenCalled();
  });

  it("a concurrent creator wins the row: its run is the day's run", async () => {
    execute
      .mockResolvedValueOnce(pgResult([]))
      .mockResolvedValueOnce(pgResult([])) // insert lost
      .mockResolvedValueOnce(pgResult([{ run_id: "theirs" }]));
    expect(await replyJudgingRunFor(ORG, PIPE, AS_OF)).toBe("theirs");
  });

  it("a sequence's run is on the sequence's recorded pipe", async () => {
    execute
      .mockResolvedValueOnce(pgResult([{ org_id: ORG, campaign_id: "camp-1", workflow_slug: "cold-email-v3", feature_slug: null, brand_ids: ["brand-1"] }]))
      .mockResolvedValueOnce(pgResult([{ run_id: RUN }]));
    expect(await replyJudgingRunForSequence("self:abc", { asOf: AS_OF })).toBe(RUN);
    expect(params(execute.mock.calls[1])).toContain("cold-email-v3");
  });

  it("pipeOfRow reads the row's recorded pipe, blanks as none", () => {
    expect(pipeOfRow({ campaign_id: " ", workflow_slug: null, brand_ids: ["b", null], feature_slug: "f" })).toEqual({
      campaignId: null,
      workflowSlug: null,
      brandIds: ["b"],
      featureSlug: "f",
    });
  });

  it("a sequence whose run cannot be resolved throws: no judgment goes out as nobody's work", async () => {
    execute.mockRejectedValueOnce(new Error("db down"));
    await expect(replyJudgingRunForSequence("self:abc")).rejects.toThrow("db down");
  });

  it("a sequence with no org throws", async () => {
    execute.mockResolvedValueOnce(pgResult([]));
    await expect(replyJudgingRunForSequence("self:abc")).rejects.toThrow("no org");
  });

  it("a sequence with no org but a reply org: that org's pipe-less run", async () => {
    execute.mockResolvedValueOnce(pgResult([])).mockResolvedValueOnce(pgResult([{ run_id: RUN }]));
    expect(await replyJudgingRunForSequence("self:abc", { fallbackOrgId: ORG })).toBe(RUN);
  });
});

describe("platform calls state WHOSE work they are (PLATFORM-CALLER-RUN-1024)", () => {
  const fetchMock = vi.fn();
  beforeEach(() => {
    fetchMock.mockReset();
    vi.stubGlobal("fetch", fetchMock);
    process.env.CHAT_SERVICE_URL = "http://chat";
    process.env.CHAT_SERVICE_API_KEY = "k";
  });
  afterEach(() => vi.unstubAllGlobals());
  const params = { message: "m", systemPrompt: "s", provider: "deepseek" as const, model: "deepseek-flash" as const };
  const ok = () => new Response(JSON.stringify({ content: "{}", tokensInput: 1, tokensOutput: 1, model: "x" }), { status: 200 });
  const okJudgment = () => new Response(JSON.stringify({ model: "x", answers: {}, usage: { inputTokens: 1, outputTokens: 0 } }), { status: 200 });
  const support = { kind: "platform_llm" as const, reason: "why" };

  it("completion for a run: x-run-id = our run, no support in the body", async () => {
    fetchMock.mockResolvedValue(ok());
    await platformComplete(params, { runId: RUN });
    expect(fetchMock.mock.calls[0][1].headers["x-run-id"]).toBe(RUN);
    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).not.toHaveProperty("support");
  });

  it("completion as support: body support {kind, reason}, no x-run-id", async () => {
    fetchMock.mockResolvedValue(ok());
    await platformComplete(params, { support });
    expect(fetchMock.mock.calls[0][1].headers).not.toHaveProperty("x-run-id");
    expect(JSON.parse(fetchMock.mock.calls[0][1].body).support).toEqual(support);
  });

  it("judgment for a run / as support: same contract", async () => {
    fetchMock.mockResolvedValue(okJudgment());
    await platformJudgment({ state: "s", questions: {} }, { runId: RUN });
    expect(fetchMock.mock.calls[0][1].headers["x-run-id"]).toBe(RUN);
    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).not.toHaveProperty("support");
    fetchMock.mockResolvedValue(okJudgment());
    await platformJudgment({ state: "s", questions: {} }, { support });
    expect(fetchMock.mock.calls[1][1].headers).not.toHaveProperty("x-run-id");
    expect(JSON.parse(fetchMock.mock.calls[1][1].body).support).toEqual(support);
  });

  it("a call stating neither is never sent", async () => {
    await expect(platformComplete(params, {} as never)).rejects.toThrow("caller run or its support");
    await expect(platformJudgment({ state: "s", questions: {} }, {} as never)).rejects.toThrow("caller run or its support");
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("a refused run is NOT retried without it: production never goes out as nobody's work", async () => {
    fetchMock.mockResolvedValueOnce(new Response('{"error":"runs-service refused x-run-id"}', { status: 400 }));
    await expect(platformComplete(params, { runId: RUN })).rejects.toThrow("400");
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("the support kinds are chat-service's, byte-equal", () => {
    expect([...SUPPORT_FUNCTIONS]).toEqual([
      "payment_fees", "brand_setup", "copilot", "platform_llm", "staff_experiment", "product_feature", "retired_product",
    ]);
  });
});
