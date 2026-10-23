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
  REPLY_JUDGING_TASK,
  replyJudgingRunForOrg,
  replyJudgingRunForSequence,
  resetReplyJudgingRunMemo,
  utcDay,
} from "../../src/lib/reply-judging-run";
import { platformComplete } from "../../src/lib/chat-client";

const pgResult = (rows: unknown[]) => ({ rows, rowCount: rows.length, command: "SELECT", fields: [] });
const ORG = "06ffd332-0000-4000-8000-000000000001";
const RUN = "4c8b7a1e-1111-4222-8333-444455556666";
const AS_OF = new Date("2026-10-23T23:59:00Z");

describe("replyJudgingRunForOrg: one reply-judging run per (org, UTC day)", () => {
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

  it("creates the day's run once, closes it (a cost anchor, not a job) and stores it", async () => {
    execute.mockResolvedValueOnce(pgResult([])).mockResolvedValueOnce(pgResult([{ run_id: RUN }]));
    expect(await replyJudgingRunForOrg(ORG, AS_OF)).toBe(RUN);
    expect(createRun.mock.calls[0][0]).toEqual({ serviceName: "instantly-service", taskName: REPLY_JUDGING_TASK });
    expect(createRun.mock.calls[0][1]).toMatchObject({ orgId: ORG });
    expect(updateRun).toHaveBeenCalledWith(RUN, "completed", expect.objectContaining({ orgId: ORG }));
    // memoized: the next call that day reads nothing
    expect(await replyJudgingRunForOrg(ORG, AS_OF)).toBe(RUN);
    expect(execute).toHaveBeenCalledTimes(2);
  });

  it("reuses a stored run: no new run", async () => {
    execute.mockResolvedValueOnce(pgResult([{ run_id: RUN }]));
    expect(await replyJudgingRunForOrg(ORG, AS_OF)).toBe(RUN);
    expect(createRun).not.toHaveBeenCalled();
  });

  it("a concurrent creator wins the row: its run is the day's run", async () => {
    execute
      .mockResolvedValueOnce(pgResult([]))
      .mockResolvedValueOnce(pgResult([])) // insert lost
      .mockResolvedValueOnce(pgResult([{ run_id: "theirs" }]));
    expect(await replyJudgingRunForOrg(ORG, AS_OF)).toBe("theirs");
  });

  it("a sequence whose run cannot be resolved returns null (the classification still runs)", async () => {
    execute.mockRejectedValueOnce(new Error("db down"));
    expect(await replyJudgingRunForSequence("self:abc")).toBeNull();
  });
});

describe("platformComplete: the call is made FOR a run of ours", () => {
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

  it("sends x-run-id = the parent run", async () => {
    fetchMock.mockResolvedValue(ok());
    await platformComplete(params, { parentRunId: RUN });
    expect(fetchMock.mock.calls[0][1].headers["x-run-id"]).toBe(RUN);
  });

  it("no parent = no header, byte-identical to before", async () => {
    fetchMock.mockResolvedValue(ok());
    await platformComplete(params);
    expect(fetchMock.mock.calls[0][1].headers).not.toHaveProperty("x-run-id");
  });

  it("a refused parent never costs the classification: retried once unparented", async () => {
    fetchMock
      .mockResolvedValueOnce(new Response('{"error":"runs-service refused x-run-id \\"x\\" as the parent run"}', { status: 400 }))
      .mockResolvedValueOnce(ok());
    await platformComplete(params, { parentRunId: RUN });
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(fetchMock.mock.calls[1][1].headers).not.toHaveProperty("x-run-id");
  });
});
