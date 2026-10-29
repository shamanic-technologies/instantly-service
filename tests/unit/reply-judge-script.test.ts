import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
  judgeWithScript,
  replyJudgeSwitch,
  resetReplyJudgeSwitchMemo,
  ReplyJudgeScriptError,
} from "../../src/lib/reply-judge-script";
import { decodeCursor, encodeCursor } from "../../src/lib/reply-qualification-record";

// FACTORY-P3 next outcomes (lead_interested): the reply judge reads its SERVICE switch from workflow-service and, while
// switched, calls the platform-billed script door with the org's reply-judging run as parent.
function stubFetch(answers: Array<{ status: number; body: unknown }>) {
  const fetchMock = vi.fn(async () => {
    const a = answers.shift();
    if (!a) throw new Error("no more answers");
    return new Response(JSON.stringify(a.body), { status: a.status });
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

beforeEach(() => {
  process.env.WORKFLOW_SERVICE_URL = "http://workflow/";
  process.env.WORKFLOW_SERVICE_API_KEY = "wk";
  resetReplyJudgeSwitchMemo();
});
afterEach(() => vi.unstubAllGlobals());

describe("replyJudgeSwitch", () => {
  it("reads the service switch once a minute", async () => {
    const f = stubFetch([{ status: 200, body: { switched: true, dynasty: "dy_reply_judge" } }, { status: 200, body: { switched: false, dynasty: null } }]);
    expect(await replyJudgeSwitch(1_000)).toEqual({ switched: true, dynasty: "dy_reply_judge" });
    expect(await replyJudgeSwitch(30_000)).toEqual({ switched: true, dynasty: "dy_reply_judge" });
    expect(f).toHaveBeenCalledTimes(1);
    expect(String((f.mock.calls[0] as unknown[])[0])).toBe("http://workflow/internal/scripts/switches/service?outcome=lead_interested&callerService=instantly");
    // Rolled back (the guard or staff): our own code within a minute.
    expect(await replyJudgeSwitch(62_000)).toEqual({ switched: false, dynasty: null });
  });

  it("an unreadable state keeps the last one read; never read = our own code", async () => {
    vi.spyOn(console, "error").mockImplementation(() => {});
    stubFetch([{ status: 500, body: {} }]);
    expect(await replyJudgeSwitch(1_000)).toEqual({ switched: false, dynasty: null });
    resetReplyJudgeSwitchMemo();
    stubFetch([{ status: 200, body: { switched: true, dynasty: "dy_reply_judge" } }, { status: 502, body: {} }]);
    await replyJudgeSwitch(1_000);
    expect(await replyJudgeSwitch(70_000)).toEqual({ switched: true, dynasty: "dy_reply_judge" });
  });
});

describe("judgeWithScript", () => {
  const input = { dynasty: "dy_reply_judge", orgId: "org-1", judgingRunId: "run-j", instantlyCampaignId: "self:abc", replyText: "Yes", subject: "Re: hi", ourOutreach: null };

  it("calls the platform-billed door on the reply-judging run and reads the label + the exact exchange", async () => {
    const sent = { message: "Subject: Re: hi\n\nYes", provider: "deepseek" };
    const raw = { content: "{}", json: { classification: "lead_interested" }, runId: "chat-run" };
    const f = stubFetch([
      {
        status: 200,
        body: { runId: "script-run", scriptSlug: "lead_interested/dy_reply_judge/v2", status: "made", unit: { content: { kind: "lead_interested" } }, trace: [{ name: "llm.complete", args: {}, answer: {}, sent, raw }] },
      },
    ]);
    const out = await judgeWithScript(input);
    expect(out).toEqual({ runId: "script-run", scriptSlug: "lead_interested/dy_reply_judge/v2", status: "made", kind: "lead_interested", request: sent, response: raw });
    const [url, init] = f.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe("http://workflow/internal/scripts/run");
    expect(init.headers).toMatchObject({ "x-api-key": "wk", "x-org-id": "org-1", "x-run-id": "run-j" });
    expect(JSON.parse(init.body as string)).toEqual({
      outcome: "lead_interested",
      dynasty: "dy_reply_judge",
      subject: { service: "instantly", id: "self:abc" },
      units: { reply_received: { content: { text: "Yes", subject: "Re: hi", ourOutreach: null } } },
      recordedBy: "instantly",
      trace: true,
    });
  });

  it("made nothing (nothing of theirs to judge) is no label; a refusal names the script run", async () => {
    stubFetch([{ status: 200, body: { runId: "r1", scriptSlug: "s", status: "nothing", unit: null, trace: [] } }]);
    expect(await judgeWithScript(input)).toMatchObject({ status: "nothing", kind: null, request: null });
    stubFetch([{ status: 502, body: { error: "script lead_interested/dy_reply_judge/v2 failed: boom", details: { runId: "r2", scriptSlug: "lead_interested/dy_reply_judge/v2" } } }]);
    const err = await judgeWithScript(input).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ReplyJudgeScriptError);
    expect(err).toMatchObject({ runId: "r2", scriptSlug: "lead_interested/dy_reply_judge/v2" });
  });
});

describe("the recorded judgments cursor", () => {
  it("round-trips and refuses a foreign one", () => {
    // Postgres's own text, microseconds kept (a JS Date would cut them and re-serve rows at a page boundary).
    const at = "2026-10-29 10:00:00.123456+00";
    expect(decodeCursor(encodeCursor(at, "id-1"))).toEqual({ createdAt: at, id: "id-1" });
    expect(decodeCursor("not-a-cursor")).toBeNull();
  });
});
