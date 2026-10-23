import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const execute = vi.fn();
vi.mock("../../src/db", () => ({ db: { execute: (...a: unknown[]) => execute(...a) } }));
const replyJudgingRunForOrg = vi.fn();
vi.mock("../../src/lib/reply-judging-run", () => ({
  replyJudgingRunForOrg: (...a: unknown[]) => replyJudgingRunForOrg(...a),
}));

import {
  buildPositiveReplyDeclaration,
  declarePositiveReplies,
} from "../../src/lib/positive-reply-outcomes";

const pgResult = (rows: unknown[]) => ({ rows, rowCount: rows.length, command: "SELECT", fields: [] });
const LEAD = "8911df0f-c8fa-402d-a27f-f939bfa7d9ba";
const ORG = "06ffd332-0000-4000-8000-000000000001";
const RUN = "4c8b7a1e-1111-4222-8333-444455556666";
const sqlText = (call: unknown[]) => JSON.stringify((call[0] as { queryChunks?: unknown[] }).queryChunks ?? call[0]);

describe("buildPositiveReplyDeclaration: what a positive reply declares", () => {
  it("produces ONE conversation (Positive reply) of the person's lead in lead-service", () => {
    expect(buildPositiveReplyDeclaration(LEAD).produced).toEqual([
      { outcomeKey: "conversation", subjectService: "lead-service", subjectId: LEAD },
    ]);
  });
  it("consumes the person's message_sent, same subject", () => {
    expect(buildPositiveReplyDeclaration(LEAD).consumed).toEqual([
      { outcomeKey: "message_sent", subjectService: "lead-service", subjectId: LEAD },
    ]);
  });
});

describe("declarePositiveReplies: the sweep", () => {
  const fetchMock = vi.fn();
  beforeEach(() => {
    execute.mockReset();
    fetchMock.mockReset();
    replyJudgingRunForOrg.mockReset();
    vi.stubGlobal("fetch", fetchMock);
  });
  afterEach(() => vi.unstubAllGlobals());

  const row = (over: Record<string, unknown> = {}) => ({
    id: "seq-1",
    org_id: ORG,
    user_id: null,
    lead_id: LEAD,
    brand_id: "brand-1",
    campaign_id: "camp-1",
    conversation_run_id: null,
    ...over,
  });

  it("declares on the org's reply-judging run of the day (where the judging spend nests), persisted BEFORE declaring", async () => {
    replyJudgingRunForOrg.mockResolvedValue(RUN);
    execute
      .mockResolvedValueOnce(pgResult([row()])) // pending
      .mockResolvedValueOnce(pgResult([{ conversation_run_id: RUN }])) // persist run
      .mockResolvedValue(pgResult([])); // mark
    fetchMock.mockResolvedValue(new Response('{"recordedCount":2}', { status: 201 }));

    const s = await declarePositiveReplies();

    expect(s).toMatchObject({ read: 1, declared: 1 });
    expect(replyJudgingRunForOrg).toHaveBeenCalledWith(ORG);
    expect(sqlText(execute.mock.calls[1])).toContain("SET conversation_run_id");
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toMatch(new RegExp(`/v1/platform-runs/${RUN}/outcomes$`));
    expect(JSON.parse(init.body)).toEqual(buildPositiveReplyDeclaration(LEAD));
    expect(sqlText(execute.mock.calls[2])).toContain("conversation_declared_at = now()");
  });

  it("a retry reuses the persisted run: no second run, nothing declared twice", async () => {
    execute.mockResolvedValueOnce(pgResult([row({ conversation_run_id: RUN })])).mockResolvedValue(pgResult([]));
    fetchMock.mockResolvedValue(new Response('{"recordedCount":0,"alreadyRecordedCount":2}', { status: 200 }));
    const s = await declarePositiveReplies();
    expect(replyJudgingRunForOrg).not.toHaveBeenCalled();
    expect(s.declared).toBe(1);
  });

  it("a failed declaration leaves the sequence undeclared (run kept for the retry)", async () => {
    execute.mockResolvedValueOnce(pgResult([row({ conversation_run_id: RUN })]));
    fetchMock.mockResolvedValue(new Response("down", { status: 502 }));
    const s = await declarePositiveReplies();
    expect(s.retry).toBe(1);
    expect(execute).toHaveBeenCalledTimes(1);
  });

  it("never invents a subject: no lead id = marked, no run, nothing posted", async () => {
    execute.mockResolvedValueOnce(pgResult([row({ lead_id: null })])).mockResolvedValue(pgResult([]));
    const s = await declarePositiveReplies();
    expect(s.noLeadId).toBe(1);
    expect(replyJudgingRunForOrg).not.toHaveBeenCalled();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("reads only sequences classified positive and not yet declared (a re-judgment never declares twice)", async () => {
    execute.mockResolvedValueOnce(pgResult([]));
    await declarePositiveReplies();
    expect(sqlText(execute.mock.calls[0])).toContain("reply_classification = 'positive' AND conversation_declared_at IS NULL");
  });
});
