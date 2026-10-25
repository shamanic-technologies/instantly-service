import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const execute = vi.fn();
vi.mock("../../src/db", () => ({ db: { execute: (...a: unknown[]) => execute(...a) } }));
const replyJudgingRunFor = vi.fn();
vi.mock("../../src/lib/reply-judging-run", async (orig) => ({
  ...(await orig<typeof import("../../src/lib/reply-judging-run")>()),
  replyJudgingRunFor: (...a: unknown[]) => replyJudgingRunFor(...a),
}));
const sendInputStates = vi.fn();
vi.mock("../../src/lib/send-input", async (orig) => ({
  ...(await orig<typeof import("../../src/lib/send-input")>()),
  sendInputStates: (...a: unknown[]) => sendInputStates(...a),
}));

import {
  POSITIVE_REPLY_EVENT_TYPES,
  buildPositiveReplyDeclaration,
  declarePositiveReplies,
  redatePastPositiveReplies,
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
    replyJudgingRunFor.mockReset();
    sendInputStates.mockReset().mockImplementation(async (ids: string[]) => new Map(ids.map((id) => [id, "declared"])));
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
    workflow_slug: "cold-email-v3",
    feature_slug: "sales-cold-email",
    brand_ids: ["brand-1"],
    conversation_run_id: null,
    ...over,
  });

  it("declares on the org's reply-judging run of the day ON THE SEQUENCE'S PIPE (where the judging spend nests), persisted BEFORE declaring", async () => {
    replyJudgingRunFor.mockResolvedValue(RUN);
    execute
      .mockResolvedValueOnce(pgResult([row()])) // pending
      .mockResolvedValueOnce(pgResult([{ conversation_run_id: RUN }])) // persist run
      .mockResolvedValue(pgResult([])); // mark
    fetchMock.mockResolvedValue(new Response('{"recordedCount":2}', { status: 201 }));

    const s = await declarePositiveReplies();

    expect(s).toMatchObject({ read: 1, declared: 1 });
    expect(replyJudgingRunFor).toHaveBeenCalledWith(ORG, {
      campaignId: "camp-1",
      workflowSlug: "cold-email-v3",
      brandIds: ["brand-1"],
      featureSlug: "sales-cold-email",
    });
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
    expect(replyJudgingRunFor).not.toHaveBeenCalled();
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
    expect(replyJudgingRunFor).not.toHaveBeenCalled();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("a send of ours no run can ever declare: declared consuming NOTHING, marked declared_no_send (LEDGER-RESIDUAL-1025)", async () => {
    sendInputStates.mockResolvedValue(new Map([[LEAD, "unplaceable"]]));
    execute.mockResolvedValueOnce(pgResult([row({ conversation_run_id: RUN })])).mockResolvedValue(pgResult([]));
    fetchMock.mockResolvedValue(new Response('{"recordedCount":1}', { status: 201 }));
    const s = await declarePositiveReplies();
    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toEqual(buildPositiveReplyDeclaration(LEAD, false));
    expect(buildPositiveReplyDeclaration(LEAD, false).consumed).toEqual([]);
    expect(JSON.stringify(execute.mock.calls[1][0])).toContain("declared_no_send");
    expect(s).toMatchObject({ declared: 1, declaredNoSend: 1 });
  });

  it("a send of ours not declared YET: waits, nothing posted, nothing marked", async () => {
    sendInputStates.mockResolvedValue(new Map([[LEAD, "pending"]]));
    execute.mockResolvedValueOnce(pgResult([row({ conversation_run_id: RUN })]));
    const s = await declarePositiveReplies();
    expect(fetchMock).not.toHaveBeenCalled();
    expect(execute).toHaveBeenCalledTimes(1);
    expect(s.waitingForSend).toBe(1);
  });

  it("reads only sequences classified positive and not yet declared (a re-judgment never declares twice)", async () => {
    execute.mockResolvedValueOnce(pgResult([]));
    await declarePositiveReplies();
    expect(sqlText(execute.mock.calls[0])).toContain("reply_classification = 'positive' AND conversation_declared_at IS NULL");
  });
});

describe("redatePastPositiveReplies: a past positive reply counts on the day it was written (LEDGER-HISTORY-1024)", () => {
  const fetchMock = vi.fn();
  beforeEach(() => {
    execute.mockReset();
    fetchMock.mockReset();
    vi.stubGlobal("fetch", fetchMock);
  });
  afterEach(() => vi.unstubAllGlobals());

  it("re-states each spend-free per-sequence run dated at the reply, through the batch door with redate", async () => {
    execute.mockResolvedValueOnce(
      pgResult([
        { run_id: RUN, lead_id: LEAD, replied_at: "2026-07-03T10:00:00.000Z" },
        { run_id: "other-run", lead_id: LEAD, replied_at: null },
      ]),
    );
    fetchMock.mockResolvedValue(new Response('{"recordedCount":0,"redatedCount":1}', { status: 200 }));

    const s = await redatePastPositiveReplies({ dryRun: false });

    expect(s).toEqual({ read: 2, restated: 1, redated: 1, noReplyEvidence: 1 });
    const q = sqlText(execute.mock.calls[0]);
    expect(q).toContain("NOT EXISTS (SELECT 1 FROM reply_judging_runs j");
    const body = JSON.parse(fetchMock.mock.calls[0][1].body);
    expect(body.runs).toEqual([{ runId: RUN, ...buildPositiveReplyDeclaration(LEAD), occurredAt: "2026-07-03T10:00:00.000Z", redate: true }]);
  });

  it("dry run posts nothing", async () => {
    execute.mockResolvedValueOnce(pgResult([{ run_id: RUN, lead_id: LEAD, replied_at: "2026-07-03T10:00:00.000Z" }]));
    const s = await redatePastPositiveReplies({ dryRun: true });
    expect(s.restated).toBe(1);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("dates on positive-classified kinds only (a referral is not a positive reply)", () => {
    expect(POSITIVE_REPLY_EVENT_TYPES).toEqual(expect.arrayContaining(["lead_interested", "lead_info_requested", "lead_meeting_requested", "lead_meeting_booked", "lead_closed"]));
    expect(POSITIVE_REPLY_EVENT_TYPES).not.toContain("lead_referral");
  });
});
