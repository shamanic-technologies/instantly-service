import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const execute = vi.fn();
// `down`: the database refuses (a plain rejection, outside the vi.fn: vitest 4 fails a test on a rejection a mock
// returned, even when the code under test awaits and catches it).
const state = vi.hoisted(() => ({ down: false }));
vi.mock("../../src/db", () => ({
  db: { execute: (...a: unknown[]) => (state.down ? Promise.reject(new Error("db down")) : execute(...a)) },
}));

import {
  buildReplySendEntry,
  classifyReplySendAnswer,
  declareReplySends,
  recordReplySend,
} from "../../src/lib/reply-send-outcomes";
import { buildSendOutcomeDeclaration } from "../../src/lib/send-outcomes";

/**
 * REPLY-SENT-UNIT-1118: an answer sent in a thread is declared as the lead's `message_sent` (the key a sequence step
 * declares), on the run that asked for it, dated when it left, consuming nothing from this side.
 */
const pgResult = (rows: unknown[]) => ({ rows, rowCount: rows.length, command: "SELECT", fields: [] });
const LEAD = "8911df0f-c8fa-402d-a27f-f939bfa7d9ba";
const RUN = "4c8b7a1e-1111-4222-8333-444455556666";
const SENT = "2026-11-18T01:22:28.000Z";
const chunks = (call: unknown[]) => (call[0] as { queryChunks: unknown[] }).queryChunks;
const sqlText = (call: unknown[]) => JSON.stringify(chunks(call));
const pending = (over: Record<string, unknown> = {}) => ({ id: "row-1", run_id: RUN, lead_id: LEAD, sent_at: SENT, ...over });

describe("buildReplySendEntry: what one answer declares", () => {
  it("is the same sent outcome a sequence step produces, on the lead, dated when it left, consuming nothing here", () => {
    const entry = buildReplySendEntry({ runId: RUN, leadId: LEAD, sentAt: SENT });
    expect(entry).toEqual({
      runId: RUN,
      produced: [{ outcomeKey: "message_sent", subjectService: "lead-service", subjectId: LEAD }],
      consumed: [],
      occurredAt: SENT,
    });
    // One outcome key for every email we send, whatever thread it goes in.
    expect(entry.produced).toEqual(buildSendOutcomeDeclaration(LEAD).produced);
  });
});

describe("classifyReplySendAnswer", () => {
  const invalid = (reason: string) => JSON.stringify({ error: "invalid_runs", details: { invalid: [{ runId: RUN, reason }] } });
  it("2xx = declared (a replay, or a caller that declared the same occurrence, is the same Unit)", () => {
    expect(classifyReplySendAnswer(201, '{"recordedCount":1}')).toBe("declared");
    expect(classifyReplySendAnswer(200, '{"recordedCount":0,"alreadyRecordedCount":1}')).toBe("declared");
  });
  it("a gone run and a refused run end the row; anything unreadable is retried", () => {
    expect(classifyReplySendAnswer(422, invalid("run_not_found"))).toBe("run_gone");
    expect(classifyReplySendAnswer(422, invalid("run_outcomes_voided"))).toBe("refused");
    expect(classifyReplySendAnswer(422, "not json")).toBe("retry");
    expect(classifyReplySendAnswer(422, JSON.stringify({ error: "other" }))).toBe("retry");
    for (const status of [400, 401, 500, 502, 503]) expect(classifyReplySendAnswer(status, "x")).toBe("retry");
  });
});

describe("recordReplySend: never fails the send", () => {
  beforeEach(() => execute.mockReset());
  const record = { orgId: "org", instantlyCampaignId: "ic-1", leadEmail: "a@b.co", messageId: "<m1@x>", transport: "smtp", sentBy: "automation" };

  it("records the answer once per message, with the run that asked for it", async () => {
    execute.mockResolvedValue(pgResult([]));
    await recordReplySend({ ...record, runId: RUN });
    expect(sqlText(execute.mock.calls[0])).toContain("INSERT INTO reply_send_outcomes");
    expect(sqlText(execute.mock.calls[0])).toContain("ON CONFLICT (instantly_campaign_id, message_id) DO NOTHING");
    expect(chunks(execute.mock.calls[0])).toContain(RUN);
  });

  it("an answer nobody named a run for is recorded with no run (never an invented one), said loud", async () => {
    const loud = vi.spyOn(console, "error").mockImplementation(() => {});
    execute.mockResolvedValue(pgResult([]));
    await recordReplySend(record);
    expect(chunks(execute.mock.calls[0])).toContain(null);
    expect(loud.mock.calls[0][0]).toMatch(/no run id: message_sent undeclared/);
    loud.mockRestore();
  });

  it("a failed record is swallowed and said loud: the answer is out", async () => {
    const loud = vi.spyOn(console, "error").mockImplementation(() => {});
    state.down = true;
    let threw: unknown = null;
    try {
      await recordReplySend({ ...record, runId: RUN });
    } catch (e) {
      threw = e;
    } finally {
      state.down = false;
    }
    const said = loud.mock.calls.map((c) => String(c[0]));
    loud.mockRestore();
    expect(threw).toBeNull();
    expect(said.some((m) => /record FAILED \(message_sent undeclared\): db down/.test(m))).toBe(true);
  });
});

describe("declareReplySends: the sweep", () => {
  const fetchMock = vi.fn();
  beforeEach(() => {
    execute.mockReset();
    fetchMock.mockReset();
    vi.stubGlobal("fetch", fetchMock);
  });
  afterEach(() => vi.unstubAllGlobals());
  const markOf = (call: unknown[]) => chunks(call).find((c) => typeof c === "string" && ["declared", "run_gone", "no_lead_id", "refused"].includes(c));

  it("reads only recorded answers that name a run and are not declared, and declares each on that run", async () => {
    execute.mockResolvedValueOnce(pgResult([pending()])).mockResolvedValue(pgResult([]));
    fetchMock.mockResolvedValue(new Response('{"recordedCount":1}', { status: 201 }));

    expect(await declareReplySends()).toEqual({ pending: 1, declared: 1, noLeadId: 0, runGone: 0, refused: 0, retry: 0 });

    expect(sqlText(execute.mock.calls[0])).toContain("r.outcomes_declaration IS NULL AND r.run_id IS NOT NULL");
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toMatch(/\/v1\/platform-runs\/outcomes\/batch$/);
    expect(init.headers["x-service-name"]).toBe("instantly-service");
    expect(JSON.parse(init.body)).toEqual({
      runs: [{ runId: RUN, produced: [{ outcomeKey: "message_sent", subjectService: "lead-service", subjectId: LEAD }], consumed: [], occurredAt: SENT }],
    });
    expect(markOf(execute.mock.calls[1])).toBe("declared");
  });

  it("a thread whose lead id we do not hold is marked, never declared under an invented subject", async () => {
    execute.mockResolvedValueOnce(pgResult([pending({ lead_id: null })])).mockResolvedValue(pgResult([]));
    expect(await declareReplySends()).toMatchObject({ pending: 1, noLeadId: 1, declared: 0 });
    expect(fetchMock).not.toHaveBeenCalled();
    expect(markOf(execute.mock.calls[1])).toBe("no_lead_id");
  });

  it("an unreadable ledger leaves the row undeclared for the next tick; the other answers still go", async () => {
    const loud = vi.spyOn(console, "error").mockImplementation(() => {});
    execute.mockResolvedValueOnce(pgResult([pending(), pending({ id: "row-2", run_id: "5d9c8b2f-1111-4222-8333-444455556666" })])).mockResolvedValue(pgResult([]));
    fetchMock.mockResolvedValueOnce(new Response("boom", { status: 503 })).mockResolvedValueOnce(new Response("{}", { status: 201 }));

    expect(await declareReplySends()).toMatchObject({ pending: 2, declared: 1, retry: 1 });
    // One mark only: the retried row was not touched.
    expect(execute).toHaveBeenCalledTimes(2);
    loud.mockRestore();
  });

  it("a gone run and a refused run are kept with the reason, never resent", async () => {
    const loud = vi.spyOn(console, "error").mockImplementation(() => {});
    const invalid = (reason: string) => new Response(JSON.stringify({ error: "invalid_runs", details: { invalid: [{ runId: RUN, reason }] } }), { status: 422 });
    execute.mockResolvedValueOnce(pgResult([pending(), pending({ id: "row-2" })])).mockResolvedValue(pgResult([]));
    fetchMock.mockResolvedValueOnce(invalid("run_not_found")).mockResolvedValueOnce(invalid("run_outcomes_voided"));

    expect(await declareReplySends()).toMatchObject({ runGone: 1, refused: 1, declared: 0 });
    expect(markOf(execute.mock.calls[1])).toBe("run_gone");
    expect(markOf(execute.mock.calls[2])).toBe("refused");
    loud.mockRestore();
  });
});
