import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const execute = vi.fn();
vi.mock("../../src/db", () => ({ db: { execute: (...a: unknown[]) => execute(...a) } }));

import {
  buildSendOutcomeDeclaration,
  classifyDeclareAnswer,
  classifyPendingSend,
  declareSendOutcomes,
  markForInvalidRun,
  planRuns,
  readBatchAnswer,
  runSendOutcomesBackfill,
  type PendingSendOutcome,
} from "../../src/lib/send-outcomes";

const pgResult = (rows: unknown[]) => ({ rows, rowCount: rows.length, command: "SELECT", fields: [] });
const LEAD = "8911df0f-c8fa-402d-a27f-f939bfa7d9ba";
const ORG = "06ffd332-0000-4000-8000-000000000001";
const RUN = "4c8b7a1e-1111-4222-8333-444455556666";

/** Every SQL text the sweep ran, flattened from drizzle's `sql` chunks. */
const sqlText = (call: unknown[]) =>
  JSON.stringify((call[0] as { queryChunks?: unknown[] }).queryChunks ?? call[0]);

describe("buildSendOutcomeDeclaration: what one sent email declares", () => {
  it("produces ONE message_sent of the person's lead in lead-service", () => {
    expect(buildSendOutcomeDeclaration(LEAD).produced).toEqual([
      { outcomeKey: "message_sent", subjectService: "lead-service", subjectId: LEAD },
    ]);
  });

  it("consumes the person's message_written and email_checked, same subject", () => {
    expect(buildSendOutcomeDeclaration(LEAD).consumed).toEqual([
      { outcomeKey: "message_written", subjectService: "lead-service", subjectId: LEAD },
      { outcomeKey: "email_checked", subjectService: "lead-service", subjectId: LEAD },
    ]);
  });

  it("states no quantity (one email = one occurrence)", () => {
    expect(buildSendOutcomeDeclaration(LEAD).produced[0]).not.toHaveProperty("quantity");
  });
});

describe("classifyDeclareAnswer", () => {
  it("201 recorded and 200 already_recorded both end the row", () => {
    expect(classifyDeclareAnswer(201, "{}")).toBe("declared");
    expect(classifyDeclareAnswer(200, '{"alreadyRecordedCount":1}')).toBe("declared");
  });
  it("a purged run can never be declared", () => {
    expect(classifyDeclareAnswer(404, '{"error":"Run not found"}')).toBe("run_gone");
  });
  it("a fall-through 404 (route not served) is retried, never read as a purged run", () => {
    expect(classifyDeclareAnswer(404, '{"error":"Not found"}')).toBe("retry");
  });
  it("an org mismatch is refused for good", () => {
    expect(classifyDeclareAnswer(409, '{"error":"run_org_mismatch"}')).toBe("refused");
  });
  it("an unknown outcome key, a 5xx or an unreadable catalogue are retried, never marked", () => {
    expect(classifyDeclareAnswer(400, '{"error":"unknown_outcome_key"}')).toBe("retry");
    expect(classifyDeclareAnswer(502, "outcome_catalogue_unavailable")).toBe("retry");
    expect(classifyDeclareAnswer(500, "")).toBe("retry");
    expect(classifyDeclareAnswer(401, "")).toBe("retry");
  });
});

const SENT = "2026-06-12T09:14:03.000Z";
const RUN2 = "5d9c8b2f-2222-4333-8444-555566667777";

const pendingRow = (over: Record<string, unknown> = {}) => ({
  hold_id: "hold-1",
  run_id: RUN,
  step: 2,
  status: "actual",
  org_id: ORG,
  lead_id: LEAD,
  matches: 1,
  sent_at: SENT,
  ...over,
});
const pending = (over: Partial<PendingSendOutcome> = {}): PendingSendOutcome => ({
  holdId: "hold-1", runId: RUN, step: 2, status: "actual", orgId: ORG, leadId: LEAD, matches: 1, sentAt: SENT, ...over,
});

/** The marks the pass wrote: [mark, hold ids] per UPDATE. */
const marks = () =>
  execute.mock.calls
    .filter((c) => sqlText(c).includes("UPDATE sequence_costs"))
    .map((c) => {
      const chunks = (c[0] as { queryChunks: unknown[] }).queryChunks;
      return chunks.filter((x) => typeof x === "string") as string[];
    });

describe("classifyPendingSend: never an invented subject or date", () => {
  it("declares a hold with one lead and a real sent email", () => {
    expect(classifyPendingSend(pending())).toBe("declare");
  });
  it("two campaign rows for one historical hold = ambiguous", () => {
    expect(classifyPendingSend(pending({ matches: 2 }))).toBe("ambiguous_lead");
  });
  it("no lead id = no_lead_id", () => {
    expect(classifyPendingSend(pending({ leadId: null }))).toBe("no_lead_id");
  });
  it("no real email_sent at the step = no_send_evidence (a failure is never an outcome)", () => {
    expect(classifyPendingSend(pending({ sentAt: null }))).toBe("no_send_evidence");
  });
});

describe("planRuns: one entry per run", () => {
  it("folds the two pre-0038 holds of one step run into ONE run, dated at the earliest send", () => {
    const { runs, marks: m } = planRuns([
      pending({ holdId: "a", sentAt: "2026-06-12T09:15:00.000Z" }),
      pending({ holdId: "b", sentAt: SENT }),
    ]);
    expect(m).toEqual([]);
    expect(runs).toEqual([{ runId: RUN, leadId: LEAD, occurredAt: SENT, holdIds: ["a", "b"] }]);
  });
  it("a run whose holds name two leads is ambiguous, never guessed", () => {
    const { runs, marks: m } = planRuns([pending({ holdId: "a" }), pending({ holdId: "b", leadId: "other" })]);
    expect(runs).toEqual([]);
    expect(m).toEqual([{ holdIds: ["a", "b"], mark: "ambiguous_lead" }]);
  });
});

describe("readBatchAnswer / markForInvalidRun", () => {
  it("201 and 200 (replay) are both done", () => {
    expect(readBatchAnswer(201, "{}")).toEqual({ ok: true });
    expect(readBatchAnswer(200, "{}")).toEqual({ ok: true });
  });
  it("a 422 invalid_runs returns the per-run list (nothing was written)", () => {
    const body = JSON.stringify({ error: "invalid_runs", details: { invalid: [{ runId: RUN, reason: "run_not_found" }] } });
    expect(readBatchAnswer(422, body)).toEqual({ ok: false, invalid: [{ runId: RUN, reason: "run_not_found" }] });
  });
  it("anything else throws loud: a fall-through 404 (route not live), 5xx, unknown key", () => {
    expect(() => readBatchAnswer(404, "Not found")).toThrow(/404/);
    expect(() => readBatchAnswer(502, "boom")).toThrow(/502/);
    expect(() => readBatchAnswer(400, '{"error":"unknown_outcome_key"}')).toThrow(/400/);
  });
  it("a purged run is gone; an early stamp is re-dated at the run's start once, then refused", () => {
    expect(markForInvalidRun("run_not_found", SENT)).toBe("run_gone");
    expect(markForInvalidRun("invalid_occurred_at", SENT)).toBe("retry_at_run_start");
    expect(markForInvalidRun("invalid_occurred_at", "run_started_at")).toBe("refused");
    expect(markForInvalidRun("run_outcomes_voided", SENT)).toBe("refused");
    expect(markForInvalidRun("run_org_mismatch", SENT)).toBe("refused");
  });
});

describe("declareSendOutcomes: the live sweep", () => {
  const fetchMock = vi.fn();
  beforeEach(() => {
    execute.mockReset();
    fetchMock.mockReset();
    vi.stubGlobal("fetch", fetchMock);
  });
  afterEach(() => vi.unstubAllGlobals());

  it("declares through the batch door, dated when the email LEFT, and marks the hold declared", async () => {
    execute.mockResolvedValueOnce(pgResult([pendingRow()])).mockResolvedValue(pgResult([]));
    fetchMock.mockResolvedValue(new Response('{"recordedCount":3}', { status: 201 }));

    const s = await declareSendOutcomes();

    expect(s).toMatchObject({ read: 1, declared: 1 });
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toMatch(/\/v1\/platform-runs\/outcomes\/batch$/);
    expect(init.headers["x-service-name"]).toBe("instantly-service");
    expect(JSON.parse(init.body)).toEqual({ runs: [{ runId: RUN, ...buildSendOutcomeDeclaration(LEAD), occurredAt: SENT }] });
    expect(marks()).toHaveLength(1);
    expect(sqlText(execute.mock.calls[1])).toContain("outcomes_declared_at = now()");
    expect(execute.mock.calls[1][0].queryChunks.some((c: unknown) => c === "declared")).toBe(true);
  });

  it("a replay (200 already_recorded) is marked declared too: nothing is declared twice", async () => {
    execute.mockResolvedValueOnce(pgResult([pendingRow()])).mockResolvedValue(pgResult([]));
    fetchMock.mockResolvedValue(new Response('{"recordedCount":0,"alreadyRecordedCount":3}', { status: 200 }));
    expect((await declareSendOutcomes()).declared).toBe(1);
  });

  it("a failed batch throws and leaves every row undeclared (no mark)", async () => {
    execute.mockResolvedValueOnce(pgResult([pendingRow()]));
    fetchMock.mockResolvedValue(new Response("boom", { status: 503 }));
    await expect(declareSendOutcomes()).rejects.toThrow(/503/);
    expect(execute).toHaveBeenCalledTimes(1);
  });

  it("a 422 marks the gone run, re-dates the early one at run_started_at, declares the rest", async () => {
    execute
      .mockResolvedValueOnce(pgResult([pendingRow(), pendingRow({ hold_id: "hold-2", run_id: RUN2 })]))
      .mockResolvedValue(pgResult([]));
    fetchMock
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ error: "invalid_runs", details: { invalid: [{ runId: RUN, reason: "run_not_found" }, { runId: RUN2, reason: "invalid_occurred_at" }] } }), { status: 422 }),
      )
      .mockResolvedValueOnce(new Response("{}", { status: 201 }));

    const s = await declareSendOutcomes();

    expect(s).toMatchObject({ declared: 1, runGone: 1, datedAtRunStart: 1 });
    const second = JSON.parse(fetchMock.mock.calls[1][1].body);
    expect(second.runs).toEqual([{ runId: RUN2, ...buildSendOutcomeDeclaration(LEAD), occurredAt: "run_started_at" }]);
  });

  it("never invents a subject: no lead id = marked no_lead_id, nothing posted", async () => {
    execute.mockResolvedValueOnce(pgResult([pendingRow({ lead_id: null })])).mockResolvedValue(pgResult([]));
    expect((await declareSendOutcomes()).noLeadId).toBe(1);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("never invents a date: no real email_sent = marked no_send_evidence, nothing posted", async () => {
    execute.mockResolvedValueOnce(pgResult([pendingRow({ sent_at: null })])).mockResolvedValue(pgResult([]));
    expect((await declareSendOutcomes()).noSendEvidence).toBe(1);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("reads only ACTUAL holds not yet declared, newest first, with the step's real email_sent", async () => {
    execute.mockResolvedValueOnce(pgResult([]));
    await declareSendOutcomes();
    const text = sqlText(execute.mock.calls[0]);
    expect(text).toContain("sc.status = 'actual' AND sc.outcomes_declared_at IS NULL");
    expect(text).toContain("ORDER BY sc.updated_at DESC");
    expect(text).toContain("ev.event_type = 'email_sent' AND ev.step = sc.step AND ev.withdrawn_at IS NULL");
  });
});

describe("runSendOutcomesBackfill: the past, same declaration", () => {
  const fetchMock = vi.fn();
  beforeEach(() => {
    execute.mockReset();
    fetchMock.mockReset();
    vi.stubGlobal("fetch", fetchMock);
  });
  afterEach(() => vi.unstubAllGlobals());

  it("walks every undeclared hold by keyset until empty; a cancelled hold whose email left is declared, one that never left is skipped unmarked", async () => {
    execute
      .mockResolvedValueOnce(
        pgResult([
          pendingRow({ hold_id: "a", status: "cancelled" }),
          pendingRow({ hold_id: "b", run_id: RUN2, status: "cancelled", sent_at: null }),
        ]),
      )
      .mockResolvedValueOnce(pgResult([])) // mark declared
      .mockResolvedValueOnce(pgResult([])); // next page: empty
    fetchMock.mockResolvedValue(new Response("{}", { status: 201 }));

    const s = await runSendOutcomesBackfill({ dryRun: false });

    expect(s).toMatchObject({ read: 1, declared: 1, noSendEvidence: 0 });
    expect(JSON.parse(fetchMock.mock.calls[0][1].body).runs.map((r: { runId: string }) => r.runId)).toEqual([RUN]);
    const pageSql = sqlText(execute.mock.calls[0]);
    expect(pageSql).toContain("sc.outcomes_declared_at IS NULL AND (sc.status = 'actual' OR (sc.status = 'cancelled' AND ");
    // One email, one occurrence: a cancelled retry with an actual (or newer) sibling on another run never declares.
    expect(pageSql).toContain("o.run_id <> sc.run_id AND o.step = sc.step");
    expect(pageSql).toContain("o.status = 'actual' OR (o.status = 'cancelled' AND o.created_at > sc.created_at)");
    expect(pageSql).toContain(" AND sc.id > ");
    // The next page starts AFTER the last hold walked ("b"), skipped or not.
    expect(execute.mock.calls[2][0].queryChunks).toContain("b");
  });

  it("a dry run plans and writes nothing", async () => {
    execute.mockResolvedValueOnce(pgResult([pendingRow(), pendingRow({ hold_id: "c", lead_id: null })])).mockResolvedValueOnce(pgResult([]));
    const s = await runSendOutcomesBackfill({ dryRun: true });
    expect(s).toMatchObject({ read: 2, declared: 1, noLeadId: 1 });
    expect(fetchMock).not.toHaveBeenCalled();
    expect(marks()).toEqual([]);
  });

  it("redate mode re-states the ALREADY declared holds with redate:true and counts what moved", async () => {
    execute.mockResolvedValueOnce(pgResult([pendingRow()])).mockResolvedValueOnce(pgResult([])).mockResolvedValueOnce(pgResult([]));
    fetchMock.mockResolvedValue(new Response('{"recordedCount":0,"redatedCount":3,"alreadyRecordedCount":0}', { status: 200 }));

    const s = await runSendOutcomesBackfill({ dryRun: false, redate: true });

    expect(s).toMatchObject({ declared: 1, redated: 3 });
    expect(sqlText(execute.mock.calls[0])).toContain("sc.outcomes_declaration = 'declared'");
    expect(JSON.parse(fetchMock.mock.calls[0][1].body).runs[0]).toMatchObject({ runId: RUN, occurredAt: SENT, redate: true });
  });
});
