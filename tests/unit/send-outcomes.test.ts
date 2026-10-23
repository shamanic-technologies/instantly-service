import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const execute = vi.fn();
vi.mock("../../src/db", () => ({ db: { execute: (...a: unknown[]) => execute(...a) } }));

import {
  buildSendOutcomeDeclaration,
  classifyDeclareAnswer,
  declareSendOutcomes,
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

describe("declareSendOutcomes: the sweep", () => {
  const fetchMock = vi.fn();
  beforeEach(() => {
    execute.mockReset();
    fetchMock.mockReset();
    vi.stubGlobal("fetch", fetchMock);
  });
  afterEach(() => vi.unstubAllGlobals());

  const pending = (over: Record<string, unknown> = {}) => ({
    hold_id: "hold-1",
    run_id: RUN,
    step: 2,
    org_id: ORG,
    lead_id: LEAD,
    matches: 1,
    ...over,
  });

  it("declares each actual hold on its own step run and marks it declared", async () => {
    execute.mockResolvedValueOnce(pgResult([pending()])).mockResolvedValue(pgResult([]));
    fetchMock.mockResolvedValue(new Response('{"recordedCount":3}', { status: 201 }));

    const s = await declareSendOutcomes();

    expect(s).toMatchObject({ read: 1, declared: 1, retry: 0 });
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toMatch(new RegExp(`/v1/platform-runs/${RUN}/outcomes$`));
    expect(init.headers["x-org-id"]).toBe(ORG);
    expect(init.headers["x-service-name"]).toBe("instantly-service");
    expect(JSON.parse(init.body)).toEqual(buildSendOutcomeDeclaration(LEAD));
    expect(sqlText(execute.mock.calls[1])).toContain("outcomes_declared_at = now()");
    expect(execute.mock.calls[1][0].queryChunks.some((c: unknown) => c === "declared")).toBe(true);
  });

  it("a replay (already_recorded) is marked declared too: nothing is declared twice", async () => {
    execute.mockResolvedValueOnce(pgResult([pending()])).mockResolvedValue(pgResult([]));
    fetchMock.mockResolvedValue(new Response('{"recordedCount":0,"alreadyRecordedCount":3}', { status: 200 }));
    expect((await declareSendOutcomes()).declared).toBe(1);
  });

  it("a failed declaration leaves the row undeclared for the next tick (no mark)", async () => {
    execute.mockResolvedValueOnce(pgResult([pending()]));
    fetchMock.mockResolvedValue(new Response("boom", { status: 503 }));
    const s = await declareSendOutcomes();
    expect(s.retry).toBe(1);
    expect(execute).toHaveBeenCalledTimes(1);
  });

  it("a network error is retried too, never thrown out of the pass", async () => {
    execute.mockResolvedValueOnce(pgResult([pending()]));
    fetchMock.mockRejectedValue(new Error("ECONNREFUSED"));
    expect((await declareSendOutcomes()).retry).toBe(1);
  });

  it("never invents a subject: no lead id = marked no_lead_id, nothing posted", async () => {
    execute.mockResolvedValueOnce(pgResult([pending({ lead_id: null })])).mockResolvedValue(pgResult([]));
    const s = await declareSendOutcomes();
    expect(s.noLeadId).toBe(1);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("two campaign rows matching one historical hold = ambiguous, nothing posted", async () => {
    execute.mockResolvedValueOnce(pgResult([pending({ matches: 2 })])).mockResolvedValue(pgResult([]));
    expect((await declareSendOutcomes()).ambiguousLead).toBe(1);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("reads only ACTUAL holds not yet declared (a real email_sent), newest first", async () => {
    execute.mockResolvedValueOnce(pgResult([]));
    await declareSendOutcomes();
    const text = sqlText(execute.mock.calls[0]);
    expect(text).toContain("sc.status = 'actual' AND sc.outcomes_declared_at IS NULL");
    expect(text).toContain("ORDER BY sc.updated_at DESC");
  });
});
