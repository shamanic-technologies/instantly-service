import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const execute = vi.fn();
vi.mock("../../src/db", () => ({ db: { execute: (...a: unknown[]) => execute(...a) } }));
const outcomeAnchorRun = vi.fn();
vi.mock("../../src/lib/outcome-anchor-run", () => ({
  outcomeAnchorRun: (...a: unknown[]) => outcomeAnchorRun(...a),
}));

import { buildWebsiteVisitDeclaration, declareWebsiteVisits } from "../../src/lib/website-visit-outcomes";

const pgResult = (rows: unknown[]) => ({ rows, rowCount: rows.length, command: "SELECT", fields: [] });
const LEAD = "8911df0f-c8fa-402d-a27f-f939bfa7d9ba";
const ORG = "06ffd332-0000-4000-8000-000000000001";
const RUN = "4c8b7a1e-1111-4222-8333-444455556666";
const CLICK = "2026-06-12T09:14:03.000Z";
const sqlText = (call: unknown[]) => JSON.stringify((call[0] as { queryChunks?: unknown[] }).queryChunks ?? call[0]);

describe("buildWebsiteVisitDeclaration: what a click on our email declares (LEDGER-HISTORY-1024)", () => {
  it("produces ONE website_visit of the person's lead in lead-service, consuming their message_sent", () => {
    expect(buildWebsiteVisitDeclaration(LEAD)).toEqual({
      produced: [{ outcomeKey: "website_visit", subjectService: "lead-service", subjectId: LEAD }],
      consumed: [{ outcomeKey: "message_sent", subjectService: "lead-service", subjectId: LEAD }],
    });
  });
});

describe("declareWebsiteVisits: the sweep, past included", () => {
  const fetchMock = vi.fn();
  beforeEach(() => {
    execute.mockReset();
    fetchMock.mockReset();
    outcomeAnchorRun.mockReset();
    vi.stubGlobal("fetch", fetchMock);
  });
  afterEach(() => vi.unstubAllGlobals());

  const row = (over: Record<string, unknown> = {}) => ({
    id: "seq-1",
    org_id: ORG,
    lead_id: LEAD,
    website_visit_run_id: null,
    visited_at: CLICK,
    ...over,
  });

  it("declares on the org's spend-free website-visit run of the CLICK's day, dated at the click, run persisted first", async () => {
    outcomeAnchorRun.mockResolvedValue(RUN);
    execute
      .mockResolvedValueOnce(pgResult([row()])) // pending
      .mockResolvedValueOnce(pgResult([{ website_visit_run_id: RUN }])) // persist run
      .mockResolvedValue(pgResult([])); // mark
    fetchMock.mockResolvedValue(new Response('{"recordedCount":2}', { status: 201 }));

    const s = await declareWebsiteVisits();

    expect(s).toMatchObject({ read: 1, declared: 1 });
    expect(outcomeAnchorRun).toHaveBeenCalledWith("website-visit", ORG, "2026-06-12");
    expect(sqlText(execute.mock.calls[1])).toContain("SET website_visit_run_id");
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toMatch(new RegExp(`/v1/platform-runs/${RUN}/outcomes$`));
    expect(JSON.parse(init.body)).toEqual({ ...buildWebsiteVisitDeclaration(LEAD), occurredAt: CLICK });
    expect(sqlText(execute.mock.calls[2])).toContain("website_visit_declared_at = now()");
  });

  it("a retry reuses the persisted run: no second run", async () => {
    execute.mockResolvedValueOnce(pgResult([row({ website_visit_run_id: RUN })])).mockResolvedValue(pgResult([]));
    fetchMock.mockResolvedValue(new Response('{"recordedCount":0,"alreadyRecordedCount":2}', { status: 200 }));
    const s = await declareWebsiteVisits();
    expect(outcomeAnchorRun).not.toHaveBeenCalled();
    expect(s.declared).toBe(1);
  });

  it("a failed declaration leaves the sequence undeclared", async () => {
    execute.mockResolvedValueOnce(pgResult([row({ website_visit_run_id: RUN })]));
    fetchMock.mockResolvedValue(new Response("down", { status: 502 }));
    const s = await declareWebsiteVisits();
    expect(s.retry).toBe(1);
    expect(execute).toHaveBeenCalledTimes(1);
  });

  it("never invents a subject: no lead id = marked, no run, nothing posted", async () => {
    execute.mockResolvedValueOnce(pgResult([row({ lead_id: null })])).mockResolvedValue(pgResult([]));
    const s = await declareWebsiteVisits();
    expect(s.noLeadId).toBe(1);
    expect(outcomeAnchorRun).not.toHaveBeenCalled();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("reads only REAL clicks (measured, not withdrawn) on sequences not yet declared", async () => {
    execute.mockResolvedValueOnce(pgResult([]));
    await declareWebsiteVisits();
    const q = sqlText(execute.mock.calls[0]);
    expect(q).toContain("e.event_type = 'email_link_clicked' AND e.inferred = false AND e.withdrawn_at IS NULL");
    expect(q).toContain("c.website_visit_declared_at IS NULL");
  });
});
