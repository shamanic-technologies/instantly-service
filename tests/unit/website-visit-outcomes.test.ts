import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const execute = vi.fn();
vi.mock("../../src/db", () => ({ db: { execute: (...a: unknown[]) => execute(...a) } }));
const outcomeAnchorRun = vi.fn();
vi.mock("../../src/lib/outcome-anchor-run", () => ({
  outcomeAnchorRun: (...a: unknown[]) => outcomeAnchorRun(...a),
}));

const sendInputStates = vi.fn();
vi.mock("../../src/lib/send-input", async (orig) => ({
  ...(await orig<typeof import("../../src/lib/send-input")>()),
  sendInputStates: (...a: unknown[]) => sendInputStates(...a),
}));

import {
  buildWebsiteVisitDeclaration,
  completeWebsiteVisitPerformers,
  declareWebsiteVisits,
  websiteVisitPerformer,
} from "../../src/lib/website-visit-outcomes";

const pgResult = (rows: unknown[]) => ({ rows, rowCount: rows.length, command: "SELECT", fields: [] });
const LEAD = "8911df0f-c8fa-402d-a27f-f939bfa7d9ba";
const ORG = "06ffd332-0000-4000-8000-000000000001";
const RUN = "4c8b7a1e-1111-4222-8333-444455556666";
const CLICK = "2026-06-12T09:14:03.000Z";
const SLUG = "sales-cold-email-outreach-rampart";
const SCRIPT = { kind: "script", workflowSlug: SLUG } as const;
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
    sendInputStates.mockReset().mockImplementation(async (ids: string[]) => new Map(ids.map((id) => [id, "declared"])));
    vi.stubGlobal("fetch", fetchMock);
  });
  afterEach(() => vi.unstubAllGlobals());

  const row = (over: Record<string, unknown> = {}) => ({
    id: "seq-1",
    org_id: ORG,
    lead_id: LEAD,
    website_visit_run_id: null,
    visited_at: CLICK,
    workflow_slug: SLUG,
    send_transport: "instantly",
    website_visit_declaration: null,
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
    expect(JSON.parse(init.body)).toEqual({ ...buildWebsiteVisitDeclaration(LEAD, true, SCRIPT), occurredAt: CLICK });
    expect(JSON.parse(init.body).produced[0].performer).toEqual(SCRIPT);
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

  it("a send of ours no run can ever declare: the visit consumes NOTHING, marked declared_no_send (LEDGER-RESIDUAL-1025)", async () => {
    sendInputStates.mockResolvedValue(new Map([[LEAD, "unplaceable"]]));
    execute.mockResolvedValueOnce(pgResult([row({ website_visit_run_id: RUN })])).mockResolvedValue(pgResult([]));
    fetchMock.mockResolvedValue(new Response('{"recordedCount":1}', { status: 201 }));
    const s = await declareWebsiteVisits();
    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toEqual({ ...buildWebsiteVisitDeclaration(LEAD, false, SCRIPT), occurredAt: CLICK });
    expect(buildWebsiteVisitDeclaration(LEAD, false).consumed).toEqual([]);
    expect(JSON.stringify(execute.mock.calls[1][0])).toContain("declared_no_send");
    expect(s).toMatchObject({ declared: 1, declaredNoSend: 1 });
  });

  it("a send of ours not declared YET: waits, nothing posted", async () => {
    sendInputStates.mockResolvedValue(new Map([[LEAD, "pending"]]));
    execute.mockResolvedValueOnce(pgResult([row({ website_visit_run_id: RUN })]));
    const s = await declareWebsiteVisits();
    expect(fetchMock).not.toHaveBeenCalled();
    expect(s.waitingForSend).toBe(1);
  });

  it("reads only REAL clicks (measured, not withdrawn) on sequences not yet declared", async () => {
    execute.mockResolvedValueOnce(pgResult([]));
    await declareWebsiteVisits();
    const q = sqlText(execute.mock.calls[0]);
    expect(q).toContain("e.event_type = 'email_link_clicked' AND e.inferred = false AND e.withdrawn_at IS NULL");
    expect(q).toContain("c.website_visit_declared_at IS NULL");
  });
});

describe("websiteVisitPerformer: WHO made the visit happen (OUTCOME-PERFORMER-1106)", () => {
  it("our script whose email was clicked: the sequence's own workflow slug", () => {
    expect(websiteVisitPerformer({ workflowSlug: SLUG, sendTransport: "smtp" })).toEqual(SCRIPT);
  });
  it("no workflow on the row (an Engine's send): never a guessed script, the tracker that measured the click", () => {
    expect(websiteVisitPerformer({ workflowSlug: null, sendTransport: "instantly" })).toEqual({ kind: "tracker", name: "instantly" });
    expect(websiteVisitPerformer({ workflowSlug: " ", sendTransport: "smtp" })).toEqual({ kind: "tracker", name: "distribute.you" });
  });
});

describe("declareWebsiteVisits: every NEW occurrence states its performer", () => {
  const fetchMock = vi.fn();
  beforeEach(() => {
    execute.mockReset();
    fetchMock.mockReset();
    sendInputStates.mockReset().mockImplementation(async (ids: string[]) => new Map(ids.map((id) => [id, "declared"])));
    vi.stubGlobal("fetch", fetchMock);
  });
  afterEach(() => vi.unstubAllGlobals());
  const pending = (over: Record<string, unknown> = {}) =>
    pgResult([{ id: "seq-1", org_id: ORG, lead_id: LEAD, website_visit_run_id: RUN, visited_at: CLICK, workflow_slug: SLUG, send_transport: "instantly", website_visit_declaration: null, ...over }]);

  it("marks the performer stated only when runs-service PROVES it stored it", async () => {
    execute.mockResolvedValueOnce(pending()).mockResolvedValue(pgResult([]));
    fetchMock.mockResolvedValue(new Response('{"recordedCount":2,"alreadyRecordedCount":0,"performerStatedCount":0}', { status: 201 }));
    const s = await declareWebsiteVisits();
    expect(s).toMatchObject({ declared: 1, performerStated: 1 });
    expect(sqlText(execute.mock.calls[2])).toContain("website_visit_performer =");
    expect(JSON.stringify(execute.mock.calls[2][0])).toContain("stated");
  });

  it("an older runs-service that drops the field: declared, performer left for the completion pass", async () => {
    execute.mockResolvedValueOnce(pending()).mockResolvedValue(pgResult([]));
    fetchMock.mockResolvedValue(new Response('{"recordedCount":2,"alreadyRecordedCount":0}', { status: 201 }));
    const s = await declareWebsiteVisits();
    expect(s).toMatchObject({ declared: 1, performerStated: 0 });
    expect(execute).toHaveBeenCalledTimes(2);
  });

  it("performer_conflict (two sequences folded into one occurrence): re-declared without it, the first performer stands", async () => {
    execute.mockResolvedValueOnce(pending()).mockResolvedValue(pgResult([]));
    fetchMock
      .mockResolvedValueOnce(new Response('{"error":"performer_conflict","conflicts":[]}', { status: 409 }))
      .mockResolvedValueOnce(new Response('{"recordedCount":0,"alreadyRecordedCount":2}', { status: 200 }));
    const s = await declareWebsiteVisits();
    expect(JSON.parse(fetchMock.mock.calls[1][1].body).produced[0].performer).toBeUndefined();
    expect(s).toMatchObject({ declared: 1, performerConflict: 1, refused: 0 });
    expect(JSON.stringify(execute.mock.calls[2][0])).toContain("conflict");
  });
});

describe("completeWebsiteVisitPerformers: past occurrences state their performer once", () => {
  const fetchMock = vi.fn();
  beforeEach(() => {
    execute.mockReset();
    fetchMock.mockReset();
    vi.stubGlobal("fetch", fetchMock);
  });
  afterEach(() => vi.unstubAllGlobals());
  const past = (over: Record<string, unknown> = {}) => ({
    id: "seq-1", org_id: ORG, lead_id: LEAD, website_visit_run_id: RUN, visited_at: CLICK,
    workflow_slug: SLUG, send_transport: "instantly", website_visit_declaration: "declared", ...over,
  });

  it("dry run counts per performer and writes nothing", async () => {
    execute.mockResolvedValueOnce(pgResult([past(), past({ id: "seq-2" }), past({ id: "seq-3", workflow_slug: null })]));
    const s = await completeWebsiteVisitPerformers({ dryRun: true });
    expect(s).toMatchObject({ read: 3, byPerformer: { [`script:${SLUG}`]: 2, "tracker:instantly": 1 } });
    expect(fetchMock).not.toHaveBeenCalled();
    expect(execute).toHaveBeenCalledTimes(1);
    const q = sqlText(execute.mock.calls[0]);
    expect(q).toContain("website_visit_performer IS NULL");
    expect(q).toContain("website_visit_declaration IN ('declared'");
  });

  it("re-posts the PRODUCED item only (same run, same instant) with its performer; marks it stated on proof", async () => {
    execute.mockResolvedValueOnce(pgResult([past()])).mockResolvedValue(pgResult([]));
    fetchMock.mockResolvedValue(new Response('{"recordedCount":0,"alreadyRecordedCount":0,"performerStatedCount":1}', { status: 201 }));
    const s = await completeWebsiteVisitPerformers({ dryRun: false });
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toMatch(new RegExp(`/v1/platform-runs/${RUN}/outcomes$`));
    expect(JSON.parse(init.body)).toEqual({
      produced: [{ outcomeKey: "website_visit", subjectService: "lead-service", subjectId: LEAD, performer: SCRIPT }],
      consumed: [],
      occurredAt: CLICK,
    });
    expect(s.stated).toBe(1);
    expect(JSON.stringify(execute.mock.calls[1][0])).toContain("stated");
  });

  it("runs-service not reading the field yet: nothing marked, the pass stops", async () => {
    execute.mockResolvedValueOnce(pgResult([past(), past({ id: "seq-2" })]));
    fetchMock.mockResolvedValue(new Response('{"recordedCount":0,"alreadyRecordedCount":1}', { status: 200 }));
    const s = await completeWebsiteVisitPerformers({ dryRun: false });
    expect(s).toMatchObject({ notServedYet: 1, stated: 0 });
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(execute).toHaveBeenCalledTimes(1);
  });

  it("another performer already stored: marked conflict, never overwritten", async () => {
    execute.mockResolvedValueOnce(pgResult([past()])).mockResolvedValue(pgResult([]));
    fetchMock.mockResolvedValue(new Response('{"error":"performer_conflict"}', { status: 409 }));
    const s = await completeWebsiteVisitPerformers({ dryRun: false });
    expect(s.conflict).toBe(1);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("a transient failure leaves the row for the next tick", async () => {
    execute.mockResolvedValueOnce(pgResult([past()]));
    fetchMock.mockResolvedValue(new Response("down", { status: 502 }));
    const s = await completeWebsiteVisitPerformers({ dryRun: false });
    expect(s.retry).toBe(1);
    expect(execute).toHaveBeenCalledTimes(1);
  });
});
