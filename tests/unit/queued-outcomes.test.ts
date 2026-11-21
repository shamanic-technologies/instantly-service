import { beforeEach, describe, expect, it, vi } from "vitest";

const mockExecute = vi.fn();
vi.mock("../../src/db", () => ({ db: { execute: (...args: unknown[]) => mockExecute(...args) } }));

import {
  QUEUED_PRODUCED_OUTCOME,
  buildQueuedOutcomeDeclaration,
  declareQueuedAtSend,
  declareQueuedOutcomes,
} from "../../src/lib/queued-outcomes";

const pgResult = (rows: unknown[] = []) => ({ rows, rowCount: rows.length });

/** Flatten a drizzle `sql` template to its text + bound params. */
function sqlOf(q: any): { text: string; params: unknown[] } {
  const parts: string[] = [];
  const params: unknown[] = [];
  const walk = (chunk: any) => {
    if (chunk == null) return;
    if (Array.isArray(chunk?.value)) parts.push(chunk.value.join(""));
    else if (Array.isArray(chunk?.queryChunks)) chunk.queryChunks.forEach(walk);
    else params.push(chunk);
  };
  walk(q);
  return { text: parts.join(""), params };
}

const fetchCalls: Array<{ url: string; headers: Record<string, string>; body: any }> = [];
function answer(...answers: Array<{ status: number; body: unknown }>) {
  const queue = [...answers];
  vi.stubGlobal("fetch", vi.fn(async (url: string, init: any) => {
    fetchCalls.push({ url, headers: init.headers, body: JSON.parse(init.body) });
    const a = queue.shift() ?? { status: 201, body: { recorded: 1 } };
    return new Response(JSON.stringify(a.body), { status: a.status });
  }));
}

const updates = () =>
  mockExecute.mock.calls.map(([q]) => sqlOf(q)).filter((q) => q.text.includes("UPDATE sequence_costs"));

beforeEach(() => {
  mockExecute.mockReset();
  mockExecute.mockResolvedValue(pgResult());
  fetchCalls.length = 0;
  process.env.RUNS_SERVICE_URL = "http://runs";
});

describe("OUTREACH-3WAYS-1121: email_queued at the queue decision", () => {
  it("states ONE produced email_queued about the lead-service person, the subject message_sent uses, nothing consumed", () => {
    expect(QUEUED_PRODUCED_OUTCOME).toBe("email_queued");
    expect(buildQueuedOutcomeDeclaration("lead-1")).toEqual({
      produced: [{ outcomeKey: "email_queued", subjectService: "lead-service", subjectId: "lead-1" }],
      consumed: [],
    });
  });

  it("declares each queued step on its own run, scoped to the org, undated (= now), and marks it declared", async () => {
    answer({ status: 201, body: {} }, { status: 200, body: { alreadyRecorded: 1 } });
    const org = "11111111-1111-4111-8111-111111111111";

    await declareQueuedAtSend([{ holdId: "h1", runId: "run-1" }, { holdId: "h2", runId: "run-2" }], org, "lead-1");

    expect(fetchCalls.map((c) => c.url)).toEqual(["http://runs/v1/platform-runs/run-1/outcomes", "http://runs/v1/platform-runs/run-2/outcomes"]);
    for (const c of fetchCalls) {
      expect(c.headers["x-org-id"]).toBe(org);
      expect(c.body.occurredAt).toBeUndefined();
      expect(c.body.produced).toEqual([{ outcomeKey: "email_queued", subjectService: "lead-service", subjectId: "lead-1" }]);
    }
    const u = updates();
    expect(u).toHaveLength(2);
    expect(u[0].params).toContain("declared");
    expect(u[0].text).toContain("queued_declaration =");
  });

  it("a runs-service outage never throws at the send and leaves the step pending for the sweep", async () => {
    answer({ status: 503, body: { error: "down" } });
    await expect(declareQueuedAtSend([{ holdId: "h1", runId: "run-1" }], "org", "lead-1")).resolves.toBeUndefined();
    expect(updates()).toHaveLength(0);

    vi.stubGlobal("fetch", vi.fn(async () => { throw new Error("ECONNREFUSED"); }));
    await expect(declareQueuedAtSend([{ holdId: "h1", runId: "run-1" }], "org", "lead-1")).resolves.toBeUndefined();
    expect(updates()).toHaveLength(0);
  });

  it("no lead id = marked no_lead_id, never declared under an invented subject", async () => {
    answer();
    await declareQueuedAtSend([{ holdId: "h1", runId: "run-1" }], "org", undefined);
    expect(fetchCalls).toHaveLength(0);
    expect(updates()[0].params).toContain("no_lead_id");
  });

  it("a gone run is marked run_gone; a refusal is marked refused (never resent)", async () => {
    answer({ status: 404, body: { error: "Run not found" } }, { status: 409, body: { error: "run_org_mismatch" } });
    await declareQueuedAtSend([{ holdId: "h1", runId: "run-1" }, { holdId: "h2", runId: "run-2" }], "org", "lead-1");
    const u = updates();
    expect(u[0].params).toContain("run_gone");
    expect(u[1].params).toContain("refused");
  });

  it("the sweep declares only the send path's pending rows, dated at the queue decision", async () => {
    mockExecute.mockImplementation(async (q: unknown) => {
      const { text } = sqlOf(q);
      if (text.includes("SELECT sc.id")) {
        return pgResult([
          { id: "h1", run_id: "run-1", lead_id: "lead-1", org_id: "org", queued_at: "2026-11-21T07:24:59.000Z" },
          { id: "h2", run_id: "run-2", lead_id: null, org_id: "org", queued_at: "2026-11-21T07:25:00.000Z" },
        ]);
      }
      return pgResult();
    });
    answer({ status: 201, body: {} });

    const summary = await declareQueuedOutcomes();

    const select = sqlOf(mockExecute.mock.calls[0][0]);
    expect(select.text).toContain("sc.queued_declaration =");
    expect(select.params).toContain("pending");
    expect(fetchCalls).toHaveLength(1);
    expect(fetchCalls[0].body.occurredAt).toBe("2026-11-21T07:24:59.000Z");
    expect(fetchCalls[0].body.produced[0]).toMatchObject({ outcomeKey: "email_queued", subjectId: "lead-1" });
    expect(summary).toMatchObject({ pending: 2, declared: 1, noLeadId: 1 });
  });

  it("a queue date runs-service finds before the run's start is re-sent once at the run's start", async () => {
    mockExecute.mockImplementation(async (q: unknown) =>
      sqlOf(q).text.includes("SELECT sc.id")
        ? pgResult([{ id: "h1", run_id: "run-1", lead_id: "lead-1", org_id: "org", queued_at: "2026-11-21T07:24:59.000Z" }])
        : pgResult(),
    );
    answer({ status: 400, body: { error: "invalid_occurred_at" } }, { status: 201, body: {} });

    const summary = await declareQueuedOutcomes();

    expect(fetchCalls.map((c) => c.body.occurredAt)).toEqual(["2026-11-21T07:24:59.000Z", "run_started_at"]);
    expect(summary.declared).toBe(1);
  });
});
