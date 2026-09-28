import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";
import express from "express";
import { CAMPAIGN_CHILD_TABLES } from "../../src/lib/transfer-brand";

const mockExecute = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    execute: (...args: unknown[]) => mockExecute(...args),
  },
}));

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function flatten(q: any): { text: string; params: unknown[] } {
  const { PgDialect } = require("drizzle-orm/pg-core");
  const out = new PgDialect().sqlToQuery(q);
  return { text: out.sql.replace(/\s+/g, " "), params: out.params };
}
const sqlText = (q: unknown) => flatten(q).text;
const sqlParams = (q: unknown) => flatten(q).params;

async function createApp() {
  const transferBrandRouter = (await import("../../src/routes/transfer-brand")).default;
  const app = express();
  app.use(express.json());
  app.use(transferBrandRouter);
  return app;
}

const IDS = ["ic-1", "self:ic-2"];

/**
 * Answers each statement the way Postgres would: the one id-resolution query
 * returns the brand's campaign ids, the multi-brand count returns its count,
 * every write reports `rowCount` rows.
 */
function respond(opts: { rowCount?: number; ids?: unknown; multi?: number } = {}) {
  return async (q: unknown) => {
    const text = sqlText(q);
    if (text.includes("array_agg(instantly_campaign_id)")) {
      return { rowCount: 1, rows: [opts.ids === undefined ? { ids: IDS } : { ids: opts.ids }] };
    }
    if (text.includes("count(*)::int AS n")) return { rowCount: 1, rows: [{ n: opts.multi ?? 0 }] };
    return { rowCount: opts.rowCount ?? 2, rows: [] };
  };
}

const post = async (body: Record<string, unknown>) => request(await createApp()).post("/").send(body);
const BODY = { sourceBrandId: "brand-1", sourceOrgId: "org-a", targetOrgId: "org-b" };

describe("POST /internal/transfer-brand", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("returns 400 for invalid body (missing fields)", async () => {
    const res = await post({ sourceBrandId: "b1" });

    expect(res.status).toBe(400);
    expect(mockExecute).not.toHaveBeenCalled();
  });

  it("returns 400 for empty body", async () => {
    const res = await post({});

    expect(res.status).toBe(400);
    expect(mockExecute).not.toHaveBeenCalled();
  });

  it("returns 400 when using old brandId field", async () => {
    const res = await post({ brandId: "brand-1", sourceOrgId: "org-a", targetOrgId: "org-b" });

    expect(res.status).toBe(400);
    expect(mockExecute).not.toHaveBeenCalled();
  });

  it("moves the campaigns, resolves their ids once, then every child table, then gold, then copies opt-outs", async () => {
    mockExecute.mockImplementation(respond());

    const res = await post(BODY);

    expect(res.status).toBe(200);
    expect(res.body.skipped).toBeUndefined();
    expect(res.body.updatedTables.map((t: { tableName: string }) => t.tableName)).toEqual([
      "instantly_campaigns",
      ...CAMPAIGN_CHILD_TABLES,
      "instantly_lead_status_current",
      "instantly_lead_optouts_raw (copied)",
    ]);
    const texts = mockExecute.mock.calls.map((c) => sqlText(c[0]));
    // Campaigns FIRST: a writer stamping org_id off the campaign row lands under the target mid-transfer.
    expect(texts[0]).toContain("UPDATE instantly_campaigns");
    // Then the campaign set, ONCE. It spans BOTH orgs, or a re-run after the
    // campaigns moved would find nothing and the children would never follow.
    const resolve = texts[1];
    expect(resolve).toContain("array_agg(instantly_campaign_id)");
    expect(resolve).toContain("FROM instantly_campaigns");
    expect(resolve).toContain("org_id IN (");
    expect(sqlParams(mockExecute.mock.calls[1][0])).toEqual(expect.arrayContaining(["org-a", "org-b"]));
    expect(texts.filter((t) => t.includes("array_agg(instantly_campaign_id)"))).toHaveLength(1);

    for (const [i, table] of CAMPAIGN_CHILD_TABLES.entries()) {
      const t = texts[i + 2];
      expect(t).toContain(`UPDATE "${table}"`);
      expect(t).toContain("WHERE org_id =");
      expect(t).toContain("instantly_campaign_id = ANY(");
      expect(sqlParams(mockExecute.mock.calls[i + 2][0])).toEqual(["org-b", "org-a", IDS]);
    }
    const gold = texts[CAMPAIGN_CHILD_TABLES.length + 2];
    expect(gold).toContain("UPDATE instantly_lead_status_current");
    expect(gold).toContain("brand_ids = c.brand_ids");
    expect(gold).toContain("g.instantly_campaign_id = ANY(");
    const optouts = texts[CAMPAIGN_CHILD_TABLES.length + 3];
    expect(optouts).toContain("INSERT INTO instantly_lead_optouts_raw");
    expect(optouts).toContain("transferredFromOptoutId");
    expect(optouts).toContain("instantly_lead_optout_withdrawals");
    expect(optouts).toContain("c.instantly_campaign_id = ANY(");
    expect(optouts).not.toContain("DELETE");
  });

  it("never re-derives the campaign set inside a later statement (the 25-minute nested loop)", async () => {
    // The first version put the set in every statement as a subquery on the
    // brand expression. The planner cannot estimate that expression, assumed ONE
    // campaign, and scanned `messages` once per campaign: 21,484 full scans for a
    // transfer with nothing left to move (prod, 2026-09-28). Every statement after
    // the resolution must read the constant list instead.
    mockExecute.mockImplementation(respond());
    await post(BODY);

    const texts = mockExecute.mock.calls.map((c) => sqlText(c[0]));
    const afterResolve = texts.slice(2, CAMPAIGN_CHILD_TABLES.length + 4);
    expect(afterResolve).toHaveLength(CAMPAIGN_CHILD_TABLES.length + 2);
    for (const t of afterResolve) {
      expect(t).not.toContain("brand_ids[1]");
      expect(t).not.toContain("array_length(brand_ids");
      expect(t).not.toMatch(/instantly_campaign_id IN \(/);
    }
    // The child tables join nothing at all: one scan of the table, filtered by the list.
    for (const t of afterResolve.slice(0, CAMPAIGN_CHILD_TABLES.length)) {
      expect(t).not.toContain("SELECT");
      expect(t).not.toMatch(/\bFROM\b/);
      expect(t).not.toMatch(/\bJOIN\b/);
    }
  });

  it("passes the campaign set as ONE array parameter, even past Postgres' 65,535-parameter ceiling", async () => {
    const many = Array.from({ length: 70_000 }, (_, i) => `ic-${i}`);
    mockExecute.mockImplementation(respond({ ids: many }));

    const res = await post(BODY);

    expect(res.status).toBe(200);
    const child = mockExecute.mock.calls[2][0];
    const params = sqlParams(child);
    expect(params).toHaveLength(3);
    expect(params[2]).toEqual(many);
    expect(sqlText(child)).toMatch(/= ANY\(\$3::text\[\]\)/);
  });

  it("runs the gold refresh even when no campaign moved (a re-run finishes a partial one)", async () => {
    mockExecute.mockImplementation(respond({ rowCount: 0 }));
    await post({ sourceBrandId: "b", sourceOrgId: "a", targetOrgId: "c" });
    const texts = mockExecute.mock.calls.map((c) => sqlText(c[0]));
    expect(texts.some((t) => t.includes("UPDATE instantly_lead_status_current"))).toBe(true);
  });

  it("scans no other table when the brand has no campaign in either org", async () => {
    mockExecute.mockImplementation(respond({ ids: [] }));

    const res = await post(BODY);

    expect(res.status).toBe(200);
    const texts = mockExecute.mock.calls.map((c) => sqlText(c[0]));
    // The campaigns update, the id resolution, the multi-brand count. Nothing else.
    expect(texts).toHaveLength(3);
    expect(res.body.updatedTables.map((t: { tableName: string }) => t.tableName)).toEqual([
      "instantly_campaigns",
      ...CAMPAIGN_CHILD_TABLES,
      "instantly_lead_status_current",
      "instantly_lead_optouts_raw (copied)",
    ]);
    for (const t of res.body.updatedTables.slice(1)) expect(t.count).toBe(0);
  });

  it("fails loud (500) when the id resolution returns no list", async () => {
    mockExecute.mockImplementation(respond({ ids: null }));
    const res = await post(BODY);
    expect(res.status).toBe(500);
    expect(res.body.detail).toContain("returned no id list");
    // Nothing after the resolution ran on a list it never got.
    expect(mockExecute).toHaveBeenCalledTimes(2);
  });

  it("rewrites the brand id when targetBrandId is given, and resolves campaigns under either id", async () => {
    mockExecute.mockImplementation(respond({ rowCount: 1 }));
    await post({ ...BODY, targetBrandId: "brand-2" });
    const first = mockExecute.mock.calls[0][0];
    expect(sqlText(first)).toContain("brand_ids = ARRAY[");
    expect(sqlParams(first)).toContain("brand-2");
    expect(sqlParams(mockExecute.mock.calls[1][0])).toContainEqual(["brand-1", "brand-2"]);
  });

  it("reports multi-brand campaigns it left behind", async () => {
    mockExecute.mockImplementation(respond({ rowCount: 0, multi: 4 }));
    const res = await post({ sourceBrandId: "b", sourceOrgId: "a", targetOrgId: "c" });
    expect(res.status).toBe(200);
    expect(res.body.skipped).toEqual([
      expect.objectContaining({ tableName: "instantly_campaigns", count: 4 }),
    ]);
  });

  it("fails loud (500) when a statement throws", async () => {
    mockExecute.mockRejectedValueOnce(new Error("boom"));
    const res = await post({ sourceBrandId: "b", sourceOrgId: "a", targetOrgId: "c" });
    expect(res.status).toBe(500);
    expect(res.body.detail).toBe("boom");
  });
});
