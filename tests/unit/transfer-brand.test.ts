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

describe("POST /internal/transfer-brand", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("returns 400 for invalid body (missing fields)", async () => {
    const app = await createApp();
    const res = await request(app).post("/").send({ sourceBrandId: "b1" });

    expect(res.status).toBe(400);
    expect(mockExecute).not.toHaveBeenCalled();
  });

  it("returns 400 for empty body", async () => {
    const app = await createApp();
    const res = await request(app).post("/").send({});

    expect(res.status).toBe(400);
    expect(mockExecute).not.toHaveBeenCalled();
  });

  it("returns 400 when using old brandId field", async () => {
    const app = await createApp();
    const res = await request(app)
      .post("/")
      .send({ brandId: "brand-1", sourceOrgId: "org-a", targetOrgId: "org-b" });

    expect(res.status).toBe(400);
    expect(mockExecute).not.toHaveBeenCalled();
  });

  it("moves the campaigns, then every child table, then gold, then copies opt-outs", async () => {
    mockExecute.mockImplementation(async () => ({ rowCount: 2, rows: [{ n: 0 }] }));

    const app = await createApp();
    const res = await request(app)
      .post("/")
      .send({ sourceBrandId: "brand-1", sourceOrgId: "org-a", targetOrgId: "org-b" });

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
    for (const [i, table] of CAMPAIGN_CHILD_TABLES.entries()) {
      const t = texts[i + 1];
      expect(t).toContain(`UPDATE "${table}"`);
      expect(t).toContain("WHERE org_id =");
      // The campaign set spans BOTH orgs, or a re-run after the campaigns moved finds nothing.
      expect(t).toContain("org_id IN (");
    }
    const gold = texts[CAMPAIGN_CHILD_TABLES.length + 1];
    expect(gold).toContain("UPDATE instantly_lead_status_current");
    expect(gold).toContain("brand_ids = c.brand_ids");
    const optouts = texts[CAMPAIGN_CHILD_TABLES.length + 2];
    expect(optouts).toContain("INSERT INTO instantly_lead_optouts_raw");
    expect(optouts).toContain("transferredFromOptoutId");
    expect(optouts).toContain("instantly_lead_optout_withdrawals");
    expect(optouts).not.toContain("DELETE");
  });

  it("runs the gold refresh even when no campaign moved (a re-run finishes a partial one)", async () => {
    mockExecute.mockImplementation(async () => ({ rowCount: 0, rows: [{ n: 0 }] }));
    const app = await createApp();
    await request(app).post("/").send({ sourceBrandId: "b", sourceOrgId: "a", targetOrgId: "c" });
    const texts = mockExecute.mock.calls.map((c) => sqlText(c[0]));
    expect(texts.some((t) => t.includes("UPDATE instantly_lead_status_current"))).toBe(true);
  });

  it("rewrites the brand id when targetBrandId is given", async () => {
    mockExecute.mockImplementation(async () => ({ rowCount: 1, rows: [{ n: 0 }] }));
    const app = await createApp();
    await request(app)
      .post("/")
      .send({ sourceBrandId: "brand-1", sourceOrgId: "org-a", targetOrgId: "org-b", targetBrandId: "brand-2" });
    const first = mockExecute.mock.calls[0][0];
    expect(sqlText(first)).toContain("brand_ids = ARRAY[");
    expect(sqlParams(first)).toContain("brand-2");
  });

  it("reports multi-brand campaigns it left behind", async () => {
    mockExecute.mockImplementation(async () => ({ rowCount: 0, rows: [{ n: 4 }] }));
    const app = await createApp();
    const res = await request(app).post("/").send({ sourceBrandId: "b", sourceOrgId: "a", targetOrgId: "c" });
    expect(res.status).toBe(200);
    expect(res.body.skipped).toEqual([
      expect.objectContaining({ tableName: "instantly_campaigns", count: 4 }),
    ]);
  });

  it("fails loud (500) when a statement throws", async () => {
    mockExecute.mockRejectedValueOnce(new Error("boom"));
    const app = await createApp();
    const res = await request(app).post("/").send({ sourceBrandId: "b", sourceOrgId: "a", targetOrgId: "c" });
    expect(res.status).toBe(500);
    expect(res.body.detail).toBe("boom");
  });
});
