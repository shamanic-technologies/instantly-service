import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";
import express from "express";

const mockExecute = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    execute: (...args: unknown[]) => mockExecute(...args),
  },
}));

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

  it("transfers solo-brand rows without targetBrandId (no conflict)", async () => {
    mockExecute.mockResolvedValueOnce({ rowCount: 3 });

    const app = await createApp();
    const res = await request(app)
      .post("/")
      .send({ sourceBrandId: "brand-1", sourceOrgId: "org-a", targetOrgId: "org-b" });

    expect(res.status).toBe(200);
    expect(res.body).toEqual({
      updatedTables: [{ tableName: "instantly_campaigns", count: 3 }],
    });
    expect(mockExecute).toHaveBeenCalledTimes(1);
  });

  it("transfers and rewrites brand_id when targetBrandId is present (conflict)", async () => {
    mockExecute.mockResolvedValueOnce({ rowCount: 2 });

    const app = await createApp();
    const res = await request(app)
      .post("/")
      .send({
        sourceBrandId: "brand-1",
        sourceOrgId: "org-a",
        targetOrgId: "org-b",
        targetBrandId: "brand-2",
      });

    expect(res.status).toBe(200);
    expect(res.body).toEqual({
      updatedTables: [{ tableName: "instantly_campaigns", count: 2 }],
    });
    expect(mockExecute).toHaveBeenCalledTimes(1);
  });

  it("returns count 0 when no rows match (idempotent)", async () => {
    mockExecute.mockResolvedValueOnce({ rowCount: 0 });

    const app = await createApp();
    const res = await request(app)
      .post("/")
      .send({ sourceBrandId: "brand-1", sourceOrgId: "org-a", targetOrgId: "org-b" });

    expect(res.status).toBe(200);
    expect(res.body).toEqual({
      updatedTables: [{ tableName: "instantly_campaigns", count: 0 }],
    });
  });

  it("handles null rowCount as 0", async () => {
    mockExecute.mockResolvedValueOnce({ rowCount: null });

    const app = await createApp();
    const res = await request(app)
      .post("/")
      .send({ sourceBrandId: "brand-1", sourceOrgId: "org-a", targetOrgId: "org-b" });

    expect(res.status).toBe(200);
    expect(res.body).toEqual({
      updatedTables: [{ tableName: "instantly_campaigns", count: 0 }],
    });
  });
});
