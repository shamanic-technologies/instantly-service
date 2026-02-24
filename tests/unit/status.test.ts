import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";
import express from "express";

const mockExecute = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    execute: (...args: unknown[]) => mockExecute(...args),
  },
}));

vi.mock("../../src/db/schema", () => ({
  instantlyCampaigns: {},
}));

async function createStatusApp() {
  const statusRouter = (await import("../../src/routes/status")).default;
  const app = express();
  app.use(express.json());
  app.use(statusRouter);
  return app;
}

const validBody = {
  brandId: "brand-1",
  campaignId: "camp-1",
  items: [{ leadId: "lead-1", email: "john@acme.com" }],
};

/** Mock all 5 queries (brand lead, brand email, global email, camp lead, camp email) returning empty */
function mockEmptyResults() {
  for (let i = 0; i < 5; i++) {
    mockExecute.mockResolvedValueOnce({ rows: [] });
  }
}

/** Mock 3 queries (brand lead, brand email, global email) — no campaign */
function mockEmptyResultsNoCampaign() {
  for (let i = 0; i < 3; i++) {
    mockExecute.mockResolvedValueOnce({ rows: [] });
  }
}

describe("POST /status", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("should return 400 when missing required fields", async () => {
    const app = await createStatusApp();
    const res = await request(app).post("/").send({});
    expect(res.status).toBe(400);
  });

  it("should return 400 when brandId is missing", async () => {
    const app = await createStatusApp();
    const res = await request(app).post("/").send({
      campaignId: "camp-1",
      items: [{ leadId: "lead-1", email: "john@acme.com" }],
    });
    expect(res.status).toBe(400);
  });

  it("should return 400 when items is empty", async () => {
    const app = await createStatusApp();
    const res = await request(app).post("/").send({
      brandId: "brand-1",
      items: [],
    });
    expect(res.status).toBe(400);
  });

  it("should return all-false when no rows found", async () => {
    mockEmptyResults();
    const app = await createStatusApp();

    const res = await request(app).post("/").send(validBody);

    expect(res.status).toBe(200);
    expect(res.body.results).toHaveLength(1);
    const r = res.body.results[0];
    expect(r.leadId).toBe("lead-1");
    expect(r.email).toBe("john@acme.com");
    expect(r.campaign).toEqual({
      lead: { contacted: false, delivered: false, replied: false, lastDeliveredAt: null },
      email: { contacted: false, delivered: false, bounced: false, unsubscribed: false, lastDeliveredAt: null },
    });
    expect(r.brand).toEqual({
      lead: { contacted: false, delivered: false, replied: false, lastDeliveredAt: null },
      email: { contacted: false, delivered: false, bounced: false, unsubscribed: false, lastDeliveredAt: null },
    });
    expect(r.global).toEqual({
      email: { bounced: false, unsubscribed: false },
    });
  });

  it("should return campaign as null when campaignId is omitted", async () => {
    mockEmptyResultsNoCampaign();
    const app = await createStatusApp();

    const res = await request(app).post("/").send({
      brandId: "brand-1",
      items: [{ leadId: "lead-1", email: "john@acme.com" }],
    });

    expect(res.status).toBe(200);
    expect(res.body.results[0].campaign).toBeNull();
    expect(res.body.results[0].brand).toBeDefined();
    expect(res.body.results[0].global).toBeDefined();
  });

  it("should return brand-scoped and campaign-scoped results separately", async () => {
    // Brand lead
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "lead-1", contacted: true, delivered: true, replied: true, bounced: null, unsubscribed: null, lastDeliveredAt: "2026-02-22T10:00:00.000Z" }],
    });
    // Brand email
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "john@acme.com", contacted: true, delivered: true, replied: null, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-02-22T10:00:00.000Z" }],
    });
    // Global email
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "john@acme.com", contacted: null, delivered: null, replied: null, bounced: false, unsubscribed: false, lastDeliveredAt: null }],
    });
    // Campaign lead
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "lead-1", contacted: true, delivered: true, replied: false, bounced: null, unsubscribed: null, lastDeliveredAt: "2026-02-20T14:30:00.000Z" }],
    });
    // Campaign email
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "john@acme.com", contacted: true, delivered: true, replied: null, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-02-20T14:30:00.000Z" }],
    });

    const app = await createStatusApp();
    const res = await request(app).post("/").send(validBody);

    expect(res.status).toBe(200);
    const r = res.body.results[0];
    // Brand: has reply (from another campaign in same brand)
    expect(r.brand.lead.replied).toBe(true);
    // Campaign: no reply in this specific campaign
    expect(r.campaign.lead.replied).toBe(false);
    // Global: only bounced/unsubscribed
    expect(r.global.email.bounced).toBe(false);
  });

  it("should return bounced globally even when brand is clean", async () => {
    // Brand lead
    mockExecute.mockResolvedValueOnce({ rows: [] });
    // Brand email
    mockExecute.mockResolvedValueOnce({ rows: [] });
    // Global email: bounced in a different brand
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "john@acme.com", contacted: null, delivered: null, replied: null, bounced: true, unsubscribed: false, lastDeliveredAt: null }],
    });
    // Campaign lead
    mockExecute.mockResolvedValueOnce({ rows: [] });
    // Campaign email
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatusApp();
    const res = await request(app).post("/").send(validBody);

    expect(res.status).toBe(200);
    const r = res.body.results[0];
    expect(r.brand.email.bounced).toBe(false);
    expect(r.global.email.bounced).toBe(true);
  });

  it("should handle batch with multiple items", async () => {
    // Brand lead
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "lead-1", contacted: true, delivered: true, replied: false, bounced: null, unsubscribed: null, lastDeliveredAt: "2026-02-20T14:30:00.000Z" }],
    });
    // Brand email
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "john@acme.com", contacted: true, delivered: true, replied: null, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-02-20T14:30:00.000Z" }],
    });
    // Global email
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "john@acme.com", contacted: null, delivered: null, replied: null, bounced: false, unsubscribed: false, lastDeliveredAt: null }],
    });
    // Campaign lead
    mockExecute.mockResolvedValueOnce({ rows: [] });
    // Campaign email
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatusApp();
    const res = await request(app).post("/").send({
      brandId: "brand-1",
      campaignId: "camp-1",
      items: [
        { leadId: "lead-1", email: "john@acme.com" },
        { leadId: "lead-2", email: "jane@acme.com" },
      ],
    });

    expect(res.status).toBe(200);
    expect(res.body.results).toHaveLength(2);
    expect(res.body.results[0].leadId).toBe("lead-1");
    expect(res.body.results[0].brand.lead.delivered).toBe(true);
    expect(res.body.results[1].leadId).toBe("lead-2");
    expect(res.body.results[1].brand.lead.contacted).toBe(false);
  });

  it("should execute 5 queries with campaignId and 3 without", async () => {
    mockEmptyResults();
    const app = await createStatusApp();

    await request(app).post("/").send(validBody);
    expect(mockExecute).toHaveBeenCalledTimes(5);

    vi.clearAllMocks();
    mockEmptyResultsNoCampaign();

    await request(app).post("/").send({
      brandId: "brand-1",
      items: [{ leadId: "lead-1", email: "a@test.com" }],
    });
    expect(mockExecute).toHaveBeenCalledTimes(3);
  });

  it("should return 500 on DB error", async () => {
    mockExecute.mockRejectedValueOnce(new Error("DB connection failed"));

    const app = await createStatusApp();
    const res = await request(app).post("/").send(validBody);

    expect(res.status).toBe(500);
    expect(res.body.error).toBe("Failed to get delivery status");
  });
});
