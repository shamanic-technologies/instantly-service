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
  campaignId: "camp-1",
  items: [{ leadId: "lead-1", email: "john@acme.com" }],
};

/** Send POST /status with x-brand-id header */
function postStatus(app: express.Express) {
  return request(app).post("/").set("x-brand-id", "brand-1");
}

/** Mock all 3 queries (brand, global, camp) returning empty */
function mockEmptyResults() {
  for (let i = 0; i < 3; i++) {
    mockExecute.mockResolvedValueOnce({ rows: [] });
  }
}

/** Mock 2 queries (brand, global) — no campaign */
function mockEmptyResultsNoCampaign() {
  for (let i = 0; i < 2; i++) {
    mockExecute.mockResolvedValueOnce({ rows: [] });
  }
}

const emptyScoped = { contacted: false, delivered: false, opened: false, replied: false, replyClassification: null, bounced: false, unsubscribed: false, lastDeliveredAt: null };

describe("POST /status", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("should return 400 when missing required fields", async () => {
    const app = await createStatusApp();
    const res = await postStatus(app).send({});
    expect(res.status).toBe(400);
  });

  it("should return brand as null when x-brand-id header is missing", async () => {
    // global (1) + campaign (1) = 2 queries (no brand)
    mockExecute.mockResolvedValueOnce({ rows: [] }); // global
    mockExecute.mockResolvedValueOnce({ rows: [] }); // campaign

    const app = await createStatusApp();
    const res = await request(app).post("/").send({
      campaignId: "camp-1",
      items: [{ email: "john@acme.com" }],
    });
    expect(res.status).toBe(200);
    expect(res.body.results[0].brand).toBeNull();
    expect(res.body.results[0].campaign).toBeDefined();
    expect(res.body.results[0].global).toBeDefined();
  });

  it("should return 400 when items is empty", async () => {
    const app = await createStatusApp();
    const res = await postStatus(app).send({
      items: [],
    });
    expect(res.status).toBe(400);
  });

  it("should return all-false when no rows found", async () => {
    mockEmptyResults();
    const app = await createStatusApp();

    const res = await postStatus(app).send(validBody);

    expect(res.status).toBe(200);
    expect(res.body.results).toHaveLength(1);
    const r = res.body.results[0];
    expect(r.email).toBe("john@acme.com");
    expect(r.leadId).toBe("lead-1"); // from input
    expect(r.campaign).toEqual(emptyScoped);
    expect(r.brand).toEqual(emptyScoped);
    expect(r.global).toEqual({
      email: { bounced: false, unsubscribed: false },
    });
  });

  it("should return campaign as null when campaignId is omitted", async () => {
    mockEmptyResultsNoCampaign();
    const app = await createStatusApp();

    const res = await postStatus(app).send({
      items: [{ email: "john@acme.com" }],
    });

    expect(res.status).toBe(200);
    expect(res.body.results[0].campaign).toBeNull();
    expect(res.body.results[0].brand).toBeDefined();
    expect(res.body.results[0].global).toBeDefined();
  });

  it("should return brand-scoped and campaign-scoped results separately", async () => {
    // Brand
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "john@acme.com", leadId: "lead-1", contacted: true, delivered: true, opened: false, replied: true, replyClassification: null, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-02-22T10:00:00.000Z" }],
    });
    // Global
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "john@acme.com", contacted: null, delivered: null, replied: null, bounced: false, unsubscribed: false, lastDeliveredAt: null }],
    });
    // Campaign
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "john@acme.com", leadId: "lead-1", contacted: true, delivered: true, opened: false, replied: false, replyClassification: null, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-02-20T14:30:00.000Z" }],
    });

    const app = await createStatusApp();
    const res = await postStatus(app).send(validBody);

    expect(res.status).toBe(200);
    const r = res.body.results[0];
    // Brand: has reply (from another campaign in same brand)
    expect(r.brand.replied).toBe(true);
    // Campaign: no reply in this specific campaign
    expect(r.campaign.replied).toBe(false);
    // Global: only bounced/unsubscribed
    expect(r.global.email.bounced).toBe(false);
  });

  it("should return bounced globally even when brand is clean", async () => {
    // Brand
    mockExecute.mockResolvedValueOnce({ rows: [] });
    // Global: bounced in a different brand
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "john@acme.com", contacted: null, delivered: null, replied: null, bounced: true, unsubscribed: false, lastDeliveredAt: null }],
    });
    // Campaign
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatusApp();
    const res = await postStatus(app).send(validBody);

    expect(res.status).toBe(200);
    const r = res.body.results[0];
    expect(r.brand.bounced).toBe(false);
    expect(r.global.email.bounced).toBe(true);
  });

  it("should handle batch with multiple items", async () => {
    // Brand
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "john@acme.com", leadId: "lead-1", contacted: true, delivered: true, opened: false, replied: false, replyClassification: null, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-02-20T14:30:00.000Z" }],
    });
    // Global
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "john@acme.com", contacted: null, delivered: null, replied: null, bounced: false, unsubscribed: false, lastDeliveredAt: null }],
    });
    // Campaign
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatusApp();
    const res = await postStatus(app).send({
      campaignId: "camp-1",
      items: [
        { leadId: "lead-1", email: "john@acme.com" },
        { email: "jane@acme.com" },
      ],
    });

    expect(res.status).toBe(200);
    expect(res.body.results).toHaveLength(2);
    expect(res.body.results[0].email).toBe("john@acme.com");
    expect(res.body.results[0].leadId).toBe("lead-1");
    expect(res.body.results[0].brand.delivered).toBe(true);
    expect(res.body.results[1].email).toBe("jane@acme.com");
    expect(res.body.results[1].leadId).toBeNull();
    expect(res.body.results[1].brand.contacted).toBe(false);
  });

  it("should execute 3 queries with brandId+campaignId, 2 with brandId only, 1 with neither", async () => {
    mockEmptyResults();
    const app = await createStatusApp();

    // brandId + campaignId = 3 queries (brand, global, camp)
    await postStatus(app).send(validBody);
    expect(mockExecute).toHaveBeenCalledTimes(3);

    vi.clearAllMocks();
    mockEmptyResultsNoCampaign();

    // brandId, no campaignId = 2 queries (brand, global)
    await postStatus(app).send({
      items: [{ email: "a@test.com" }],
    });
    expect(mockExecute).toHaveBeenCalledTimes(2);

    vi.clearAllMocks();
    mockExecute.mockResolvedValueOnce({ rows: [] }); // global only

    // no brandId, no campaignId = 1 query (global)
    await request(app).post("/").send({
      items: [{ email: "a@test.com" }],
    });
    expect(mockExecute).toHaveBeenCalledTimes(1);
  });

  it("should return contacted=true even when deliveryStatus is still pending", async () => {
    // Brand — row exists with contacted=true (from TRUE AS "contacted")
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "john@acme.com", leadId: "lead-1", contacted: true, delivered: false, opened: false, replied: false, replyClassification: null, bounced: false, unsubscribed: false, lastDeliveredAt: null }],
    });
    // Global
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "john@acme.com", contacted: null, delivered: null, replied: null, bounced: false, unsubscribed: false, lastDeliveredAt: null }],
    });
    // Campaign
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "john@acme.com", leadId: "lead-1", contacted: true, delivered: false, opened: false, replied: false, replyClassification: null, bounced: false, unsubscribed: false, lastDeliveredAt: null }],
    });

    const app = await createStatusApp();
    const res = await postStatus(app).send(validBody);

    expect(res.status).toBe(200);
    const r = res.body.results[0];
    // contacted=true even though delivery hasn't happened yet
    expect(r.campaign.contacted).toBe(true);
    expect(r.brand.contacted).toBe(true);
    // but delivered=false since no email_sent webhook yet
    expect(r.campaign.delivered).toBe(false);
    expect(r.brand.delivered).toBe(false);
  });

  it("should return replyClassification from scoped query results", async () => {
    // Brand — has positive classification
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "john@acme.com", leadId: "lead-1", contacted: true, delivered: true, opened: false, replied: true, replyClassification: "positive", bounced: false, unsubscribed: false, lastDeliveredAt: "2026-02-22T10:00:00.000Z" }],
    });
    // Global
    mockExecute.mockResolvedValueOnce({ rows: [] });
    // Campaign — has negative classification
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "john@acme.com", leadId: "lead-1", contacted: true, delivered: true, opened: false, replied: true, replyClassification: "negative", bounced: false, unsubscribed: false, lastDeliveredAt: "2026-02-20T14:30:00.000Z" }],
    });

    const app = await createStatusApp();
    const res = await postStatus(app).send(validBody);

    expect(res.status).toBe(200);
    const r = res.body.results[0];
    expect(r.brand.replyClassification).toBe("positive");
    expect(r.campaign.replyClassification).toBe("negative");
  });

  it("should return replyClassification as null when no classification exists", async () => {
    // Brand — no classification
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "john@acme.com", leadId: "lead-1", contacted: true, delivered: true, opened: false, replied: false, replyClassification: null, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-02-22T10:00:00.000Z" }],
    });
    // Global
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatusApp();
    const res = await postStatus(app).send({
      items: [{ email: "john@acme.com" }],
    });

    expect(res.status).toBe(200);
    const r = res.body.results[0];
    expect(r.brand.replyClassification).toBeNull();
  });

  it("should return 500 on DB error", async () => {
    mockExecute.mockRejectedValueOnce(new Error("DB connection failed"));

    const app = await createStatusApp();
    const res = await postStatus(app).send(validBody);

    expect(res.status).toBe(500);
    expect(res.body.error).toBe("Failed to get delivery status");
  });

  it("should return opened=true when email_opened event exists", async () => {
    // Brand — opened=true
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "john@acme.com", leadId: "lead-1", contacted: true, delivered: true, opened: true, replied: false, replyClassification: null, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-02-22T10:00:00.000Z" }],
    });
    // Global
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "john@acme.com", contacted: null, delivered: null, replied: null, bounced: false, unsubscribed: false, lastDeliveredAt: null }],
    });
    // Campaign — not opened
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "john@acme.com", leadId: "lead-1", contacted: true, delivered: true, opened: false, replied: false, replyClassification: null, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-02-20T14:30:00.000Z" }],
    });

    const app = await createStatusApp();
    const res = await postStatus(app).send(validBody);

    expect(res.status).toBe(200);
    const r = res.body.results[0];
    // Brand: opened (from another campaign in same brand)
    expect(r.brand.opened).toBe(true);
    // Campaign: not opened in this specific campaign
    expect(r.campaign.opened).toBe(false);
  });

  it("should return opened=false when no email_opened event exists", async () => {
    // Brand — no opened event
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "john@acme.com", leadId: "lead-1", contacted: true, delivered: true, opened: false, replied: false, replyClassification: null, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-02-22T10:00:00.000Z" }],
    });
    // Global
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatusApp();
    const res = await postStatus(app).send({
      items: [{ email: "john@acme.com" }],
    });

    expect(res.status).toBe(200);
    const r = res.body.results[0];
    expect(r.brand.opened).toBe(false);
  });

  it("should accept items without leadId (email-only)", async () => {
    // Brand
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "sarah@test.com", leadId: "db-lead-1", contacted: true, delivered: true, opened: false, replied: false, replyClassification: null, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-03-01T10:00:00.000Z" }],
    });
    // Global
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatusApp();
    const res = await postStatus(app).send({
      items: [{ email: "sarah@test.com" }],
    });

    expect(res.status).toBe(200);
    const r = res.body.results[0];
    expect(r.email).toBe("sarah@test.com");
    expect(r.leadId).toBe("db-lead-1");
    expect(r.brand.contacted).toBe(true);
    expect(r.brand.delivered).toBe(true);
    expect(r.campaign).toBeNull();
  });
});
