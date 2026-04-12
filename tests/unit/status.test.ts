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

const emptyScoped = { contacted: false, delivered: false, opened: false, replied: false, replyClassification: null, bounced: false, unsubscribed: false, lastDeliveredAt: null };

describe("POST /status", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  // ── Validation ──────────────────────────────────────────────────────────

  it("should return 400 when missing required fields", async () => {
    const app = await createStatusApp();
    const res = await request(app).post("/").send({});
    expect(res.status).toBe(400);
  });

  it("should return 400 when items is empty", async () => {
    const app = await createStatusApp();
    const res = await request(app).post("/").send({ items: [] });
    expect(res.status).toBe(400);
  });

  // ── Global-only mode (no brandId, no campaignId) ───────────────────────

  it("should return only global when neither brandId nor campaignId provided", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [{ key: "john@acme.com", campaignId: null, bounced: true, unsubscribed: false }] });

    const app = await createStatusApp();
    const res = await request(app).post("/").send({
      items: [{ email: "john@acme.com" }],
    });

    expect(res.status).toBe(200);
    expect(mockExecute).toHaveBeenCalledTimes(1);
    const r = res.body.results[0];
    expect(r.email).toBe("john@acme.com");
    expect(r.byCampaign).toBeNull();
    expect(r.brand).toBeNull();
    expect(r.campaign).toBeNull();
    expect(r.global.email.bounced).toBe(true);
  });

  // ── Campaign mode (campaignId provided) ────────────────────────────────

  it("should return campaign-scoped status when campaignId provided", async () => {
    // Global
    mockExecute.mockResolvedValueOnce({ rows: [] });
    // Campaign
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "john@acme.com", campaignId: null, contacted: true, delivered: true, opened: false, replied: false, replyClassification: null, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-02-20T14:30:00.000Z" }],
    });

    const app = await createStatusApp();
    const res = await request(app).post("/").send({
      campaignId: "camp-1",
      items: [{ email: "john@acme.com" }],
    });

    expect(res.status).toBe(200);
    expect(mockExecute).toHaveBeenCalledTimes(2);
    const r = res.body.results[0];
    expect(r.campaign.contacted).toBe(true);
    expect(r.campaign.delivered).toBe(true);
    expect(r.campaign.lastDeliveredAt).toBe("2026-02-20T14:30:00.000Z");
    expect(r.byCampaign).toBeNull();
    expect(r.brand).toBeNull();
  });

  it("should return campaign as emptyScoped when campaignId provided but no data", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [] }); // global
    mockExecute.mockResolvedValueOnce({ rows: [] }); // campaign

    const app = await createStatusApp();
    const res = await request(app).post("/").send({
      campaignId: "camp-1",
      items: [{ email: "john@acme.com" }],
    });

    expect(res.status).toBe(200);
    expect(res.body.results[0].campaign).toEqual(emptyScoped);
  });

  it("should treat brandId + campaignId as campaign mode (brandId ignored)", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [] }); // global
    mockExecute.mockResolvedValueOnce({ rows: [] }); // campaign

    const app = await createStatusApp();
    const res = await request(app).post("/").send({
      brandId: "brand-1",
      campaignId: "camp-1",
      items: [{ email: "john@acme.com" }],
    });

    expect(res.status).toBe(200);
    // Only 2 queries: global + campaign (no brand breakdown)
    expect(mockExecute).toHaveBeenCalledTimes(2);
    const r = res.body.results[0];
    expect(r.byCampaign).toBeNull();
    expect(r.brand).toBeNull();
    expect(r.campaign).toBeDefined();
  });

  // ── Brand mode (brandId, no campaignId) ────────────────────────────────

  it("should return byCampaign breakdown and aggregated brand when brandId provided", async () => {
    // Global
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "alice@media.com", campaignId: null, bounced: false, unsubscribed: false }],
    });
    // Brand breakdown (2 campaigns)
    mockExecute.mockResolvedValueOnce({
      rows: [
        { key: "alice@media.com", campaignId: "camp-1", contacted: true, delivered: true, opened: true, replied: false, replyClassification: null, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-03-01T10:00:00.000Z" },
        { key: "alice@media.com", campaignId: "camp-2", contacted: true, delivered: true, opened: false, replied: true, replyClassification: "interested", bounced: false, unsubscribed: false, lastDeliveredAt: "2026-03-02T12:00:00.000Z" },
      ],
    });

    const app = await createStatusApp();
    const res = await request(app).post("/").send({
      brandId: "brand-1",
      items: [{ email: "alice@media.com" }],
    });

    expect(res.status).toBe(200);
    expect(mockExecute).toHaveBeenCalledTimes(2);
    const r = res.body.results[0];

    // byCampaign
    expect(r.byCampaign["camp-1"].contacted).toBe(true);
    expect(r.byCampaign["camp-1"].opened).toBe(true);
    expect(r.byCampaign["camp-1"].replied).toBe(false);
    expect(r.byCampaign["camp-2"].replied).toBe(true);
    expect(r.byCampaign["camp-2"].replyClassification).toBe("interested");
    expect(r.byCampaign["camp-2"].opened).toBe(false);

    // brand aggregate = most advanced across campaigns
    expect(r.brand.contacted).toBe(true);
    expect(r.brand.delivered).toBe(true);
    expect(r.brand.opened).toBe(true);  // opened in camp-1
    expect(r.brand.replied).toBe(true); // replied in camp-2
    expect(r.brand.replyClassification).toBe("interested"); // from camp-2 (most recent)
    expect(r.brand.lastDeliveredAt).toBe("2026-03-02T12:00:00.000Z"); // max
    expect(r.brand.bounced).toBe(false);

    // campaign should be null in brand mode
    expect(r.campaign).toBeNull();

    // global
    expect(r.global.email.bounced).toBe(false);
  });

  it("should return empty byCampaign and emptyScoped brand when no data for brand", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [] }); // global
    mockExecute.mockResolvedValueOnce({ rows: [] }); // brand breakdown

    const app = await createStatusApp();
    const res = await request(app).post("/").send({
      brandId: "brand-1",
      items: [{ email: "john@acme.com" }],
    });

    expect(res.status).toBe(200);
    const r = res.body.results[0];
    expect(r.byCampaign).toEqual({});
    expect(r.brand).toEqual(emptyScoped);
  });

  it("should aggregate bounced at brand level from any campaign", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [] }); // global
    mockExecute.mockResolvedValueOnce({
      rows: [
        { key: "john@acme.com", campaignId: "camp-1", contacted: true, delivered: true, opened: false, replied: false, replyClassification: null, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-03-01T10:00:00.000Z" },
        { key: "john@acme.com", campaignId: "camp-2", contacted: true, delivered: false, opened: false, replied: false, replyClassification: null, bounced: true, unsubscribed: false, lastDeliveredAt: null },
      ],
    });

    const app = await createStatusApp();
    const res = await request(app).post("/").send({
      brandId: "brand-1",
      items: [{ email: "john@acme.com" }],
    });

    expect(res.status).toBe(200);
    const r = res.body.results[0];
    expect(r.brand.bounced).toBe(true);
    expect(r.byCampaign["camp-1"].bounced).toBe(false);
    expect(r.byCampaign["camp-2"].bounced).toBe(true);
  });

  // ── Batch with multiple items ──────────────────────────────────────────

  it("should handle batch with multiple items in brand mode", async () => {
    // Global
    mockExecute.mockResolvedValueOnce({
      rows: [
        { key: "alice@media.com", campaignId: null, bounced: false, unsubscribed: false },
      ],
    });
    // Brand breakdown
    mockExecute.mockResolvedValueOnce({
      rows: [
        { key: "alice@media.com", campaignId: "camp-1", contacted: true, delivered: true, opened: false, replied: false, replyClassification: null, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-03-01T10:00:00.000Z" },
      ],
    });

    const app = await createStatusApp();
    const res = await request(app).post("/").send({
      brandId: "brand-1",
      items: [
        { email: "alice@media.com" },
        { email: "bob@test.com" },
      ],
    });

    expect(res.status).toBe(200);
    expect(res.body.results).toHaveLength(2);
    // alice has data
    expect(res.body.results[0].byCampaign["camp-1"].delivered).toBe(true);
    expect(res.body.results[0].brand.delivered).toBe(true);
    // bob has no data
    expect(res.body.results[1].byCampaign).toEqual({});
    expect(res.body.results[1].brand).toEqual(emptyScoped);
  });

  // ── Global bounced across brands ───────────────────────────────────────

  it("should return global bounced even when brand campaigns are clean", async () => {
    // Global: bounced across all campaigns
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "john@acme.com", campaignId: null, bounced: true, unsubscribed: false }],
    });
    // Brand breakdown: clean
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "john@acme.com", campaignId: "camp-1", contacted: true, delivered: true, opened: false, replied: false, replyClassification: null, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-03-01T10:00:00.000Z" }],
    });

    const app = await createStatusApp();
    const res = await request(app).post("/").send({
      brandId: "brand-1",
      items: [{ email: "john@acme.com" }],
    });

    expect(res.status).toBe(200);
    const r = res.body.results[0];
    expect(r.brand.bounced).toBe(false);
    expect(r.global.email.bounced).toBe(true);
  });

  // ── Reply classification ───────────────────────────────────────────────

  it("should pick the most recent replyClassification for brand aggregate", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [] }); // global
    mockExecute.mockResolvedValueOnce({
      rows: [
        { key: "john@acme.com", campaignId: "camp-1", contacted: true, delivered: true, opened: false, replied: true, replyClassification: "notInterested", bounced: false, unsubscribed: false, lastDeliveredAt: "2026-03-01T10:00:00.000Z" },
        { key: "john@acme.com", campaignId: "camp-2", contacted: true, delivered: true, opened: false, replied: true, replyClassification: "interested", bounced: false, unsubscribed: false, lastDeliveredAt: "2026-03-05T10:00:00.000Z" },
      ],
    });

    const app = await createStatusApp();
    const res = await request(app).post("/").send({
      brandId: "brand-1",
      items: [{ email: "john@acme.com" }],
    });

    expect(res.status).toBe(200);
    const r = res.body.results[0];
    // camp-2 is more recent, so brand picks "interested"
    expect(r.brand.replyClassification).toBe("interested");
    expect(r.byCampaign["camp-1"].replyClassification).toBe("notInterested");
    expect(r.byCampaign["camp-2"].replyClassification).toBe("interested");
  });

  // ── Query count ────────────────────────────────────────────────────────

  it("should execute correct number of queries per mode", async () => {
    const app = await createStatusApp();

    // Brand mode: 2 queries (global + brand breakdown)
    mockExecute.mockResolvedValueOnce({ rows: [] });
    mockExecute.mockResolvedValueOnce({ rows: [] });
    await request(app).post("/").send({ brandId: "b-1", items: [{ email: "a@test.com" }] });
    expect(mockExecute).toHaveBeenCalledTimes(2);

    vi.clearAllMocks();

    // Campaign mode: 2 queries (global + campaign)
    mockExecute.mockResolvedValueOnce({ rows: [] });
    mockExecute.mockResolvedValueOnce({ rows: [] });
    await request(app).post("/").send({ campaignId: "c-1", items: [{ email: "a@test.com" }] });
    expect(mockExecute).toHaveBeenCalledTimes(2);

    vi.clearAllMocks();

    // Global only: 1 query
    mockExecute.mockResolvedValueOnce({ rows: [] });
    await request(app).post("/").send({ items: [{ email: "a@test.com" }] });
    expect(mockExecute).toHaveBeenCalledTimes(1);

    vi.clearAllMocks();

    // brandId + campaignId = campaign mode: 2 queries
    mockExecute.mockResolvedValueOnce({ rows: [] });
    mockExecute.mockResolvedValueOnce({ rows: [] });
    await request(app).post("/").send({ brandId: "b-1", campaignId: "c-1", items: [{ email: "a@test.com" }] });
    expect(mockExecute).toHaveBeenCalledTimes(2);
  });

  // ── Error handling ─────────────────────────────────────────────────────

  it("should return 500 on DB error", async () => {
    mockExecute.mockRejectedValueOnce(new Error("DB connection failed"));

    const app = await createStatusApp();
    const res = await request(app).post("/").send({
      items: [{ email: "john@acme.com" }],
    });

    expect(res.status).toBe(500);
    expect(res.body.error).toBe("Failed to get delivery status");
  });

  // ── No leadId in response ──────────────────────────────────────────────

  it("should not include leadId in response", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [] }); // global
    mockExecute.mockResolvedValueOnce({ rows: [] }); // campaign

    const app = await createStatusApp();
    const res = await request(app).post("/").send({
      campaignId: "camp-1",
      items: [{ email: "john@acme.com" }],
    });

    expect(res.status).toBe(200);
    expect(res.body.results[0]).not.toHaveProperty("leadId");
  });
});
