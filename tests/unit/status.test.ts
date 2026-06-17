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

const emptyScoped = { contacted: false, sent: false, delivered: false, opened: false, clicked: false, replied: false, replyClassification: null, bounced: false, unsubscribed: false, cancelled: false, lastDeliveredAt: null, firstContactedAt: null, firstSentAt: null, firstDeliveredAt: null, firstOpenedAt: null, firstClickedAt: null, firstRepliedAt: null, firstBouncedAt: null, firstUnsubscribedAt: null };

/** Recursively concatenate every string fragment in a drizzle SQL query. */
function chunkText(query: unknown): string {
  if (query == null) return "";
  if (typeof query === "string") return query;
  if (typeof query !== "object") return String(query);

  const chunks = (query as { queryChunks?: unknown[] }).queryChunks;
  if (Array.isArray(chunks)) {
    return chunks.map(chunkText).join("");
  }

  const v = (query as { value?: unknown }).value;
  if (typeof v === "string") return v;
  if (Array.isArray(v)) return v.map(chunkText).join("");

  return "";
}

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
      rows: [{ key: "john@acme.com", campaignId: null, contacted: true, delivered: true, opened: false, clicked: false, replied: false, replyClassification: null, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-02-20T14:30:00.000Z" }],
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
    expect(r.campaign.clicked).toBe(false);
    expect(r.campaign.lastDeliveredAt).toBe("2026-02-20T14:30:00.000Z");
    expect(r.byCampaign).toBeNull();
    expect(r.brand).toBeNull();
  });

  it("should surface cancelled=true when delivery_status is cancelled", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [] }); // global
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "stuck@acme.com", campaignId: null, contacted: true, sent: false, delivered: false, opened: false, clicked: false, replied: false, replyClassification: null, bounced: false, unsubscribed: false, cancelled: true, lastDeliveredAt: null }],
    });

    const app = await createStatusApp();
    const res = await request(app).post("/").send({
      campaignId: "camp-1",
      items: [{ email: "stuck@acme.com" }],
    });

    expect(res.status).toBe(200);
    expect(res.body.results[0].campaign.cancelled).toBe(true);
    expect(res.body.results[0].campaign.sent).toBe(false);
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
        { key: "alice@media.com", campaignId: "camp-1", contacted: true, delivered: true, opened: true, clicked: false, replied: false, replyClassification: null, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-03-01T10:00:00.000Z" },
        { key: "alice@media.com", campaignId: "camp-2", contacted: true, delivered: true, opened: false, clicked: true, replied: true, replyClassification: "positive", bounced: false, unsubscribed: false, lastDeliveredAt: "2026-03-02T12:00:00.000Z" },
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
    expect(r.byCampaign["camp-2"].replyClassification).toBe("positive");
    expect(r.byCampaign["camp-1"].clicked).toBe(false);
    expect(r.byCampaign["camp-2"].opened).toBe(false);
    expect(r.byCampaign["camp-2"].clicked).toBe(true);

    // brand aggregate = most advanced across campaigns
    expect(r.brand.contacted).toBe(true);
    expect(r.brand.delivered).toBe(true);
    expect(r.brand.opened).toBe(true);  // opened in camp-1
    expect(r.brand.clicked).toBe(true); // clicked in camp-2
    expect(r.brand.replied).toBe(true); // replied in camp-2
    expect(r.brand.replyClassification).toBe("positive"); // from camp-2 (most recent)
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
        { key: "john@acme.com", campaignId: "camp-1", contacted: true, delivered: true, opened: false, clicked: false, replied: false, replyClassification: null, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-03-01T10:00:00.000Z" },
        { key: "john@acme.com", campaignId: "camp-2", contacted: true, delivered: false, opened: false, clicked: false, replied: false, replyClassification: null, bounced: true, unsubscribed: false, lastDeliveredAt: null },
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
        { key: "alice@media.com", campaignId: "camp-1", contacted: true, delivered: true, opened: false, clicked: false, replied: false, replyClassification: null, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-03-01T10:00:00.000Z" },
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
      rows: [{ key: "john@acme.com", campaignId: "camp-1", contacted: true, delivered: true, opened: false, clicked: false, replied: false, replyClassification: null, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-03-01T10:00:00.000Z" }],
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
        { key: "john@acme.com", campaignId: "camp-1", contacted: true, delivered: true, opened: false, clicked: false, replied: true, replyClassification: "negative", bounced: false, unsubscribed: false, lastDeliveredAt: "2026-03-01T10:00:00.000Z" },
        { key: "john@acme.com", campaignId: "camp-2", contacted: true, delivered: true, opened: false, clicked: false, replied: true, replyClassification: "positive", bounced: false, unsubscribed: false, lastDeliveredAt: "2026-03-05T10:00:00.000Z" },
      ],
    });

    const app = await createStatusApp();
    const res = await request(app).post("/").send({
      brandId: "brand-1",
      items: [{ email: "john@acme.com" }],
    });

    expect(res.status).toBe(200);
    const r = res.body.results[0];
    // camp-2 is more recent, so brand picks "positive"
    expect(r.brand.replyClassification).toBe("positive");
    expect(r.byCampaign["camp-1"].replyClassification).toBe("negative");
    expect(r.byCampaign["camp-2"].replyClassification).toBe("positive");
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

  it("reads status from the Gold projection, not raw silver event joins", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [] }); // global
    mockExecute.mockResolvedValueOnce({ rows: [] }); // campaign

    const app = await createStatusApp();
    await request(app).post("/").send({
      campaignId: "camp-1",
      items: [{ email: "john@acme.com" }],
    });

    const sqlText = mockExecute.mock.calls.map((c) => chunkText(c[0])).join("\n");
    expect(sqlText).toContain("instantly_lead_status_current");
    expect(sqlText).not.toContain("instantly_events");
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

  // ── Clicked field ────────────────────────────────────────────────────────

  it("should return clicked=true when email_link_clicked event exists (campaign mode)", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [] }); // global
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "john@acme.com", campaignId: null, contacted: true, delivered: true, opened: true, clicked: true, replied: false, replyClassification: null, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-03-01T10:00:00.000Z" }],
    });

    const app = await createStatusApp();
    const res = await request(app).post("/").send({
      campaignId: "camp-1",
      items: [{ email: "john@acme.com" }],
    });

    expect(res.status).toBe(200);
    const r = res.body.results[0];
    expect(r.campaign.clicked).toBe(true);
    expect(r.campaign.opened).toBe(true);
  });

  it("should aggregate clicked at brand level via BOOL_OR", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [] }); // global
    mockExecute.mockResolvedValueOnce({
      rows: [
        { key: "john@acme.com", campaignId: "camp-1", contacted: true, delivered: true, opened: true, clicked: false, replied: false, replyClassification: null, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-03-01T10:00:00.000Z" },
        { key: "john@acme.com", campaignId: "camp-2", contacted: true, delivered: true, opened: false, clicked: true, replied: false, replyClassification: null, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-03-02T10:00:00.000Z" },
      ],
    });

    const app = await createStatusApp();
    const res = await request(app).post("/").send({
      brandId: "brand-1",
      items: [{ email: "john@acme.com" }],
    });

    expect(res.status).toBe(200);
    const r = res.body.results[0];
    expect(r.byCampaign["camp-1"].clicked).toBe(false);
    expect(r.byCampaign["camp-2"].clicked).toBe(true);
    expect(r.brand.clicked).toBe(true); // BOOL_OR
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

  // ── First-occurrence (MIN) timestamps — DIS-229 ────────────────────────────

  it("AC: campaign opened@T1 then clicked@T2>T1 → firstOpenedAt==T1 < firstClickedAt==T2, distinguishable", async () => {
    const T1 = "2026-03-01T10:00:00.000Z";
    const T2 = "2026-03-01T15:30:00.000Z";
    mockExecute.mockResolvedValueOnce({ rows: [] }); // global
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "john@acme.com", campaignId: null, contacted: true, sent: true, delivered: true, opened: true, clicked: true, replied: false, replyClassification: null, bounced: false, unsubscribed: false, cancelled: false, lastDeliveredAt: T1, firstContactedAt: T1, firstSentAt: T1, firstDeliveredAt: T1, firstOpenedAt: T1, firstClickedAt: T2, firstRepliedAt: null, firstBouncedAt: null, firstUnsubscribedAt: null }],
    });

    const app = await createStatusApp();
    const res = await request(app).post("/").send({
      campaignId: "camp-1",
      items: [{ email: "john@acme.com" }],
    });

    expect(res.status).toBe(200);
    const c = res.body.results[0].campaign;
    expect(c.firstOpenedAt).toBe(T1);
    expect(c.firstClickedAt).toBe(T2);
    expect(c.firstOpenedAt).not.toBeNull();
    expect(c.firstClickedAt).not.toBeNull();
    expect(new Date(c.firstOpenedAt).getTime()).toBeLessThan(new Date(c.firstClickedAt).getTime());
    // distinguishable by field
    expect(c.firstOpenedAt).not.toBe(c.firstClickedAt);
  });

  it("un-engaged recipient (sent only) → engagement first*At are null, firstSentAt set", async () => {
    const T = "2026-03-01T10:00:00.000Z";
    mockExecute.mockResolvedValueOnce({ rows: [] }); // global
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "john@acme.com", campaignId: null, contacted: true, sent: true, delivered: true, opened: false, clicked: false, replied: false, replyClassification: null, bounced: false, unsubscribed: false, cancelled: false, lastDeliveredAt: T, firstContactedAt: T, firstSentAt: T, firstDeliveredAt: T, firstOpenedAt: null, firstClickedAt: null, firstRepliedAt: null, firstBouncedAt: null, firstUnsubscribedAt: null }],
    });

    const app = await createStatusApp();
    const res = await request(app).post("/").send({
      campaignId: "camp-1",
      items: [{ email: "john@acme.com" }],
    });

    expect(res.status).toBe(200);
    const c = res.body.results[0].campaign;
    expect(c.firstSentAt).toBe(T);
    expect(c.firstOpenedAt).toBeNull();
    expect(c.firstClickedAt).toBeNull();
    expect(c.firstRepliedAt).toBeNull();
    expect(c.firstBouncedAt).toBeNull();
    expect(c.firstUnsubscribedAt).toBeNull();
  });

  it("emptyScoped campaign (no row) → all 8 first*At null", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [] }); // global
    mockExecute.mockResolvedValueOnce({ rows: [] }); // campaign

    const app = await createStatusApp();
    const res = await request(app).post("/").send({
      campaignId: "camp-1",
      items: [{ email: "john@acme.com" }],
    });

    expect(res.status).toBe(200);
    const c = res.body.results[0].campaign;
    expect(c).toEqual(emptyScoped);
    for (const f of ["firstContactedAt", "firstSentAt", "firstDeliveredAt", "firstOpenedAt", "firstClickedAt", "firstRepliedAt", "firstBouncedAt", "firstUnsubscribedAt"]) {
      expect(c[f]).toBeNull();
    }
  });

  it("brand aggregate firstOpenedAt = MIN across campaigns", async () => {
    const Ta = "2026-03-01T10:00:00.000Z"; // camp-1 opened earlier
    const Tb = "2026-03-05T10:00:00.000Z"; // camp-2 opened later
    mockExecute.mockResolvedValueOnce({ rows: [] }); // global
    mockExecute.mockResolvedValueOnce({
      rows: [
        { key: "john@acme.com", campaignId: "camp-1", contacted: true, sent: true, delivered: true, opened: true, clicked: false, replied: false, replyClassification: null, bounced: false, unsubscribed: false, cancelled: false, lastDeliveredAt: Ta, firstContactedAt: Ta, firstSentAt: Ta, firstDeliveredAt: Ta, firstOpenedAt: Ta, firstClickedAt: null, firstRepliedAt: null, firstBouncedAt: null, firstUnsubscribedAt: null },
        { key: "john@acme.com", campaignId: "camp-2", contacted: true, sent: true, delivered: true, opened: true, clicked: false, replied: false, replyClassification: null, bounced: false, unsubscribed: false, cancelled: false, lastDeliveredAt: Tb, firstContactedAt: Tb, firstSentAt: Tb, firstDeliveredAt: Tb, firstOpenedAt: Tb, firstClickedAt: null, firstRepliedAt: null, firstBouncedAt: null, firstUnsubscribedAt: null },
      ],
    });

    const app = await createStatusApp();
    const res = await request(app).post("/").send({
      brandId: "brand-1",
      items: [{ email: "john@acme.com" }],
    });

    expect(res.status).toBe(200);
    const r = res.body.results[0];
    expect(r.byCampaign["camp-1"].firstOpenedAt).toBe(Ta);
    expect(r.byCampaign["camp-2"].firstOpenedAt).toBe(Tb);
    // brand = MIN across campaigns
    expect(r.brand.firstOpenedAt).toBe(Ta);
    expect(r.brand.firstContactedAt).toBe(Ta);
    expect(r.brand.firstClickedAt).toBeNull(); // never clicked in any campaign
  });

  it("firstDeliveredAt agrees with delivered boolean (non-null iff delivered)", async () => {
    const T = "2026-03-01T10:00:00.000Z";
    mockExecute.mockResolvedValueOnce({ rows: [] }); // global
    mockExecute.mockResolvedValueOnce({
      rows: [
        // delivered=true → firstDeliveredAt set
        { key: "ok@acme.com", campaignId: "camp-1", contacted: true, sent: true, delivered: true, opened: false, clicked: false, replied: false, replyClassification: null, bounced: false, unsubscribed: false, cancelled: false, lastDeliveredAt: T, firstContactedAt: T, firstSentAt: T, firstDeliveredAt: T, firstOpenedAt: null, firstClickedAt: null, firstRepliedAt: null, firstBouncedAt: null, firstUnsubscribedAt: null },
      ],
    });

    const app = await createStatusApp();
    const okRes = await request(app).post("/").send({
      brandId: "brand-1",
      items: [{ email: "ok@acme.com" }],
    });
    expect(okRes.body.results[0].byCampaign["camp-1"].delivered).toBe(true);
    expect(okRes.body.results[0].byCampaign["camp-1"].firstDeliveredAt).toBe(T);

    // bounced → delivered=false → firstDeliveredAt null (SQL CASE returns NULL)
    mockExecute.mockResolvedValueOnce({ rows: [] }); // global
    mockExecute.mockResolvedValueOnce({
      rows: [
        { key: "bad@acme.com", campaignId: "camp-2", contacted: true, sent: true, delivered: false, opened: false, clicked: false, replied: false, replyClassification: null, bounced: true, unsubscribed: false, cancelled: false, lastDeliveredAt: null, firstContactedAt: T, firstSentAt: T, firstDeliveredAt: null, firstOpenedAt: null, firstClickedAt: null, firstRepliedAt: null, firstBouncedAt: T, firstUnsubscribedAt: null },
      ],
    });
    const badRes = await request(app).post("/").send({
      brandId: "brand-1",
      items: [{ email: "bad@acme.com" }],
    });
    expect(badRes.body.results[0].byCampaign["camp-2"].delivered).toBe(false);
    expect(badRes.body.results[0].byCampaign["camp-2"].firstDeliveredAt).toBeNull();
    expect(badRes.body.results[0].byCampaign["camp-2"].firstBouncedAt).toBe(T);
  });

  it("firstContactedAt non-null when contacted=true", async () => {
    const T = "2026-03-01T10:00:00.000Z";
    mockExecute.mockResolvedValueOnce({ rows: [] }); // global
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "john@acme.com", campaignId: null, contacted: true, sent: false, delivered: false, opened: false, clicked: false, replied: false, replyClassification: null, bounced: false, unsubscribed: false, cancelled: false, lastDeliveredAt: null, firstContactedAt: T, firstSentAt: null, firstDeliveredAt: null, firstOpenedAt: null, firstClickedAt: null, firstRepliedAt: null, firstBouncedAt: null, firstUnsubscribedAt: null }],
    });

    const app = await createStatusApp();
    const res = await request(app).post("/").send({
      campaignId: "camp-1",
      items: [{ email: "john@acme.com" }],
    });

    expect(res.status).toBe(200);
    const c = res.body.results[0].campaign;
    expect(c.contacted).toBe(true);
    expect(c.firstContactedAt).toBe(T);
    expect(c.firstSentAt).toBeNull();
  });
});
