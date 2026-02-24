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
  items: [
    { leadId: "lead-1", email: "john@acme.com" },
  ],
};

/** Mock all 4 queries returning empty/null rows */
function mockEmptyResults() {
  // campaign lead, campaign email, global lead, global email
  mockExecute.mockResolvedValueOnce({ rows: [] });
  mockExecute.mockResolvedValueOnce({ rows: [] });
  mockExecute.mockResolvedValueOnce({ rows: [] });
  mockExecute.mockResolvedValueOnce({ rows: [] });
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

  it("should return 400 when campaignId is missing", async () => {
    const app = await createStatusApp();
    const res = await request(app).post("/").send({
      items: [{ leadId: "lead-1", email: "john@acme.com" }],
    });
    expect(res.status).toBe(400);
  });

  it("should return 400 when items is empty", async () => {
    const app = await createStatusApp();
    const res = await request(app).post("/").send({
      campaignId: "camp-1",
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
    expect(r.campaign.lead).toEqual({ contacted: false, delivered: false, replied: false, lastDeliveredAt: null });
    expect(r.campaign.email).toEqual({ contacted: false, delivered: false, bounced: false, unsubscribed: false, lastDeliveredAt: null });
    expect(r.global.lead).toEqual({ contacted: false, delivered: false, replied: false, lastDeliveredAt: null });
    expect(r.global.email).toEqual({ contacted: false, delivered: false, bounced: false, unsubscribed: false, lastDeliveredAt: null });
  });

  it("should return campaign-scoped and global results separately", async () => {
    // Campaign lead: contacted in this campaign
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "lead-1", contacted: true, delivered: true, replied: false, bounced: null, unsubscribed: null, lastDeliveredAt: "2026-02-20T14:30:00.000Z" }],
    });
    // Campaign email
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "john@acme.com", contacted: true, delivered: true, replied: null, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-02-20T14:30:00.000Z" }],
    });
    // Global lead: also contacted in another campaign with reply
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "lead-1", contacted: true, delivered: true, replied: true, bounced: null, unsubscribed: null, lastDeliveredAt: "2026-02-22T10:00:00.000Z" }],
    });
    // Global email
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "john@acme.com", contacted: true, delivered: true, replied: null, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-02-22T10:00:00.000Z" }],
    });

    const app = await createStatusApp();
    const res = await request(app).post("/").send(validBody);

    expect(res.status).toBe(200);
    const r = res.body.results[0];
    // Campaign: no reply
    expect(r.campaign.lead.replied).toBe(false);
    expect(r.campaign.lead.delivered).toBe(true);
    // Global: has reply
    expect(r.global.lead.replied).toBe(true);
    expect(r.global.lead.lastDeliveredAt).toBe("2026-02-22T10:00:00.000Z");
  });

  it("should return email bounced globally but not in campaign", async () => {
    // Campaign: clean
    mockExecute.mockResolvedValueOnce({ rows: [] });
    mockExecute.mockResolvedValueOnce({ rows: [] });
    // Global: bounced in another campaign
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "lead-1", contacted: true, delivered: false, replied: false, bounced: null, unsubscribed: null, lastDeliveredAt: null }],
    });
    mockExecute.mockResolvedValueOnce({
      rows: [{ key: "john@acme.com", contacted: true, delivered: false, replied: null, bounced: true, unsubscribed: false, lastDeliveredAt: null }],
    });

    const app = await createStatusApp();
    const res = await request(app).post("/").send(validBody);

    expect(res.status).toBe(200);
    const r = res.body.results[0];
    expect(r.campaign.email.bounced).toBe(false);
    expect(r.global.email.bounced).toBe(true);
  });

  it("should handle batch with multiple items", async () => {
    // All 4 queries return rows keyed by different leads/emails
    mockExecute.mockResolvedValueOnce({
      rows: [
        { key: "lead-1", contacted: true, delivered: true, replied: false, bounced: null, unsubscribed: null, lastDeliveredAt: "2026-02-20T14:30:00.000Z" },
      ],
    });
    mockExecute.mockResolvedValueOnce({
      rows: [
        { key: "john@acme.com", contacted: true, delivered: true, replied: null, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-02-20T14:30:00.000Z" },
      ],
    });
    mockExecute.mockResolvedValueOnce({
      rows: [
        { key: "lead-1", contacted: true, delivered: true, replied: false, bounced: null, unsubscribed: null, lastDeliveredAt: "2026-02-20T14:30:00.000Z" },
        { key: "lead-2", contacted: false, delivered: false, replied: false, bounced: null, unsubscribed: null, lastDeliveredAt: null },
      ],
    });
    mockExecute.mockResolvedValueOnce({
      rows: [
        { key: "john@acme.com", contacted: true, delivered: true, replied: null, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-02-20T14:30:00.000Z" },
      ],
    });

    const app = await createStatusApp();
    const res = await request(app).post("/").send({
      campaignId: "camp-1",
      items: [
        { leadId: "lead-1", email: "john@acme.com" },
        { leadId: "lead-2", email: "jane@acme.com" },
      ],
    });

    expect(res.status).toBe(200);
    expect(res.body.results).toHaveLength(2);
    expect(res.body.results[0].leadId).toBe("lead-1");
    expect(res.body.results[0].global.lead.delivered).toBe(true);
    expect(res.body.results[1].leadId).toBe("lead-2");
    expect(res.body.results[1].campaign.lead.contacted).toBe(false);
    // jane@acme.com not in results → defaults to false
    expect(res.body.results[1].global.email.contacted).toBe(false);
  });

  it("should execute exactly 4 queries regardless of batch size", async () => {
    mockEmptyResults();
    const app = await createStatusApp();

    await request(app).post("/").send({
      campaignId: "camp-1",
      items: [
        { leadId: "lead-1", email: "a@test.com" },
        { leadId: "lead-2", email: "b@test.com" },
        { leadId: "lead-3", email: "c@test.com" },
      ],
    });

    expect(mockExecute).toHaveBeenCalledTimes(4);
  });

  it("should return 500 on DB error", async () => {
    mockExecute.mockRejectedValueOnce(new Error("DB connection failed"));

    const app = await createStatusApp();
    const res = await request(app).post("/").send(validBody);

    expect(res.status).toBe(500);
    expect(res.body.error).toBe("Failed to get delivery status");
  });
});
