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

describe("POST /status", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("should return 400 when missing required fields", async () => {
    const app = await createStatusApp();

    const res = await request(app).post("/").send({});

    expect(res.status).toBe(400);
  });

  it("should return 400 when leadId is missing", async () => {
    const app = await createStatusApp();

    const res = await request(app).post("/").send({ email: "test@example.com" });

    expect(res.status).toBe(400);
  });

  it("should return 400 when email is missing", async () => {
    const app = await createStatusApp();

    const res = await request(app).post("/").send({ leadId: "lead-1" });

    expect(res.status).toBe(400);
  });

  it("should return all-false when no rows found", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [{ contacted: null, delivered: null, replied: null, lastDeliveredAt: null }] });
    mockExecute.mockResolvedValueOnce({ rows: [{ contacted: null, delivered: null, bounced: null, unsubscribed: null, lastDeliveredAt: null }] });

    const app = await createStatusApp();

    const res = await request(app).post("/").send({
      leadId: "lead-1",
      email: "test@example.com",
    });

    expect(res.status).toBe(200);
    expect(res.body).toEqual({
      lead: {
        contacted: false,
        delivered: false,
        replied: false,
        lastDeliveredAt: null,
      },
      email: {
        contacted: false,
        delivered: false,
        bounced: false,
        unsubscribed: false,
        lastDeliveredAt: null,
      },
    });
  });

  it("should return lead contacted and delivered", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [{ contacted: true, delivered: true, replied: false, lastDeliveredAt: "2026-02-20T14:30:00.000Z" }],
    });
    mockExecute.mockResolvedValueOnce({
      rows: [{ contacted: true, delivered: true, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-02-20T14:30:00.000Z" }],
    });

    const app = await createStatusApp();

    const res = await request(app).post("/").send({
      leadId: "lead-1",
      email: "test@example.com",
    });

    expect(res.status).toBe(200);
    expect(res.body.lead.contacted).toBe(true);
    expect(res.body.lead.delivered).toBe(true);
    expect(res.body.lead.replied).toBe(false);
    expect(res.body.lead.lastDeliveredAt).toBe("2026-02-20T14:30:00.000Z");
  });

  it("should return lead replied", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [{ contacted: true, delivered: true, replied: true, lastDeliveredAt: "2026-02-20T14:30:00.000Z" }],
    });
    mockExecute.mockResolvedValueOnce({
      rows: [{ contacted: true, delivered: true, bounced: false, unsubscribed: false, lastDeliveredAt: "2026-02-20T14:30:00.000Z" }],
    });

    const app = await createStatusApp();

    const res = await request(app).post("/").send({
      leadId: "lead-1",
      email: "test@example.com",
    });

    expect(res.status).toBe(200);
    expect(res.body.lead.replied).toBe(true);
  });

  it("should return email bounced", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [{ contacted: true, delivered: true, replied: false, lastDeliveredAt: "2026-02-20T14:30:00.000Z" }],
    });
    mockExecute.mockResolvedValueOnce({
      rows: [{ contacted: true, delivered: false, bounced: true, unsubscribed: false, lastDeliveredAt: null }],
    });

    const app = await createStatusApp();

    const res = await request(app).post("/").send({
      leadId: "lead-1",
      email: "bounced@example.com",
    });

    expect(res.status).toBe(200);
    expect(res.body.lead.delivered).toBe(true);
    expect(res.body.email.bounced).toBe(true);
    expect(res.body.email.delivered).toBe(false);
  });

  it("should return email unsubscribed", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [{ contacted: true, delivered: true, replied: false, lastDeliveredAt: "2026-02-20T14:30:00.000Z" }],
    });
    mockExecute.mockResolvedValueOnce({
      rows: [{ contacted: true, delivered: false, bounced: false, unsubscribed: true, lastDeliveredAt: null }],
    });

    const app = await createStatusApp();

    const res = await request(app).post("/").send({
      leadId: "lead-1",
      email: "unsub@example.com",
    });

    expect(res.status).toBe(200);
    expect(res.body.email.unsubscribed).toBe(true);
  });

  it("should accept optional campaignId", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [{ contacted: null, delivered: null, replied: null, lastDeliveredAt: null }] });
    mockExecute.mockResolvedValueOnce({ rows: [{ contacted: null, delivered: null, bounced: null, unsubscribed: null, lastDeliveredAt: null }] });

    const app = await createStatusApp();

    const res = await request(app).post("/").send({
      campaignId: "camp-1",
      leadId: "lead-1",
      email: "test@example.com",
    });

    expect(res.status).toBe(200);
  });

  it("should return 500 on DB error", async () => {
    mockExecute.mockRejectedValueOnce(new Error("DB connection failed"));

    const app = await createStatusApp();

    const res = await request(app).post("/").send({
      leadId: "lead-1",
      email: "test@example.com",
    });

    expect(res.status).toBe(500);
    expect(res.body.error).toBe("Failed to get delivery status");
  });
});
