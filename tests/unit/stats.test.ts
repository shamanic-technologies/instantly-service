import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";
import express from "express";

// Mock DB — db.execute returns { rows: [...] }
const mockExecute = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    execute: (...args: unknown[]) => mockExecute(...args),
  },
}));

vi.mock("../../src/db/schema", () => ({
  instantlyCampaigns: {},
  instantlyAnalyticsSnapshots: {},
}));

vi.mock("../../src/lib/instantly-client", () => ({
  getCampaignAnalytics: vi.fn(),
}));

process.env.INSTANTLY_SERVICE_API_KEY = "test-api-key";

async function createStatsApp() {
  const analyticsRouter = (await import("../../src/routes/analytics")).default;
  const app = express();
  app.use(express.json());
  app.use(analyticsRouter);
  return app;
}

describe("POST /stats", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("should return 400 when no filters provided", async () => {
    const app = await createStatsApp();

    const response = await request(app).post("/stats").send({});

    expect(response.status).toBe(400);
    expect(response.body.error).toContain("At least one filter required");
  });

  it("should return zeros when no events match", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [
        {
          emailsSent: 0,
          emailsDelivered: 0,
          emailsOpened: 0,
          emailsClicked: 0,
          emailsReplied: 0,
          emailsBounced: 0,
          repliesAutoReply: 0,
          repliesNotInterested: 0,
          repliesOutOfOffice: 0,
          repliesUnsubscribe: 0,
          recipients: 0,
        },
      ],
    });

    const app = await createStatsApp();

    const response = await request(app)
      .post("/stats")
      .send({ appId: "test-app" });

    expect(response.status).toBe(200);
    expect(response.body).toEqual({
      stats: {
        emailsSent: 0,
        emailsDelivered: 0,
        emailsOpened: 0,
        emailsClicked: 0,
        emailsReplied: 0,
        emailsBounced: 0,
        repliesAutoReply: 0,
        repliesNotInterested: 0,
        repliesOutOfOffice: 0,
        repliesUnsubscribe: 0,
      },
      recipients: 0,
    });
  });

  it("should aggregate event counts correctly", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [
        {
          emailsSent: 80,
          emailsDelivered: 75,
          emailsOpened: 40,
          emailsClicked: 3,
          emailsReplied: 2,
          emailsBounced: 5,
          repliesAutoReply: 1,
          repliesNotInterested: 1,
          repliesOutOfOffice: 2,
          repliesUnsubscribe: 0,
          recipients: 75,
        },
      ],
    });

    const app = await createStatsApp();

    const response = await request(app)
      .post("/stats")
      .send({ appId: "mcpfactory", clerkOrgId: "org_123" });

    expect(response.status).toBe(200);
    expect(response.body).toEqual({
      stats: {
        emailsSent: 80,
        emailsDelivered: 75,
        emailsOpened: 40,
        emailsClicked: 3,
        emailsReplied: 2,
        emailsBounced: 5,
        repliesAutoReply: 1,
        repliesNotInterested: 1,
        repliesOutOfOffice: 2,
        repliesUnsubscribe: 0,
      },
      recipients: 75,
    });
  });

  it("should return zero stats when db returns empty rows", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatsApp();

    const response = await request(app)
      .post("/stats")
      .send({ clerkOrgId: "org_nonexistent" });

    expect(response.status).toBe(200);
    expect(response.body.stats.emailsSent).toBe(0);
    expect(response.body.recipients).toBe(0);
  });

  it("should accept runIds filter", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [
        {
          emailsSent: 10,
          emailsDelivered: 10,
          emailsOpened: 5,
          emailsClicked: 0,
          emailsReplied: 0,
          emailsBounced: 0,
          repliesAutoReply: 0,
          repliesNotInterested: 0,
          repliesOutOfOffice: 0,
          repliesUnsubscribe: 0,
          recipients: 10,
        },
      ],
    });

    const app = await createStatsApp();

    const response = await request(app)
      .post("/stats")
      .send({ runIds: ["run-1", "run-2"] });

    expect(response.status).toBe(200);
    expect(response.body.stats.emailsSent).toBe(10);
    expect(mockExecute).toHaveBeenCalledTimes(1);
  });

  it("should return 500 on db error", async () => {
    mockExecute.mockRejectedValueOnce(new Error("DB connection failed"));

    const app = await createStatsApp();

    const response = await request(app)
      .post("/stats")
      .send({ appId: "test-app" });

    expect(response.status).toBe(500);
    expect(response.body.error).toBe("DB connection failed");
  });
});
