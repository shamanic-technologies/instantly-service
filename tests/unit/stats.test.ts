import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";
import express from "express";

/** Recursively extract SQL text fragments from a drizzle SQL object */
function extractSqlText(obj: unknown): string {
  if (typeof obj === "string") return obj;
  if (obj == null) return "";
  if (Array.isArray(obj)) return obj.map(extractSqlText).join("");
  if (typeof obj === "object") {
    const o = obj as Record<string, unknown>;
    // StringChunk has a `value` array of strings
    if (Array.isArray(o.value)) return o.value.join("");
    // SQL has queryChunks
    if (Array.isArray(o.queryChunks)) return extractSqlText(o.queryChunks);
    return Object.values(o).map(extractSqlText).join("");
  }
  return "";
}

// Mock DB — db.execute returns { rows: [...] }
const mockExecute = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    execute: (...args: unknown[]) => mockExecute(...args),
  },
}));

vi.mock("../../src/db/schema", () => ({
  instantlyCampaigns: {},
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
    // Second call for step stats
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatsApp();

    const response = await request(app)
      .post("/stats")
      .send({ appId: "test-app" });

    expect(response.status).toBe(200);
    expect(response.body.stats.emailsSent).toBe(0);
    expect(response.body.recipients).toBe(0);
    // stepStats should not be present when empty
    expect(response.body.stepStats).toBeUndefined();
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
    // Step stats
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatsApp();

    const response = await request(app)
      .post("/stats")
      .send({ appId: "mcpfactory", orgId: "org_123" });

    expect(response.status).toBe(200);
    expect(response.body.stats.emailsSent).toBe(80);
    expect(response.body.stats.emailsReplied).toBe(2);
    expect(response.body.recipients).toBe(75);
  });

  it("should include per-step stats when step data exists", async () => {
    // Aggregate stats
    mockExecute.mockResolvedValueOnce({
      rows: [{
        emailsSent: 30,
        emailsDelivered: 28,
        emailsOpened: 15,
        emailsClicked: 1,
        emailsReplied: 3,
        emailsBounced: 2,
        repliesAutoReply: 0,
        repliesNotInterested: 0,
        repliesOutOfOffice: 0,
        repliesUnsubscribe: 0,
        recipients: 10,
      }],
    });
    // Step stats
    mockExecute.mockResolvedValueOnce({
      rows: [
        { step: 1, emailsSent: 10, emailsOpened: 8, emailsReplied: 1, emailsBounced: 1 },
        { step: 2, emailsSent: 10, emailsOpened: 5, emailsReplied: 1, emailsBounced: 1 },
        { step: 3, emailsSent: 10, emailsOpened: 2, emailsReplied: 1, emailsBounced: 0 },
      ],
    });

    const app = await createStatsApp();

    const response = await request(app)
      .post("/stats")
      .send({ campaignId: "camp-1" });

    expect(response.status).toBe(200);
    expect(response.body.stepStats).toHaveLength(3);
    expect(response.body.stepStats[0]).toEqual({
      step: 1, emailsSent: 10, emailsOpened: 8, emailsReplied: 1, emailsBounced: 1,
    });
    expect(response.body.stepStats[2]).toEqual({
      step: 3, emailsSent: 10, emailsOpened: 2, emailsReplied: 1, emailsBounced: 0,
    });
  });

  it("should return zero stats when db returns empty rows", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatsApp();

    const response = await request(app)
      .post("/stats")
      .send({ orgId: "org_nonexistent" });

    expect(response.status).toBe(200);
    expect(response.body.stats.emailsSent).toBe(0);
    expect(response.body.recipients).toBe(0);
  });

  it("should accept runIds filter", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [{
        emailsSent: 10, emailsDelivered: 10, emailsOpened: 5,
        emailsClicked: 0, emailsReplied: 0, emailsBounced: 0,
        repliesAutoReply: 0, repliesNotInterested: 0,
        repliesOutOfOffice: 0, repliesUnsubscribe: 0, recipients: 10,
      }],
    });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatsApp();

    const response = await request(app)
      .post("/stats")
      .send({ runIds: ["run-1", "run-2"] });

    expect(response.status).toBe(200);
    expect(response.body.stats.emailsSent).toBe(10);
  });

  it("should exclude internal emails and sender from stats query", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [] });
    const app = await createStatsApp();

    await request(app).post("/stats").send({ appId: "test-app" });

    expect(mockExecute).toHaveBeenCalledTimes(1);

    const sqlObj = mockExecute.mock.calls[0][0];
    const sqlText = extractSqlText(sqlObj);

    expect(sqlText).toContain("lead_email != e.account_email");
    expect(sqlText).toContain("lead_email NOT IN");
    expect(sqlText).toContain("LIKE");
  });

  it("should return 500 on db error", async () => {
    mockExecute.mockRejectedValueOnce(new Error("DB connection failed"));

    const app = await createStatsApp();

    const response = await request(app)
      .post("/stats")
      .send({ appId: "test-app" });

    expect(response.status).toBe(500);
    expect(response.body.error).toBe("Failed to aggregate stats");
  });

  it("should return overall stats when step query fails", async () => {
    // Aggregate stats succeed
    mockExecute.mockResolvedValueOnce({
      rows: [{
        emailsSent: 50,
        emailsDelivered: 48,
        emailsOpened: 20,
        emailsClicked: 2,
        emailsReplied: 5,
        emailsBounced: 2,
        repliesAutoReply: 0,
        repliesNotInterested: 1,
        repliesOutOfOffice: 0,
        repliesUnsubscribe: 0,
        recipients: 40,
      }],
    });
    // Step query fails
    mockExecute.mockRejectedValueOnce(new Error("step query timeout"));

    const app = await createStatsApp();

    const response = await request(app)
      .post("/stats")
      .send({ appId: "test-app", orgId: "org_123" });

    expect(response.status).toBe(200);
    expect(response.body.stats.emailsSent).toBe(50);
    expect(response.body.stats.emailsReplied).toBe(5);
    expect(response.body.recipients).toBe(40);
    expect(response.body.stepStats).toBeUndefined();
  });

  it("should log cause message from DrizzleQueryError", async () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    // Aggregate stats succeed
    mockExecute.mockResolvedValueOnce({
      rows: [{
        emailsSent: 10, emailsDelivered: 10, emailsOpened: 5,
        emailsClicked: 0, emailsReplied: 0, emailsBounced: 0,
        repliesAutoReply: 0, repliesNotInterested: 0,
        repliesOutOfOffice: 0, repliesUnsubscribe: 0, recipients: 10,
      }],
    });
    // Step query fails with a DrizzleQueryError-like structure
    const pgError = new Error("canceling statement due to statement timeout");
    const drizzleError = new Error("Failed query: SELECT ...");
    drizzleError.cause = pgError;
    mockExecute.mockRejectedValueOnce(drizzleError);

    const app = await createStatsApp();

    const response = await request(app)
      .post("/stats")
      .send({ appId: "test-app" });

    expect(response.status).toBe(200);
    expect(consoleSpy).toHaveBeenCalledWith(
      expect.stringContaining("canceling statement due to statement timeout"),
    );

    consoleSpy.mockRestore();
  });
});
