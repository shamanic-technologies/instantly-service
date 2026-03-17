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
    if (Array.isArray(o.value)) return o.value.join("");
    if (Array.isArray(o.queryChunks)) return extractSqlText(o.queryChunks);
    return Object.values(o).map(extractSqlText).join("");
  }
  return "";
}

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

async function createPublicStatsApp() {
  const publicRouter = (await import("../../src/routes/analytics-public")).default;
  const app = express();
  app.use(express.json());
  app.use(publicRouter);
  return app;
}

function makeStatsRow(overrides: Partial<Record<string, number>> = {}) {
  return {
    emailsSent: 0, emailsDelivered: 0, emailsOpened: 0,
    emailsClicked: 0, emailsReplied: 0, emailsBounced: 0,
    repliesAutoReply: 0, repliesNotInterested: 0,
    repliesOutOfOffice: 0, repliesUnsubscribe: 0,
    recipients: 0,
    ...overrides,
  };
}

describe("GET /stats/public", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("should return stats without requiring identity headers", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({
        emailsSent: 100, emailsDelivered: 95, emailsOpened: 50,
        emailsReplied: 3, recipients: 90,
      })],
    });
    // Contacted count
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 110 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createPublicStatsApp();

    const response = await request(app).get("/stats/public");

    expect(response.status).toBe(200);
    expect(response.body.stats.emailsContacted).toBe(110);
    expect(response.body.stats.emailsSent).toBe(100);
    expect(response.body.stats.emailsReplied).toBe(3);
    expect(response.body.recipients).toBe(90);
  });

  it("should NOT include org_id in WHERE clause", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [makeStatsRow()] });
    // Contacted count
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 0 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createPublicStatsApp();

    await request(app).get("/stats/public").query({ runIds: "run-1" });

    const sqlObj = mockExecute.mock.calls[0][0];
    const sqlText = extractSqlText(sqlObj);
    expect(sqlText).not.toContain("org_id");
    expect(sqlText).toContain("run_id IN");
  });

  it("should accept runIds, brandId, and campaignId filters", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({ emailsSent: 10, recipients: 5 })],
    });
    // Contacted count
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 5 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createPublicStatsApp();

    const response = await request(app)
      .get("/stats/public")
      .query({ runIds: "run-1", brandId: "brand-1", campaignId: "camp-1" });

    expect(response.status).toBe(200);
    expect(response.body.stats.emailsSent).toBe(10);

    const sqlObj = mockExecute.mock.calls[0][0];
    const sqlText = extractSqlText(sqlObj);
    expect(sqlText).toContain("run_id IN");
    expect(sqlText).toContain("brand_id");
  });

  it("should use TRUE when no filters provided", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [makeStatsRow()] });
    // Contacted count
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 0 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createPublicStatsApp();

    await request(app).get("/stats/public");

    const sqlObj = mockExecute.mock.calls[0][0];
    const sqlText = extractSqlText(sqlObj);
    expect(sqlText).toContain("TRUE");
    expect(sqlText).not.toContain("org_id");
  });

  it("should include per-step stats when step data exists", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({ emailsSent: 30, recipients: 10 })],
    });
    // Contacted count
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 10 }] });
    mockExecute.mockResolvedValueOnce({
      rows: [
        { step: 1, emailsSent: 10, emailsOpened: 8, emailsReplied: 1, emailsBounced: 1 },
        { step: 2, emailsSent: 10, emailsOpened: 5, emailsReplied: 1, emailsBounced: 0 },
      ],
    });

    const app = await createPublicStatsApp();

    const response = await request(app).get("/stats/public");

    expect(response.status).toBe(200);
    expect(response.body.stepStats).toHaveLength(2);
    expect(response.body.stepStats[0].step).toBe(1);
  });

  it("should exclude internal emails from stats query", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [makeStatsRow()] });
    // Contacted count
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 0 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createPublicStatsApp();

    await request(app).get("/stats/public");

    const sqlObj = mockExecute.mock.calls[0][0];
    const sqlText = extractSqlText(sqlObj);
    expect(sqlText).toContain("lead_email != e.account_email");
    expect(sqlText).toContain("lead_email NOT IN");
  });

  it("should return 500 on db error", async () => {
    mockExecute.mockRejectedValueOnce(new Error("DB connection failed"));

    const app = await createPublicStatsApp();

    const response = await request(app).get("/stats/public");

    expect(response.status).toBe(500);
    expect(response.body.error).toBe("Failed to aggregate stats");
  });

  it("should return overall stats when step query fails", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({ emailsSent: 50, emailsReplied: 5, recipients: 40 })],
    });
    // Contacted count
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 50 }] });
    mockExecute.mockRejectedValueOnce(new Error("step query timeout"));

    const app = await createPublicStatsApp();

    const response = await request(app).get("/stats/public");

    expect(response.status).toBe(200);
    expect(response.body.stats.emailsSent).toBe(50);
    expect(response.body.stepStats).toBeUndefined();
  });
});
