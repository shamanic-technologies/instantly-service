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
    esSent: 0, esOpened: 0, esClicked: 0, esBounced: 0,
    rsSent: 0, rsOpened: 0, rsClicked: 0, rsBounced: 0,
    rdInterested: 0, rdMeetingBooked: 0, rdClosed: 0,
    rdNotInterested: 0, rdWrongPerson: 0, rdUnsubscribe: 0,
    rdNeutral: 0, rdAutoReply: 0, rdOutOfOffice: 0,
    ...overrides,
  };
}

describe("GET /stats", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("should return stats without requiring identity headers", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({
        esSent: 100, esOpened: 55, esBounced: 5,
        rsSent: 90, rsOpened: 50, rsBounced: 3,
        rdInterested: 3,
      })],
    });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 110 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createPublicStatsApp();

    const response = await request(app).get("/stats");

    expect(response.status).toBe(200);
    expect(response.body.recipientStats.contacted).toBe(110);
    expect(response.body.recipientStats.sent).toBe(90);
    expect(response.body.recipientStats.repliesPositive).toBe(3);
    expect(response.body.emailStats.sent).toBe(100);
  });

  it("should NOT include org_id in WHERE clause", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [makeStatsRow()] });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 0 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createPublicStatsApp();

    await request(app).get("/stats").query({ runIds: "run-1" });

    const sqlObj = mockExecute.mock.calls[0][0];
    const sqlText = extractSqlText(sqlObj);
    expect(sqlText).not.toContain("org_id");
    expect(sqlText).toContain("run_id IN");
  });

  it("should accept runIds, brandId, and campaignId filters", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({ esSent: 10, rsSent: 5 })],
    });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 5 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createPublicStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({ runIds: "run-1", brandId: "brand-1", campaignId: "camp-1" });

    expect(response.status).toBe(200);
    expect(response.body.emailStats.sent).toBe(10);

    const sqlObj = mockExecute.mock.calls[0][0];
    const sqlText = extractSqlText(sqlObj);
    expect(sqlText).toContain("run_id IN");
    expect(sqlText).toContain("brand_ids");
  });

  it("should use TRUE when no filters provided", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [makeStatsRow()] });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 0 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createPublicStatsApp();

    await request(app).get("/stats");

    const sqlObj = mockExecute.mock.calls[0][0];
    const sqlText = extractSqlText(sqlObj);
    expect(sqlText).toContain("TRUE");
    expect(sqlText).not.toContain("org_id");
  });

  it("should include per-step stats in emailStats when step data exists", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({ esSent: 30, rsSent: 10 })],
    });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 10 }] });
    mockExecute.mockResolvedValueOnce({
      rows: [
        { step: 1, sent: 10, opened: 8, clicked: 0, bounced: 1, rdInterested: 1, rdMeetingBooked: 0, rdClosed: 0, rdNotInterested: 0, rdWrongPerson: 0, rdUnsubscribe: 0, rdNeutral: 0, rdAutoReply: 0, rdOutOfOffice: 0 },
        { step: 2, sent: 10, opened: 5, clicked: 0, bounced: 0, rdInterested: 1, rdMeetingBooked: 0, rdClosed: 0, rdNotInterested: 0, rdWrongPerson: 0, rdUnsubscribe: 0, rdNeutral: 0, rdAutoReply: 0, rdOutOfOffice: 0 },
      ],
    });

    const app = await createPublicStatsApp();

    const response = await request(app).get("/stats");

    expect(response.status).toBe(200);
    expect(response.body.emailStats.stepStats).toHaveLength(2);
    expect(response.body.emailStats.stepStats[0].step).toBe(1);
    expect(response.body.emailStats.stepStats[0].delivered).toBe(9); // 10 - 1
  });

  it("should exclude internal emails from stats query", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [makeStatsRow()] });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 0 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createPublicStatsApp();

    await request(app).get("/stats");

    const sqlObj = mockExecute.mock.calls[0][0];
    const sqlText = extractSqlText(sqlObj);
    expect(sqlText).toContain("account_email IS NULL OR e.lead_email != e.account_email");
    expect(sqlText).toContain("lead_email NOT IN");
  });

  it("should return 500 on db error", async () => {
    mockExecute.mockRejectedValueOnce(new Error("DB connection failed"));

    const app = await createPublicStatsApp();

    const response = await request(app).get("/stats");

    expect(response.status).toBe(500);
    expect(response.body.error).toBe("Failed to aggregate stats");
  });

  it("should return overall stats when step query fails", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({ esSent: 50, rsSent: 40, rdInterested: 5 })],
    });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 50 }] });
    mockExecute.mockRejectedValueOnce(new Error("step query timeout"));

    const app = await createPublicStatsApp();

    const response = await request(app).get("/stats");

    expect(response.status).toBe(200);
    expect(response.body.recipientStats.sent).toBe(40);
    expect(response.body.emailStats.sent).toBe(50);
    expect(response.body.emailStats.stepStats).toBeUndefined();
  });

  // ─── workflowSlugs (plural, comma-separated) filter ─────────────────────────

  it("should filter by workflowSlugs (comma-separated)", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({ esSent: 35, rsSent: 15 })],
    });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 15 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createPublicStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({ workflowSlugs: "cold-email-v1,cold-email-v2" });

    expect(response.status).toBe(200);
    const sqlText = extractSqlText(mockExecute.mock.calls[0][0]);
    expect(sqlText).toContain("workflow_slug IN");
  });

  // ─── featureSlugs (plural, comma-separated) filter ─────────────────────────

  it("should filter by featureSlugs (comma-separated)", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({ esSent: 25, rsSent: 12 })],
    });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 12 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createPublicStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({ featureSlugs: "feat-a,feat-b" });

    expect(response.status).toBe(200);
    const sqlText = extractSqlText(mockExecute.mock.calls[0][0]);
    expect(sqlText).toContain("feature_slug IN");
  });

});
