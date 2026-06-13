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

import { clearStatsCache } from "../../src/lib/stats-cache";

async function createPublicStatsApp() {
  const publicRouter = (await import("../../src/routes/analytics-public")).default;
  const app = express();
  app.use(express.json());
  app.use(publicRouter);
  return app;
}

function makeStatsRow(overrides: Partial<Record<string, number>> = {}) {
  return {
    esSent: 0, esOpened: 0, esClicked: 0, esBounced: 0, esUnsubscribed: 0,
    rsSent: 0, rsOpened: 0, rsClicked: 0, rsBounced: 0, rsUnsubscribed: 0,
    rdUnsubscribe: 0,
    ...overrides,
  };
}

/** A row from the latest-sentiment query (querySentiment). */
function makeSentimentRow(overrides: Partial<Record<string, number>> = {}) {
  return {
    rdInterested: 0, rdMeetingBooked: 0, rdClosed: 0,
    rdNotInterested: 0, rdWrongPerson: 0,
    rdNeutral: 0, rdAutoReply: 0, rdOutOfOffice: 0,
    ...overrides,
  };
}

describe("GET /stats", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockExecute.mockReset();
    mockExecute.mockResolvedValue({ rows: [] });
    clearStatsCache();
  });

  it("should return stats without requiring identity headers", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({
        esSent: 100, esOpened: 55, esBounced: 5,
        rsSent: 90, rsOpened: 50, rsBounced: 3,
      })],
    });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 110 }] });
    mockExecute.mockResolvedValueOnce({ rows: [makeSentimentRow({ rdInterested: 3 })] });

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
    mockExecute.mockResolvedValueOnce({ rows: [makeSentimentRow()] }); // latest-sentiment query
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
      rows: [makeStatsRow({ esSent: 50, rsSent: 40 })],
    });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 50 }] });
    mockExecute.mockResolvedValueOnce({ rows: [makeSentimentRow({ rdInterested: 5 })] });
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

describe("GET /stats/engagement-latency", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("should aggregate public engagement latency for a workflow slug set", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [
        {
          groupKey: "__total__",
          clickSampleSize: 4,
          clickAverageMs: 86_400_000,
          clickMedianMs: 43_200_000,
          positiveReplySampleSize: 3,
          positiveReplyAverageMs: 172_800_000,
          positiveReplyMedianMs: 129_600_000,
        },
      ],
    });

    const app = await createPublicStatsApp();
    const response = await request(app)
      .get("/stats/engagement-latency")
      .query({ workflowSlugs: "sales-outreach-v1,sales-outreach-v2" });

    expect(response.status).toBe(200);
    expect(response.body).toEqual({
      workflowSlugs: ["sales-outreach-v1", "sales-outreach-v2"],
      timeToFirstLinkClick: {
        averageMs: 86_400_000,
        medianMs: 43_200_000,
        sampleSize: 4,
      },
      timeToFirstPositiveReply: {
        averageMs: 172_800_000,
        medianMs: 129_600_000,
        sampleSize: 3,
      },
    });

    const sqlText = extractSqlText(mockExecute.mock.calls[0][0]);
    expect(sqlText).toContain("workflow_slug");
    expect(sqlText).toContain("email_sent");
    expect(sqlText).toContain("email_link_clicked");
    expect(sqlText).toContain("lead_interested");
    expect(sqlText).not.toContain("c.org_id");
  });

  it("should return null averages and medians when sample sizes are zero", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [
        {
          groupKey: "__total__",
          clickSampleSize: 0,
          clickAverageMs: null,
          clickMedianMs: null,
          positiveReplySampleSize: 0,
          positiveReplyAverageMs: null,
          positiveReplyMedianMs: null,
        },
      ],
    });

    const app = await createPublicStatsApp();
    const response = await request(app)
      .get("/stats/engagement-latency")
      .query({ workflowSlugs: "sales-outreach-v1" });

    expect(response.status).toBe(200);
    expect(response.body.timeToFirstLinkClick).toEqual({
      averageMs: null,
      medianMs: null,
      sampleSize: 0,
    });
    expect(response.body.timeToFirstPositiveReply).toEqual({
      averageMs: null,
      medianMs: null,
      sampleSize: 0,
    });
  });

  it("should fail loudly when workflowSlugs is missing", async () => {
    const app = await createPublicStatsApp();
    const response = await request(app).get("/stats/engagement-latency");

    expect(response.status).toBe(400);
    expect(response.body.error).toBe("Query parameter 'workflowSlugs' is required");
    expect(mockExecute).not.toHaveBeenCalled();
  });

  it("should reject unsupported groupBy values", async () => {
    const app = await createPublicStatsApp();
    const response = await request(app)
      .get("/stats/engagement-latency")
      .query({ workflowSlugs: "sales-outreach-v1", groupBy: "workflowSlug" });

    expect(response.status).toBe(400);
    expect(response.body.error).toBe("Query parameter 'groupBy' is not supported; pass workflowSlugs instead");
    expect(mockExecute).not.toHaveBeenCalled();
  });
});

describe("POST /stats/engagement-latency/grouped", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("should aggregate public engagement latency for workflow slug groups", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [
        {
          groupKey: "sales-outreach",
          clickSampleSize: 2,
          clickAverageMs: 10,
          clickMedianMs: 10,
          positiveReplySampleSize: 0,
          positiveReplyAverageMs: null,
          positiveReplyMedianMs: null,
        },
        {
          groupKey: "sales-outreach-premium",
          clickSampleSize: 0,
          clickAverageMs: null,
          clickMedianMs: null,
          positiveReplySampleSize: 1,
          positiveReplyAverageMs: 20,
          positiveReplyMedianMs: 20,
        },
      ],
    });

    const app = await createPublicStatsApp();
    const response = await request(app)
      .post("/stats/engagement-latency/grouped")
      .send({
        groups: {
          "sales-outreach": { workflowSlugs: ["sales-outreach-v1", "sales-outreach-v2"] },
          "sales-outreach-premium": { workflowSlugs: ["sales-outreach-premium-v1"] },
        },
      });

    expect(response.status).toBe(200);
    expect(response.body.groups).toEqual([
      {
        key: "sales-outreach",
        workflowSlugs: ["sales-outreach-v1", "sales-outreach-v2"],
        timeToFirstLinkClick: { averageMs: 10, medianMs: 10, sampleSize: 2 },
        timeToFirstPositiveReply: { averageMs: null, medianMs: null, sampleSize: 0 },
      },
      {
        key: "sales-outreach-premium",
        workflowSlugs: ["sales-outreach-premium-v1"],
        timeToFirstLinkClick: { averageMs: null, medianMs: null, sampleSize: 0 },
        timeToFirstPositiveReply: { averageMs: 20, medianMs: 20, sampleSize: 1 },
      },
    ]);
    expect(mockExecute).toHaveBeenCalledTimes(1);
  });
});
