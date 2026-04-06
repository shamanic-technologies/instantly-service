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

// Mock dynasty client
const mockResolveWorkflow = vi.fn();
const mockResolveFeature = vi.fn();
const mockFetchWorkflowDynasties = vi.fn();
const mockFetchFeatureDynasties = vi.fn();

vi.mock("../../src/lib/dynasty-client", () => ({
  resolveWorkflowDynastySlugs: (...args: unknown[]) => mockResolveWorkflow(...args),
  resolveFeatureDynastySlugs: (...args: unknown[]) => mockResolveFeature(...args),
  fetchWorkflowDynasties: (...args: unknown[]) => mockFetchWorkflowDynasties(...args),
  fetchFeatureDynasties: (...args: unknown[]) => mockFetchFeatureDynasties(...args),
  buildSlugToDynastyMap: (dynasties: { dynastySlug: string; slugs: string[] }[]) => {
    const map = new Map<string, string>();
    for (const d of dynasties) { for (const s of d.slugs) map.set(s, d.dynastySlug); }
    return map;
  },
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

describe("GET /stats", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockResolveWorkflow.mockResolvedValue([]);
    mockResolveFeature.mockResolvedValue([]);
    mockFetchWorkflowDynasties.mockResolvedValue([]);
    mockFetchFeatureDynasties.mockResolvedValue([]);
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

    const response = await request(app).get("/stats");

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

    await request(app).get("/stats").query({ runIds: "run-1" });

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
      .get("/stats")
      .query({ runIds: "run-1", brandId: "brand-1", campaignId: "camp-1" });

    expect(response.status).toBe(200);
    expect(response.body.stats.emailsSent).toBe(10);

    const sqlObj = mockExecute.mock.calls[0][0];
    const sqlText = extractSqlText(sqlObj);
    expect(sqlText).toContain("run_id IN");
    expect(sqlText).toContain("brand_ids");
  });

  it("should use TRUE when no filters provided", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [makeStatsRow()] });
    // Contacted count
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 0 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createPublicStatsApp();

    await request(app).get("/stats");

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

    const response = await request(app).get("/stats");

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

    await request(app).get("/stats");

    const sqlObj = mockExecute.mock.calls[0][0];
    const sqlText = extractSqlText(sqlObj);
    expect(sqlText).toContain("lead_email != e.account_email");
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
      rows: [makeStatsRow({ emailsSent: 50, emailsReplied: 5, recipients: 40 })],
    });
    // Contacted count
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 50 }] });
    mockExecute.mockRejectedValueOnce(new Error("step query timeout"));

    const app = await createPublicStatsApp();

    const response = await request(app).get("/stats");

    expect(response.status).toBe(200);
    expect(response.body.stats.emailsSent).toBe(50);
    expect(response.body.stepStats).toBeUndefined();
  });

  // ─── workflowSlugs (plural, comma-separated) filter ─────────────────────────

  it("should filter by workflowSlugs (comma-separated)", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({ emailsSent: 35, recipients: 15 })],
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
      rows: [makeStatsRow({ emailsSent: 25, recipients: 12 })],
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

  // ─── workflowDynastySlug filter ──────────────────────────────────────────────

  it("should resolve workflowDynastySlug and use IN clause", async () => {
    mockResolveWorkflow.mockResolvedValueOnce(["cold-email", "cold-email-v2"]);

    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({ emailsSent: 50, recipients: 30 })],
    });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 30 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createPublicStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({ workflowDynastySlug: "cold-email" });

    expect(response.status).toBe(200);
    expect(response.body.stats.emailsSent).toBe(50);

    const sqlText = extractSqlText(mockExecute.mock.calls[0][0]);
    expect(sqlText).toContain("workflow_slug IN");
  });

  it("should return zero stats when workflowDynastySlug resolves to empty", async () => {
    mockResolveWorkflow.mockResolvedValueOnce([]);

    const app = await createPublicStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({ workflowDynastySlug: "nonexistent-dynasty" });

    expect(response.status).toBe(200);
    expect(response.body.stats.emailsSent).toBe(0);
    expect(response.body.recipients).toBe(0);
    expect(mockExecute).not.toHaveBeenCalled();
  });

  // ─── featureDynastySlug filter ───────────────────────────────────────────────

  it("should resolve featureDynastySlug and use IN clause", async () => {
    mockResolveFeature.mockResolvedValueOnce(["feat-alpha", "feat-alpha-v2"]);

    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({ emailsSent: 40, recipients: 20 })],
    });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 20 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createPublicStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({ featureDynastySlug: "feat-alpha" });

    expect(response.status).toBe(200);
    expect(response.body.stats.emailsSent).toBe(40);

    const sqlText = extractSqlText(mockExecute.mock.calls[0][0]);
    expect(sqlText).toContain("feature_slug IN");
  });

  it("should return zero stats when featureDynastySlug resolves to empty", async () => {
    mockResolveFeature.mockResolvedValueOnce([]);

    const app = await createPublicStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({ featureDynastySlug: "nonexistent" });

    expect(response.status).toBe(200);
    expect(response.body.stats.emailsSent).toBe(0);
    expect(mockExecute).not.toHaveBeenCalled();
  });

  // ─── groupBy: workflowDynastySlug ───────────────────────────────────────────

  it("should group by workflowDynastySlug and merge versioned slugs", async () => {
    mockFetchWorkflowDynasties.mockResolvedValueOnce([
      { dynastySlug: "cold-email", slugs: ["cold-email", "cold-email-v2"] },
    ]);

    mockExecute.mockResolvedValueOnce({
      rows: [
        { groupKey: "cold-email", emailsSent: 30, emailsDelivered: 28, emailsOpened: 10, emailsClicked: 1, emailsReplied: 2, emailsBounced: 2, repliesAutoReply: 0, repliesNotInterested: 0, repliesOutOfOffice: 0, repliesUnsubscribe: 0, recipients: 15 },
        { groupKey: "cold-email-v2", emailsSent: 20, emailsDelivered: 18, emailsOpened: 8, emailsClicked: 0, emailsReplied: 1, emailsBounced: 2, repliesAutoReply: 0, repliesNotInterested: 0, repliesOutOfOffice: 0, repliesUnsubscribe: 0, recipients: 10 },
      ],
    });
    mockExecute.mockResolvedValueOnce({
      rows: [
        { groupKey: "cold-email", emailsContacted: 15 },
        { groupKey: "cold-email-v2", emailsContacted: 10 },
      ],
    });

    const app = await createPublicStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({ groupBy: "workflowDynastySlug" });

    expect(response.status).toBe(200);
    expect(response.body.groups).toHaveLength(1);
    expect(response.body.groups[0].key).toBe("cold-email");
    expect(response.body.groups[0].stats.emailsSent).toBe(50);
  });

  // ─── groupBy: featureDynastySlug ────────────────────────────────────────────

  it("should group by featureDynastySlug and merge versioned slugs", async () => {
    mockFetchFeatureDynasties.mockResolvedValueOnce([
      { dynastySlug: "feat-alpha", slugs: ["feat-alpha", "feat-alpha-v2"] },
    ]);

    mockExecute.mockResolvedValueOnce({
      rows: [
        { groupKey: "feat-alpha", emailsSent: 20, emailsDelivered: 18, emailsOpened: 5, emailsClicked: 0, emailsReplied: 1, emailsBounced: 2, repliesAutoReply: 0, repliesNotInterested: 0, repliesOutOfOffice: 0, repliesUnsubscribe: 0, recipients: 10 },
        { groupKey: "feat-alpha-v2", emailsSent: 10, emailsDelivered: 9, emailsOpened: 3, emailsClicked: 0, emailsReplied: 0, emailsBounced: 1, repliesAutoReply: 0, repliesNotInterested: 0, repliesOutOfOffice: 0, repliesUnsubscribe: 0, recipients: 5 },
      ],
    });
    mockExecute.mockResolvedValueOnce({
      rows: [
        { groupKey: "feat-alpha", emailsContacted: 10 },
        { groupKey: "feat-alpha-v2", emailsContacted: 5 },
      ],
    });

    const app = await createPublicStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({ groupBy: "featureDynastySlug" });

    expect(response.status).toBe(200);
    expect(response.body.groups).toHaveLength(1);
    expect(response.body.groups[0].key).toBe("feat-alpha");
    expect(response.body.groups[0].stats.emailsSent).toBe(30);
  });

  // ─── groupBy: orphan slugs ──────────────────────────────────────────────────

  it("should fall back to raw slug for orphan slugs not in any dynasty", async () => {
    mockFetchWorkflowDynasties.mockResolvedValueOnce([
      { dynastySlug: "cold-email", slugs: ["cold-email"] },
    ]);

    mockExecute.mockResolvedValueOnce({
      rows: [
        { groupKey: "cold-email", emailsSent: 10, emailsDelivered: 10, emailsOpened: 5, emailsClicked: 0, emailsReplied: 1, emailsBounced: 0, repliesAutoReply: 0, repliesNotInterested: 0, repliesOutOfOffice: 0, repliesUnsubscribe: 0, recipients: 5 },
        { groupKey: "orphan-wf", emailsSent: 5, emailsDelivered: 5, emailsOpened: 2, emailsClicked: 0, emailsReplied: 0, emailsBounced: 0, repliesAutoReply: 0, repliesNotInterested: 0, repliesOutOfOffice: 0, repliesUnsubscribe: 0, recipients: 3 },
      ],
    });
    mockExecute.mockResolvedValueOnce({
      rows: [
        { groupKey: "cold-email", emailsContacted: 5 },
        { groupKey: "orphan-wf", emailsContacted: 3 },
      ],
    });

    const app = await createPublicStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({ groupBy: "workflowDynastySlug" });

    expect(response.status).toBe(200);
    expect(response.body.groups).toHaveLength(2);
    const orphan = response.body.groups.find((g: any) => g.key === "orphan-wf");
    expect(orphan.stats.emailsSent).toBe(5);
  });
});
