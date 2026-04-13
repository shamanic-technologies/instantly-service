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

import { requireOrgId } from "../../src/middleware/requireOrgId";

const identityHeadersObj = { "x-org-id": "test-org", "x-user-id": "test-user", "x-run-id": "test-run" };

async function createStatsApp() {
  const analyticsRouter = (await import("../../src/routes/analytics")).default;
  const app = express();
  app.use(express.json());
  app.use(requireOrgId, analyticsRouter);
  return app;
}

/** Helper: full stats row with all fields */
function makeStatsRow(overrides: Partial<Record<string, number>> = {}) {
  return {
    emailsSent: 0, emailsDelivered: 0, emailsOpened: 0,
    emailsClicked: 0, emailsBounced: 0,
    rdInterested: 0, rdMeetingBooked: 0, rdClosed: 0,
    rdNotInterested: 0, rdWrongPerson: 0, rdUnsubscribe: 0,
    rdNeutral: 0, rdAutoReply: 0, rdOutOfOffice: 0,
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

  it("should strip trailing commas from x-org-id before querying", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [makeStatsRow({ emailsSent: 5 })] });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 2 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .set({ ...identityHeadersObj, "x-org-id": "test-org," });

    expect(response.status).toBe(200);

    // Verify the SQL uses the cleaned org_id (no trailing comma)
    const sqlObj = mockExecute.mock.calls[0][0];
    const sqlText = extractSqlText(sqlObj);
    expect(sqlText).toContain("c.org_id =");
    // The parameterized value should NOT contain a trailing comma
    const params = sqlObj.queryChunks || [];
    const flatParams = JSON.stringify(params);
    expect(flatParams).not.toContain("test-org,");
  });

  it("should return global stats when no filters provided", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({
        emailsSent: 100, emailsDelivered: 95, emailsOpened: 50,
        emailsClicked: 5, emailsBounced: 5,
        rdInterested: 3, rdAutoReply: 1, rdNotInterested: 2,
        rdOutOfOffice: 1, recipients: 90,
      })],
    });
    // Contacted count query
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 120 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatsApp();

    const response = await request(app).get("/stats").set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(response.body.stats.emailsContacted).toBe(120);
    expect(response.body.stats.emailsSent).toBe(100);
    expect(response.body.stats.repliesPositive).toBe(3);
    expect(response.body.stats.repliesDetail.interested).toBe(3);
    expect(response.body.recipients).toBe(90);
  });

  it("should return zeros when no events match", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [makeStatsRow()] });
    // Contacted count query
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 0 }] });
    // Step stats
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(response.body.stats.emailsSent).toBe(0);
    expect(response.body.stats.repliesPositive).toBe(0);
    expect(response.body.stats.repliesNegative).toBe(0);
    expect(response.body.stats.repliesNeutral).toBe(0);
    expect(response.body.stats.repliesAutoReply).toBe(0);
    expect(response.body.recipients).toBe(0);
    // stepStats should not be present when empty
    expect(response.body.stepStats).toBeUndefined();
  });

  it("should aggregate event counts correctly", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({
        emailsSent: 80, emailsDelivered: 75, emailsOpened: 40,
        emailsClicked: 3, emailsBounced: 5,
        rdInterested: 1, rdAutoReply: 1, rdNotInterested: 1,
        rdOutOfOffice: 2, recipients: 75,
      })],
    });
    // Contacted count
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 80 }] });
    // Step stats
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(response.body.stats.emailsSent).toBe(80);
    expect(response.body.stats.repliesPositive).toBe(1);
    expect(response.body.stats.repliesAutoReply).toBe(3); // autoReply(1) + outOfOffice(2)
    expect(response.body.recipients).toBe(75);
  });

  it("should compute reply aggregates from detail correctly", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({
        emailsSent: 500, emailsDelivered: 480, emailsOpened: 200,
        rdInterested: 3, rdMeetingBooked: 2, rdClosed: 1,
        rdNotInterested: 4, rdWrongPerson: 1, rdUnsubscribe: 2,
        rdNeutral: 5,
        rdAutoReply: 11, rdOutOfOffice: 13,
        recipients: 400,
      })],
    });
    // Contacted count
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 500 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatsApp();

    const response = await request(app).get("/stats").set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(response.body.stats.repliesPositive).toBe(6);  // 3+2+1
    expect(response.body.stats.repliesNegative).toBe(7);  // 4+1+2
    expect(response.body.stats.repliesNeutral).toBe(5);
    expect(response.body.stats.repliesAutoReply).toBe(24); // 11+13
    expect(response.body.stats.repliesDetail.interested).toBe(3);
    expect(response.body.stats.repliesDetail.meetingBooked).toBe(2);
    expect(response.body.stats.repliesDetail.closed).toBe(1);
    expect(response.body.stats.repliesDetail.wrongPerson).toBe(1);
    expect(response.body.stats.repliesDetail.neutral).toBe(5);
  });

  it("should include per-step stats when step data exists", async () => {
    // Aggregate stats
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({
        emailsSent: 30, emailsDelivered: 28, emailsOpened: 15,
        emailsClicked: 1, emailsBounced: 2, rdInterested: 3,
        recipients: 10,
      })],
    });
    // Contacted count
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 10 }] });
    // Step stats
    mockExecute.mockResolvedValueOnce({
      rows: [
        { step: 1, emailsSent: 10, emailsOpened: 8, emailsClicked: 3, emailsBounced: 1, rdInterested: 1, rdMeetingBooked: 0, rdClosed: 0, rdNotInterested: 0, rdWrongPerson: 0, rdUnsubscribe: 0, rdNeutral: 0, rdAutoReply: 0, rdOutOfOffice: 0 },
        { step: 2, emailsSent: 10, emailsOpened: 5, emailsClicked: 1, emailsBounced: 1, rdInterested: 1, rdMeetingBooked: 0, rdClosed: 0, rdNotInterested: 0, rdWrongPerson: 0, rdUnsubscribe: 0, rdNeutral: 0, rdAutoReply: 0, rdOutOfOffice: 0 },
        { step: 3, emailsSent: 10, emailsOpened: 2, emailsClicked: 0, emailsBounced: 0, rdInterested: 1, rdMeetingBooked: 0, rdClosed: 0, rdNotInterested: 0, rdWrongPerson: 0, rdUnsubscribe: 0, rdNeutral: 0, rdAutoReply: 0, rdOutOfOffice: 0 },
      ],
    });

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({ campaignId: "camp-1" })
      .set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(response.body.stepStats).toHaveLength(3);
    expect(response.body.stepStats[0].step).toBe(1);
    expect(response.body.stepStats[0].emailsSent).toBe(10);
    expect(response.body.stepStats[0].emailsClicked).toBe(3);
    expect(response.body.stepStats[0].repliesPositive).toBe(1);
    expect(response.body.stepStats[0].repliesDetail.interested).toBe(1);
    expect(response.body.stepStats[2].step).toBe(3);
    expect(response.body.stepStats[2].emailsClicked).toBe(0);
    expect(response.body.stepStats[2].emailsBounced).toBe(0);
    expect(response.body.stepStats[2].repliesPositive).toBe(1);
  });

  it("should return zero stats when db returns empty rows", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [] });
    // Contacted count
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 0 }] });

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(response.body.stats.emailsContacted).toBe(0);
    expect(response.body.stats.emailsSent).toBe(0);
    expect(response.body.stats.repliesPositive).toBe(0);
    expect(response.body.recipients).toBe(0);
  });

  it("should accept runIds filter and use IN clause (not ANY)", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({ emailsSent: 10, emailsDelivered: 10, emailsOpened: 5, recipients: 10 })],
    });
    // Contacted count
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 10 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({ runIds: "run-1,run-2" })
      .set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(response.body.stats.emailsSent).toBe(10);

    // Verify SQL uses IN (not ANY) to avoid drizzle array serialization bug
    const sqlObj = mockExecute.mock.calls[0][0];
    const sqlText = extractSqlText(sqlObj);
    expect(sqlText).toContain("run_id IN");
    expect(sqlText).not.toContain("ANY");
  });

  it("should exclude internal emails and sender from stats query", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [makeStatsRow()] });
    // Contacted count
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 0 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] }); // step query
    const app = await createStatsApp();

    await request(app).get("/stats").set(identityHeadersObj);

    // Stats query + contacted count + step query
    expect(mockExecute).toHaveBeenCalledTimes(3);

    const sqlObj = mockExecute.mock.calls[0][0];
    const sqlText = extractSqlText(sqlObj);

    expect(sqlText).toContain("account_email IS NULL OR e.recipient_email != e.account_email");
    expect(sqlText).toContain("recipient_email NOT IN");
    expect(sqlText).toContain("LIKE");
  });

  it("should return 500 on db error", async () => {
    mockExecute.mockRejectedValueOnce(new Error("DB connection failed"));

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .set(identityHeadersObj);

    expect(response.status).toBe(500);
    expect(response.body.error).toBe("Failed to aggregate stats");
  });

  it("should return overall stats when step query fails", async () => {
    // Aggregate stats succeed
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({
        emailsSent: 50, emailsDelivered: 48, emailsOpened: 20,
        emailsClicked: 2, emailsBounced: 2,
        rdInterested: 5, rdNotInterested: 1, recipients: 40,
      })],
    });
    // Contacted count
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 50 }] });
    // Step query fails
    mockExecute.mockRejectedValueOnce(new Error("step query timeout"));

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(response.body.stats.emailsSent).toBe(50);
    expect(response.body.stats.repliesPositive).toBe(5);
    expect(response.body.recipients).toBe(40);
    expect(response.body.stepStats).toBeUndefined();
  });

  it("should log cause message from DrizzleQueryError", async () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    // Aggregate stats succeed
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({ emailsSent: 10, emailsDelivered: 10, emailsOpened: 5, recipients: 10 })],
    });
    // Contacted count
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 10 }] });
    // Step query fails with a DrizzleQueryError-like structure
    const pgError = new Error("canceling statement due to statement timeout");
    const drizzleError = new Error("Failed query: SELECT ...");
    drizzleError.cause = pgError;
    mockExecute.mockRejectedValueOnce(drizzleError);

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(consoleSpy).toHaveBeenCalledWith(
      expect.stringContaining("canceling statement due to statement timeout"),
    );

    consoleSpy.mockRestore();
  });

  it("should query all 9 reply event types in SQL", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [] });
    // Contacted count
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 0 }] });
    const app = await createStatsApp();

    await request(app).get("/stats").set(identityHeadersObj);

    const sqlObj = mockExecute.mock.calls[0][0];
    const sqlText = extractSqlText(sqlObj);
    expect(sqlText).toContain("lead_interested");
    expect(sqlText).toContain("lead_meeting_booked");
    expect(sqlText).toContain("lead_closed");
    expect(sqlText).toContain("lead_not_interested");
    expect(sqlText).toContain("lead_wrong_person");
    expect(sqlText).toContain("lead_unsubscribed");
    expect(sqlText).toContain("lead_neutral");
    expect(sqlText).toContain("auto_reply_received");
    expect(sqlText).toContain("lead_out_of_office");
    // reply_received should NOT appear as a standalone filter (auto_reply_received is fine)
    expect(sqlText).not.toMatch(/event_type = 'reply_received'/);
  });

  // ─── featureSlug filter ──────────────────────────────────────────────────────

  it("should filter by featureSlugs (comma-separated)", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({ emailsSent: 20, recipients: 10 })],
    });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 10 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({ featureSlugs: "cold-email-sophia,cold-email-beta" })
      .set(identityHeadersObj);

    expect(response.status).toBe(200);
    const sqlText = extractSqlText(mockExecute.mock.calls[0][0]);
    expect(sqlText).toContain("feature_slug IN");
  });

  // ─── workflowDynastySlug filter ──────────────────────────────────────────────

  it("should resolve workflowDynastySlug and use IN clause", async () => {
    mockResolveWorkflow.mockResolvedValueOnce(["cold-email", "cold-email-v2", "cold-email-v3"]);

    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({ emailsSent: 50, recipients: 30 })],
    });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 30 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({ workflowDynastySlug: "cold-email" })
      .set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(response.body.stats.emailsSent).toBe(50);

    expect(mockResolveWorkflow).toHaveBeenCalledWith("cold-email", expect.any(Object));

    const sqlText = extractSqlText(mockExecute.mock.calls[0][0]);
    expect(sqlText).toContain("workflow_slug IN");
  });

  it("should return zero stats when workflowDynastySlug resolves to empty", async () => {
    mockResolveWorkflow.mockResolvedValueOnce([]);

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({ workflowDynastySlug: "nonexistent-dynasty" })
      .set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(response.body.stats.emailsSent).toBe(0);
    expect(response.body.recipients).toBe(0);
    // DB should not have been hit
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

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({ featureDynastySlug: "feat-alpha" })
      .set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(response.body.stats.emailsSent).toBe(40);

    const sqlText = extractSqlText(mockExecute.mock.calls[0][0]);
    expect(sqlText).toContain("feature_slug IN");
  });

  it("should return zero stats when featureDynastySlug resolves to empty", async () => {
    mockResolveFeature.mockResolvedValueOnce([]);

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({ featureDynastySlug: "nonexistent" })
      .set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(response.body.stats.emailsSent).toBe(0);
    expect(mockExecute).not.toHaveBeenCalled();
  });

  // ─── Dynasty slug takes priority over exact slug ─────────────────────────────

  it("should prefer workflowDynastySlug over workflowSlug when both provided", async () => {
    mockResolveWorkflow.mockResolvedValueOnce(["cold-email", "cold-email-v2"]);

    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({ emailsSent: 30, recipients: 15 })],
    });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 15 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({ workflowSlugs: "cold-email", workflowDynastySlug: "cold-email" })
      .set(identityHeadersObj);

    expect(response.status).toBe(200);

    const sqlText = extractSqlText(mockExecute.mock.calls[0][0]);
    // Should use IN (dynasty), not exact match
    expect(sqlText).toContain("workflow_slug IN");
  });

  // ─── brandId filter uses ANY(brand_ids) for multi-brand support ─────────────

  it("should filter brandId using ANY(c.brand_ids) for multi-brand campaigns", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({ emailsSent: 10, recipients: 5 })],
    });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 5 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({ brandId: "brand-1" })
      .set(identityHeadersObj);

    expect(response.status).toBe(200);

    const sqlText = extractSqlText(mockExecute.mock.calls[0][0]);
    expect(sqlText).toContain("ANY");
    expect(sqlText).toContain("brand_ids");
  });

  // ─── Combined dynasty + other filters ────────────────────────────────────────

  it("should combine workflowDynastySlug with brandId filter", async () => {
    mockResolveWorkflow.mockResolvedValueOnce(["cold-email", "cold-email-v2"]);

    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({ emailsSent: 10, recipients: 5 })],
    });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 5 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({ workflowDynastySlug: "cold-email", brandId: "brand-1" })
      .set(identityHeadersObj);

    expect(response.status).toBe(200);

    const sqlText = extractSqlText(mockExecute.mock.calls[0][0]);
    expect(sqlText).toContain("workflow_slug IN");
    expect(sqlText).toContain("brand_ids");
  });

  // ─── groupBy: workflowDynastySlug ───────────────────────────────────────────

  it("should group by workflowDynastySlug and merge versioned slugs", async () => {
    mockFetchWorkflowDynasties.mockResolvedValueOnce([
      { dynastySlug: "cold-email", slugs: ["cold-email", "cold-email-v2"] },
    ]);

    // Events grouped by workflow_slug
    mockExecute.mockResolvedValueOnce({
      rows: [
        { groupKey: "cold-email", emailsSent: 30, emailsDelivered: 28, emailsOpened: 10, emailsClicked: 1, emailsBounced: 2, rdInterested: 2, rdMeetingBooked: 0, rdClosed: 0, rdNotInterested: 0, rdWrongPerson: 0, rdUnsubscribe: 0, rdNeutral: 0, rdAutoReply: 0, rdOutOfOffice: 0, recipients: 15 },
        { groupKey: "cold-email-v2", emailsSent: 20, emailsDelivered: 18, emailsOpened: 8, emailsClicked: 0, emailsBounced: 2, rdInterested: 1, rdMeetingBooked: 0, rdClosed: 0, rdNotInterested: 0, rdWrongPerson: 0, rdUnsubscribe: 0, rdNeutral: 0, rdAutoReply: 0, rdOutOfOffice: 0, recipients: 10 },
      ],
    });
    // Contacted counts grouped
    mockExecute.mockResolvedValueOnce({
      rows: [
        { groupKey: "cold-email", emailsContacted: 15 },
        { groupKey: "cold-email-v2", emailsContacted: 10 },
      ],
    });

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({ groupBy: "workflowDynastySlug" })
      .set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(response.body.groups).toHaveLength(1);
    expect(response.body.groups[0].key).toBe("cold-email");
    // Merged: 30 + 20 = 50
    expect(response.body.groups[0].stats.emailsSent).toBe(50);
    expect(response.body.groups[0].stats.emailsContacted).toBe(25);
    expect(response.body.groups[0].recipients).toBe(25);
  });

  // ─── groupBy: featureDynastySlug ────────────────────────────────────────────

  it("should group by featureDynastySlug and merge versioned slugs", async () => {
    mockFetchFeatureDynasties.mockResolvedValueOnce([
      { dynastySlug: "feat-alpha", slugs: ["feat-alpha", "feat-alpha-v2"] },
    ]);

    mockExecute.mockResolvedValueOnce({
      rows: [
        { groupKey: "feat-alpha", emailsSent: 20, emailsDelivered: 18, emailsOpened: 5, emailsClicked: 0, emailsBounced: 2, rdInterested: 1, rdMeetingBooked: 0, rdClosed: 0, rdNotInterested: 0, rdWrongPerson: 0, rdUnsubscribe: 0, rdNeutral: 0, rdAutoReply: 0, rdOutOfOffice: 0, recipients: 10 },
        { groupKey: "feat-alpha-v2", emailsSent: 10, emailsDelivered: 9, emailsOpened: 3, emailsClicked: 0, emailsBounced: 1, rdInterested: 0, rdMeetingBooked: 0, rdClosed: 0, rdNotInterested: 0, rdWrongPerson: 0, rdUnsubscribe: 0, rdNeutral: 0, rdAutoReply: 0, rdOutOfOffice: 0, recipients: 5 },
      ],
    });
    mockExecute.mockResolvedValueOnce({
      rows: [
        { groupKey: "feat-alpha", emailsContacted: 10 },
        { groupKey: "feat-alpha-v2", emailsContacted: 5 },
      ],
    });

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({ groupBy: "featureDynastySlug" })
      .set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(response.body.groups).toHaveLength(1);
    expect(response.body.groups[0].key).toBe("feat-alpha");
    expect(response.body.groups[0].stats.emailsSent).toBe(30);
    expect(response.body.groups[0].recipients).toBe(15);
  });

  // ─── groupBy: orphan slugs fall back to raw value ───────────────────────────

  it("should fall back to raw slug for orphan slugs not in any dynasty", async () => {
    mockFetchWorkflowDynasties.mockResolvedValueOnce([
      { dynastySlug: "cold-email", slugs: ["cold-email", "cold-email-v2"] },
    ]);

    mockExecute.mockResolvedValueOnce({
      rows: [
        { groupKey: "cold-email", emailsSent: 10, emailsDelivered: 10, emailsOpened: 5, emailsClicked: 0, emailsBounced: 0, rdInterested: 1, rdMeetingBooked: 0, rdClosed: 0, rdNotInterested: 0, rdWrongPerson: 0, rdUnsubscribe: 0, rdNeutral: 0, rdAutoReply: 0, rdOutOfOffice: 0, recipients: 5 },
        { groupKey: "orphan-workflow", emailsSent: 5, emailsDelivered: 5, emailsOpened: 2, emailsClicked: 0, emailsBounced: 0, rdInterested: 0, rdMeetingBooked: 0, rdClosed: 0, rdNotInterested: 0, rdWrongPerson: 0, rdUnsubscribe: 0, rdNeutral: 0, rdAutoReply: 0, rdOutOfOffice: 0, recipients: 3 },
      ],
    });
    mockExecute.mockResolvedValueOnce({
      rows: [
        { groupKey: "cold-email", emailsContacted: 5 },
        { groupKey: "orphan-workflow", emailsContacted: 3 },
      ],
    });

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({ groupBy: "workflowDynastySlug" })
      .set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(response.body.groups).toHaveLength(2);
    const coldEmail = response.body.groups.find((g: any) => g.key === "cold-email");
    const orphan = response.body.groups.find((g: any) => g.key === "orphan-workflow");
    expect(coldEmail.stats.emailsSent).toBe(10);
    expect(orphan.stats.emailsSent).toBe(5);
  });

  // ─── groupBy: empty dynasty → empty groups ──────────────────────────────────

  it("should return empty groups when dynasty resolves to empty for groupBy with dynasty filter", async () => {
    mockResolveWorkflow.mockResolvedValueOnce([]);

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({ workflowDynastySlug: "nonexistent", groupBy: "brandId" })
      .set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(response.body.groups).toEqual([]);
    expect(mockExecute).not.toHaveBeenCalled();
  });

  // ─── groupBy: brandId (lateral unnest) ──────────────────────────────────────

  it("should use CROSS JOIN LATERAL unnest for groupBy brandId instead of inline unnest in WHERE", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [
        { groupKey: "brand-1", emailsSent: 10, emailsDelivered: 10, emailsOpened: 5, emailsClicked: 0, emailsBounced: 0, rdInterested: 1, rdMeetingBooked: 0, rdClosed: 0, rdNotInterested: 0, rdWrongPerson: 0, rdUnsubscribe: 0, rdNeutral: 0, rdAutoReply: 0, rdOutOfOffice: 0, recipients: 5 },
      ],
    });
    mockExecute.mockResolvedValueOnce({
      rows: [{ groupKey: "brand-1", emailsContacted: 5 }],
    });

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({ groupBy: "brandId" })
      .set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(response.body.groups).toHaveLength(1);
    expect(response.body.groups[0].key).toBe("brand-1");

    // Both queries (events + contacted) must use LATERAL unnest and reference
    // the alias "brand_id" for GROUP BY / IS NOT NULL — not inline unnest()
    for (const call of mockExecute.mock.calls) {
      const sqlText = extractSqlText(call[0]);
      expect(sqlText).toContain("CROSS JOIN LATERAL unnest");
      expect(sqlText).toContain("brand_id IS NOT NULL");
      expect(sqlText).not.toContain("unnest(c.brand_ids) IS NOT NULL");
    }
  });

  // ─── groupBy: featureSlug ───────────────────────────────────────────────────

  it("should support groupBy featureSlug", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [
        { groupKey: "feat-1", emailsSent: 10, emailsDelivered: 10, emailsOpened: 5, emailsClicked: 0, emailsBounced: 0, rdInterested: 1, rdMeetingBooked: 0, rdClosed: 0, rdNotInterested: 0, rdWrongPerson: 0, rdUnsubscribe: 0, rdNeutral: 0, rdAutoReply: 0, rdOutOfOffice: 0, recipients: 5 },
      ],
    });
    mockExecute.mockResolvedValueOnce({
      rows: [{ groupKey: "feat-1", emailsContacted: 5 }],
    });

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({ groupBy: "featureSlug" })
      .set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(response.body.groups).toHaveLength(1);
    expect(response.body.groups[0].key).toBe("feat-1");
  });
});

describe("POST /stats/grouped", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("should return stats per group", async () => {
    // Promise.all interleaves: both events queries fire, then both contacted queries
    // Group 1 events query
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({
        emailsSent: 800, emailsDelivered: 750, emailsOpened: 310,
        rdInterested: 3, emailsBounced: 20, recipients: 400,
      })],
    });
    // Group 2 events query
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({
        emailsSent: 200, emailsDelivered: 190, emailsOpened: 80,
        rdInterested: 1, emailsBounced: 5, recipients: 100,
      })],
    });
    // Group 1 contacted count
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 450 }] });
    // Group 2 contacted count
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 120 }] });

    const app = await createStatsApp();

    const response = await request(app)
      .post("/stats/grouped")
      .set(identityHeadersObj)
      .send({
        groups: {
          "workflow-alpha": { runIds: ["run-1", "run-2"] },
          "workflow-beta": { runIds: ["run-3"] },
        },
      });

    expect(response.status).toBe(200);
    expect(response.body.groups).toHaveLength(2);

    const alpha = response.body.groups.find((g: any) => g.key === "workflow-alpha");
    expect(alpha.stats.emailsSent).toBe(800);
    expect(alpha.stats.repliesPositive).toBe(3);
    expect(alpha.recipients).toBe(400);

    const beta = response.body.groups.find((g: any) => g.key === "workflow-beta");
    expect(beta.stats.emailsSent).toBe(200);
    expect(beta.stats.repliesPositive).toBe(1);
    expect(beta.recipients).toBe(100);
  });

  it("should return zero stats for groups with no matching events", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [] });
    // Contacted count
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 0 }] });

    const app = await createStatsApp();

    const response = await request(app)
      .post("/stats/grouped")
      .set(identityHeadersObj)
      .send({
        groups: {
          "empty-workflow": { runIds: ["run-nonexistent"] },
        },
      });

    expect(response.status).toBe(200);
    expect(response.body.groups).toHaveLength(1);
    expect(response.body.groups[0].key).toBe("empty-workflow");
    expect(response.body.groups[0].stats.emailsSent).toBe(0);
    expect(response.body.groups[0].stats.repliesPositive).toBe(0);
    expect(response.body.groups[0].recipients).toBe(0);
  });

  it("should return empty groups array when no groups provided", async () => {
    const app = await createStatsApp();

    const response = await request(app)
      .post("/stats/grouped")
      .set(identityHeadersObj)
      .send({ groups: {} });

    expect(response.status).toBe(200);
    expect(response.body.groups).toEqual([]);
    expect(mockExecute).not.toHaveBeenCalled();
  });

  it("should return 400 for invalid request body", async () => {
    const app = await createStatsApp();

    const response = await request(app)
      .post("/stats/grouped")
      .set(identityHeadersObj)
      .send({ groups: { "bad-group": { runIds: [] } } });

    expect(response.status).toBe(400);
    expect(response.body.error).toBe("Invalid request");
  });

  it("should return 400 when groups field is missing", async () => {
    const app = await createStatsApp();

    const response = await request(app)
      .post("/stats/grouped")
      .set(identityHeadersObj)
      .send({});

    expect(response.status).toBe(400);
    expect(response.body.error).toBe("Invalid request");
  });

  it("should return 500 on db error", async () => {
    mockExecute.mockRejectedValueOnce(new Error("DB connection failed"));

    const app = await createStatsApp();

    const response = await request(app)
      .post("/stats/grouped")
      .set(identityHeadersObj)
      .send({
        groups: {
          "failing-group": { runIds: ["run-1"] },
        },
      });

    expect(response.status).toBe(500);
    expect(response.body.error).toBe("Failed to aggregate grouped stats");
  });

  it("should use IN clause for runIds in each group query", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({ emailsSent: 10, recipients: 5 })],
    });
    // Contacted count
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 5 }] });

    const app = await createStatsApp();

    await request(app)
      .post("/stats/grouped")
      .set(identityHeadersObj)
      .send({
        groups: {
          "test-group": { runIds: ["run-1", "run-2", "run-3"] },
        },
      });

    expect(mockExecute).toHaveBeenCalledTimes(2);
    const sqlObj = mockExecute.mock.calls[0][0];
    const sqlText = extractSqlText(sqlObj);
    expect(sqlText).toContain("run_id IN");
    expect(sqlText).not.toContain("ANY");
  });

  it("should exclude internal emails from grouped stats", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [] });
    // Contacted count
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 0 }] });

    const app = await createStatsApp();

    await request(app)
      .post("/stats/grouped")
      .set(identityHeadersObj)
      .send({
        groups: {
          "test-group": { runIds: ["run-1"] },
        },
      });

    const sqlObj = mockExecute.mock.calls[0][0];
    const sqlText = extractSqlText(sqlObj);
    expect(sqlText).toContain("account_email IS NULL OR e.recipient_email != e.account_email");
    expect(sqlText).toContain("recipient_email NOT IN");
  });
});
