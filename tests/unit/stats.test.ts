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

import { requireOrgId } from "../../src/middleware/requireOrgId";

const identityHeadersObj = { "x-org-id": "test-org", "x-user-id": "test-user", "x-run-id": "test-run" };

async function createStatsApp() {
  const analyticsRouter = (await import("../../src/routes/analytics")).default;
  const app = express();
  app.use(express.json());
  app.use(requireOrgId, analyticsRouter);
  return app;
}

/** Helper: full stats row with all fields (new column names) */
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

  it("should strip trailing commas from x-org-id before querying", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [makeStatsRow({ esSent: 5, rsSent: 3 })] });
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
    const params = sqlObj.queryChunks || [];
    const flatParams = JSON.stringify(params);
    expect(flatParams).not.toContain("test-org,");
  });

  it("should return recipientStats and emailStats when no filters provided", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({
        esSent: 100, esOpened: 55, esClicked: 5, esBounced: 5,
        rsSent: 90, rsOpened: 50, rsClicked: 4, rsBounced: 3,
        rdInterested: 3, rdAutoReply: 1, rdNotInterested: 2, rdOutOfOffice: 1,
      })],
    });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 120 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatsApp();

    const response = await request(app).get("/stats").set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(response.body.recipientStats.contacted).toBe(120);
    expect(response.body.recipientStats.sent).toBe(90);
    expect(response.body.recipientStats.delivered).toBe(87); // 90 - 3
    expect(response.body.recipientStats.opened).toBe(50);
    expect(response.body.recipientStats.bounced).toBe(3);
    expect(response.body.recipientStats.clicked).toBe(4);
    expect(response.body.recipientStats.repliesPositive).toBe(3);
    expect(response.body.recipientStats.repliesDetail.interested).toBe(3);
    expect(response.body.emailStats.sent).toBe(100);
    expect(response.body.emailStats.delivered).toBe(95); // 100 - 5
    expect(response.body.emailStats.opened).toBe(55);
    expect(response.body.emailStats.clicked).toBe(5);
    expect(response.body.emailStats.bounced).toBe(5);
  });

  it("should return zeros when no events match", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [makeStatsRow()] });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 0 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(response.body.recipientStats.sent).toBe(0);
    expect(response.body.recipientStats.repliesPositive).toBe(0);
    expect(response.body.recipientStats.repliesNegative).toBe(0);
    expect(response.body.recipientStats.repliesNeutral).toBe(0);
    expect(response.body.recipientStats.repliesAutoReply).toBe(0);
    expect(response.body.emailStats.sent).toBe(0);
    // stepStats should not be present when empty
    expect(response.body.emailStats.stepStats).toBeUndefined();
  });

  it("should aggregate event counts correctly", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({
        esSent: 80, esOpened: 45, esClicked: 3, esBounced: 5,
        rsSent: 75, rsOpened: 40, rsClicked: 2, rsBounced: 4,
        rdInterested: 1, rdAutoReply: 1, rdNotInterested: 1, rdOutOfOffice: 2,
      })],
    });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 80 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(response.body.recipientStats.sent).toBe(75);
    expect(response.body.recipientStats.repliesPositive).toBe(1);
    expect(response.body.recipientStats.repliesAutoReply).toBe(3); // autoReply(1) + outOfOffice(2)
    expect(response.body.emailStats.sent).toBe(80);
  });

  it("should compute reply aggregates from detail correctly", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({
        esSent: 500, esOpened: 210, esBounced: 20,
        rsSent: 400, rsOpened: 200, rsBounced: 15,
        rdInterested: 3, rdMeetingBooked: 2, rdClosed: 1,
        rdNotInterested: 4, rdWrongPerson: 1, rdUnsubscribe: 2,
        rdNeutral: 5,
        rdAutoReply: 11, rdOutOfOffice: 13,
      })],
    });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 500 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatsApp();

    const response = await request(app).get("/stats").set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(response.body.recipientStats.repliesPositive).toBe(6);  // 3+2+1
    expect(response.body.recipientStats.repliesNegative).toBe(7);  // 4+1+2
    expect(response.body.recipientStats.repliesNeutral).toBe(5);
    expect(response.body.recipientStats.repliesAutoReply).toBe(24); // 11+13
    expect(response.body.recipientStats.repliesDetail.interested).toBe(3);
    expect(response.body.recipientStats.repliesDetail.meetingBooked).toBe(2);
    expect(response.body.recipientStats.repliesDetail.closed).toBe(1);
    expect(response.body.recipientStats.repliesDetail.wrongPerson).toBe(1);
    expect(response.body.recipientStats.repliesDetail.neutral).toBe(5);
  });

  it("should include per-step stats in emailStats when step data exists", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({
        esSent: 30, esOpened: 16, esClicked: 1, esBounced: 2,
        rsSent: 10, rsOpened: 8, rsClicked: 1, rsBounced: 1,
        rdInterested: 3,
      })],
    });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 10 }] });
    mockExecute.mockResolvedValueOnce({
      rows: [
        { step: 1, sent: 10, opened: 8, clicked: 3, bounced: 1, rdInterested: 1, rdMeetingBooked: 0, rdClosed: 0, rdNotInterested: 0, rdWrongPerson: 0, rdUnsubscribe: 0, rdNeutral: 0, rdAutoReply: 0, rdOutOfOffice: 0 },
        { step: 2, sent: 10, opened: 5, clicked: 1, bounced: 1, rdInterested: 1, rdMeetingBooked: 0, rdClosed: 0, rdNotInterested: 0, rdWrongPerson: 0, rdUnsubscribe: 0, rdNeutral: 0, rdAutoReply: 0, rdOutOfOffice: 0 },
        { step: 3, sent: 10, opened: 2, clicked: 0, bounced: 0, rdInterested: 1, rdMeetingBooked: 0, rdClosed: 0, rdNotInterested: 0, rdWrongPerson: 0, rdUnsubscribe: 0, rdNeutral: 0, rdAutoReply: 0, rdOutOfOffice: 0 },
      ],
    });

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({ campaignId: "camp-1" })
      .set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(response.body.emailStats.stepStats).toHaveLength(3);
    expect(response.body.emailStats.stepStats[0].step).toBe(1);
    expect(response.body.emailStats.stepStats[0].sent).toBe(10);
    expect(response.body.emailStats.stepStats[0].delivered).toBe(9); // 10 - 1
    expect(response.body.emailStats.stepStats[0].clicked).toBe(3);
    expect(response.body.emailStats.stepStats[0].repliesPositive).toBe(1);
    expect(response.body.emailStats.stepStats[0].repliesDetail.interested).toBe(1);
    expect(response.body.emailStats.stepStats[2].step).toBe(3);
    expect(response.body.emailStats.stepStats[2].clicked).toBe(0);
    expect(response.body.emailStats.stepStats[2].bounced).toBe(0);
    expect(response.body.emailStats.stepStats[2].delivered).toBe(10); // 10 - 0
    expect(response.body.emailStats.stepStats[2].repliesPositive).toBe(1);
  });

  it("should return zero stats when db returns empty rows", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [] });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 0 }] });

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(response.body.recipientStats.contacted).toBe(0);
    expect(response.body.recipientStats.sent).toBe(0);
    expect(response.body.recipientStats.repliesPositive).toBe(0);
    expect(response.body.emailStats.sent).toBe(0);
  });

  it("should accept runIds filter and use IN clause (not ANY)", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({ esSent: 10, rsSent: 10 })],
    });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 10 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({ runIds: "run-1,run-2" })
      .set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(response.body.emailStats.sent).toBe(10);

    const sqlObj = mockExecute.mock.calls[0][0];
    const sqlText = extractSqlText(sqlObj);
    expect(sqlText).toContain("run_id IN");
    expect(sqlText).not.toContain("ANY");
  });

  it("should exclude internal emails and sender from stats query", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [makeStatsRow()] });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 0 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] }); // step query
    const app = await createStatsApp();

    await request(app).get("/stats").set(identityHeadersObj);

    // Stats query + contacted count + step query
    expect(mockExecute).toHaveBeenCalledTimes(3);

    const sqlObj = mockExecute.mock.calls[0][0];
    const sqlText = extractSqlText(sqlObj);

    expect(sqlText).toContain("account_email IS NULL OR e.lead_email != e.account_email");
    expect(sqlText).toContain("lead_email NOT IN");
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
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({
        esSent: 50, esOpened: 22, esBounced: 2,
        rsSent: 40, rsOpened: 20, rsBounced: 1,
        rdInterested: 5, rdNotInterested: 1,
      })],
    });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 50 }] });
    mockExecute.mockRejectedValueOnce(new Error("step query timeout"));

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(response.body.recipientStats.sent).toBe(40);
    expect(response.body.recipientStats.repliesPositive).toBe(5);
    expect(response.body.emailStats.sent).toBe(50);
    expect(response.body.emailStats.stepStats).toBeUndefined();
  });

  it("should log cause message from DrizzleQueryError", async () => {
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({ esSent: 10, rsSent: 10 })],
    });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 10 }] });
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
    expect(sqlText).not.toMatch(/event_type = 'reply_received'/);
  });

  // ─── featureSlug filter ──────────────────────────────────────────────────────

  it("should filter by featureSlugs (comma-separated)", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({ esSent: 20, rsSent: 10 })],
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

  // ─── brandId filter uses ANY(brand_ids) for multi-brand support ─────────────

  it("should filter brandId using ANY(c.brand_ids) for multi-brand campaigns", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({ esSent: 10, rsSent: 5 })],
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

  // ─── groupBy: brandId (lateral unnest) ──────────────────────────────────────

  it("should use CROSS JOIN LATERAL unnest for groupBy brandId instead of inline unnest in WHERE", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [
        { groupKey: "brand-1", esSent: 10, esOpened: 5, esClicked: 0, esBounced: 0, rsSent: 5, rsOpened: 3, rsClicked: 0, rsBounced: 0, rdInterested: 1, rdMeetingBooked: 0, rdClosed: 0, rdNotInterested: 0, rdWrongPerson: 0, rdUnsubscribe: 0, rdNeutral: 0, rdAutoReply: 0, rdOutOfOffice: 0 },
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
        { groupKey: "feat-1", esSent: 10, esOpened: 5, esClicked: 0, esBounced: 0, rsSent: 5, rsOpened: 3, rsClicked: 0, rsBounced: 0, rdInterested: 1, rdMeetingBooked: 0, rdClosed: 0, rdNotInterested: 0, rdWrongPerson: 0, rdUnsubscribe: 0, rdNeutral: 0, rdAutoReply: 0, rdOutOfOffice: 0 },
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

  it("should return recipientStats and emailStats per group", async () => {
    // Group 1 events query
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({
        esSent: 800, esOpened: 320, esBounced: 20,
        rsSent: 400, rsOpened: 310, rsBounced: 10,
        rdInterested: 3,
      })],
    });
    // Group 2 events query
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({
        esSent: 200, esOpened: 85, esBounced: 5,
        rsSent: 100, rsOpened: 80, rsBounced: 3,
        rdInterested: 1,
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
    expect(alpha.emailStats.sent).toBe(800);
    expect(alpha.recipientStats.sent).toBe(400);
    expect(alpha.recipientStats.repliesPositive).toBe(3);

    const beta = response.body.groups.find((g: any) => g.key === "workflow-beta");
    expect(beta.emailStats.sent).toBe(200);
    expect(beta.recipientStats.sent).toBe(100);
    expect(beta.recipientStats.repliesPositive).toBe(1);
  });

  it("should return zero stats for groups with no matching events", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [] });
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
    expect(response.body.groups[0].recipientStats.sent).toBe(0);
    expect(response.body.groups[0].recipientStats.repliesPositive).toBe(0);
    expect(response.body.groups[0].emailStats.sent).toBe(0);
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
      rows: [makeStatsRow({ esSent: 10, rsSent: 5 })],
    });
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
    expect(sqlText).toContain("account_email IS NULL OR e.lead_email != e.account_email");
    expect(sqlText).toContain("lead_email NOT IN");
  });
});
