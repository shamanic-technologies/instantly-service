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
import { clearStatsCache } from "../../src/lib/stats-cache";

const identityHeadersObj = { "x-org-id": "test-org", "x-user-id": "test-user", "x-run-id": "test-run" };

async function createStatsApp() {
  const analyticsRouter = (await import("../../src/routes/analytics")).default;
  const app = express();
  app.use(express.json());
  app.use(requireOrgId, analyticsRouter);
  return app;
}

async function waitUntil(predicate: () => boolean): Promise<void> {
  for (let i = 0; i < 20; i += 1) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
  throw new Error("Timed out waiting for condition");
}

/** Helper: full stats row with all fields. Reply-sentiment counts (rdInterested
 *  etc.) no longer come from this query — the main events query keeps only
 *  rdUnsubscribe; sentiment counts come from the separate latest-sentiment query
 *  (see makeSentimentRow). */
function makeStatsRow(overrides: Partial<Record<string, number>> = {}) {
  return {
    esSent: 0, esOpened: 0, esClicked: 0, esBounced: 0, esUnsubscribed: 0,
    rsSent: 0, rsOpened: 0, rsClicked: 0, rsBounced: 0, rsUnsubscribed: 0,
    rdUnsubscribe: 0,
    ...overrides,
  };
}

/** Helper: a row from the latest-sentiment query (queryGroupedSentiment / querySentiment).
 *  Optionally carries a groupKey for grouped responses. */
function makeSentimentRow(overrides: Partial<Record<string, number | string>> = {}) {
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
    // Reset clears the mockResolvedValueOnce queue too; default fallback so any
    // call not explicitly queued (e.g. the latest-sentiment query, step query)
    // returns empty rows → ZERO instead of crashing on undefined.
    mockExecute.mockReset();
    mockExecute.mockResolvedValue({ rows: [] });
    clearStatsCache();
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

  it("should include cancelled count in recipientStats funnel", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [makeStatsRow({ rsSent: 50 })] });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 100, notSending: 2, cancelled: 7 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatsApp();
    const response = await request(app).get("/stats").set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(response.body.recipientStats.cancelled).toBe(7);
    expect(response.body.recipientStats.contacted).toBe(100);
    expect(response.body.recipientStats.notSending).toBe(2);
  });

  it("should issue a queryCampaignAggregates SQL with delivery_status = 'cancelled'", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [makeStatsRow()] });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 0, notSending: 0, cancelled: 0 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatsApp();
    await request(app).get("/stats").set(identityHeadersObj);

    const aggregatesSqlText = extractSqlText(mockExecute.mock.calls[1][0]);
    expect(aggregatesSqlText).toContain("delivery_status = 'cancelled'");
  });

  it("should return recipientStats and emailStats when no filters provided", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({
        esSent: 100, esOpened: 55, esClicked: 5, esBounced: 5,
        rsSent: 90, rsOpened: 50, rsClicked: 4, rsBounced: 3,
      })],
    });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 120 }] });
    // latest-sentiment query
    mockExecute.mockResolvedValueOnce({
      rows: [makeSentimentRow({ rdInterested: 3, rdAutoReply: 1, rdNotInterested: 2, rdOutOfOffice: 1 })],
    });

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
      })],
    });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 80 }] });
    mockExecute.mockResolvedValueOnce({
      rows: [makeSentimentRow({ rdInterested: 1, rdAutoReply: 1, rdNotInterested: 1, rdOutOfOffice: 2 })],
    });

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
    // unsubscribe stays an event count on the main query; the 8 sentiment types
    // come from the latest-sentiment query.
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({
        esSent: 500, esOpened: 210, esBounced: 20,
        rsSent: 400, rsOpened: 200, rsBounced: 15,
        rdUnsubscribe: 2,
      })],
    });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 500 }] });
    mockExecute.mockResolvedValueOnce({
      rows: [makeSentimentRow({
        rdInterested: 3, rdMeetingBooked: 2, rdClosed: 1,
        rdNotInterested: 4, rdWrongPerson: 1,
        rdNeutral: 5,
        rdAutoReply: 11, rdOutOfOffice: 13,
      })],
    });

    const app = await createStatsApp();

    const response = await request(app).get("/stats").set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(response.body.recipientStats.repliesPositive).toBe(6);  // 3+2+1
    expect(response.body.recipientStats.repliesNegative).toBe(7);  // 4+1+2 (notInterested+wrongPerson+unsubscribe)
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
      })],
    });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 10 }] });
    // overall latest-sentiment query
    mockExecute.mockResolvedValueOnce({ rows: [makeSentimentRow({ rdInterested: 3 })] });
    // step email-metrics query (sentiment columns no longer read here)
    mockExecute.mockResolvedValueOnce({
      rows: [
        { step: 1, sent: 10, opened: 8, clicked: 3, bounced: 1, rdUnsubscribe: 0 },
        { step: 2, sent: 10, opened: 5, clicked: 1, bounced: 1, rdUnsubscribe: 0 },
        { step: 3, sent: 10, opened: 2, clicked: 0, bounced: 0, rdUnsubscribe: 0 },
      ],
    });
    // queryStepSentiment: current sentiment attributed to each lead's last step
    mockExecute.mockResolvedValueOnce({
      rows: [
        makeSentimentRow({ step: 1, rdInterested: 1 }),
        makeSentimentRow({ step: 2, rdInterested: 1 }),
        makeSentimentRow({ step: 3, rdInterested: 1 }),
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

  it("should attribute a re-qualified NEGATIVE reply to the last step, never positive on an earlier one", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [makeStatsRow({ rsSent: 1, esSent: 2 })] });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 1 }] });
    // overall sentiment: the lead's current sentiment is negative
    mockExecute.mockResolvedValueOnce({ rows: [makeSentimentRow({ rdNotInterested: 1 })] });
    // step email metrics for steps 1 and 2 (2 emails sent before the reply)
    mockExecute.mockResolvedValueOnce({
      rows: [
        { step: 1, sent: 1, opened: 1, clicked: 0, bounced: 0, rdUnsubscribe: 0 },
        { step: 2, sent: 1, opened: 1, clicked: 0, bounced: 0, rdUnsubscribe: 0 },
      ],
    });
    // queryStepSentiment: current sentiment lands on the LAST step (2), as negative
    mockExecute.mockResolvedValueOnce({ rows: [makeSentimentRow({ step: 2, rdNotInterested: 1 })] });

    const app = await createStatsApp();
    const response = await request(app).get("/stats").query({ campaignId: "c1" }).set(identityHeadersObj);

    expect(response.status).toBe(200);
    const steps = response.body.emailStats.stepStats;
    const step1 = steps.find((s: any) => s.step === 1);
    const step2 = steps.find((s: any) => s.step === 2);
    // No stale positive anywhere; negative only on the last step.
    expect(step1.repliesPositive).toBe(0);
    expect(step1.repliesNegative).toBe(0);
    expect(step2.repliesPositive).toBe(0);
    expect(step2.repliesNegative).toBe(1);
  });

  it("queryStepSentiment SQL attributes sentiment to MAX(email_sent step) per lead", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [makeStatsRow({ rsSent: 1 })] });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 1 }] });
    const app = await createStatsApp();

    await request(app).get("/stats").set(identityHeadersObj);

    const stepSentimentSql = mockExecute.mock.calls
      .map((c) => extractSqlText(c[0]))
      .find((t) => t.includes("latest_sentiment") && t.includes("GROUP BY ls.step"));
    expect(stepSentimentSql).toBeDefined();
    expect(stepSentimentSql).toContain("MAX(es.step)");
    expect(stepSentimentSql).toContain("es.event_type = 'email_sent'");
    expect(stepSentimentSql).toContain("DISTINCT ON (e.campaign_id, e.lead_email)");
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
    mockExecute.mockResolvedValueOnce({ rows: [] }); // latest-sentiment query
    mockExecute.mockResolvedValueOnce({ rows: [] }); // step email-metrics query
    mockExecute.mockResolvedValueOnce({ rows: [] }); // step-sentiment query
    const app = await createStatsApp();

    await request(app).get("/stats").set(identityHeadersObj);

    // Stats query + campaign-aggregates + latest-sentiment + step + step-sentiment
    expect(mockExecute).toHaveBeenCalledTimes(5);

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
      })],
    });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 50 }] });
    mockExecute.mockResolvedValueOnce({ rows: [makeSentimentRow({ rdInterested: 5, rdNotInterested: 1 })] });
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
    mockExecute.mockResolvedValueOnce({ rows: [] }); // latest-sentiment query
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

  it("should query all 9 reply event types across the stats SQL", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [] });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 0 }] });
    const app = await createStatsApp();

    await request(app).get("/stats").set(identityHeadersObj);

    // Sentiment types now live in the latest-sentiment query; lead_unsubscribed
    // stays in the main events query. Scan all issued SQL.
    const allSql = mockExecute.mock.calls.map((c) => extractSqlText(c[0])).join(" ");
    expect(allSql).toContain("lead_interested");
    expect(allSql).toContain("lead_meeting_booked");
    expect(allSql).toContain("lead_closed");
    expect(allSql).toContain("lead_not_interested");
    expect(allSql).toContain("lead_wrong_person");
    expect(allSql).toContain("lead_unsubscribed");
    expect(allSql).toContain("lead_neutral");
    expect(allSql).toContain("auto_reply_received");
    expect(allSql).toContain("lead_out_of_office");
    expect(allSql).not.toMatch(/event_type = 'reply_received'/);
  });

  it("should derive current sentiment from the LATEST event per lead (manual wins ties)", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [makeStatsRow()] });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 0 }] });
    const app = await createStatsApp();

    await request(app).get("/stats").set(identityHeadersObj);

    // The sentiment query is the one carrying the latest-per-lead CTE.
    const sentimentSql = mockExecute.mock.calls
      .map((c) => extractSqlText(c[0]))
      .find((t) => t.includes("latest_sentiment"));
    expect(sentimentSql).toBeDefined();
    expect(sentimentSql).toContain("DISTINCT ON (e.campaign_id, e.lead_email)");
    expect(sentimentSql).toContain("e.timestamp DESC");
    expect(sentimentSql).toContain("e.source = 'manual'");
  });

  it("should count a re-qualified reply by its CURRENT sentiment, not the stale one", async () => {
    // A reply auto-classified lead_interested then manually re-qualified
    // lead_not_interested: the latest-sentiment query returns ONLY the current
    // (negative) sentiment — positive must be 0, negative 1.
    mockExecute.mockResolvedValueOnce({ rows: [makeStatsRow({ rsSent: 1, esSent: 1 })] });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 1 }] });
    mockExecute.mockResolvedValueOnce({ rows: [makeSentimentRow({ rdNotInterested: 1 })] });
    const app = await createStatsApp();

    const response = await request(app).get("/stats").set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(response.body.recipientStats.repliesPositive).toBe(0);
    expect(response.body.recipientStats.repliesNegative).toBe(1);
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

  // ─── notSending recipient stat ──────────────────────────────────────────────

  it("should return notSending count from queryCampaignAggregates", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({ esSent: 50, rsSent: 30 })],
    });
    mockExecute.mockResolvedValueOnce({
      rows: [{ emailsContacted: 100, notSending: 17 }],
    });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatsApp();

    const response = await request(app).get("/stats").set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(response.body.recipientStats.notSending).toBe(17);
    expect(response.body.recipientStats.contacted).toBe(100);
  });

  it("should default notSending to 0 when DB returns no notSending field", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [makeStatsRow()] });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 0 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatsApp();

    const response = await request(app).get("/stats").set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(response.body.recipientStats.notSending).toBe(0);
  });

  it("should use COUNT DISTINCT lead_email FILTER (not_sending_status) in aggregates query", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [makeStatsRow()] });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 0, notSending: 0 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatsApp();

    await request(app).get("/stats").set(identityHeadersObj);

    // 2nd call is queryCampaignAggregates (1st = events, 3rd = step)
    const aggregatesSql = extractSqlText(mockExecute.mock.calls[1][0]);
    expect(aggregatesSql).toContain("COUNT(DISTINCT c.lead_email)");
    expect(aggregatesSql).toContain("not_sending_status IS NOT NULL");
  });

  it("should propagate notSending per-group when grouping by brandId", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [
        { groupKey: "brand-1", esSent: 10, esOpened: 5, esClicked: 0, esBounced: 0, rsSent: 5, rsOpened: 3, rsClicked: 0, rsBounced: 0, rdInterested: 0, rdMeetingBooked: 0, rdClosed: 0, rdNotInterested: 0, rdWrongPerson: 0, rdUnsubscribe: 0, rdNeutral: 0, rdAutoReply: 0, rdOutOfOffice: 0 },
      ],
    });
    mockExecute.mockResolvedValueOnce({
      rows: [{ groupKey: "brand-1", emailsContacted: 5, notSending: 2 }],
    });

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({ groupBy: "brandId" })
      .set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(response.body.groups[0].recipientStats.notSending).toBe(2);
    expect(response.body.groups[0].recipientStats.contacted).toBe(5);
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

  it("should filter stats by explicit persona attribution metadata", async () => {
    mockExecute.mockResolvedValueOnce({ rows: [makeStatsRow({ esSent: 3, rsSent: 2 })] });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 2 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({
        goal: "signup",
        brandProfileId: "brand-profile-1",
        audienceId: "audience-1",
      })
      .set(identityHeadersObj);

    expect(response.status).toBe(200);
    for (const call of mockExecute.mock.calls) {
      const sqlText = extractSqlText(call[0]);
      expect(sqlText).toContain("metadata->>'goal'");
      expect(sqlText).toContain("metadata->>'brandProfileId'");
      expect(sqlText).toContain("metadata->>'audienceId'");
    }
  });

  it("should support groupBy audienceId without fallback rows", async () => {
    mockExecute.mockResolvedValueOnce({
      rows: [
        { groupKey: "audience-a", esSent: 10, esOpened: 5, esClicked: 4, esBounced: 0, rsSent: 5, rsOpened: 3, rsClicked: 2, rsBounced: 0, rdUnsubscribe: 0 },
        { groupKey: "audience-b", esSent: 3, esOpened: 1, esClicked: 1, esBounced: 0, rsSent: 2, rsOpened: 1, rsClicked: 1, rsBounced: 0, rdUnsubscribe: 0 },
      ],
    });
    mockExecute.mockResolvedValueOnce({
      rows: [
        { groupKey: "audience-a", emailsContacted: 5 },
        { groupKey: "audience-b", emailsContacted: 2 },
      ],
    });
    mockExecute.mockResolvedValueOnce({
      rows: [
        makeSentimentRow({ groupKey: "audience-a", rdInterested: 2 }),
        makeSentimentRow({ groupKey: "audience-b", rdInterested: 1 }),
      ],
    });

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({ groupBy: "audienceId", goal: "signup", brandProfileId: "brand-profile-1" })
      .set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(response.body.groups.map((g: { key: string }) => g.key)).toEqual([
      "audience-a",
      "audience-b",
    ]);
    expect(response.body.groups[0].recipientStats.clicked).toBe(2);
    expect(response.body.groups[0].recipientStats.repliesPositive).toBe(2);
    expect(response.body.groups[1].recipientStats.clicked).toBe(1);
    expect(response.body.groups[1].recipientStats.repliesPositive).toBe(1);

    for (const call of mockExecute.mock.calls) {
      const sqlText = extractSqlText(call[0]);
      expect(sqlText).toContain("metadata->>'audienceId'");
      expect(sqlText).toContain("metadata->>'audienceId' IS NOT NULL");
    }
  });

  // ─── groupBy: day ──────────────────────────────────────────────────────────

  it("should support groupBy day with stats grouped by local YYYY-MM-DD key", async () => {
    // Call order under Promise.all: [events, campaign-aggregates, sentiment].
    mockExecute.mockResolvedValueOnce({
      rows: [
        {
          groupKey: "2026-06-17",
          esSent: 8,
          esOpened: 4,
          esClicked: 2,
          esBounced: 1,
          esUnsubscribed: 0,
          rsSent: 6,
          rsOpened: 3,
          rsClicked: 2,
          rsBounced: 1,
          rsUnsubscribed: 0,
          rdUnsubscribe: 0,
        },
      ],
    });
    mockExecute.mockResolvedValueOnce({
      rows: [{ groupKey: "2026-06-17", emailsContacted: 12, notSending: 2, cancelled: 1 }],
    });
    mockExecute.mockResolvedValueOnce({
      rows: [
        makeSentimentRow({ groupKey: "2026-06-17", rdInterested: 1, rdNotInterested: 1 }),
      ],
    });

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({ groupBy: "day", timezone: "UTC" })
      .set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(response.body.groups).toEqual([
      {
        key: "2026-06-17",
        recipientStats: {
          contacted: 12,
          sent: 6,
          delivered: 5,
          opened: 3,
          bounced: 1,
          clicked: 2,
          unsubscribed: 0,
          notSending: 2,
          cancelled: 1,
          repliesPositive: 1,
          repliesNegative: 1,
          repliesNeutral: 0,
          repliesAutoReply: 0,
          repliesDetail: {
            interested: 1,
            meetingBooked: 0,
            closed: 0,
            notInterested: 1,
            wrongPerson: 0,
            unsubscribe: 0,
            neutral: 0,
            autoReply: 0,
            outOfOffice: 0,
          },
        },
        emailStats: {
          sent: 8,
          delivered: 7,
          opened: 4,
          clicked: 2,
          bounced: 1,
          unsubscribed: 0,
        },
      },
    ]);
    // Day grouping now queries campaign-table aggregates (contacted /
    // notSending / cancelled) bucketed by c.created_at, same as other groupings.
    expect(mockExecute).toHaveBeenCalledTimes(3);
  });

  it("should group day buckets with the requested IANA timezone", async () => {
    // Call order: [events, campaign-aggregates, sentiment].
    mockExecute.mockResolvedValueOnce({ rows: [{ groupKey: "2026-06-16", ...makeStatsRow({ esSent: 1, rsSent: 1 }) }] });
    mockExecute.mockResolvedValueOnce({ rows: [{ groupKey: "2026-06-16", emailsContacted: 3, notSending: 0, cancelled: 0 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({ groupBy: "day", timezone: "America/New_York" })
      .set(identityHeadersObj);

    expect(response.status).toBe(200);
    expect(response.body.groups[0].key).toBe("2026-06-16");
    expect(response.body.groups[0].recipientStats.contacted).toBe(3);

    const eventSqlText = extractSqlText(mockExecute.mock.calls[0][0]);
    const aggregateSqlText = extractSqlText(mockExecute.mock.calls[1][0]);
    const sentimentSqlText = extractSqlText(mockExecute.mock.calls[2][0]);
    const allChunks = JSON.stringify(mockExecute.mock.calls.map((call) => call[0]));

    expect(eventSqlText).toContain("e.timestamp");
    expect(eventSqlText).toContain("AT TIME ZONE 'UTC'");
    expect(eventSqlText).toContain("YYYY-MM-DD");
    expect(eventSqlText).toContain("AS group_key");
    expect(eventSqlText).toContain("GROUP BY e.group_key");
    expect(eventSqlText).toContain("ORDER BY e.group_key");
    expect(eventSqlText).not.toContain("GROUP BY TO_CHAR");
    // Contacted is bucketed by the campaign row's created_at in the same TZ.
    expect(aggregateSqlText).toContain("c.created_at");
    expect(aggregateSqlText).toContain("AT TIME ZONE 'UTC'");
    expect(aggregateSqlText).toContain("YYYY-MM-DD");
    expect(aggregateSqlText).toContain('"emailsContacted"');
    // The parameterized localDayKey fragment must appear EXACTLY ONCE (inside the
    // CTE) — emitting it again in WHERE/GROUP BY re-emits the timezone bind under
    // a different $N and drifts param positions (42803 / 08P01). Compute it once
    // as `group_key` in the CTE, then group by the alias.
    expect(aggregateSqlText).toContain("AS group_key");
    expect(aggregateSqlText).toContain("FROM grouped_campaigns c");
    expect(aggregateSqlText).toContain("GROUP BY c.group_key");
    expect(aggregateSqlText).not.toContain("GROUP BY 1");
    expect(aggregateSqlText).not.toContain("GROUP BY TO_CHAR");
    expect(sentimentSqlText).toContain("ls.timestamp");
    expect(sentimentSqlText).toContain("AT TIME ZONE 'UTC'");
    expect(sentimentSqlText).toContain("AS group_key");
    expect(sentimentSqlText).toContain("FROM grouped_sentiment ls");
    expect(sentimentSqlText).toContain("GROUP BY ls.group_key");
    expect(sentimentSqlText).not.toContain("GROUP BY TO_CHAR");
    expect(allChunks).toContain("America/New_York");
  });

  it("should apply existing filters to groupBy day", async () => {
    // Call order: [events, campaign-aggregates, sentiment].
    mockExecute.mockResolvedValueOnce({ rows: [{ groupKey: "2026-06-17", ...makeStatsRow({ esSent: 1, rsSent: 1 }) }] });
    mockExecute.mockResolvedValueOnce({ rows: [{ groupKey: "2026-06-17", emailsContacted: 1, notSending: 0, cancelled: 0 }] });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({
        groupBy: "day",
        brandId: "brand-1",
        campaignId: "camp-1",
        workflowSlugs: "wf-a,wf-b",
        featureSlugs: "feat-a,feat-b",
        runIds: "run-1,run-2",
      })
      .set(identityHeadersObj);

    expect(response.status).toBe(200);
    // Same filters reach both the events query and the campaign-aggregate query.
    for (const idx of [0, 1]) {
      const sqlText = extractSqlText(mockExecute.mock.calls[idx][0]);
      expect(sqlText).toContain("run_id IN");
      expect(sqlText).toContain("brand_ids");
      expect(sqlText).toContain("campaign_id");
      expect(sqlText).toContain("workflow_slug IN");
      expect(sqlText).toContain("feature_slug IN");
    }
  });

  it("should reject invalid groupBy day timezone values", async () => {
    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({ groupBy: "day", timezone: "not-a-zone" })
      .set(identityHeadersObj);

    expect(response.status).toBe(400);
    expect(response.body.error).toBe("Invalid request");
    expect(mockExecute).not.toHaveBeenCalled();
  });

  it("should surface a day with contacted leads but zero email events", async () => {
    // Events query: only 2026-06-17 has email activity.
    mockExecute.mockResolvedValueOnce({
      rows: [{ groupKey: "2026-06-17", ...makeStatsRow({ esSent: 5, rsSent: 4 }) }],
    });
    // Campaign aggregates: 2026-06-17 (10 contacted) AND 2026-06-18 (22 contacted,
    // no email events yet — leads pushed today, nothing sent).
    mockExecute.mockResolvedValueOnce({
      rows: [
        { groupKey: "2026-06-17", emailsContacted: 10, notSending: 1, cancelled: 0 },
        { groupKey: "2026-06-18", emailsContacted: 22, notSending: 3, cancelled: 2 },
      ],
    });
    mockExecute.mockResolvedValueOnce({ rows: [] });

    const app = await createStatsApp();

    const response = await request(app)
      .get("/stats")
      .query({ groupBy: "day", timezone: "Europe/Paris" })
      .set(identityHeadersObj);

    expect(response.status).toBe(200);
    const groups = response.body.groups;
    // Both days present, sorted ascending — the contacted-only day is NOT dropped.
    expect(groups.map((g: any) => g.key)).toEqual(["2026-06-17", "2026-06-18"]);

    const noEventDay = groups.find((g: any) => g.key === "2026-06-18");
    expect(noEventDay.recipientStats.contacted).toBe(22);
    expect(noEventDay.recipientStats.notSending).toBe(3);
    expect(noEventDay.recipientStats.cancelled).toBe(2);
    // Zero email activity on that day.
    expect(noEventDay.recipientStats.sent).toBe(0);
    expect(noEventDay.recipientStats.opened).toBe(0);
    expect(noEventDay.recipientStats.clicked).toBe(0);
    expect(noEventDay.emailStats).toEqual({
      sent: 0, delivered: 0, opened: 0, clicked: 0, bounced: 0, unsubscribed: 0,
    });

    // AC1: day-grouped contacted summed over the window == cumulative contacted
    // (cumulative = COUNT(*) over the SAME campaign rows = 10 + 22).
    const daySum = groups.reduce((acc: number, g: any) => acc + g.recipientStats.contacted, 0);
    expect(daySum).toBe(32);
  });
});

describe("POST /stats/grouped", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockExecute.mockReset();
    mockExecute.mockResolvedValue({ rows: [] });
    clearStatsCache();
  });

  it("should return recipientStats and emailStats per group", async () => {
    // queryStats now parallelizes its 3 reads internally (main / aggregates /
    // sentiment) via Promise.all, so each group's 3 calls are issued together
    // before the next group's. Call order is therefore group-sequential:
    // [main1, aggregates1, sentiment1, main2, aggregates2, sentiment2].
    // Group 1: events, contacted, sentiment
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({
        esSent: 800, esOpened: 320, esBounced: 20,
        rsSent: 400, rsOpened: 310, rsBounced: 10,
      })],
    });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 450 }] });
    mockExecute.mockResolvedValueOnce({ rows: [makeSentimentRow({ rdInterested: 3 })] });
    // Group 2: events, contacted, sentiment
    mockExecute.mockResolvedValueOnce({
      rows: [makeStatsRow({
        esSent: 200, esOpened: 85, esBounced: 5,
        rsSent: 100, rsOpened: 80, rsBounced: 3,
      })],
    });
    mockExecute.mockResolvedValueOnce({ rows: [{ emailsContacted: 120 }] });
    mockExecute.mockResolvedValueOnce({ rows: [makeSentimentRow({ rdInterested: 1 })] });

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

    // Stats query + campaign-aggregates + latest-sentiment (per group)
    expect(mockExecute).toHaveBeenCalledTimes(3);
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

describe("GET /stats caching (DIS perf)", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockExecute.mockReset();
    mockExecute.mockResolvedValue({ rows: [] });
    clearStatsCache();
  });

  it("serves a repeated identical request from cache (no second DB roundtrip)", async () => {
    const app = await createStatsApp();

    const first = await request(app).get("/stats").set(identityHeadersObj);
    expect(first.status).toBe(200);
    const callsAfterFirst = mockExecute.mock.calls.length;
    expect(callsAfterFirst).toBeGreaterThan(0);

    const second = await request(app).get("/stats").set(identityHeadersObj);
    expect(second.status).toBe(200);
    // No new db.execute calls — served from cache.
    expect(mockExecute.mock.calls.length).toBe(callsAfterFirst);
    expect(second.body).toEqual(first.body);
  });

  it("shares one in-flight aggregation for concurrent identical requests", async () => {
    let releaseDb!: () => void;
    const dbGate = new Promise<void>((resolve) => {
      releaseDb = resolve;
    });
    mockExecute.mockImplementation(async () => {
      await dbGate;
      return { rows: [] };
    });

    const app = await createStatsApp();

    const first = request(app).get("/stats").set(identityHeadersObj).then((res) => res);
    const second = request(app).get("/stats").set(identityHeadersObj).then((res) => res);

    await waitUntil(() => mockExecute.mock.calls.length > 0);
    const callsBeforeRelease = mockExecute.mock.calls.length;
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(mockExecute.mock.calls.length).toBe(callsBeforeRelease);

    releaseDb();
    const [firstResponse, secondResponse] = await Promise.all([first, second]);

    expect(firstResponse.status).toBe(200);
    expect(secondResponse.status).toBe(200);
    expect(secondResponse.body).toEqual(firstResponse.body);
    expect(mockExecute.mock.calls.length).toBe(callsBeforeRelease);
  });

  it("does not serve a different query from cache (cache key includes params)", async () => {
    const app = await createStatsApp();

    await request(app).get("/stats").set(identityHeadersObj);
    const callsAfterFirst = mockExecute.mock.calls.length;

    await request(app).get("/stats").query({ brandId: "brand-xyz" }).set(identityHeadersObj);
    // Different params → cache miss → DB hit again.
    expect(mockExecute.mock.calls.length).toBeGreaterThan(callsAfterFirst);
  });

  it("scopes the cache by org (different org bypasses another org's cached entry)", async () => {
    const app = await createStatsApp();

    await request(app).get("/stats").set(identityHeadersObj);
    const callsAfterFirst = mockExecute.mock.calls.length;

    await request(app)
      .get("/stats")
      .set({ ...identityHeadersObj, "x-org-id": "other-org" });
    // Different org → different cache key → DB hit again.
    expect(mockExecute.mock.calls.length).toBeGreaterThan(callsAfterFirst);
  });
});
