import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";
import express from "express";

/**
 * BOX-DB-LOAD-1027: the per-campaign and per-audience grouped reads of one org/brand/feature share ONE
 * computation, split by campaign (and audience) then the requested dimension, and each read is served
 * its slice.
 */

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
vi.mock("../../src/db", () => ({ db: { execute: (...args: unknown[]) => mockExecute(...args) } }));
vi.mock("../../src/db/schema", () => ({ instantlyCampaigns: {} }));
process.env.INSTANTLY_SERVICE_API_KEY = "test-api-key";

import { requireOrgId } from "../../src/middleware/requireOrgId";
import { clearStatsCache } from "../../src/lib/stats-cache";

const headers = { "x-org-id": "test-org", "x-user-id": "test-user", "x-run-id": "test-run" };

async function app() {
  const analyticsRouter = (await import("../../src/routes/analytics")).default;
  const publicRouter = (await import("../../src/routes/analytics-public")).default;
  const a = express();
  a.use(express.json());
  a.use("/public", publicRouter);
  a.use(requireOrgId, analyticsRouter);
  return a;
}

const events = (_: string, groupKey: string, esSent: number) => ({
  groupKey, esSent, esDelivered: esSent, esOpened: 0, esClicked: 0, esBounced: 0, esUnsubscribed: 0,
  rsSent: esSent, rsDelivered: esSent, rsOpened: 0, rsClicked: 0, rsBounced: 0, rsUnsubscribed: 0, rdUnsubscribe: 0,
});

/** Answers each statement by its shape, as a partitioned read issues them. */
let campaignIdNamesARow = false;

/** Answers each statement by its shape, as a (campaign, audience) partitioned read issues them. */
function answerPartitioned(sqlText: string) {
  if (sqlText.includes("namesARow")) return { rows: [{ namesARow: campaignIdNamesARow }] };
  const p = (camp: string, aud: string | null) => ({ partKey0: camp, partKey1: aud });
  if (sqlText.includes("latest_sentiment")) return { rows: [{ ...p("camp-1", "aud-a"), groupKey: "wf-1", rdInterested: 2 }] };
  if (sqlText.includes("SUM(q.steps)")) return { rows: [{ ...p("camp-1", "aud-a"), groupKey: "wf-1", queued: 3 }] };
  if (sqlText.includes("emailsContacted"))
    return {
      rows: [
        { ...p("camp-1", "aud-a"), groupKey: "wf-1", emailsContacted: 10, notSending: 0, cancelled: 0 },
        // Contacted, no event yet: an aggregate-only group of aud-b alone.
        { ...p("camp-1", "aud-b"), groupKey: "wf-2", emailsContacted: 4, notSending: 1, cancelled: 0 },
      ],
    };
  return {
    rows: [
      { ...events("x", "wf-1", 7), ...p("camp-1", "aud-a") },
      { ...events("x", "wf-1", 5), ...p("camp-1", "aud-b") },
      { ...events("x", "wf-1", 99), ...p("camp-1", null) },
      { ...events("x", "wf-1", 11), ...p("camp-2", "aud-a") },
    ],
  };
}

const statsCalls = () => mockExecute.mock.calls.filter((c) => !extractSqlText(c[0]).includes("namesARow"));

describe("audience family read (BOX-DB-LOAD-1027)", () => {
  beforeEach(() => {
    mockExecute.mockReset();
    mockExecute.mockImplementation(async (q: unknown) => answerPartitioned(extractSqlText(q)));
    campaignIdNamesARow = false;
    clearStatsCache();
  });

  it("serves every campaign x audience of one scope from ONE set of four statements", async () => {
    const a = await app();
    const q = "?groupBy=workflowSlug&featureSlugs=f";
    const resA = await request(a).get(`/stats${q}&campaignId=camp-1&audienceId=aud-a`).set(headers);
    const resB = await request(a).get(`/stats${q}&campaignId=camp-1&audienceId=aud-b`).set(headers);
    const resNone = await request(a).get(`/stats${q}&campaignId=camp-1&audienceId=aud-unknown`).set(headers);
    const res2 = await request(a).get(`/stats${q}&campaignId=camp-2&audienceId=aud-a`).set(headers);

    expect(statsCalls()).toHaveLength(4);
    for (const call of statsCalls()) {
      const text = extractSqlText(call[0]);
      expect(text).toContain(`c.campaign_id AS "partKey0", c.metadata->>'audienceId' AS "partKey1"`);
      expect(text).toContain("GROUP BY c.campaign_id, c.metadata->>'audienceId', c.workflow_slug");
      expect(text).not.toContain("c.metadata->>'audienceId' =");
      expect(text).not.toContain("OR c.campaign_id =");
    }
    expect(res2.body.groups[0].emailStats.sent).toBe(11);

    expect(resA.status).toBe(200);
    expect(resA.body.groups).toHaveLength(1);
    expect(resA.body.groups[0].key).toBe("wf-1");
    expect(resA.body.groups[0].emailStats.sent).toBe(7);
    expect(resA.body.groups[0].emailStats.queued).toBe(3);
    expect(resA.body.groups[0].recipientStats.contacted).toBe(10);
    expect(resA.body.groups[0].recipientStats.repliesPositive).toBe(2);

    expect(resB.body.groups.map((g: any) => g.key)).toEqual(["wf-1", "wf-2"]);
    expect(resB.body.groups[0].emailStats.sent).toBe(5);
    expect(resB.body.groups[0].emailStats.queued).toBe(0);
    expect(resB.body.groups[0].recipientStats.contacted).toBe(0);
    expect(resB.body.groups[0].recipientStats.repliesPositive).toBe(0);
    expect(resB.body.groups[1].emailStats.sent).toBe(0);
    expect(resB.body.groups[1].recipientStats.contacted).toBe(4);
    expect(resB.body.groups[1].recipientStats.notSending).toBe(1);

    // No row for this audience: the same empty answer the per-audience read gave.
    expect(resNone.body).toEqual({ groups: [] });
  });

  it("does the same on the public read, under its own fleet scope", async () => {
    const a = await app();
    const res = await request(a).get("/public/stats?campaignId=camp-1&groupBy=workflowSlug&audienceId=aud-a");
    expect(res.status).toBe(200);
    expect(res.body.groups[0].emailStats.sent).toBe(7);
    expect(statsCalls()).toHaveLength(4);
    expect(extractSqlText(statsCalls()[0][0])).toContain(`AS "partKey0"`);
  });

  it("splits a campaign read without audience by campaign alone", async () => {
    mockExecute.mockImplementation(async (q: unknown) => {
      const text = extractSqlText(q);
      if (text.includes("namesARow")) return { rows: [{ namesARow: false }] };
      if (text.includes("esSent")) return { rows: [{ ...events("x", "wf-1", 3), partKey0: "camp-9" }] };
      return { rows: [] };
    });
    const a = await app();
    const res = await request(a).get("/stats?campaignId=camp-9&groupBy=workflowSlug").set(headers);
    expect(res.body.groups.map((g: any) => [g.key, g.emailStats.sent])).toEqual([["wf-1", 3]]);
    const text = extractSqlText(statsCalls()[0][0]);
    expect(text).toContain(`c.campaign_id AS "partKey0", c.workflow_slug`);
    expect(text).not.toContain("partKey1");
  });

  it("keeps the per-key read when the campaign id is ALSO a row id of another campaign", async () => {
    campaignIdNamesARow = true;
    mockExecute.mockImplementation(async (q: unknown) =>
      extractSqlText(q).includes("namesARow") ? { rows: [{ namesARow: true }] } : { rows: [] },
    );
    const a = await app();
    await request(a).get("/stats?campaignId=camp-1&groupBy=workflowSlug&audienceId=aud-a").set(headers);
    expect(statsCalls().length).toBeGreaterThan(0);
    for (const call of statsCalls()) {
      const text = extractSqlText(call[0]);
      expect(text).not.toContain("partKey");
      expect(text).toContain("OR c.campaign_id =");
    }
  });

  it("keeps the per-key read with no campaign nor audience, and for groupBy=day", async () => {
    mockExecute.mockReset();
    mockExecute.mockResolvedValue({ rows: [] });
    const a = await app();
    await request(a).get("/stats?brandId=b&groupBy=workflowSlug").set(headers);
    await request(a).get("/stats?campaignId=camp-1&groupBy=day&audienceId=aud-a").set(headers);
    expect(mockExecute.mock.calls.length).toBeGreaterThan(0);
    for (const call of mockExecute.mock.calls) {
      expect(extractSqlText(call[0])).not.toContain("partKey");
    }
  });
});

describe("feature family read on the fleet (BOX-DB-LOAD-1027, round 2)", () => {
  beforeEach(() => {
    mockExecute.mockReset();
    clearStatsCache();
  });

  it("serves every one-feature fleet read from ONE set of four statements, split by feature", async () => {
    mockExecute.mockImplementation(async (q: unknown) => {
      const text = extractSqlText(q);
      if (text.includes("latest_sentiment")) return { rows: [{ partKey0: "sales", groupKey: "wf-1", rdInterested: 1 }] };
      if (text.includes("SUM(q.steps)")) return { rows: [{ partKey0: "hiring", groupKey: "wf-9", queued: 4 }] };
      if (text.includes("emailsContacted"))
        return {
          rows: [
            { partKey0: "sales", groupKey: "wf-1", emailsContacted: 20, notSending: 0, cancelled: 0 },
            { partKey0: "hiring", groupKey: "wf-9", emailsContacted: 6, notSending: 0, cancelled: 0 },
          ],
        };
      return { rows: [{ ...events("x", "wf-1", 12), partKey0: "sales" }, { ...events("x", "wf-9", 2), partKey0: "hiring" }] };
    });
    const a = await app();
    const sales = await request(a).get("/public/stats?featureSlugs=sales&groupBy=workflowSlug");
    const hiring = await request(a).get("/public/stats?featureSlugs=hiring&groupBy=workflowSlug");
    const none = await request(a).get("/public/stats?featureSlugs=unknown&groupBy=workflowSlug");

    expect(mockExecute.mock.calls).toHaveLength(4);
    for (const call of mockExecute.mock.calls) {
      const text = extractSqlText(call[0]);
      expect(text).toContain(`c.feature_slug AS "partKey0"`);
      expect(text).toContain("GROUP BY c.feature_slug, c.workflow_slug");
      expect(text).not.toContain("c.feature_slug IN");
    }
    expect(sales.body.groups.map((g: any) => [g.key, g.emailStats.sent, g.recipientStats.contacted, g.emailStats.queued])).toEqual([
      ["wf-1", 12, 20, 0],
    ]);
    expect(sales.body.groups[0].recipientStats.repliesPositive).toBe(1);
    expect(hiring.body.groups.map((g: any) => [g.key, g.emailStats.sent, g.recipientStats.contacted, g.emailStats.queued])).toEqual([
      ["wf-9", 2, 6, 4],
    ]);
    expect(none.body).toEqual({ groups: [] });
  });

  it("keeps the per-key read for several features, groupBy=day, an org read, or a campaign read", async () => {
    mockExecute.mockResolvedValue({ rows: [] });
    const a = await app();
    await request(a).get("/public/stats?featureSlugs=sales,hiring&groupBy=workflowSlug");
    await request(a).get("/public/stats?featureSlugs=sales&groupBy=day");
    await request(a).get("/stats?featureSlugs=sales&groupBy=workflowSlug").set(headers);
    for (const call of mockExecute.mock.calls) {
      const text = extractSqlText(call[0]);
      expect(text).not.toContain("c.feature_slug AS");
    }
    mockExecute.mockClear();
    await request(a).get("/public/stats?featureSlugs=sales&groupBy=workflowSlug&campaignId=camp-1");
    const stats = statsCalls();
    expect(stats.length).toBeGreaterThan(0);
    for (const call of stats) expect(extractSqlText(call[0])).not.toContain("c.feature_slug AS");
  });
});
