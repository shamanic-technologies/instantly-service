import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";
import express from "express";

/**
 * BOX-DB-LOAD-1027: the ~21 per-audience grouped reads of one campaign share ONE computation, grouped
 * by audience then the requested dimension, and each read is served its audience's slice.
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

const events = (partKey: string | null, groupKey: string, esSent: number) => ({
  partKey, groupKey, esSent, esDelivered: esSent, esOpened: 0, esClicked: 0, esBounced: 0, esUnsubscribed: 0,
  rsSent: esSent, rsDelivered: esSent, rsOpened: 0, rsClicked: 0, rsBounced: 0, rsUnsubscribed: 0, rdUnsubscribe: 0,
});

/** Answers each statement by its shape, as a partitioned read issues them. */
function answerPartitioned(sqlText: string) {
  if (sqlText.includes("latest_sentiment")) return { rows: [{ partKey: "aud-a", groupKey: "wf-1", rdInterested: 2 }] };
  if (sqlText.includes("SUM(q.steps)")) return { rows: [{ partKey: "aud-a", groupKey: "wf-1", queued: 3 }] };
  if (sqlText.includes("emailsContacted"))
    return {
      rows: [
        { partKey: "aud-a", groupKey: "wf-1", emailsContacted: 10, notSending: 0, cancelled: 0 },
        // Contacted, no event yet: an aggregate-only group of aud-b alone.
        { partKey: "aud-b", groupKey: "wf-2", emailsContacted: 4, notSending: 1, cancelled: 0 },
      ],
    };
  return { rows: [events("aud-a", "wf-1", 7), events("aud-b", "wf-1", 5), events(null, "wf-1", 99)] };
}

describe("audience family read (BOX-DB-LOAD-1027)", () => {
  beforeEach(() => {
    mockExecute.mockReset();
    mockExecute.mockImplementation(async (q: unknown) => answerPartitioned(extractSqlText(q)));
    clearStatsCache();
  });

  it("serves every audience of one campaign from ONE set of four statements, grouped by audience", async () => {
    const a = await app();
    const q = "?campaignId=camp-1&groupBy=workflowSlug&featureSlugs=f";
    const resA = await request(a).get(`/stats${q}&audienceId=aud-a`).set(headers);
    const resB = await request(a).get(`/stats${q}&audienceId=aud-b`).set(headers);
    const resNone = await request(a).get(`/stats${q}&audienceId=aud-unknown`).set(headers);

    expect(mockExecute).toHaveBeenCalledTimes(4);
    for (const call of mockExecute.mock.calls) {
      const text = extractSqlText(call[0]);
      expect(text).toContain(`c.metadata->>'audienceId' AS "partKey"`);
      expect(text).toContain("GROUP BY c.metadata->>'audienceId', c.workflow_slug");
      expect(text).not.toContain("c.metadata->>'audienceId' =");
    }

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
    expect(mockExecute).toHaveBeenCalledTimes(4);
    expect(extractSqlText(mockExecute.mock.calls[0][0])).toContain(`AS "partKey"`);
  });

  it("keeps the per-key read for a read with no audience and for groupBy=day", async () => {
    mockExecute.mockReset();
    mockExecute.mockResolvedValue({ rows: [] });
    const a = await app();
    await request(a).get("/stats?campaignId=camp-1&groupBy=workflowSlug").set(headers);
    await request(a).get("/stats?campaignId=camp-1&groupBy=day&audienceId=aud-a").set(headers);
    expect(mockExecute.mock.calls.length).toBeGreaterThan(0);
    for (const call of mockExecute.mock.calls) {
      expect(extractSqlText(call[0])).not.toContain("partKey");
    }
  });
});
