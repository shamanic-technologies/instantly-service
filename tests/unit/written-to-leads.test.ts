import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";
import express from "express";

const mockDbExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: { execute: (...a: unknown[]) => mockDbExecute(...a) },
}));

import {
  decodeCursor,
  encodeCursor,
  fetchWrittenToLeads,
} from "../../src/lib/written-to-leads";

/** Recursively extract SQL text fragments from a drizzle SQL object. */
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

/** node-postgres returns a QueryResult OBJECT, never a bare array. */
function pgResult<T>(rows: T[]) {
  return { command: "SELECT", rowCount: rows.length, oid: null, fields: [], rows };
}

function goldRow(over: Record<string, unknown> = {}) {
  return {
    campaignId: "camp-1",
    instantlyCampaignId: "ic-1",
    leadEmail: "patrick@tyromotion.com",
    brandIds: ["b-1"],
    // node-postgres hands a `timestamp` column back NAIVE.
    firstSentAt: "2026-09-29 06:11:49.827",
    lastSentAt: "2026-10-02 07:00:00",
    engaged: false,
    replied: false,
    clicked: false,
    unsubscribed: false,
    bounced: false,
    firstRepliedAt: null,
    firstClickedAt: null,
    replyClassification: null,
    replyKind: "auto_reply_received",
    ...over,
  };
}

beforeEach(() => {
  vi.resetAllMocks();
  mockDbExecute.mockResolvedValue(pgResult([]));
});

describe("written-to-leads — the population", () => {
  it("lists a lead we wrote to who never answered (auto-reply only)", async () => {
    mockDbExecute.mockResolvedValue(pgResult([goldRow()]));

    const page = await fetchWrittenToLeads({ orgId: "org-1" });

    expect(page.leads).toEqual([
      {
        campaignId: "camp-1",
        instantlyCampaignId: "ic-1",
        leadEmail: "patrick@tyromotion.com",
        brandIds: ["b-1"],
        firstSentAt: "2026-09-29T06:11:49.827Z",
        lastSentAt: "2026-10-02T07:00:00.000Z",
        engaged: false,
        replied: false,
        clicked: false,
        unsubscribed: false,
        bounced: false,
        firstRepliedAt: null,
        firstClickedAt: null,
        replyClassification: null,
        replyKind: "auto_reply_received",
        disqualified: false,
      },
    ]);
    expect(page.nextCursor).toBeNull();
  });

  it("gates on a real send, NOT on engagement, scoped to the caller's org", async () => {
    await fetchWrittenToLeads({ orgId: "org-1", brandId: "b-1", campaignId: "c-1" });

    const text = extractSqlText(mockDbExecute.mock.calls[0][0]);
    expect(text).toContain("FROM instantly_lead_status_current");
    expect(text).toMatch(/org_id = /);
    expect(text).toMatch(/AND sent/);
    expect(text).toContain("ANY(brand_ids)");
    expect(text).toContain("campaign_id = ");
    // The engagement predicate is a COLUMN here, never a WHERE gate.
    expect(text).not.toMatch(/WHERE[\s\S]*\(\(replied AND NOT unsubscribed\) OR clicked\)[\s\S]*ORDER BY/);
    expect(text).toContain("((replied AND NOT unsubscribed) OR clicked) AS engaged");
  });

  it("derives disqualified from the reply kind", async () => {
    mockDbExecute.mockResolvedValue(
      pgResult([goldRow({ replied: true, replyKind: "lead_wrong_person" })]),
    );
    const page = await fetchWrittenToLeads({ orgId: "org-1" });
    expect(page.leads[0].disqualified).toBe(true);
  });

  it("fails loud on a written-to row with no send instant", async () => {
    mockDbExecute.mockResolvedValue(pgResult([goldRow({ firstSentAt: null })]));
    await expect(fetchWrittenToLeads({ orgId: "org-1" })).rejects.toThrow(
      /no send timestamp/,
    );
  });
});

describe("written-to-leads — paging", () => {
  it("asks for one row past the page and mints a cursor from the last kept row", async () => {
    mockDbExecute.mockResolvedValue(
      pgResult([
        goldRow({ instantlyCampaignId: "ic-1", leadEmail: "a@x.com" }),
        goldRow({ instantlyCampaignId: "ic-2", leadEmail: "b@x.com" }),
        goldRow({ instantlyCampaignId: "ic-3", leadEmail: "c@x.com" }),
      ]),
    );

    const page = await fetchWrittenToLeads({ orgId: "org-1", limit: 2 });

    const call = mockDbExecute.mock.calls[0][0];
    expect(JSON.stringify(call)).toContain("3");
    expect(page.leads.map((l) => l.leadEmail)).toEqual(["a@x.com", "b@x.com"]);
    expect(decodeCursor(page.nextCursor!)).toEqual({
      instantlyCampaignId: "ic-2",
      leadEmail: "b@x.com",
    });
  });

  it("returns a null cursor when the page is not full", async () => {
    mockDbExecute.mockResolvedValue(pgResult([goldRow()]));
    const page = await fetchWrittenToLeads({ orgId: "org-1", limit: 2 });
    expect(page.nextCursor).toBeNull();
  });

  it("walks the keyset strictly after the cursor, in primary-key order", async () => {
    const cursor = encodeCursor({ instantlyCampaignId: "ic-2", leadEmail: "b@x.com" });
    await fetchWrittenToLeads({ orgId: "org-1", cursor });

    const text = extractSqlText(mockDbExecute.mock.calls[0][0]);
    expect(text).toContain("(instantly_campaign_id, lead_email) > (");
    expect(text).toContain("ORDER BY instantly_campaign_id ASC, lead_email ASC");
  });

  it("round-trips a cursor and rejects a forged one", () => {
    const key = { instantlyCampaignId: "self:abc", leadEmail: "x@y.com" };
    expect(decodeCursor(encodeCursor(key))).toEqual(key);
    expect(() => decodeCursor("not-a-cursor")).toThrow(/invalid cursor/);
    expect(() =>
      decodeCursor(Buffer.from(JSON.stringify({ a: 1 })).toString("base64url")),
    ).toThrow(/invalid cursor/);
  });
});

describe("GET /orgs/written-to-leads", () => {
  async function createApp() {
    const router = (await import("../../src/routes/written-to-leads")).default;
    const app = express();
    app.use((req, res, next) => {
      res.locals.orgId = "org-1";
      next();
    });
    app.use(router);
    return app;
  }

  it("returns the page with count and nextCursor", async () => {
    mockDbExecute.mockResolvedValue(pgResult([goldRow()]));
    const res = await request(await createApp()).get("/");
    expect(res.status).toBe(200);
    expect(res.body.success).toBe(true);
    expect(res.body.count).toBe(1);
    expect(res.body.nextCursor).toBeNull();
    expect(res.body.leads[0].leadEmail).toBe("patrick@tyromotion.com");
  });

  it("400s a forged cursor and an oversized limit", async () => {
    const app = await createApp();
    expect((await request(app).get("/").query({ cursor: "zzz" })).status).toBe(400);
    expect((await request(app).get("/").query({ limit: "5001" })).status).toBe(400);
  });

  it("500s on a read failure instead of an empty page", async () => {
    mockDbExecute.mockRejectedValue(new Error("connection terminated"));
    const res = await request(await createApp()).get("/");
    expect(res.status).toBe(500);
    expect(res.body.error).toContain("connection terminated");
  });
});
