import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";
import express from "express";

const mockDbExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: { execute: (...a: unknown[]) => mockDbExecute(...a) },
}));

import {
  classifyStep,
  decodeSequenceCursor,
  encodeSequenceCursor,
  fetchBrandEmailSequences,
} from "../../src/lib/brand-email-sequences";

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

const BRAND = "d0965c2c-a58f-4497-addd-6a19e556b068";

function seqRow(over: Record<string, unknown> = {}) {
  return {
    sequenceKey: "ic-1",
    leadId: "lead-1",
    leadEmail: "jane@acme.com",
    campaignId: "camp-1",
    status: "active",
    // jsonb of a naive `timestamp` comes back without a zone.
    steps: [
      { step: 1, sentAt: "2026-10-09T10:45:37.185", held: false, anyHold: true },
      { step: 2, sentAt: null, held: true, anyHold: true },
      { step: 3, sentAt: null, held: true, anyHold: true },
    ],
    ...over,
  };
}

beforeEach(() => {
  vi.resetAllMocks();
  mockDbExecute.mockResolvedValue(pgResult([]));
});

describe("classifyStep — the customer's reading of one email", () => {
  it("a real send is sent, with its instant in UTC", () => {
    expect(classifyStep({ step: 1, sentAt: "2026-10-09 10:45:37.185", held: false }, true)).toEqual({
      step: 1,
      kind: "first_email",
      status: "sent",
      sentAt: "2026-10-09T10:45:37.185Z",
    });
  });

  it("a real send stays sent even once the sequence stopped", () => {
    expect(classifyStep({ step: 2, sentAt: "2026-10-09T10:45:37Z", held: false }, false).status).toBe("sent");
  });

  it("a live hold on an active sequence is queued, never pending or failed", () => {
    expect(classifyStep({ step: 2, sentAt: null, held: true }, true)).toEqual({
      step: 2,
      kind: "follow_up",
      status: "queued",
      sentAt: null,
    });
  });

  it("a cancelled hold is stopped", () => {
    expect(classifyStep({ step: 3, sentAt: null, held: false }, true).status).toBe("stopped");
  });

  it("an unsent step on a paused/completed sequence is stopped, even with a live hold", () => {
    expect(classifyStep({ step: 2, sentAt: null, held: true }, false).status).toBe("stopped");
  });
});

describe("fetchBrandEmailSequences", () => {
  it("labels every step of a sequence (first email sent + 2 follow-ups queued)", async () => {
    mockDbExecute.mockResolvedValue(pgResult([seqRow()]));
    const page = await fetchBrandEmailSequences({ orgId: "org-1", brandId: BRAND });
    expect(page.nextCursor).toBeNull();
    expect(page.sequences).toEqual([
      {
        leadId: "lead-1",
        leadEmail: "jane@acme.com",
        campaignId: "camp-1",
        steps: [
          { step: 1, kind: "first_email", status: "sent", sentAt: "2026-10-09T10:45:37.185Z" },
          { step: 2, kind: "follow_up", status: "queued", sentAt: null },
          { step: 3, kind: "follow_up", status: "queued", sentAt: null },
        ],
      },
    ]);
  });

  it("a step known only from a self-send step row (no hold record) on an active sequence is queued", async () => {
    mockDbExecute.mockResolvedValue(
      pgResult([seqRow({ steps: [{ step: 1, sentAt: null, held: false, anyHold: false }] })]),
    );
    const page = await fetchBrandEmailSequences({ orgId: "org-1", brandId: BRAND });
    expect(page.sequences[0].steps[0].status).toBe("queued");
  });

  it("exposes no internal sequence id", async () => {
    mockDbExecute.mockResolvedValue(pgResult([seqRow()]));
    const page = await fetchBrandEmailSequences({ orgId: "org-1", brandId: BRAND });
    expect(JSON.stringify(page.sequences)).not.toContain("ic-1");
    expect(Object.keys(page.sequences[0]).sort()).toEqual(["campaignId", "leadEmail", "leadId", "steps"]);
  });

  it("scopes the read to the org AND the brand, and skips reservation sentinels", async () => {
    await fetchBrandEmailSequences({ orgId: "org-1", brandId: BRAND });
    const call = mockDbExecute.mock.calls[0][0];
    const text = extractSqlText(call);
    expect(text).toContain("c.org_id = ");
    expect(text).toContain("= ANY(c.brand_ids)");
    expect(text).toContain("NOT LIKE 'reserving:%'");
    expect(text).toContain("e.inferred = false");
    expect(text).toContain("e.withdrawn_at IS NULL");
    expect(JSON.stringify(call)).toContain("org-1");
    expect(JSON.stringify(call)).toContain(BRAND);
  });

  it("pages with a keyset cursor: one extra row means another page", async () => {
    mockDbExecute.mockResolvedValue(
      pgResult([seqRow({ sequenceKey: "a" }), seqRow({ sequenceKey: "b" }), seqRow({ sequenceKey: "c" })]),
    );
    const page = await fetchBrandEmailSequences({ orgId: "org-1", brandId: BRAND, limit: 2 });
    expect(page.sequences).toHaveLength(2);
    expect(page.nextCursor).toBe(encodeSequenceCursor("b"));
    expect(decodeSequenceCursor(page.nextCursor as string)).toBe("b");

    mockDbExecute.mockResolvedValue(pgResult([]));
    await fetchBrandEmailSequences({ orgId: "org-1", brandId: BRAND, cursor: page.nextCursor as string });
    const text = extractSqlText(mockDbExecute.mock.calls[1][0]);
    expect(text).toContain("AND c.instantly_campaign_id > ");
  });

  it("rejects a cursor it did not mint", () => {
    expect(() => decodeSequenceCursor("nope")).toThrow(/invalid cursor/);
  });
});

describe("GET /orgs/email-sequences", () => {
  async function createApp() {
    const router = (await import("../../src/routes/email-sequences")).default;
    const app = express();
    app.use((req, res, next) => {
      res.locals.orgId = "org-1";
      next();
    });
    app.use(router);
    return app;
  }

  it("answers the page with brandId, count and nextCursor", async () => {
    mockDbExecute.mockResolvedValue(pgResult([seqRow()]));
    const res = await request(await createApp()).get(`/?brandId=${BRAND}`);
    expect(res.status).toBe(200);
    expect(res.body.brandId).toBe(BRAND);
    expect(res.body.count).toBe(1);
    expect(res.body.nextCursor).toBeNull();
    expect(res.body.sequences[0].steps).toHaveLength(3);
  });

  it("400s without a brandId", async () => {
    const res = await request(await createApp()).get("/");
    expect(res.status).toBe(400);
    expect(mockDbExecute).not.toHaveBeenCalled();
  });

  it("400s on a foreign cursor", async () => {
    const res = await request(await createApp()).get(`/?brandId=${BRAND}&cursor=nope`);
    expect(res.status).toBe(400);
  });

  it("fails loud (500) on a read failure, never an empty page", async () => {
    mockDbExecute.mockRejectedValue(new Error("boom"));
    const res = await request(await createApp()).get(`/?brandId=${BRAND}`);
    expect(res.status).toBe(500);
    expect(res.body.error).toBe("boom");
  });
});
