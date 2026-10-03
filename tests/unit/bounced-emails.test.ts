import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";
import express from "express";

const mockDbExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: { execute: (...a: unknown[]) => mockDbExecute(...a) },
}));

import { findBouncedEmails } from "../../src/lib/bounced-emails";

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

async function createApp() {
  const router = (await import("../../src/routes/bounced-emails")).default;
  const app = express();
  app.use(express.json());
  app.use(router);
  return app;
}

beforeEach(() => {
  vi.resetAllMocks();
  mockDbExecute.mockResolvedValue(pgResult([]));
});

describe("findBouncedEmails", () => {
  it("returns the bounced subset with its first bounce date", async () => {
    mockDbExecute.mockResolvedValue(
      pgResult([{ email: "gone@acme.com", first_bounced_at: new Date("2026-09-01T10:00:00.000Z") }]),
    );

    const out = await findBouncedEmails(["gone@acme.com", "fine@acme.com"]);

    expect(out).toEqual([{ email: "gone@acme.com", firstBouncedAt: "2026-09-01T10:00:00.000Z" }]);
  });

  it("reads email_bounced events fleet-wide: no org predicate anywhere", async () => {
    await findBouncedEmails(["a@b.com"]);

    const text = extractSqlText(mockDbExecute.mock.calls[0][0]);
    expect(text).toContain("instantly_events");
    expect(text).toContain("email_bounced");
    expect(text).not.toMatch(/org_id/);
  });

  it("normalizes and dedupes the input before the lookup", async () => {
    await findBouncedEmails(["  Gone@ACME.com ", "gone@acme.com"]);

    const query = mockDbExecute.mock.calls[0][0];
    const json = JSON.stringify(query);
    expect(json).toContain("gone@acme.com");
    expect(json).not.toContain("Gone@ACME.com");
  });

  it("does not query at all for an empty set", async () => {
    expect(await findBouncedEmails(["   "])).toEqual([]);
    expect(mockDbExecute).not.toHaveBeenCalled();
  });
});

describe("POST /internal/bounced-emails", () => {
  it("answers the bounced subset", async () => {
    mockDbExecute.mockResolvedValue(
      pgResult([{ email: "gone@acme.com", first_bounced_at: "2026-09-01T10:00:00.000Z" }]),
    );
    const app = await createApp();

    const res = await request(app).post("/").send({ emails: ["gone@acme.com", "fine@acme.com"] });

    expect(res.status).toBe(200);
    expect(res.body).toEqual({
      bounced: [{ email: "gone@acme.com", firstBouncedAt: "2026-09-01T10:00:00.000Z" }],
    });
  });

  it("answers 200 with an empty list when nobody bounced", async () => {
    const app = await createApp();
    const res = await request(app).post("/").send({ emails: ["fine@acme.com"] });
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ bounced: [] });
  });

  it("400s an empty or oversized batch", async () => {
    const app = await createApp();
    expect((await request(app).post("/").send({ emails: [] })).status).toBe(400);
    const tooMany = Array.from({ length: 1001 }, (_, i) => `p${i}@acme.com`);
    expect((await request(app).post("/").send({ emails: tooMany })).status).toBe(400);
    expect(mockDbExecute).not.toHaveBeenCalled();
  });

  it("500s on a read failure instead of reporting nobody bounced", async () => {
    mockDbExecute.mockRejectedValue(new Error("connection terminated"));
    const app = await createApp();

    const res = await request(app).post("/").send({ emails: ["gone@acme.com"] });

    expect(res.status).toBe(500);
    expect(res.body.error).toContain("connection terminated");
  });
});
