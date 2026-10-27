import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";
import express from "express";

/**
 * BOX-DB-LOAD-1027: an address answered for a scope in one `POST /orgs/status` batch is served from
 * that answer in any other batch within the minute; only the other addresses are queried.
 */

import { clearStatsCache } from "../../src/lib/stats-cache";
import { invalidateOrgStatusCache } from "../../src/lib/evidence-changed";
import { setStatusEmailResults, getStatusEmailResult } from "../../src/lib/status-email-cache";

const mockExecute = vi.fn();
vi.mock("../../src/db", () => ({ db: { execute: (...args: unknown[]) => mockExecute(...args) } }));
vi.mock("../../src/db/schema", () => ({ instantlyCampaigns: {} }));

const mockFindSendRefusals = vi.fn(async (..._args: unknown[]) => new Map());
vi.mock("../../src/lib/send-refusal", () => ({
  findSendRefusals: (...args: unknown[]) => mockFindSendRefusals(...args),
}));

let bounced = new Set<string>();

function chunkText(query: unknown): string {
  if (query == null) return "";
  if (typeof query === "string") return query;
  if (typeof query !== "object") return String(query);
  const chunks = (query as { queryChunks?: unknown[] }).queryChunks;
  if (Array.isArray(chunks)) return chunks.map(chunkText).join("");
  const v = (query as { value?: unknown }).value;
  if (typeof v === "string") return v;
  if (Array.isArray(v)) return v.map(chunkText).join("");
  return "";
}

async function app() {
  const statusRouter = (await import("../../src/routes/status")).default;
  const a = express();
  a.use(express.json());
  a.use((_req, res, next) => {
    res.locals.orgId = "org-1";
    next();
  });
  a.use(statusRouter);
  return a;
}

/** Addresses each status read asked the refusal gates about (one call per read that queried). */
const queried = () => mockFindSendRefusals.mock.calls.map((c) => [...(c[2] as string[])].sort());

const post = (a: express.Express, emails: string[], brandId = "brand-1") =>
  request(a).post("/").send({ brandId, items: emails.map((email) => ({ email })) });

describe("per-address status answers (BOX-DB-LOAD-1027)", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    clearStatsCache();
    bounced = new Set(["a@x.com"]);
    mockExecute.mockImplementation(async (q: unknown) => {
      const text = chunkText(q);
      // The global read: bounced per address, from the org's gold rows.
      if (text.includes('CAST(NULL AS boolean) AS "contacted"')) {
        return { rows: [...bounced].map((key) => ({ key, bounced: true, unsubscribed: false })) };
      }
      return { rows: [] };
    });
  });

  it("queries only the addresses no earlier batch answered, and serves the same answer for the others", async () => {
    const a = await app();
    const first = await post(a, ["a@x.com", "b@x.com"]);
    const second = await post(a, ["b@x.com", "a@x.com", "c@x.com"]);

    expect(queried()).toEqual([["a@x.com", "b@x.com"], ["c@x.com"]]);
    expect(second.body.results.map((r: any) => r.email)).toEqual(["b@x.com", "a@x.com", "c@x.com"]);
    expect(second.body.results[1]).toEqual(first.body.results[0]);
    expect(second.body.results[0]).toEqual(first.body.results[1]);
    expect(second.body.results[1].global.email.bounced).toBe(true);
  });

  it("never reuses an answer across scopes", async () => {
    const a = await app();
    await post(a, ["a@x.com"], "brand-1");
    await post(a, ["a@x.com"], "brand-2");
    expect(queried()).toEqual([["a@x.com"], ["a@x.com"]]);
  });

  it("re-reads an address once its evidence changed", async () => {
    const a = await app();
    await post(a, ["a@x.com", "b@x.com"]);
    bounced = new Set();
    invalidateOrgStatusCache("org-1", ["A@x.com"]);
    const after = await post(a, ["a@x.com", "b@x.com", "z@x.com"]);
    expect(queried()).toEqual([["a@x.com", "b@x.com"], ["a@x.com", "z@x.com"]]);
    expect(after.body.results[0].global.email.bounced).toBe(false);
  });

  it("does not store an answer whose load overlapped an invalidation of that address", () => {
    const scope = { orgId: "org-1", brandId: "b", campaignId: "", refusalBrandIds: "b" };
    const loadStartedAt = Date.now() - 1;
    invalidateOrgStatusCache("org-1", ["a@x.com"]);
    setStatusEmailResults(scope, new Map([["a@x.com", { stale: true }], ["b@x.com", { ok: true }]]), loadStartedAt);
    expect(getStatusEmailResult(scope, "a@x.com")).toBeUndefined();
    expect(getStatusEmailResult(scope, "b@x.com")).toEqual({ ok: true });
  });
});
