import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";
import express from "express";
import { OpenApiGeneratorV3 } from "@asteasolutions/zod-to-openapi";

const mockDbExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: { execute: (...a: unknown[]) => mockDbExecute(...a) },
}));

import {
  fetchQueuedEmails,
  fetchSentEmails,
  ProviderEmailFilterError,
} from "../../src/lib/provider-emails";
import { registry } from "../../src/schemas";

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

/** The values bound into a drizzle SQL object (anything that is not a text chunk). */
function boundValues(obj: unknown, out: unknown[] = []): unknown[] {
  if (obj == null) return out;
  if (Array.isArray(obj)) {
    for (const x of obj) boundValues(x, out);
    return out;
  }
  if (typeof obj === "object") {
    const o = obj as Record<string, unknown>;
    if (Array.isArray(o.value)) return out;
    if (Array.isArray(o.queryChunks)) return boundValues(o.queryChunks, out);
    return out;
  }
  out.push(obj);
  return out;
}

function pgResult<T>(rows: T[]) {
  return { command: "SELECT", rowCount: rows.length, oid: null, fields: [], rows };
}

function queuedRow(over: Record<string, unknown> = {}) {
  return {
    id: "row-1",
    email: "ada@clinic.com",
    leadId: "lead-1",
    queuedAt: "2026-11-09T01:25:08.482Z",
    followUp: false,
    state: "queued",
    orgId: "org-1",
    brandIds: ["b-1"],
    campaignId: "camp-1",
    ...over,
  };
}

const lastSql = () => extractSqlText(mockDbExecute.mock.calls.at(-1)![0]);
const lastValues = () => boundValues(mockDbExecute.mock.calls.at(-1)![0]);

beforeEach(() => {
  vi.resetAllMocks();
  mockDbExecute.mockResolvedValue(pgResult([]));
});

describe("provider read email_queued: the person and the scope", () => {
  it("refuses a read that names no person, without touching the database", async () => {
    await expect(fetchQueuedEmails({ orgId: "org-1", brandId: "b-1" })).rejects.toBeInstanceOf(ProviderEmailFilterError);
    await expect(fetchQueuedEmails({ orgId: "org-1", email: "   " })).rejects.toThrow(/email and\/or lead_id/);
    expect(mockDbExecute).not.toHaveBeenCalled();
  });

  it("matches the address trimmed and lowercased, inside the caller's org by default", async () => {
    await fetchQueuedEmails({ orgId: "org-1", email: "  Ada@Clinic.com " });
    expect(lastSql()).toContain("lower(c.lead_email) = ");
    expect(lastSql()).toContain("c.org_id = ");
    expect(lastValues()).toContain("ada@clinic.com");
    expect(lastValues()).toContain("org-1");
  });

  it("reads the queue decision table itself, so an email queued a second ago is returned", async () => {
    await fetchQueuedEmails({ orgId: "org-1", email: "ada@clinic.com" });
    const text = lastSql();
    expect(text).toContain("FROM instantly_campaigns c");
    // No join on a send, a hold or an event: the row exists from the reservation.
    expect(text).not.toContain("JOIN");
    expect(text).toContain("ORDER BY c.created_at DESC");
  });

  it("a lead id alone names the person; with an address too, either one does", async () => {
    await fetchQueuedEmails({ orgId: "org-1", leadId: "lead-1" });
    expect(lastSql()).toContain("c.lead_id = ");
    expect(lastSql()).not.toContain("lower(c.lead_email) = ");
    await fetchQueuedEmails({ orgId: "org-1", leadId: "lead-1", email: "ada@clinic.com" });
    expect(lastSql()).toMatch(/\(lower\(c\.lead_email\) = .* OR c\.lead_id = /);
  });

  it("applies the brand, the campaign, the window and first-email filters only when stated", async () => {
    await fetchQueuedEmails({ orgId: "org-1", email: "ada@clinic.com" });
    for (const absent of ["c.brand_ids @>", "c.campaign_id = ", "c.created_at >=", "c.created_at <", "c.follow_up = "]) {
      expect(lastSql()).not.toContain(absent);
    }
    await fetchQueuedEmails({
      orgId: "org-1",
      email: "ada@clinic.com",
      brandId: "b-1",
      campaignId: "camp-1",
      since: "2026-08-23T00:00:00Z",
      before: "2026-11-23T10:00:00+01:00",
      firstEmail: true,
    });
    const text = lastSql();
    for (const present of ["c.brand_ids @>", "c.campaign_id = ", "c.created_at >=", "c.created_at <", "c.follow_up = false"]) {
      expect(text).toContain(present);
    }
    expect(lastValues()).toContain("2026-08-23T00:00:00.000Z");
    // An offset instant is bound as the UTC instant it is.
    expect(lastValues()).toContain("2026-11-23T09:00:00.000Z");
  });

  it("refuses a date that is not one and a limit out of bounds", async () => {
    await expect(fetchQueuedEmails({ orgId: "org-1", email: "a@b.co", since: "three months" })).rejects.toThrow(/since is not a date/);
    await expect(fetchQueuedEmails({ orgId: "org-1", email: "a@b.co", limit: 501 })).rejects.toThrow(/limit is 1-500/);
    expect(mockDbExecute).not.toHaveBeenCalled();
  });

  it("leaves out the decisions that never sent unless asked", async () => {
    await fetchQueuedEmails({ orgId: "org-1", email: "ada@clinic.com" });
    expect(lastSql()).toContain("<> 'stopped'");
    await fetchQueuedEmails({ orgId: "org-1", email: "ada@clinic.com", includeStopped: true });
    expect(lastSql()).not.toContain("<> 'stopped'");
  });

  it("across orgs: drops the org condition, and another org's Unit names no brand and no campaign", async () => {
    mockDbExecute.mockResolvedValue(
      pgResult([queuedRow(), queuedRow({ id: "row-2", orgId: "org-2", brandIds: ["b-9"], campaignId: "camp-9", state: "sent" })]),
    );
    const units = await fetchQueuedEmails({ orgId: "org-1", email: "ada@clinic.com", acrossOrgs: true });
    expect(lastSql()).not.toContain("c.org_id = ");
    expect(units[0]).toEqual({
      id: "row-1",
      outcome: "email_queued",
      email: "ada@clinic.com",
      leadId: "lead-1",
      queuedAt: "2026-11-09T01:25:08.482Z",
      firstEmail: true,
      state: "queued",
      ownOrg: true,
      brandIds: ["b-1"],
      campaignId: "camp-1",
    });
    expect(units[1]).toMatchObject({ id: "row-2", ownOrg: false, brandIds: null, campaignId: null, state: "sent" });
  });

  it("writes nothing: one SELECT, no declaration, no update", async () => {
    await fetchQueuedEmails({ orgId: "org-1", email: "ada@clinic.com" });
    expect(mockDbExecute).toHaveBeenCalledTimes(1);
    expect(lastSql()).not.toMatch(/\b(INSERT|UPDATE|DELETE)\b/);
  });
});

describe("provider read email_sent", () => {
  it("reads real sends only, by the person, newest first", async () => {
    mockDbExecute.mockResolvedValue(
      pgResult([
        { id: "ev-1", email: "ada@clinic.com", leadId: "lead-1", sentAt: "2026-11-09T12:00:00.000Z", step: 1, followUp: false, orgId: "org-1", brandIds: ["b-1"], campaignId: "camp-1" },
        { id: "ev-2", email: "ada@clinic.com", leadId: "lead-1", sentAt: "2026-11-01T12:00:00.000Z", step: 2, followUp: false, orgId: "org-1", brandIds: ["b-1"], campaignId: "camp-1" },
      ]),
    );
    const units = await fetchSentEmails({ orgId: "org-1", email: "ada@clinic.com", since: "2026-08-23T00:00:00Z" });
    const text = lastSql();
    expect(text).toContain("e.event_type = 'email_sent'");
    expect(text).toContain("e.inferred = false");
    expect(text).toContain("e.timestamp >=");
    expect(text).toContain("ORDER BY e.timestamp DESC");
    expect(units.map((u) => [u.outcome, u.step, u.firstEmail])).toEqual([
      ["email_sent", 1, true],
      ["email_sent", 2, false],
    ]);
  });

  it("refuses a read that names no person", async () => {
    await expect(fetchSentEmails({ orgId: "org-1" })).rejects.toBeInstanceOf(ProviderEmailFilterError);
  });
});

describe("GET /orgs/emails/queued and /orgs/emails/sent", () => {
  async function createApp() {
    const router = (await import("../../src/routes/provider-emails")).default;
    const app = express();
    app.use((req, res, next) => {
      res.locals.orgId = "org-1";
      next();
    });
    app.use(router);
    return app;
  }

  it("answers the Units with their outcome and count", async () => {
    mockDbExecute.mockResolvedValue(pgResult([queuedRow()]));
    const res = await request(await createApp()).get("/queued").query({ email: "ada@clinic.com", brand_id: "a179bbd9-8eed-4dba-9338-78125922b0c6", since: "2026-08-23T00:00:00Z" });
    expect(res.status).toBe(200);
    expect(res.body.outcome).toBe("email_queued");
    expect(res.body.count).toBe(1);
    expect(res.body.units[0].state).toBe("queued");
  });

  it("a never-contacted person: 200 with no Unit", async () => {
    const res = await request(await createApp()).get("/queued").query({ email: "new@person.com" });
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ outcome: "email_queued", count: 0, units: [] });
    const sent = await request(await createApp()).get("/sent").query({ lead_id: "lead-404" });
    expect(sent.body).toEqual({ outcome: "email_sent", count: 0, units: [] });
  });

  it("400s a read naming no person, a bad boolean, a bad brand and a bad date", async () => {
    const app = await createApp();
    expect((await request(app).get("/queued")).status).toBe(400);
    expect((await request(app).get("/queued").query({ email: "a@b.co", across_orgs: "yes" })).status).toBe(400);
    expect((await request(app).get("/queued").query({ email: "a@b.co", brand_id: "nope" })).status).toBe(400);
    expect((await request(app).get("/sent").query({ email: "a@b.co", since: "soon" })).status).toBe(400);
    expect((await request(app).get("/sent").query({ email: "a@b.co", include_stopped: "true" })).status).toBe(200);
  });

  it("passes across_orgs and include_stopped through", async () => {
    await request(await createApp()).get("/queued").query({ email: "a@b.co", across_orgs: "true", include_stopped: "true" });
    expect(lastSql()).not.toContain("c.org_id = ");
    expect(lastSql()).not.toContain("<> 'stopped'");
  });

  it("500s on a read failure instead of answering 'never emailed'", async () => {
    mockDbExecute.mockRejectedValue(new Error("connection terminated"));
    const res = await request(await createApp()).get("/queued").query({ email: "a@b.co" });
    expect(res.status).toBe(500);
    expect(res.body.error).toContain("connection terminated");
  });
});

describe("the served OpenAPI names both reads as Provider reads", () => {
  // The exact rule workflow-service's provider index applies (provider-index.ts, ENGINE-GET-ASK-1123): an
  // operation whose `tags` holds `provider:<outcome>`, on get/post/put.
  const doc = new OpenApiGeneratorV3(registry.definitions).generateDocument({ openapi: "3.0.0", info: { title: "t", version: "1" } });
  function providerOps(outcome: string) {
    const found: Array<{ method: string; path: string; operationId: unknown }> = [];
    for (const [path, item] of Object.entries(doc.paths ?? {})) {
      for (const method of ["get", "post", "put"] as const) {
        const op = (item as Record<string, { tags?: string[]; operationId?: string } | undefined>)[method];
        if (op?.tags?.includes(`provider:${outcome}`)) found.push({ method, path, operationId: op.operationId });
      }
    }
    return found;
  }

  it("exactly one read per outcome, so a Script names no route", () => {
    expect(providerOps("email_queued")).toEqual([{ method: "get", path: "/orgs/emails/queued", operationId: "readQueuedEmails" }]);
    expect(providerOps("email_sent")).toEqual([{ method: "get", path: "/orgs/emails/sent", operationId: "readSentEmails" }]);
  });

  it("documents the filters a Script chooses", () => {
    const params = (doc.paths!["/orgs/emails/queued"] as { get: { parameters: Array<{ name: string; in: string }> } }).get.parameters
      .filter((p) => p.in === "query")
      .map((p) => p.name);
    for (const name of ["email", "lead_id", "brand_id", "campaign_id", "since", "before", "first_email", "across_orgs", "include_stopped", "limit"]) {
      expect(params).toContain(name);
    }
  });
});
