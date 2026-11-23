/**
 * Provider reads of our Units (ENGINE-GET-ASK-1123): query parsing, cursor, row
 * mapping, the two routes over a mocked `db.execute` (real driver shape), and
 * the tags + operationIds workflow-service finds the reads by.
 */
import fs from "node:fs";
import path from "node:path";
import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";
import express from "express";

const mockDbExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: { execute: (...a: unknown[]) => mockDbExecute(...a) },
}));

import {
  InvalidCursorError,
  UnitsQueryError,
  decodeUnitCursor,
  encodeUnitCursor,
  fetchEmailQueuedUnits,
  fetchEmailSentUnits,
  parseUnitsQuery,
  queuedStateOf,
  splitList,
  toEmailQueuedUnit,
  toEmailSentUnit,
  type UnitRow,
} from "../../src/lib/provider-units";

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

function row(over: Partial<UnitRow> = {}): UnitRow {
  return {
    unitId: "ev-1",
    atText: "2026-10-02T07:00:00.123456",
    step: 1,
    kind: "first",
    email: "jane@acme.com",
    leadId: "lead-1",
    continuesThread: false,
    campaignId: "camp-1",
    brandIds: ["b-1"],
    sequenceId: "self:1",
    bounced: false,
    ...over,
  };
}

const sqlOfCall = (n = 0) => extractSqlText(mockDbExecute.mock.calls[n]![0]);

beforeEach(() => {
  vi.resetAllMocks();
  mockDbExecute.mockResolvedValue(pgResult([]));
});

describe("provider-units: the query", () => {
  it("reads comma lists and repeated params, trimmed, lowercased, de-duplicated", () => {
    expect(splitList(" a , b,,a ")).toEqual(["a", "b"]);
    expect(splitList(["a,b", "c"])).toEqual(["a", "b", "c"]);
    expect(splitList(undefined)).toEqual([]);

    const f = parseUnitsQuery("email_sent", "org-1", {
      emails: ["Jane@Acme.com, bob@acme.com", "JANE@acme.com"],
      leadIds: "L1, L2",
      domains: "@Acme.COM, beta.io",
    });
    expect(f.emails).toEqual(["jane@acme.com", "bob@acme.com"]);
    expect(f.leadIds).toEqual(["L1", "L2"]);
    expect(f.domains).toEqual(["acme.com", "beta.io"]);
    expect(f.limit).toBe(100);
    expect(f.kinds).toBeUndefined();
    expect(f.since).toBeUndefined();
    expect(f.orgId).toBe("org-1");
  });

  it("requires one of emails / leadIds / domains", () => {
    const attempt = () => parseUnitsQuery("email_sent", "org-1", { since: "2026-08-23T00:00:00Z" });
    expect(attempt).toThrow(UnitsQueryError);
    try {
      attempt();
    } catch (e) {
      expect((e as UnitsQueryError).code).toBe("person_filter_required");
    }
    expect(() => parseUnitsQuery("email_sent", "org-1", { emails: " , " })).toThrow(/at least one/);
  });

  it("refuses what is not an address, a domain carrying '@', and more than 100 values", () => {
    expect(() => parseUnitsQuery("email_sent", "o", { emails: "acme.com" })).toThrow(/not an address/);
    expect(() => parseUnitsQuery("email_sent", "o", { domains: "jane@acme.com" })).toThrow(/part after '@'/);
    const many = Array.from({ length: 101 }, (_, i) => `p${i}@acme.com`).join(",");
    expect(() => parseUnitsQuery("email_sent", "o", { emails: many })).toThrow(/at most 100/);
    expect(parseUnitsQuery("email_sent", "o", { emails: many.split(",").slice(0, 100).join(",") }).emails).toHaveLength(100);
    expect(() => parseUnitsQuery("email_sent", "o", { leadIds: Array.from({ length: 101 }, (_, i) => `l${i}`) })).toThrow(/at most 100/);
    expect(() => parseUnitsQuery("email_sent", "o", { domains: Array.from({ length: 101 }, (_, i) => `d${i}.com`) })).toThrow(/at most 100/);
  });

  it("validates kinds per outcome, and states on email-queued only", () => {
    expect(parseUnitsQuery("email_sent", "o", { emails: "a@b.c", kinds: "first,answer" }).kinds).toEqual(["first", "answer"]);
    expect(() => parseUnitsQuery("email_sent", "o", { emails: "a@b.c", kinds: "reply" })).toThrow(/unknown value/);
    expect(() => parseUnitsQuery("email_queued", "o", { emails: "a@b.c", kinds: "answer" })).toThrow(/unknown value/);
    expect(parseUnitsQuery("email_queued", "o", { emails: "a@b.c", states: "waiting, sent" }).states).toEqual(["waiting", "sent"]);
    expect(() => parseUnitsQuery("email_queued", "o", { emails: "a@b.c", states: "provisioned" })).toThrow(/unknown value/);
    // `states` means nothing on email-sent: ignored, never a filter.
    expect(parseUnitsQuery("email_sent", "o", { emails: "a@b.c", states: "waiting" }).states).toBeUndefined();
  });

  it("reads since / until as instants (no zone = UTC) and bounds limit", () => {
    const f = parseUnitsQuery("email_sent", "o", {
      emails: "a@b.c",
      since: "2026-08-23T00:00:00+02:00",
      until: "2026-09-01T10:00:00",
      limit: "1000",
    });
    expect(f.since).toBe("2026-08-22T22:00:00.000Z");
    expect(f.until).toBe("2026-09-01T10:00:00.000Z");
    expect(f.limit).toBe(1000);
    expect(parseUnitsQuery("email_sent", "o", { emails: "a@b.c", since: "2026-08-23" }).since).toBe("2026-08-23T00:00:00.000Z");
    expect(() => parseUnitsQuery("email_sent", "o", { emails: "a@b.c", since: "3 months ago" })).toThrow(/ISO 8601/);
    expect(() => parseUnitsQuery("email_sent", "o", { emails: "a@b.c", limit: "1001" })).toThrow(/limit/);
    expect(() => parseUnitsQuery("email_sent", "o", { emails: "a@b.c", limit: "0" })).toThrow(/limit/);
  });
});

describe("provider-units: the cursor", () => {
  it("round-trips at full microsecond precision", () => {
    const key = { at: "2026-10-02T07:00:00.123456", unitId: "answer:7f" };
    expect(decodeUnitCursor(encodeUnitCursor(key))).toEqual(key);
  });

  it("refuses a cursor it did not mint", () => {
    expect(() => decodeUnitCursor("not-a-cursor")).toThrow(InvalidCursorError);
    const forged = (v: unknown) => Buffer.from(JSON.stringify(v)).toString("base64url");
    expect(() => decodeUnitCursor(forged({ a: 1 }))).toThrow(/invalid cursor/);
    expect(() => decodeUnitCursor(forged(["2026-10-02", "x"]))).toThrow(/invalid cursor/);
    expect(() => decodeUnitCursor(forged(["2026-10-02T07:00:00.123456", ""]))).toThrow(/invalid cursor/);
  });
});

describe("provider-units: a row becomes a Unit", () => {
  it("email-sent: identity, UTC instant, ledger key", () => {
    expect(toEmailSentUnit(row({ email: "Jane@Acme.com" }))).toEqual({
      unitId: "ev-1",
      outcome: "email_sent",
      ledgerOutcome: "message_sent",
      subject: { service: "lead-service", id: "lead-1" },
      email: "jane@acme.com",
      domain: "acme.com",
      at: "2026-10-02T07:00:00.123Z",
      step: 1,
      continuesThread: false,
      campaignId: "camp-1",
      brandIds: ["b-1"],
      sequenceId: "self:1",
      kind: "first",
      bounced: false,
    });
  });

  it("no lead id = null subject; a raw-text array is never trusted as brand ids", () => {
    const unit = toEmailSentUnit(row({ leadId: null, brandIds: "{b-1,b-2}", campaignId: null }));
    expect(unit.subject).toBeNull();
    expect(unit.brandIds).toEqual([]);
    expect(unit.campaignId).toBeNull();
  });

  it("an answer has no step and is never bounced; a bounced step says so", () => {
    const answer = toEmailSentUnit(row({ unitId: "answer:1", kind: "answer", step: null, bounced: true }));
    expect(answer).toMatchObject({ kind: "answer", step: null, bounced: false });
    expect(toEmailSentUnit(row({ kind: "follow_up", step: 2, bounced: true, continuesThread: true }))).toMatchObject({
      kind: "follow_up",
      step: 2,
      bounced: true,
      continuesThread: true,
    });
  });

  it("email-queued: the queue status maps to a closed set of states, anything else throws", () => {
    expect(queuedStateOf("provisioned")).toBe("waiting");
    expect(queuedStateOf("actual")).toBe("sent");
    expect(queuedStateOf("cancelled")).toBe("cancelled");
    expect(() => queuedStateOf("refunded")).toThrow(/unknown queue status/);
    expect(() => toEmailQueuedUnit(row({ holdStatus: null }))).toThrow(/unknown queue status/);

    const unit = toEmailQueuedUnit(row({ unitId: "hold-1", holdStatus: "provisioned", declaredOnLedger: true }));
    expect(unit).toMatchObject({
      unitId: "hold-1",
      outcome: "email_queued",
      ledgerOutcome: "email_queued",
      state: "waiting",
      declaredOnLedger: true,
      kind: "first",
    });
    expect(unit).not.toHaveProperty("bounced");
    expect(toEmailQueuedUnit(row({ holdStatus: "actual", declaredOnLedger: null })).declaredOnLedger).toBe(false);
  });

  it("fails loud on an unknown kind or an unreadable instant", () => {
    expect(() => toEmailSentUnit(row({ kind: "nudge" }))).toThrow(/unknown kind/);
    expect(() => toEmailQueuedUnit(row({ kind: "answer", holdStatus: "actual" }))).toThrow(/unknown kind/);
    expect(() => toEmailSentUnit(row({ atText: null }))).toThrow(/no readable instant/);
  });
});

describe("provider-units: the statements", () => {
  const base = { orgId: "org-1", emails: [], leadIds: [], domains: [], limit: 2 };

  it("adds an OR branch only for the lists given, always inside the org", async () => {
    await fetchEmailSentUnits({ ...base, emails: ["jane@acme.com"] });
    let text = sqlOfCall(0);
    expect(text).toContain("c.org_id = ");
    expect(text).toContain("lower(c.lead_email) = ANY(");
    expect(text).not.toContain("c.lead_id = ANY(");
    expect(text).not.toContain("split_part(lower(c.lead_email), '@', 2) = ANY(");
    expect(text).not.toContain("u.at >=");
    expect(text).not.toContain("= ANY(c.brand_ids)");

    await fetchEmailSentUnits({ ...base, leadIds: ["l1"], domains: ["acme.com"], brandId: "b", campaignId: "c", since: "2026-08-23T00:00:00.000Z", until: "2026-09-23T00:00:00.000Z", kinds: ["first"] });
    text = sqlOfCall(1);
    expect(text).not.toContain("lower(c.lead_email) = ANY(");
    expect(text).toContain("c.lead_id = ANY(");
    // The expression migration 0116 indexes, verbatim.
    expect(text).toContain("split_part(lower(c.lead_email), '@', 2) = ANY(");
    expect(text).toContain("= ANY(c.brand_ids)");
    expect(text).toContain("c.campaign_id = ");
    expect(text).toContain("u.at >=");
    expect(text).toContain("u.at <");
    expect(text).toContain("u.kind = ANY(");
  });

  it("email-sent reads real, non-withdrawn sends and thread answers, newest first", async () => {
    await fetchEmailSentUnits({ ...base, emails: ["jane@acme.com"] });
    const text = sqlOfCall();
    expect(text).toContain("ev.event_type = 'email_sent'");
    expect(text).toContain("ev.withdrawn_at IS NULL");
    expect(text).toContain("b.event_type = 'email_bounced'");
    expect(text).toContain("JOIN reply_send_outcomes r ON r.instantly_campaign_id = seq.instantly_campaign_id");
    expect(text).toContain("ORDER BY u.at DESC, u.unit_id DESC");
  });

  it("email-queued reads the send queue, legacy holds by campaign + address, states as queue statuses", async () => {
    await fetchEmailQueuedUnits({ ...base, emails: ["jane@acme.com"], states: ["waiting"] });
    const text = sqlOfCall();
    expect(text).toContain("JOIN sequence_costs sc ON sc.instantly_campaign_id = seq.instantly_campaign_id");
    expect(text).toContain("sc.instantly_campaign_id IS NULL");
    expect(text).toContain("AND NOT seq.follow_up");
    expect(text).toContain("u.hold_status = ANY(");
    expect(text).toContain("sc.queued_declaration = 'declared'");
  });

  it("never lists a whole org: no person filter throws before any statement", async () => {
    await expect(fetchEmailSentUnits({ ...base })).rejects.toThrow(UnitsQueryError);
    expect(mockDbExecute).not.toHaveBeenCalled();
  });

  it("pages on limit + 1 and mints the cursor from the last Unit served", async () => {
    mockDbExecute.mockResolvedValue(
      pgResult([
        row({ unitId: "ev-3", atText: "2026-10-03T07:00:00.000003" }),
        row({ unitId: "ev-2", atText: "2026-10-03T07:00:00.000002" }),
        row({ unitId: "ev-1", atText: "2026-10-01T07:00:00.000000" }),
      ]),
    );
    const page = await fetchEmailSentUnits({ ...base, emails: ["jane@acme.com"] });
    expect(page.units.map((u) => u.unitId)).toEqual(["ev-3", "ev-2"]);
    expect(decodeUnitCursor(page.nextCursor!)).toEqual({ at: "2026-10-03T07:00:00.000002", unitId: "ev-2" });

    await fetchEmailSentUnits({ ...base, emails: ["jane@acme.com"], cursor: page.nextCursor! });
    expect(sqlOfCall(1)).toContain("(u.at, u.unit_id) < (");

    mockDbExecute.mockResolvedValue(pgResult([row()]));
    expect((await fetchEmailSentUnits({ ...base, emails: ["jane@acme.com"] })).nextCursor).toBeNull();
  });
});

describe("GET /orgs/units/email-sent and /email-queued", () => {
  async function createApp() {
    const router = (await import("../../src/routes/provider-units")).default;
    const app = express();
    app.use((_req, res, next) => {
      res.locals.orgId = "org-1";
      next();
    });
    app.use(router);
    return app;
  }

  it("200 with the Units of email_sent", async () => {
    mockDbExecute.mockResolvedValue(pgResult([row(), row({ unitId: "answer:9", kind: "answer", step: null })]));
    const res = await request(await createApp())
      .get("/email-sent")
      .query({ emails: "Jane@Acme.com", since: "2026-08-23T00:00:00Z" });
    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({ success: true, outcome: "email_sent", count: 2, nextCursor: null });
    expect(res.body.units[0]).toMatchObject({ unitId: "ev-1", outcome: "email_sent", ledgerOutcome: "message_sent", email: "jane@acme.com" });
    expect(res.body.units[1]).toMatchObject({ kind: "answer", step: null, bounced: false });
  });

  it("200 with the Units of email_queued", async () => {
    mockDbExecute.mockResolvedValue(pgResult([row({ unitId: "hold-1", holdStatus: "provisioned", declaredOnLedger: true })]));
    const res = await request(await createApp()).get("/email-queued").query({ leadIds: "lead-1", states: "waiting" });
    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({ success: true, outcome: "email_queued", count: 1 });
    expect(res.body.units[0]).toMatchObject({ state: "waiting", declaredOnLedger: true, ledgerOutcome: "email_queued" });
  });

  it("200 with an empty units for someone we never wrote to (never a 404)", async () => {
    const app = await createApp();
    for (const path of ["/email-sent", "/email-queued"]) {
      const res = await request(app).get(path).query({ emails: "nobody@nowhere.test" });
      expect(res.status).toBe(200);
      expect(res.body).toMatchObject({ success: true, count: 0, units: [], nextCursor: null });
    }
  });

  it("accepts a repeated parameter", async () => {
    const res = await request(await createApp()).get("/email-sent?emails=a@b.c&emails=d@e.f");
    expect(res.status).toBe(200);
  });

  it("400 person_filter_required when nobody is named", async () => {
    const res = await request(await createApp()).get("/email-sent").query({ since: "2026-08-23T00:00:00Z" });
    expect(res.status).toBe(400);
    expect(res.body.error).toBe("person_filter_required");
    expect(mockDbExecute).not.toHaveBeenCalled();
  });

  it("400 on a forged cursor, an oversized limit, an unknown kind or state, a bad date", async () => {
    const app = await createApp();
    const q = { emails: "jane@acme.com" };
    const cursor = await request(app).get("/email-sent").query({ ...q, cursor: "zzz" });
    expect(cursor.status).toBe(400);
    expect(cursor.body.error).toBe("invalid_cursor");
    expect((await request(app).get("/email-sent").query({ ...q, limit: "1001" })).status).toBe(400);
    expect((await request(app).get("/email-sent").query({ ...q, kinds: "nudge" })).body.error).toBe("invalid_kinds");
    expect((await request(app).get("/email-queued").query({ ...q, kinds: "answer" })).status).toBe(400);
    expect((await request(app).get("/email-queued").query({ ...q, states: "pending" })).body.error).toBe("invalid_states");
    expect((await request(app).get("/email-sent").query({ ...q, since: "yesterday" })).body.error).toBe("invalid_since");
    expect(mockDbExecute).not.toHaveBeenCalled();
  });

  it("500 on a read failure instead of an empty page", async () => {
    mockDbExecute.mockRejectedValue(new Error("connection terminated"));
    const app = await createApp();
    for (const path of ["/email-sent", "/email-queued"]) {
      const res = await request(app).get(path).query({ emails: "jane@acme.com" });
      expect(res.status).toBe(500);
      expect(res.body.error).toContain("connection terminated");
    }
  });

  it("500 on a queue status outside the closed set (never a guessed state)", async () => {
    mockDbExecute.mockResolvedValue(pgResult([row({ holdStatus: "refunded" })]));
    const res = await request(await createApp()).get("/email-queued").query({ emails: "jane@acme.com" });
    expect(res.status).toBe(500);
    expect(res.body.error).toContain("unknown queue status");
  });
});

describe("the served OpenAPI: how workflow-service finds the reads", () => {
  const openapi = JSON.parse(fs.readFileSync(path.join(__dirname, "..", "..", "openapi.json"), "utf-8"));

  it("tags each operation `provider:<outcome>` with its operationId", () => {
    const sent = openapi.paths["/orgs/units/email-sent"].get;
    expect(sent.operationId).toBe("listEmailSentUnits");
    expect(sent.tags).toEqual(["provider:email_sent"]);
    const queued = openapi.paths["/orgs/units/email-queued"].get;
    expect(queued.operationId).toBe("listEmailQueuedUnits");
    expect(queued.tags).toEqual(["provider:email_queued"]);
  });

  it("documents every filter, and `states` on email-queued only", () => {
    const names = (p: string) =>
      (openapi.paths[p].get.parameters as Array<{ name: string; in: string; description?: string }>).filter((x) => x.in === "query");
    const sent = names("/orgs/units/email-sent");
    expect(sent.map((x) => x.name)).toEqual(["emails", "leadIds", "domains", "since", "until", "brandId", "campaignId", "kinds", "limit", "cursor"]);
    expect(sent.every((x) => (x.description ?? "").length > 0)).toBe(true);
    const queued = names("/orgs/units/email-queued");
    expect(queued.map((x) => x.name)).toContain("states");
    expect(queued.every((x) => (x.description ?? "").length > 0)).toBe(true);
  });
});
