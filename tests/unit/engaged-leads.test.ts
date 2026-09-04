import { describe, it, expect, vi, beforeEach } from "vitest";

const mockDbExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: { execute: (...a: unknown[]) => mockDbExecute(...a) },
}));

import { fetchEngagedLeads, toEngagedLead } from "../../src/lib/engaged-leads";

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
    leadEmail: "alice@media.com",
    brandIds: ["b-1"],
    engagedAt: new Date("2026-09-01T10:00:00.000Z"),
    replied: true,
    clicked: false,
    firstRepliedAt: new Date("2026-09-01T10:00:00.000Z"),
    firstClickedAt: null,
    replyClassification: "positive",
    replyKind: "lead_interested",
    ...over,
  };
}

beforeEach(() => {
  vi.resetAllMocks();
  mockDbExecute.mockResolvedValue(pgResult([]));
});

describe("engaged-leads — the gate", () => {
  it("keeps a lead who replied and did not unsubscribe, whatever the sentiment", async () => {
    mockDbExecute.mockResolvedValue(
      pgResult([
        goldRow({ replyClassification: "negative", replyKind: "lead_not_interested" }),
      ]),
    );

    const [lead] = await fetchEngagedLeads({ orgId: "org-1" });

    // A "no" is still a conversation someone should be able to read, and a
    // "not right now" is recyclable pipeline.
    expect(lead.replied).toBe(true);
    expect(lead.replyClassification).toBe("negative");
    expect(lead.disqualified).toBe(false);
  });

  it("keeps a lead who only clicked", async () => {
    mockDbExecute.mockResolvedValue(
      pgResult([
        goldRow({
          replied: false,
          clicked: true,
          firstRepliedAt: null,
          firstClickedAt: new Date("2026-09-02T08:00:00.000Z"),
          engagedAt: new Date("2026-09-02T08:00:00.000Z"),
          replyClassification: null,
          replyKind: null,
        }),
      ]),
    );

    const [lead] = await fetchEngagedLeads({ orgId: "org-1" });

    expect(lead.clicked).toBe(true);
    expect(lead.replied).toBe(false);
    expect(lead.firstRepliedAt).toBeNull();
    expect(lead.engagedAt).toBe("2026-09-02T08:00:00.000Z");
  });

  it("excludes an unsubscribed replier and a never-engaged lead in SQL, not in JS", async () => {
    await fetchEngagedLeads({ orgId: "org-1" });
    const sqlText = extractSqlText(mockDbExecute.mock.calls[0][0]);

    // The gate is a WHERE clause: a lead who asked to stop, and a lead who only
    // ever received mail, never reach the mapper at all.
    expect(sqlText).toContain("(replied AND NOT unsubscribed) OR clicked");
  });

  it("needs no autoresponder exclusion — an auto-reply never sets `replied`", async () => {
    await fetchEngagedLeads({ orgId: "org-1" });
    const sqlText = extractSqlText(mockDbExecute.mock.calls[0][0]);

    // Guards the reasoning, not just the output: if someone ever makes
    // `auto_reply_received` set `replied`, this gate silently starts surfacing
    // out-of-office notices as sales interest.
    expect(sqlText).not.toContain("auto_reply_received");
    expect(sqlText).not.toContain("lead_out_of_office");
  });
});

describe("engaged-leads — engagedAt", () => {
  it("is the EARLIEST of the two signals when a lead clicked then replied", async () => {
    mockDbExecute.mockResolvedValue(
      pgResult([
        goldRow({
          clicked: true,
          firstClickedAt: new Date("2026-08-25T09:00:00.000Z"),
          firstRepliedAt: new Date("2026-08-29T15:00:00.000Z"),
          engagedAt: new Date("2026-08-25T09:00:00.000Z"),
        }),
      ]),
    );

    const [lead] = await fetchEngagedLeads({ orgId: "org-1" });

    expect(lead.engagedAt).toBe("2026-08-25T09:00:00.000Z");
    // Both signals stay visible and separately timestamped, so nothing has to
    // be collapsed into a single label that would contradict this instant.
    expect(lead.clicked).toBe(true);
    expect(lead.replied).toBe(true);
    expect(lead.firstClickedAt).toBe("2026-08-25T09:00:00.000Z");
    expect(lead.firstRepliedAt).toBe("2026-08-29T15:00:00.000Z");
  });

  it("fails loud rather than emitting a lead with no engagement instant", () => {
    expect(() =>
      toEngagedLead(goldRow({ engagedAt: null }) as never),
    ).toThrow(/no engagement timestamp/);
  });
});

describe("engaged-leads — timestamps", () => {
  it("normalises a naive pg timestamp to ISO 8601 UTC", async () => {
    // node-postgres hands a `timestamp` (no time zone) column back as
    // "2026-09-03 13:21:37.397". `new Date()` reads that as LOCAL time, so a
    // consumer in any non-UTC zone would shift the instant by its own offset —
    // and POST /orgs/status already emits ISO for the same instants.
    mockDbExecute.mockResolvedValue(
      pgResult([
        goldRow({
          engagedAt: "2026-09-03 13:21:37.397",
          firstRepliedAt: "2026-09-03 13:21:37.397",
        }),
      ]),
    );

    const [lead] = await fetchEngagedLeads({ orgId: "org-1" });

    expect(lead.engagedAt).toBe("2026-09-03T13:21:37.397Z");
    expect(lead.firstRepliedAt).toBe("2026-09-03T13:21:37.397Z");
  });

  it("does not depend on the runtime's timezone", async () => {
    const spy = vi
      .spyOn(Date.prototype, "getTimezoneOffset")
      .mockReturnValue(-480); // UTC+8

    mockDbExecute.mockResolvedValue(
      pgResult([goldRow({ engagedAt: "2026-09-03 13:21:37.397" })]),
    );
    const [lead] = await fetchEngagedLeads({ orgId: "org-1" });

    expect(lead.engagedAt).toBe("2026-09-03T13:21:37.397Z");
    spy.mockRestore();
  });

  it("passes a Date straight through", async () => {
    mockDbExecute.mockResolvedValue(
      pgResult([goldRow({ engagedAt: new Date("2026-09-03T13:21:37.397Z") })]),
    );
    const [lead] = await fetchEngagedLeads({ orgId: "org-1" });
    expect(lead.engagedAt).toBe("2026-09-03T13:21:37.397Z");
  });

  it("fails loud on a timestamp it cannot parse", () => {
    // Emitting the raw string would put a value no consumer can parse into a
    // field typed as an instant.
    expect(() => toEngagedLead(goldRow({ engagedAt: "not a date" }) as never)).toThrow(
      /unparseable timestamp/,
    );
  });
});

describe("engaged-leads — disqualified", () => {
  it("is true only for a kind that is permanent about the PERSON", async () => {
    mockDbExecute.mockResolvedValue(
      pgResult([
        goldRow({ replyKind: "lead_changed_job", replyClassification: "negative" }),
      ]),
    );

    const [lead] = await fetchEngagedLeads({ orgId: "org-1" });
    expect(lead.disqualified).toBe(true);
  });

  it("is false when no reply kind is on record", async () => {
    mockDbExecute.mockResolvedValue(
      pgResult([
        goldRow({
          replied: false,
          clicked: true,
          firstRepliedAt: null,
          firstClickedAt: new Date("2026-09-02T08:00:00.000Z"),
          replyKind: null,
          replyClassification: null,
        }),
      ]),
    );

    const [lead] = await fetchEngagedLeads({ orgId: "org-1" });
    expect(lead.disqualified).toBe(false);
  });

  it("does not invent a verdict for a kind it does not recognise", async () => {
    mockDbExecute.mockResolvedValue(
      pgResult([goldRow({ replyKind: "something_nobody_defined" })]),
    );

    const [lead] = await fetchEngagedLeads({ orgId: "org-1" });
    expect(lead.disqualified).toBe(false);
    expect(lead.replyKind).toBe("something_nobody_defined");
  });
});

describe("engaged-leads — query shape", () => {
  it("scopes to the caller's org and reads gold, not the event log", async () => {
    await fetchEngagedLeads({ orgId: "org-1" });
    const sqlText = extractSqlText(mockDbExecute.mock.calls[0][0]);

    expect(sqlText).toContain("org_id =");
    expect(sqlText).toContain("instantly_lead_status_current");
    expect(sqlText).not.toContain("instantly_events");
  });

  it("applies brand, campaign and since filters only when given", async () => {
    await fetchEngagedLeads({ orgId: "org-1" });
    const bare = extractSqlText(mockDbExecute.mock.calls[0][0]);
    expect(bare).not.toContain("brand_ids)");
    expect(bare).not.toContain("campaign_id =");

    await fetchEngagedLeads({
      orgId: "org-1",
      brandId: "b-1",
      campaignId: "camp-1",
      since: "2026-08-01T00:00:00.000Z",
    });
    const filtered = extractSqlText(mockDbExecute.mock.calls[1][0]);
    expect(filtered).toContain("ANY(brand_ids)");
    expect(filtered).toContain("campaign_id =");
    expect(filtered).toContain("::timestamp");
  });

  it("returns every engaged lead when no limit is given", async () => {
    await fetchEngagedLeads({ orgId: "org-1" });
    expect(extractSqlText(mockDbExecute.mock.calls[0][0])).not.toContain("LIMIT");

    await fetchEngagedLeads({ orgId: "org-1", limit: 10 });
    expect(extractSqlText(mockDbExecute.mock.calls[1][0])).toContain("LIMIT");
  });

  it("orders most recently engaged first", async () => {
    await fetchEngagedLeads({ orgId: "org-1" });
    expect(extractSqlText(mockDbExecute.mock.calls[0][0])).toContain(
      'ORDER BY "engagedAt" DESC',
    );
  });
});

describe("engaged-leads — driver shape", () => {
  it("reads the QueryResult object node-postgres actually returns", async () => {
    // A postgres.js-shaped mock (a bare array) is what makes this class of bug
    // pass its own tests, so assert against the real driver shape.
    mockDbExecute.mockResolvedValue(pgResult([goldRow()]));
    await expect(fetchEngagedLeads({ orgId: "org-1" })).resolves.toHaveLength(1);
  });

  it("carries the identity GET /orgs/conversations takes", async () => {
    mockDbExecute.mockResolvedValue(pgResult([goldRow()]));
    const [lead] = await fetchEngagedLeads({ orgId: "org-1" });

    expect(lead.campaignId).toBe("camp-1");
    expect(lead.leadEmail).toBe("alice@media.com");
    // Present even on a platform send, where campaignId is null.
    expect(lead.instantlyCampaignId).toBe("ic-1");
  });

  it("keeps instantlyCampaignId when the caller campaign id is null", async () => {
    mockDbExecute.mockResolvedValue(pgResult([goldRow({ campaignId: null })]));
    const [lead] = await fetchEngagedLeads({ orgId: "org-1" });

    expect(lead.campaignId).toBeNull();
    expect(lead.instantlyCampaignId).toBe("ic-1");
  });
});
