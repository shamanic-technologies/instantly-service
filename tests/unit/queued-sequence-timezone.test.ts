import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const mockExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: { execute: (...args: unknown[]) => mockExecute(...args) },
}));

process.env.LEAD_SERVICE_URL = "http://lead-service";
process.env.LEAD_SERVICE_API_KEY = "lead-key";

const {
  healQueuedSequenceTimezones,
  resetTimezoneHealMemory,
  RECHECK_AFTER_MS,
  isUsableZone,
} = await import("../../src/lib/queued-sequence-timezone");
const { findLeadTimezoneOnCampaignByEmail } = await import("../../src/lib/lead-client");

/** node-postgres shape. */
function pgResult(rows: Record<string, unknown>[]) {
  return { rows, rowCount: rows.length };
}

function sqlText(call: unknown[]): string {
  const q = call[0] as { queryChunks?: Array<{ value?: string[] } | unknown> };
  return (q.queryChunks ?? [])
    .map((c) => (c && typeof c === "object" && "value" in (c as object) ? ((c as { value: string[] }).value ?? []).join("") : ""))
    .join("");
}

// Prod 2026-10-14: queued with no zone, lead-service holds one.
const UK = { instantlyCampaignId: "self:uk", orgId: "org-1", campaignId: "camp-1", leadEmail: "sam@carter.co.uk" };
const UNKNOWN = { instantlyCampaignId: "self:unknown", orgId: "org-1", campaignId: "camp-1", leadEmail: "who@example.com" };

/** lead-service `GET /orgs/leads` (full view) serving `zoneByEmail`. */
function serveLeads(zoneByEmail: Record<string, string | null>) {
  const fetchMock = vi.fn(async (url: string) => {
    const q = new URL(url).searchParams.get("q") ?? "";
    if (!(q in zoneByEmail)) return new Response(JSON.stringify({ leads: [] }), { status: 200 });
    return new Response(
      JSON.stringify({ leads: [{ email: q.toUpperCase(), lead: { timezone: zoneByEmail[q] } }] }),
      { status: 200 },
    );
  });
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

function primeQueue(rows: Array<Record<string, unknown>>) {
  mockExecute.mockImplementation(async (query: unknown) => {
    const text = sqlText([query]);
    if (text.includes("SELECT")) return pgResult(rows);
    return pgResult([]);
  });
}

function updates(): string[] {
  return mockExecute.mock.calls.filter((c) => sqlText(c).includes("UPDATE")).map((c) => JSON.stringify(c[0]));
}

beforeEach(() => {
  vi.resetAllMocks();
  resetTimezoneHealMemory();
});
afterEach(() => vi.unstubAllGlobals());

describe("findLeadTimezoneOnCampaignByEmail", () => {
  it("reads the FULL view (the slim one carries no zone) and matches the address exactly", async () => {
    const fetchMock = serveLeads({ "sam@carter.co.uk": "Europe/London" });
    const zone = await findLeadTimezoneOnCampaignByEmail({ orgId: "org-1", campaignId: "camp-1", email: "sam@carter.co.uk" });
    expect(zone).toBe("Europe/London");
    const url = new URL(fetchMock.mock.calls[0]![0] as string);
    expect(url.searchParams.get("view")).toBeNull();
    expect(url.searchParams.get("campaignId")).toBe("camp-1");
  });

  it("null when the address resolves to nobody", async () => {
    serveLeads({});
    expect(await findLeadTimezoneOnCampaignByEmail({ orgId: "o", campaignId: "c", email: "x@y.z" })).toBeNull();
  });

  it("fails loud on a non-2xx", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => new Response("boom", { status: 503 })));
    await expect(
      findLeadTimezoneOnCampaignByEmail({ orgId: "o", campaignId: "c", email: "x@y.z" }),
    ).rejects.toThrow(/503/);
  });
});

describe("healQueuedSequenceTimezones", () => {
  it("persists the person's zone onto a zoneless queued sequence, never over an existing one", async () => {
    primeQueue([UK]);
    serveLeads({ "sam@carter.co.uk": "Europe/London" });

    const summary = await healQueuedSequenceTimezones(new Date("2026-10-14T06:00:00Z"));

    expect(summary).toEqual({ candidates: 1, resolved: 1, unknown: 0, failed: 0 });
    const writes = mockExecute.mock.calls.filter((c) => sqlText(c).includes("UPDATE"));
    expect(writes).toHaveLength(1);
    expect(sqlText(writes[0]!)).toContain("timezone IS NULL");
    expect(JSON.stringify(writes[0]![0])).toContain("Europe/London");
    expect(JSON.stringify(writes[0]![0])).toContain("self:uk");
  });

  it("only reads ACTIVE self-send sequences with a provisioned step and no zone", async () => {
    primeQueue([]);
    serveLeads({});
    await healQueuedSequenceTimezones();
    const select = sqlText(mockExecute.mock.calls[0]!);
    expect(select).toContain("c.timezone IS NULL");
    expect(select).toContain("c.status = 'active'");
    expect(select).toContain("LIKE 'self:%'");
    expect(select).toContain("s.status = 'provisioned'");
  });

  it("asks again about an unknown person only after the recheck delay", async () => {
    primeQueue([UNKNOWN]);
    const fetchMock = serveLeads({ "who@example.com": null });
    const t0 = new Date("2026-10-14T06:00:00Z");

    expect((await healQueuedSequenceTimezones(t0)).unknown).toBe(1);
    expect((await healQueuedSequenceTimezones(new Date(t0.getTime() + 60_000))).candidates).toBe(0);
    expect((await healQueuedSequenceTimezones(new Date(t0.getTime() + RECHECK_AFTER_MS))).candidates).toBe(1);
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(updates()).toHaveLength(0);
  });

  it("an unusable zone is never persisted", async () => {
    primeQueue([UK]);
    serveLeads({ "sam@carter.co.uk": "GMT+1" });
    const summary = await healQueuedSequenceTimezones();
    expect(summary.unknown).toBe(1);
    expect(updates()).toHaveLength(0);
  });

  it("one unreadable person does not stop the others", async () => {
    primeQueue([UNKNOWN, UK]);
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string) => {
        const q = new URL(url).searchParams.get("q");
        if (q === "who@example.com") return new Response("boom", { status: 503 });
        return new Response(JSON.stringify({ leads: [{ email: q, lead: { timezone: "Europe/London" } }] }), { status: 200 });
      }),
    );
    const summary = await healQueuedSequenceTimezones();
    expect(summary).toEqual({ candidates: 2, resolved: 1, unknown: 0, failed: 1 });
  });

  it("isUsableZone", () => {
    expect(isUsableZone("Europe/London")).toBe(true);
    expect(isUsableZone("GMT+1")).toBe(false);
  });
});
