/**
 * SOURCE-YIELD-1016: which lead source feeds our domains the dead addresses.
 * The join of first emails to lead-service's source tags, and the counting rule
 * (a multi-tag person counts once under EACH tag; 502 / no answer = unknown).
 */
import { describe, it, expect, vi, beforeEach } from "vitest";

const execute = vi.fn();
vi.mock("../../src/db", () => ({ db: { execute: (...a: unknown[]) => execute(...a) } }));

import {
  NO_SOURCE_KEY,
  UNKNOWN_KEY,
  aggregateSourceYield,
  keysForSequence,
  provenanceKey,
  type YieldSequence,
} from "../../src/lib/lead-source-yield";
import { planProvenanceBatches, runPass, type ProvenanceFetcher } from "../../src/lib/lead-provenance-sync";

const ORG = "org-1";
const BRAND = "brand-1";

function seq(email: string, over: Partial<YieldSequence> = {}): YieldSequence {
  return {
    orgId: ORG,
    brandIds: [BRAND],
    email,
    day: "2026-10-15",
    domain: "send.example",
    hardBounced: false,
    refused: false,
    refusedBlocklist: false,
    ...over,
  };
}

const pgResult = (rows: unknown[]) => ({ rows, rowCount: rows.length });

describe("joining a first email to its sources", () => {
  const provenance = new Map<string, string[]>([
    [provenanceKey(ORG, BRAND, "multi@x.com"), ["sourcing-apollo-buying-signals", "sourcing-apollo-cold-filters"]],
    [provenanceKey(ORG, BRAND, "cold@x.com"), ["sourcing-apollo-cold-filters"]],
    [provenanceKey(ORG, BRAND, "none@x.com"), []],
  ]);

  it("a person carrying two tags counts under BOTH", () => {
    expect(keysForSequence(seq("multi@x.com"), provenance)).toEqual([
      "sourcing-apollo-buying-signals",
      "sourcing-apollo-cold-filters",
    ]);
  });

  it("an answer naming no source is `no source recorded`, never unknown", () => {
    expect(keysForSequence(seq("none@x.com"), provenance)).toEqual([NO_SOURCE_KEY]);
  });

  it("no answer held (never fetched, or lead-service answered 502) is unknown, never `no source`", () => {
    expect(keysForSequence(seq("never@x.com"), provenance)).toEqual([UNKNOWN_KEY]);
  });

  it("a sequence with no org or no brand cannot be asked about: unknown", () => {
    expect(keysForSequence(seq("cold@x.com", { orgId: null }), provenance)).toEqual([UNKNOWN_KEY]);
    expect(keysForSequence(seq("cold@x.com", { brandIds: [] }), provenance)).toEqual([UNKNOWN_KEY]);
  });

  it("a multi-brand sequence takes the union of every brand's answer", () => {
    const p = new Map([
      [provenanceKey(ORG, "b1", "a@x.com"), ["crm_signup"]],
      [provenanceKey(ORG, "b2", "a@x.com"), ["sourcing-apollo-cold-filters"]],
    ]);
    expect(keysForSequence(seq("a@x.com", { brandIds: ["b1", "b2", "b3"] }), p)).toEqual([
      "crm_signup",
      "sourcing-apollo-cold-filters",
    ]);
  });

  it("counts per tag at full weight; `total` counts each first email once", () => {
    const agg = aggregateSourceYield(
      [
        seq("multi@x.com", { hardBounced: true, refused: true, refusedBlocklist: true, domain: "listed.example" }),
        seq("cold@x.com"),
        seq("none@x.com", { hardBounced: true }),
        seq("never@x.com"),
      ],
      provenance,
      new Set(["listed.example"]),
      ["2026-10-14", "2026-10-15"],
    );
    expect(agg.total).toMatchObject({ firstEmails: 4, hardBounces: 2, refusals: 1, blocklistRefusals: 1, firstEmailsFromListedDomains: 1 });
    const byKey = Object.fromEntries(agg.sources.map((s) => [s.key, s]));
    expect(byKey["sourcing-apollo-cold-filters"]).toMatchObject({ firstEmails: 2, hardBounces: 1, hardBounceRate: 0.5, blocklistRefusals: 1, firstEmailsFromListedDomains: 1 });
    expect(byKey["sourcing-apollo-buying-signals"]).toMatchObject({ firstEmails: 1, hardBounces: 1, hardBounceRate: 1, kind: "source" });
    expect(byKey[NO_SOURCE_KEY]).toMatchObject({ firstEmails: 1, hardBounces: 1, kind: "no_source_recorded" });
    expect(byKey[UNKNOWN_KEY]).toMatchObject({ firstEmails: 1, kind: "unknown" });
    // sources ranked by hard-bounce rate, pseudo keys last
    expect(agg.sources.map((s) => s.key)).toEqual([
      "sourcing-apollo-buying-signals",
      "sourcing-apollo-cold-filters",
      NO_SOURCE_KEY,
      UNKNOWN_KEY,
    ]);
    expect(agg.coverage).toEqual({ firstEmails: 4, withSource: 2, noSourceRecorded: 1, unknown: 1 });
  });

  it("the daily series has every day and every key, zero included, on the first email's day", () => {
    const agg = aggregateSourceYield(
      [seq("cold@x.com", { day: "2026-10-15", hardBounced: true }), seq("cold@x.com", { day: "2026-01-01" })],
      provenance,
      new Set(),
      ["2026-10-14", "2026-10-15"],
    );
    expect(agg.series.days).toEqual(["2026-10-14", "2026-10-15"]);
    expect(agg.series.hardBounces).toEqual([
      { day: "2026-10-14", total: 0, byKey: { "sourcing-apollo-cold-filters": 0 } },
      { day: "2026-10-15", total: 1, byKey: { "sourcing-apollo-cold-filters": 1 } },
    ]);
    // since inception still counts the January email
    expect(agg.sources[0]).toMatchObject({ key: "sourcing-apollo-cold-filters", firstEmails: 2 });
  });
});

describe("copying lead-service's provenance", () => {
  beforeEach(() => execute.mockReset());

  it("batches by (org, brand), at most 1000 addresses a call, never-asked people first", () => {
    const missing = Array.from({ length: 2500 }, (_, i) => ({ orgId: ORG, brandId: BRAND, email: `p${i}@x.com` }));
    const stale = [{ orgId: ORG, brandId: "b2", email: "s@x.com" }];
    const batches = planProvenanceBatches(missing, stale);
    expect(batches.map((b) => [b.brandId, b.emails.length])).toEqual([
      [BRAND, 1000],
      [BRAND, 1000],
      [BRAND, 500],
      ["b2", 1],
    ]);
    expect(planProvenanceBatches(missing, stale, 1000, 2)).toHaveLength(2);
  });

  it("a 502 stores NOTHING: those people stay unknown and are asked again next pass", async () => {
    execute.mockResolvedValueOnce(
      pgResult([{ orgId: ORG, brandId: BRAND, email: "a@x.com", state: "missing" }]),
    );
    const fetcher: ProvenanceFetcher = vi.fn(async () => ({
      ok: false as const,
      status: 502,
      unavailable: true,
      error: '{"code":"provenance_source_unavailable"}',
    }));
    const out = await runPass({ fetcher });
    expect(fetcher).toHaveBeenCalledWith(ORG, BRAND, ["a@x.com"]);
    expect(out).toMatchObject({ batches: 1, asked: 1, unavailableBatches: 1, inserted: 0, confirmed: 0 });
    // only the target read ran: no insert, no update
    expect(execute).toHaveBeenCalledTimes(1);
  });

  it("an answer is stored with its tags; an identical re-read only confirms it", async () => {
    const answers: Record<string, unknown> = {
      "a@x.com": { email: "A@x.com", tags: ["sourcing-apollo-cold-filters"], sources: [], filings: [] },
      "b@x.com": { email: "b@x.com", tags: [], sources: [], filings: [] },
    };
    execute
      .mockResolvedValueOnce(pgResult([
        { orgId: ORG, brandId: BRAND, email: "a@x.com", state: "missing" },
        { orgId: ORG, brandId: BRAND, email: "b@x.com", state: "stale" },
      ]))
      // batch 1 (never asked: a): nothing stored yet -> INSERT
      .mockResolvedValueOnce(pgResult([]))
      .mockResolvedValueOnce(pgResult([]))
      // batch 2 (stale: b): same answer stored (other key order) -> UPDATE confirmed_at only
      .mockResolvedValueOnce(pgResult([
        { id: "row-b", email: "b@x.com", payload: { filings: [], sources: [], tags: [], email: "b@x.com" } },
      ]))
      .mockResolvedValueOnce(pgResult([]));
    const fetcher: ProvenanceFetcher = async (_o, _b, emails) => ({
      ok: true,
      people: emails.map((e) => answers[e] as never),
    });
    const out = await runPass({ fetcher });
    expect(out).toMatchObject({ batches: 2, inserted: 1, confirmed: 1, unavailableBatches: 0, failedBatches: 0 });
    const statements = execute.mock.calls.map((c) => JSON.stringify(c[0]));
    expect(statements[2]).toContain("INSERT INTO lead_provenance_raw");
    expect(statements[2]).toContain("sourcing-apollo-cold-filters");
    expect(statements[4]).toContain("UPDATE lead_provenance_raw SET confirmed_at");
    expect(execute).toHaveBeenCalledTimes(5);
  });
});
