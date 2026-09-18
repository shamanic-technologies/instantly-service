import { describe, it, expect, vi, beforeEach } from "vitest";

const mockExecute = vi.fn();
const mockInsertValues = vi.fn(async () => undefined);
vi.mock("../../src/db", () => ({
  db: {
    execute: (...a: unknown[]) => mockExecute(...a),
    insert: () => ({ values: (...a: unknown[]) => mockInsertValues(...a) }),
  },
}));

import {
  DKIM_SELECTORS,
  parseDmarc,
  parseSpf,
  resolveDomainRecords,
  summarizeDns,
  syncDomainDns,
  type DnsRecordRow,
} from "../../src/lib/domain-dns-sync";

function pgResult(rows: Record<string, unknown>[]) {
  return { command: "SELECT", rowCount: rows.length, oid: 0, fields: [], rows };
}

function notFound(): Error & { code: string } {
  return Object.assign(new Error("queryTxt ENOTFOUND"), { code: "ENOTFOUND" });
}

/** A resolver that answers from a map; anything else is absent. */
function fakeResolver(txt: Record<string, string[][]>, mx: Array<{ priority: number; exchange: string }> = []) {
  return {
    resolveTxt: vi.fn(async (name: string) => {
      if (name in txt) return txt[name];
      throw notFound();
    }),
    resolveMx: vi.fn(async () => mx),
  };
}

describe("parseSpf / parseDmarc — what the domain publishes", () => {
  it("reads the all-qualifier and includes, and ignores unrelated TXT", () => {
    const spf = parseSpf(["google-site-verification=abc", "v=spf1 include:_spf.google.com include:spf.mtasv.net ~all"]);
    expect(spf.present).toBe(true);
    expect(spf.allQualifier).toBe("~all");
    expect(spf.includes).toEqual(["_spf.google.com", "spf.mtasv.net"]);
  });

  it("absent SPF is present:false with no qualifier — never a guess", () => {
    expect(parseSpf(["something=else"])).toEqual({ present: false, allQualifier: null, includes: [], raw: null });
  });

  it("reads DMARC policy, subdomain policy, pct and every rua", () => {
    const d = parseDmarc(["v=DMARC1; p=none; sp=none; pct=100; rua=mailto:dmarc@distribute.you, mailto:x@y.z; fo=1"]);
    expect(d.policy).toBe("none");
    expect(d.subdomainPolicy).toBe("none");
    expect(d.pct).toBe(100);
    expect(d.rua).toEqual(["mailto:dmarc@distribute.you", "mailto:x@y.z"]);
  });

  it("a DMARC record with no pct reports null pct, not 100", () => {
    expect(parseDmarc(["v=DMARC1; p=reject"]).pct).toBeNull();
  });
});

describe("resolveDomainRecords — the photograph", () => {
  it("records SPF, DMARC, ONLY the DKIM selectors that answered, and MX in priority order", async () => {
    const resolver = fakeResolver(
      {
        "distribute.you": [["v=spf1 include:_spf.google.com ~all"], ["google-site-verification=1"]],
        "_dmarc.distribute.you": [["v=DMARC1; p=none"]],
        "google._domainkey.distribute.you": [["v=DKIM1; k=rsa; p=MIIB"]],
      },
      [{ priority: 20, exchange: "alt.mx" }, { priority: 10, exchange: "primary.mx" }],
    );
    const rows = await resolveDomainRecords("distribute.you", resolver);
    const byType = Object.fromEntries(rows.map((r) => [`${r.recordType}:${r.selector ?? ""}`, r]));
    expect(byType["spf:"].values).toEqual(["v=spf1 include:_spf.google.com ~all"]);
    expect(byType["dmarc:"].values).toEqual(["v=DMARC1; p=none"]);
    expect(rows.filter((r) => r.recordType === "dkim").map((r) => r.selector)).toEqual(["google"]);
    expect(byType["mx:"].values).toEqual(["10 primary.mx", "20 alt.mx"]);
    // Every probed selector was asked, so absence is an answer, not an omission.
    expect(resolver.resolveTxt).toHaveBeenCalledTimes(2 + DKIM_SELECTORS.length);
  });

  it("a lookup that ERRORS is recorded with its code; an absent name is an empty answer with no error", async () => {
    const resolver = {
      resolveTxt: vi.fn(async (name: string) => {
        if (name === "x.io") throw Object.assign(new Error("timeout"), { code: "ETIMEOUT" });
        throw notFound();
      }),
      resolveMx: vi.fn(async () => {
        throw notFound();
      }),
    };
    const rows = await resolveDomainRecords("x.io", resolver);
    const spf = rows.find((r) => r.recordType === "spf")!;
    const dmarc = rows.find((r) => r.recordType === "dmarc")!;
    const mx = rows.find((r) => r.recordType === "mx")!;
    expect(spf.error).toBe("ETIMEOUT");
    expect(dmarc).toMatchObject({ values: [], error: null });
    expect(mx).toMatchObject({ values: [], error: null });
  });
});

describe("summarizeDns — what a read shows", () => {
  it("folds rows into spf / dmarc / dkim selectors / mx / errors", () => {
    const rows: DnsRecordRow[] = [
      { domain: "d", recordType: "spf", selector: null, name: "d", values: ["v=spf1 -all"], error: null },
      { domain: "d", recordType: "dmarc", selector: null, name: "_dmarc.d", values: [], error: "ETIMEOUT" },
      { domain: "d", recordType: "dkim", selector: "gm1", name: "gm1._domainkey.d", values: ["v=DKIM1; p=abc"], error: null },
      { domain: "d", recordType: "dkim", selector: "k1", name: "k1._domainkey.d", values: ["not a key"], error: null },
      { domain: "d", recordType: "mx", selector: null, name: "d", values: ["10 mx.d"], error: null },
    ];
    const s = summarizeDns(rows);
    expect(s.spf.allQualifier).toBe("-all");
    expect(s.dmarc.present).toBe(false);
    expect(s.errors.dmarc).toBe("ETIMEOUT");
    expect(s.dkimSelectors).toEqual(["gm1"]);
    expect(s.mx).toEqual(["10 mx.d"]);
  });
});

describe("syncDomainDns — IO", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockInsertValues.mockResolvedValue(undefined);
  });

  it("photographs every owned + sending domain and inserts one batch per domain", async () => {
    mockExecute.mockResolvedValueOnce(pgResult([{ domain: "a.com" }, { domain: "b.com" }]));
    const resolver = fakeResolver({ "a.com": [["v=spf1 ~all"]] });
    const summary = await syncDomainDns(resolver);
    expect(summary).toEqual({ domains: 2, records: 6, domainsWithErrors: 0 });
    expect(mockInsertValues).toHaveBeenCalledTimes(2);
    const inserted = (mockInsertValues.mock.calls[0][0] as Array<{ domain: string; recordType: string }>);
    expect(inserted.every((r) => r.domain === "a.com" || r.domain === "b.com")).toBe(true);
  });

  it("reads the domain list through the QueryResult shape and unions owned with sending domains", async () => {
    mockExecute.mockResolvedValueOnce(pgResult([]));
    await syncDomainDns(fakeResolver({}));
    const q = JSON.stringify((mockExecute.mock.calls[0][0] as { queryChunks?: unknown[] }).queryChunks);
    expect(q).toContain("FROM infra_domains");
    expect(q).toContain("split_part(email, '@', 2)");
    expect(q).toContain("UNION");
  });
});
