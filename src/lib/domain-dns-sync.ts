/**
 * Domain DNS snapshot — the authentication records a domain actually serves.
 *
 * Deliverability starts at DNS and nothing in this service looked at it: the
 * only auth signal we held was per seed observation (`spf_pass` on a message),
 * which says whether a receiver accepted it, not what the domain publishes.
 * `domain_dns_raw` is an append-only daily photograph of SPF, DMARC, the DKIM
 * selectors we can find, and MX, for every domain we own or send from — so a
 * change in a record is a change in the series, not a mystery in the score.
 *
 * Bronze only. Pure parsing (`parseSpf`, `parseDmarc`, `summarizeDns`) is
 * what a read applies; the sync records what the resolver answered, including
 * the error when it did not.
 *
 * ⚠️ DKIM has no discovery mechanism — a selector is a name only the signer
 * knows. We probe a short list of the selectors our providers use; "no DKIM
 * found among probed selectors" is the honest reading of an empty result,
 * never "no DKIM".
 */

import dns from "node:dns/promises";
import { sql } from "drizzle-orm";
import { db } from "../db";
import { domainDnsRaw } from "../db/schema";

export type DnsRecordType = "spf" | "dmarc" | "dkim" | "mx";

/** Selectors probed for DKIM, in order. Google Workspace, Gandi, Postmark-style names, common defaults. */
export const DKIM_SELECTORS = [
  "google",
  "gm1",
  "gm2",
  "default",
  "mail",
  "k1",
  "k2",
  "s1",
  "s2",
  "selector1",
  "selector2",
  "dkim",
  "mailforge",
  "pf",
] as const;

export interface DnsRecordRow {
  domain: string;
  recordType: DnsRecordType;
  /** The DKIM selector; null for the other types. */
  selector: string | null;
  /** The name queried. */
  name: string;
  /** TXT strings (joined per record) or MX `priority exchange` strings. Empty when absent. */
  values: string[];
  /** Resolver error code when the lookup itself failed (not NXDATA/NXDOMAIN, which are an empty `values`). */
  error: string | null;
}

// ─── Pure parsers ────────────────────────────────────────────────────────────

export interface SpfSummary {
  present: boolean;
  /** -all | ~all | ?all | +all | null when absent or unqualified. */
  allQualifier: string | null;
  includes: string[];
  raw: string | null;
}

export function parseSpf(txtRecords: string[]): SpfSummary {
  const raw = txtRecords.find((t) => /^v=spf1\b/i.test(t.trim())) ?? null;
  if (!raw) return { present: false, allQualifier: null, includes: [], raw: null };
  const terms = raw.trim().split(/\s+/).slice(1);
  const all = terms.find((t) => /^[+\-~?]?all$/i.test(t));
  const allQualifier = all ? (/^[+\-~?]/.test(all) ? all.slice(0, 1) + "all" : "+all") : null;
  const includes = terms
    .filter((t) => /^include:/i.test(t))
    .map((t) => t.slice("include:".length));
  return { present: true, allQualifier, includes, raw };
}

export interface DmarcSummary {
  present: boolean;
  /** none | quarantine | reject | null. */
  policy: string | null;
  subdomainPolicy: string | null;
  pct: number | null;
  rua: string[];
  raw: string | null;
}

export function parseDmarc(txtRecords: string[]): DmarcSummary {
  const raw = txtRecords.find((t) => /^v=DMARC1\b/i.test(t.trim())) ?? null;
  if (!raw) return { present: false, policy: null, subdomainPolicy: null, pct: null, rua: [], raw: null };
  const tags = new Map<string, string>();
  for (const part of raw.split(";")) {
    const [k, ...rest] = part.split("=");
    if (!k || rest.length === 0) continue;
    tags.set(k.trim().toLowerCase(), rest.join("=").trim());
  }
  const pctRaw = tags.get("pct");
  const pct = pctRaw !== undefined && /^\d+$/.test(pctRaw) ? Number(pctRaw) : null;
  return {
    present: true,
    policy: tags.get("p") ?? null,
    subdomainPolicy: tags.get("sp") ?? null,
    pct,
    rua: (tags.get("rua") ?? "")
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean),
    raw,
  };
}

export interface DnsSummary {
  spf: SpfSummary;
  dmarc: DmarcSummary;
  /** Selectors that answered with a DKIM key. */
  dkimSelectors: string[];
  mx: string[];
  /** Record types whose lookup errored (as opposed to answering empty). */
  errors: Partial<Record<DnsRecordType, string>>;
}

/** Fold one domain's latest rows into what a read shows. */
export function summarizeDns(rows: DnsRecordRow[]): DnsSummary {
  const errors: Partial<Record<DnsRecordType, string>> = {};
  const spfRows = rows.filter((r) => r.recordType === "spf");
  const dmarcRows = rows.filter((r) => r.recordType === "dmarc");
  const dkimRows = rows.filter((r) => r.recordType === "dkim");
  const mxRows = rows.filter((r) => r.recordType === "mx");
  for (const r of rows) if (r.error && !errors[r.recordType]) errors[r.recordType] = r.error;
  return {
    spf: parseSpf(spfRows.flatMap((r) => r.values)),
    dmarc: parseDmarc(dmarcRows.flatMap((r) => r.values)),
    dkimSelectors: dkimRows
      .filter((r) => r.values.some((v) => /(^|;)\s*v=DKIM1\b|(^|;)\s*p=/i.test(v)))
      .map((r) => r.selector ?? "")
      .filter(Boolean)
      .sort(),
    mx: mxRows.flatMap((r) => r.values).sort(),
    errors,
  };
}

// ─── Resolver ────────────────────────────────────────────────────────────────

type Resolver = {
  resolveTxt: (name: string) => Promise<string[][]>;
  resolveMx: (name: string) => Promise<Array<{ priority: number; exchange: string }>>;
};

function isAbsence(error: unknown): boolean {
  const code = (error as { code?: string })?.code;
  return code === "ENOTFOUND" || code === "ENODATA";
}

function errorCode(error: unknown): string {
  const code = (error as { code?: string })?.code;
  return code ?? (error instanceof Error ? error.message : String(error));
}

async function txt(resolver: Resolver, name: string): Promise<{ values: string[]; error: string | null }> {
  try {
    const answer = await resolver.resolveTxt(name);
    return { values: answer.map((chunks) => chunks.join("")), error: null };
  } catch (error) {
    if (isAbsence(error)) return { values: [], error: null };
    return { values: [], error: errorCode(error) };
  }
}

/** Every record we photograph for one domain. */
export async function resolveDomainRecords(
  domain: string,
  resolver: Resolver = dns,
): Promise<DnsRecordRow[]> {
  const rows: DnsRecordRow[] = [];

  const spf = await txt(resolver, domain);
  rows.push({ domain, recordType: "spf", selector: null, name: domain, values: spf.values.filter((v) => /^v=spf1\b/i.test(v)), error: spf.error });

  const dmarcName = `_dmarc.${domain}`;
  const dmarc = await txt(resolver, dmarcName);
  rows.push({ domain, recordType: "dmarc", selector: null, name: dmarcName, values: dmarc.values, error: dmarc.error });

  for (const selector of DKIM_SELECTORS) {
    const name = `${selector}._domainkey.${domain}`;
    const answer = await txt(resolver, name);
    if (answer.values.length === 0 && answer.error === null) continue; // absent selectors are not recorded
    rows.push({ domain, recordType: "dkim", selector, name, values: answer.values, error: answer.error });
  }

  try {
    const mx = await resolver.resolveMx(domain);
    rows.push({
      domain,
      recordType: "mx",
      selector: null,
      name: domain,
      values: mx.sort((a, b) => a.priority - b.priority).map((m) => `${m.priority} ${m.exchange}`),
      error: null,
    });
  } catch (error) {
    rows.push({ domain, recordType: "mx", selector: null, name: domain, values: [], error: isAbsence(error) ? null : errorCode(error) });
  }

  return rows;
}

// ─── Sync ────────────────────────────────────────────────────────────────────

function rowsOf<T>(result: unknown): T[] {
  if (Array.isArray(result)) return result as T[];
  const rows = (result as { rows?: unknown })?.rows;
  return Array.isArray(rows) ? (rows as T[]) : [];
}

export interface DnsSyncSummary {
  domains: number;
  records: number;
  /** Domains where at least one lookup errored (recorded, not skipped). */
  domainsWithErrors: number;
}

export const DNS_SYNC_CONCURRENCY = 8;

/** Every domain we own (infra_domains, live) plus every domain we send from (instantly_accounts). */
export async function loadDomainsToPhotograph(): Promise<string[]> {
  const result = await db.execute(sql`
    SELECT DISTINCT lower(domain) AS domain FROM infra_domains WHERE absent_since IS NULL
    UNION
    SELECT DISTINCT lower(split_part(email, '@', 2)) FROM instantly_accounts WHERE absent_since IS NULL
    ORDER BY 1
  `);
  return rowsOf<{ domain: string }>(result).map((r) => r.domain).filter(Boolean);
}

export async function syncDomainDns(resolver: Resolver = dns): Promise<DnsSyncSummary> {
  const domains = await loadDomainsToPhotograph();
  const fetchedAt = new Date();
  const summary: DnsSyncSummary = { domains: domains.length, records: 0, domainsWithErrors: 0 };

  let cursor = 0;
  async function worker(): Promise<void> {
    while (cursor < domains.length) {
      const domain = domains[cursor++];
      const rows = await resolveDomainRecords(domain, resolver);
      if (rows.some((r) => r.error)) summary.domainsWithErrors += 1;
      await db.insert(domainDnsRaw).values(
        rows.map((r) => ({
          domain: r.domain,
          recordType: r.recordType,
          selector: r.selector,
          name: r.name,
          values: r.values,
          error: r.error,
          fetchedAt,
        })),
      );
      summary.records += rows.length;
    }
  }
  await Promise.all(Array.from({ length: Math.min(DNS_SYNC_CONCURRENCY, domains.length) }, worker));

  console.log(`[instantly-service] dns-sync: ${JSON.stringify(summary)}`);
  return summary;
}
