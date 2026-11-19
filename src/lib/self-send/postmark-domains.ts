/**
 * Put our sending domains on Postmark (POSTMARK-COLD-1117, owner 2026-11-17:
 * « migre tout ce que tu peux sur Postmark »).
 *
 * Candidates = every Gandi domain of ours with a present address in production
 * or recovery, minus the brand/product domains of `instantly_domain_policy`
 * (distribute.you & co never send cold, and must never move to the cold server).
 * Primeforge domains wait for their transfer (they are not ours yet).
 *
 * Per domain, idempotent, one step further each pass:
 *   1. the domain exists on the Postmark ACCOUNT (key-service `postmark-account`),
 *      created with Return-Path `pm-bounces.<domain>` when absent;
 *   2. its DKIM TXT and Return-Path CNAME are written at Gandi LiveDNS (the org's
 *      token, `gandi-<provider_account>`). ONLY those two records: MX, SPF and
 *      every other record are never touched, so the mailboxes keep receiving;
 *   3. Postmark is asked to verify both; once both pass, `live_since` is set and
 *      the outbound door (`outbound.ts`) sends that domain's mail through Postmark.
 * The account's domain cap (ErrorCode 614) stops the pass and is stored on every
 * waiting domain as `domain_cap` (the owner's plan decides it); the next pass
 * retries alone.
 */

import { sql } from "drizzle-orm";

import { db } from "../../db";
import { errorMessage } from "../db-error.js";
import { resolvePlatformKey, type CallerInfo } from "../key-client";
import { clearLivePipesCache } from "./outbound";

export const POSTMARK_ACCOUNT_PROVIDER = "postmark-account";
export const POSTMARK_DOMAIN_CAP_ERROR = 614;

const CALLER: CallerInfo = { method: "POST", path: "/internal/postmark-sending/onboard" };

function rowsOf<T>(result: unknown): T[] {
  return (((result as { rows?: unknown[] }).rows ?? []) as T[]);
}

export interface DomainCandidate {
  domain: string;
  gandiOrg: string | null;
}

export async function loadOnboardingCandidates(only?: readonly string[]): Promise<DomainCandidate[]> {
  const rows = rowsOf<{ domain: string; org: string | null }>(
    await db.execute(sql`
      SELECT DISTINCT i.domain, i.provider_account AS org
      FROM infra_domains i
      JOIN instantly_accounts a
        ON lower(split_part(a.email, '@', 2)) = i.domain
       AND a.absent_since IS NULL
       AND a.lifecycle_status IN ('in_production', 'in_recovery')
      WHERE i.provider = 'gandi'
        AND i.absent_since IS NULL
        AND NOT EXISTS (SELECT 1 FROM instantly_domain_policy p WHERE p.domain = i.domain)
      ORDER BY i.domain
    `),
  );
  const wanted = only && only.length > 0 ? new Set(only.map((d) => d.toLowerCase())) : null;
  return rows
    .filter((r) => !wanted || wanted.has(r.domain))
    .map((r) => ({ domain: r.domain, gandiOrg: r.org }));
}

async function postmarkAccount(
  token: string,
  path: string,
  init: { method?: string; body?: unknown } = {},
): Promise<{ status: number; body: Record<string, unknown> }> {
  const response = await fetch(`https://api.postmarkapp.com${path}`, {
    method: init.method ?? "GET",
    headers: { Accept: "application/json", "Content-Type": "application/json", "X-Postmark-Account-Token": token },
    ...(init.body !== undefined ? { body: JSON.stringify(init.body) } : {}),
  });
  const text = await response.text();
  let body: Record<string, unknown>;
  try {
    body = text ? (JSON.parse(text) as Record<string, unknown>) : {};
  } catch {
    body = { Message: text };
  }
  return { status: response.status, body };
}

/** The record name of a DKIM host inside its zone (pure): `2026…pm._domainkey`. */
export function recordNameInZone(host: string, domain: string): string {
  const h = host.trim().toLowerCase().replace(/\.$/, "");
  const suffix = `.${domain.toLowerCase()}`;
  return h.endsWith(suffix) ? h.slice(0, -suffix.length) : h;
}

async function writeGandiRecord(
  token: string,
  domain: string,
  name: string,
  type: "TXT" | "CNAME",
  value: string,
): Promise<void> {
  const response = await fetch(
    `https://api.gandi.net/v5/livedns/domains/${domain}/records/${encodeURIComponent(name)}/${type}`,
    {
      method: "PUT",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify({ rrset_values: [value], rrset_ttl: 300 }),
    },
  );
  if (!response.ok) {
    throw new Error(`Gandi LiveDNS PUT ${name} ${type} on ${domain}: ${response.status} ${await response.text()}`);
  }
}

export interface OnboardingSummary {
  dryRun: boolean;
  candidates: number;
  live: string[];
  becameLive: string[];
  pendingVerification: string[];
  blockedByDomainCap: string[];
  failed: { domain: string; error: string }[];
}

async function upsertState(domain: string, fields: Record<string, unknown>): Promise<void> {
  const f = {
    postmarkDomainId: null as number | null,
    dkimVerified: false,
    returnPathVerified: false,
    dnsWritten: false,
    live: false,
    lastError: null as string | null,
    ...fields,
  };
  await db.execute(sql`
    INSERT INTO postmark_sending_domains
      (domain, postmark_domain_id, dkim_verified, return_path_verified, dns_written_at, live_since, last_error, checked_at)
    VALUES (${domain}, ${f.postmarkDomainId}, ${f.dkimVerified}, ${f.returnPathVerified},
            CASE WHEN ${f.dnsWritten} THEN now() END, CASE WHEN ${f.live} THEN now() END, ${f.lastError}, now())
    ON CONFLICT (domain) DO UPDATE SET
      postmark_domain_id = COALESCE(EXCLUDED.postmark_domain_id, postmark_sending_domains.postmark_domain_id),
      dkim_verified = EXCLUDED.dkim_verified,
      return_path_verified = EXCLUDED.return_path_verified,
      dns_written_at = COALESCE(postmark_sending_domains.dns_written_at, EXCLUDED.dns_written_at),
      live_since = COALESCE(postmark_sending_domains.live_since, EXCLUDED.live_since),
      last_error = EXCLUDED.last_error,
      checked_at = now()
  `);
}

export async function onboardPostmarkDomains(
  options: { dryRun?: boolean; domains?: string[] } = {},
): Promise<OnboardingSummary> {
  const dryRun = options.dryRun !== false;
  const candidates = await loadOnboardingCandidates(options.domains);
  const summary: OnboardingSummary = {
    dryRun,
    candidates: candidates.length,
    live: [],
    becameLive: [],
    pendingVerification: [],
    blockedByDomainCap: [],
    failed: [],
  };
  const already = new Set(
    rowsOf<{ domain: string }>(await db.execute(sql`SELECT domain FROM postmark_sending_domains WHERE live_since IS NOT NULL`)).map((r) => r.domain),
  );
  const token = await resolvePlatformKey(POSTMARK_ACCOUNT_PROVIDER, CALLER);

  // Every domain the account holds, once (paginated 100).
  const onAccount = new Map<string, number>();
  for (let offset = 0; ; offset += 100) {
    const page = await postmarkAccount(token, `/domains?count=100&offset=${offset}`);
    if (page.status !== 200) throw new Error(`Postmark GET /domains ${page.status}: ${JSON.stringify(page.body)}`);
    const list = (page.body.Domains ?? []) as { ID: number; Name: string }[];
    for (const d of list) onAccount.set(d.Name.toLowerCase(), d.ID);
    if (list.length < 100) break;
  }

  const gandiTokens = new Map<string, string>();
  let capReached = false;

  for (const candidate of candidates) {
    const domain = candidate.domain;
    if (already.has(domain)) {
      summary.live.push(domain);
      continue;
    }
    if (capReached && !onAccount.has(domain)) {
      summary.blockedByDomainCap.push(domain);
      if (!dryRun) await upsertState(domain, { lastError: "domain_cap" });
      continue;
    }
    if (dryRun) {
      summary.pendingVerification.push(domain);
      continue;
    }
    try {
      let id = onAccount.get(domain) ?? null;
      if (id === null) {
        const created = await postmarkAccount(token, "/domains", {
          method: "POST",
          body: { Name: domain, ReturnPathDomain: `pm-bounces.${domain}` },
        });
        if (created.body.ErrorCode === POSTMARK_DOMAIN_CAP_ERROR) {
          capReached = true;
          summary.blockedByDomainCap.push(domain);
          await upsertState(domain, { lastError: "domain_cap" });
          continue;
        }
        if (created.status !== 200) throw new Error(`Postmark POST /domains ${created.status}: ${JSON.stringify(created.body)}`);
        id = Number(created.body.ID);
      }
      const details = await postmarkAccount(token, `/domains/${id}`);
      if (details.status !== 200) throw new Error(`Postmark GET /domains/${id} ${details.status}`);
      let dkim = details.body.DKIMVerified === true;
      let returnPath = details.body.ReturnPathDomainVerified === true;
      let dnsWritten = false;

      if (!dkim || !returnPath) {
        if (!candidate.gandiOrg) throw new Error(`no Gandi organisation recorded for ${domain}`);
        let gandi = gandiTokens.get(candidate.gandiOrg);
        if (!gandi) {
          gandi = await resolvePlatformKey(`gandi-${candidate.gandiOrg}`, CALLER);
          gandiTokens.set(candidate.gandiOrg, gandi);
        }
        const dkimHost = String(details.body.DKIMPendingHost || details.body.DKIMHost || "");
        const dkimValue = String(details.body.DKIMPendingTextValue || details.body.DKIMTextValue || "");
        const cname = String(details.body.ReturnPathDomainCNAMEValue || "pm.mtasv.net");
        if (!dkim) {
          if (!dkimHost || !dkimValue) throw new Error(`Postmark gave no DKIM record for ${domain}`);
          await writeGandiRecord(gandi, domain, recordNameInZone(dkimHost, domain), "TXT", `"${dkimValue}"`);
        }
        if (!returnPath) await writeGandiRecord(gandi, domain, "pm-bounces", "CNAME", `${cname.replace(/\.$/, "")}.`);
        dnsWritten = true;
        if (!dkim) {
          const v = await postmarkAccount(token, `/domains/${id}/verifyDkim`, { method: "PUT" });
          dkim = v.body.DKIMVerified === true;
        }
        if (!returnPath) {
          const v = await postmarkAccount(token, `/domains/${id}/verifyReturnPath`, { method: "PUT" });
          returnPath = v.body.ReturnPathDomainVerified === true;
        }
      }
      const live = dkim && returnPath;
      await upsertState(domain, {
        postmarkDomainId: id,
        dkimVerified: dkim,
        returnPathVerified: returnPath,
        dnsWritten,
        live,
        lastError: live ? null : "awaiting_dns_verification",
      });
      if (live) summary.becameLive.push(domain);
      else summary.pendingVerification.push(domain);
    } catch (error) {
      const message = errorMessage(error);
      summary.failed.push({ domain, error: message });
      await upsertState(domain, { lastError: message.slice(0, 500) });
      console.error(`[instantly-service] postmark onboarding ${domain} failed: ${message}`);
    }
  }
  if (summary.becameLive.length > 0) clearLivePipesCache();
  return summary;
}

export async function listPostmarkSendingDomains(): Promise<Record<string, unknown>[]> {
  return rowsOf<Record<string, unknown>>(
    await db.execute(sql`
      SELECT domain, postmark_domain_id AS "postmarkDomainId", dkim_verified AS "dkimVerified",
             return_path_verified AS "returnPathVerified", dns_written_at AS "dnsWrittenAt",
             live_since AS "liveSince", last_error AS "lastError", checked_at AS "checkedAt"
      FROM postmark_sending_domains ORDER BY live_since NULLS LAST, domain
    `),
  );
}
