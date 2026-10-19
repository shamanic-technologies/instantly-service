/**
 * Who hosts a prospect domain's mail, from its MX records. MS-SEED-PLACEMENT-1018.
 *
 * A prospect at a Microsoft-hosted domain (Outlook.com, Hotmail, any Microsoft
 * 365 tenant) is filtered by Microsoft, so their first email must come from a
 * mailbox Microsoft puts in Inbox (`microsoft-placement.ts`). The MX record is
 * the public fact that says who receives a domain's mail: Microsoft 365 tenants
 * point at `<tenant>.mail.protection.outlook.com`, consumer Outlook.com/Hotmail
 * at `*.olc.protection.outlook.com`.
 *
 * Known blind spot, accepted: a tenant behind a third-party gateway (Proofpoint,
 * Mimecast) shows the gateway's MX and reads `other`; its mail then goes out as
 * it does today.
 *
 * Stored per domain in `recipient_mail_hosts`, refreshed by the dispatcher for
 * the domains of queued first emails (`refreshRecipientMailHosts`).
 */

import { promises as dns } from "node:dns";
import { sql } from "drizzle-orm";

import { db } from "../db";

export type RecipientMailHost = "microsoft" | "google" | "other" | "none" | "unknown";

/** How long one MX reading is trusted. */
export const MAIL_HOST_FRESH_DAYS = 30;

/** At most this many domains are looked up per dispatcher tick. */
export const MAIL_HOST_LOOKUPS_PER_TICK = 300;

const MICROSOFT_MX_SUFFIXES = [".protection.outlook.com", ".outlook.com", ".hotmail.com"];
const GOOGLE_MX_SUFFIXES = [".google.com", ".googlemail.com"];

/** The provider behind a list of MX exchanges. Pure. */
export function mailHostForMx(exchanges: readonly string[]): RecipientMailHost {
  const hosts = exchanges
    .map((e) => e.trim().toLowerCase().replace(/\.$/, ""))
    .filter((e) => e.length > 0);
  if (hosts.length === 0) return "none";
  const endsWithAny = (host: string, suffixes: readonly string[]) =>
    suffixes.some((s) => host.endsWith(s) || host === s.slice(1));
  if (hosts.some((h) => endsWithAny(h, MICROSOFT_MX_SUFFIXES))) return "microsoft";
  if (hosts.some((h) => endsWithAny(h, GOOGLE_MX_SUFFIXES))) return "google";
  return "other";
}

export function domainOfAddress(email: string): string {
  const at = email.lastIndexOf("@");
  return at === -1 ? "" : email.slice(at + 1).trim().toLowerCase();
}

/** DNS answers that mean "this domain receives no mail", not "the lookup failed". */
const NO_MX_CODES = new Set(["ENOTFOUND", "ENODATA", "ENONAME"]);

export interface MailHostReading {
  domain: string;
  provider: RecipientMailHost;
  mx: string[];
  error: string | null;
}

export type ResolveMx = (domain: string) => Promise<{ exchange: string; priority: number }[]>;

/** Look one domain up. Never throws: a failed lookup is a reading with `error`. */
export async function readMailHost(
  domain: string,
  resolveMx: ResolveMx = (d) => dns.resolveMx(d),
): Promise<MailHostReading> {
  try {
    const records = await resolveMx(domain);
    const mx = [...records].sort((a, b) => a.priority - b.priority).map((r) => r.exchange);
    return { domain, provider: mailHostForMx(mx), mx, error: null };
  } catch (error) {
    const code = (error as { code?: string }).code ?? "";
    if (NO_MX_CODES.has(code)) return { domain, provider: "none", mx: [], error: null };
    return {
      domain,
      provider: "unknown",
      mx: [],
      error: code || (error instanceof Error ? error.message : String(error)),
    };
  }
}

function rowsOf<T>(result: unknown): T[] {
  return ((result as { rows?: T[] }).rows ?? []) as T[];
}

/**
 * Read the MX of every prospect domain a queued FIRST email will go to and that
 * has no fresh reading. Bounded per tick; the rest is read on the next tick.
 */
export async function refreshRecipientMailHosts(
  resolveMx?: ResolveMx,
): Promise<{ looked: number; microsoft: number; failed: number }> {
  const result = await db.execute(sql`
    SELECT DISTINCT lower(split_part(c.lead_email, '@', 2)) AS domain
    FROM instantly_campaigns c
    JOIN sequence_costs sc ON sc.instantly_campaign_id = c.instantly_campaign_id
    LEFT JOIN recipient_mail_hosts h ON h.domain = lower(split_part(c.lead_email, '@', 2))
    WHERE c.status = 'active'
      AND sc.status = 'provisioned'
      AND c.lead_email LIKE '%@%'
      AND (
        h.domain IS NULL
        OR h.checked_at < now() - make_interval(days => ${MAIL_HOST_FRESH_DAYS})
        -- A failed lookup is retried within hours, not a month.
        OR (h.provider = 'unknown' AND h.checked_at < now() - interval '6 hours')
      )
    LIMIT ${MAIL_HOST_LOOKUPS_PER_TICK}
  `);
  const domains = rowsOf<{ domain: string }>(result).map((r) => r.domain).filter(Boolean);

  let microsoft = 0;
  let failed = 0;
  // Twenty lookups at a time: the first tick after deploy reads a few hundred
  // domains, and it runs inside the dispatch sweep.
  const readings: MailHostReading[] = [];
  for (let i = 0; i < domains.length; i += 20) {
    readings.push(...(await Promise.all(domains.slice(i, i + 20).map((d) => readMailHost(d, resolveMx)))));
  }
  for (const reading of readings) {
    if (reading.provider === "microsoft") microsoft += 1;
    if (reading.provider === "unknown") failed += 1;
    await db.execute(sql`
      INSERT INTO recipient_mail_hosts (domain, provider, mx, error, checked_at)
      VALUES (${reading.domain}, ${reading.provider}, ${JSON.stringify(reading.mx)}::jsonb, ${reading.error}, now())
      ON CONFLICT (domain) DO UPDATE SET
        provider = EXCLUDED.provider, mx = EXCLUDED.mx, error = EXCLUDED.error, checked_at = EXCLUDED.checked_at
    `);
  }
  return { looked: domains.length, microsoft, failed };
}
