/**
 * Every domain WE send from: the fleet's sending addresses plus every domain the
 * infra sync knows we own (warmup-only domains such as salesmolt.com carry no
 * Instantly account). Mail from these is our own warmup mesh, seed tests and
 * staff, never a prospect's answer. Measured 2026-10-08: 636,409 of the 716,030
 * `unrelated` inbound rows of 30 days came from them.
 *
 * Cached 10 minutes per replica: read once per poll, the set changes when a
 * batch of domains is bought.
 */

import { sql } from "drizzle-orm";

import { db } from "../../db";
import { getOrSetCachedStats } from "../stats-cache";
import { staffDomains } from "../staff-senders";

const OWN_DOMAINS_TTL_MS = 10 * 60 * 1000;

export async function loadOwnMailDomains(): Promise<ReadonlySet<string>> {
  return getOrSetCachedStats(
    "own-mail-domains",
    async () => {
      const result = await db.execute(sql`
        SELECT DISTINCT lower(split_part(email, '@', 2)) AS "domain" FROM instantly_accounts
        UNION
        SELECT DISTINCT lower(domain) AS "domain" FROM infra_domains
      `);
      const rows = (result as { rows?: Array<{ domain?: unknown }> }).rows ?? [];
      const domains = new Set<string>(staffDomains());
      for (const row of rows) {
        if (typeof row.domain === "string" && row.domain) domains.add(row.domain);
      }
      return domains;
    },
    OWN_DOMAINS_TTL_MS,
  );
}
