/**
 * Staff read `GET /internal/lead-source-yield` (SOURCE-YIELD-1016): per lead
 * SOURCE tag, the first emails we sent, how many hard-bounced, how many a
 * receiving server refused after accepting them, how many of those refusals
 * cite a blocklist, and how many left from a domain listed TODAY. Since
 * inception (ranking) and as a daily series (one chart per measure).
 *
 * Sources come from our bronze copy of lead-service's provenance
 * (`lead_provenance_raw`, lib/lead-provenance-sync.ts): the read never calls
 * lead-service. The join and the counting rule live in lib/lead-source-yield.ts.
 *
 * What each measure reads:
 *  - first email = the earliest `email_sent` at step 1 of a sequence (real, or
 *    inferred from a delivery notice that proves it left), every transport;
 *  - hard bounce = any `email_bounced` on the sequence (every one is permanent:
 *    lib/bounced-emails.ts), every transport;
 *  - refusal = a delivery-failure notice (`imap_messages_raw` kind `bounce`) for
 *    the sequence: the receiver accepted, then refused. Only OUR sender reads
 *    those notices (from 2026-08-25); Instantly's bounces cannot be told apart
 *    and count as hard bounces only;
 *  - blocklist refusal = that notice's words cite a blocklist
 *    (`BLOCKLIST_CITATION_PATTERN`, shared with the blocklist read);
 *  - from a listed domain = the first email's sending domain is listed on any
 *    list in `domain_blocklist_status` NOW.
 */

import { sql } from "drizzle-orm";

import { db } from "../db";
import { BLOCKLIST_CITATION_PATTERN, iso, utcDay, windowDays } from "./domain-blocklist-read";
import { lastProvenanceSyncAt } from "./lead-provenance-sync";
import {
  NO_SOURCE_KEY,
  UNKNOWN_KEY,
  aggregateSourceYield,
  provenanceKey,
  type YieldAggregate,
  type YieldSequence,
} from "./lead-source-yield";

function rowsOf<T>(result: unknown): T[] {
  if (Array.isArray(result)) return result as T[];
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as T[]) : [];
}

export const LEAD_SOURCE_YIELD_DEFAULT_DAYS = 60;
export const LEAD_SOURCE_YIELD_MAX_DAYS = 365;

export const COUNTING_RULE = "per_tag" as const;
export const COUNTING_RULE_NOTE =
  "A person found by several sources counts once under EACH of their tags (full weight, no 1/n split), so a column summed over sources can exceed `total`, which counts each first email once.";

export interface LeadSourceYieldRead extends YieldAggregate {
  asOf: string;
  countingRule: typeof COUNTING_RULE;
  countingRuleNote: string;
  pseudoKeys: { noSourceRecorded: string; unknown: string };
  /** Last time the provenance copy was written; null = never synced. */
  provenanceSyncedAt: string | null;
  /** Sending domains listed on any blocklist now. */
  listedDomainsToday: string[];
  /** First day a refusal notice can exist (our sender's first stored notice); null = none. */
  refusalsMeasuredSince: string | null;
}

async function loadSequences(): Promise<YieldSequence[]> {
  const [sendResult, bounceResult, refusalResult] = await Promise.all([
    db.execute(sql`
      WITH first_sends AS (
        -- index-only on instantly_events_stats_covering_idx
        SELECT e.campaign_id, e.lead_email,
               MIN(e."timestamp") AS first_at,
               (array_agg(e.account_email ORDER BY e."timestamp"))[1] AS account_email
        FROM instantly_events e
        WHERE e.event_type = 'email_sent' AND e.step = 1 AND e.lead_email IS NOT NULL
        GROUP BY 1, 2
      )
      SELECT f.campaign_id AS "campaignId", lower(trim(f.lead_email)) AS "email",
             to_char(f.first_at, 'YYYY-MM-DD') AS "day",
             split_part(lower(COALESCE(f.account_email, '')), '@', 2) AS "domain",
             c.org_id AS "orgId", c.brand_ids AS "brandIds"
      FROM first_sends f
      LEFT JOIN instantly_campaigns c ON c.instantly_campaign_id = f.campaign_id
    `),
    db.execute(sql`
      SELECT DISTINCT campaign_id AS "campaignId", lower(trim(lead_email)) AS "email"
      FROM instantly_events
      WHERE event_type = 'email_bounced' AND withdrawn_at IS NULL AND campaign_id IS NOT NULL
    `),
    db.execute(sql`
      SELECT instantly_campaign_id AS "campaignId",
             BOOL_OR(COALESCE(payload->>'textSnippet', '') ~* ${BLOCKLIST_CITATION_PATTERN}) AS "blocklist"
      FROM imap_messages_raw
      WHERE kind = 'bounce' AND instantly_campaign_id IS NOT NULL
      GROUP BY 1
    `),
  ]);

  const bounced = new Set(rowsOf<{ campaignId: string; email: string }>(bounceResult).map((r) => `${r.campaignId}|${r.email}`));
  const refused = new Map(rowsOf<{ campaignId: string; blocklist: boolean }>(refusalResult).map((r) => [r.campaignId, r.blocklist === true]));

  return rowsOf<{
    campaignId: string; email: string; day: string; domain: string; orgId: string | null; brandIds: string[] | null;
  }>(sendResult).map((r) => ({
    orgId: r.orgId,
    brandIds: Array.isArray(r.brandIds) ? r.brandIds : [],
    email: r.email,
    day: r.day,
    domain: r.domain,
    hardBounced: bounced.has(`${r.campaignId}|${r.email}`),
    refused: refused.has(r.campaignId),
    refusedBlocklist: refused.get(r.campaignId) === true,
  }));
}

async function loadProvenance(): Promise<Map<string, string[]>> {
  const rows = rowsOf<{ orgId: string; brandId: string; email: string; tags: string[] | null }>(
    await db.execute(sql`
      SELECT DISTINCT ON (org_id, brand_id, email)
             org_id AS "orgId", brand_id AS "brandId", email, tags
      FROM lead_provenance_raw
      ORDER BY org_id, brand_id, email, fetched_at DESC
    `),
  );
  return new Map(rows.map((r) => [provenanceKey(r.orgId, r.brandId, r.email), Array.isArray(r.tags) ? r.tags : []]));
}

export async function loadLeadSourceYield(days: number, asOf: Date = new Date()): Promise<LeadSourceYieldRead> {
  const [sequences, provenance, listedResult, refusalStart, syncedAt] = await Promise.all([
    loadSequences(),
    loadProvenance(),
    db.execute(sql`SELECT DISTINCT domain FROM domain_blocklist_status WHERE listed = true ORDER BY domain`),
    db.execute(sql`SELECT MIN(COALESCE(received_at, polled_at)) AS at FROM imap_messages_raw WHERE kind = 'bounce'`),
    lastProvenanceSyncAt(),
  ]);
  const listed = rowsOf<{ domain: string }>(listedResult).map((r) => r.domain);
  const refusalAt = iso(rowsOf<{ at: Date | string | null }>(refusalStart)[0]?.at ?? null);
  const aggregate = aggregateSourceYield(sequences, provenance, new Set(listed), windowDays(asOf, days));
  return {
    asOf: asOf.toISOString(),
    countingRule: COUNTING_RULE,
    countingRuleNote: COUNTING_RULE_NOTE,
    pseudoKeys: { noSourceRecorded: NO_SOURCE_KEY, unknown: UNKNOWN_KEY },
    provenanceSyncedAt: syncedAt?.toISOString() ?? null,
    listedDomainsToday: listed,
    refusalsMeasuredSince: refusalAt ? utcDay(new Date(refusalAt)) : null,
    ...aggregate,
  };
}
