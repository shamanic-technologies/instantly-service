/**
 * Our bronze copy of lead-service's lead provenance (SOURCE-YIELD-1016): for
 * every person we sent a first email to, every source tag lead-service holds,
 * per (org, brand). lead-service owns provenance (human-service memberships +
 * features-service origin names); we read it through its bulk by-email route
 * (lead-service #951) and keep a copy in `lead_provenance_raw`, so the
 * source-yield read never re-asks it for history.
 *
 * WHO IS ASKED, per pass:
 *  - every (org, brand, address) with a first email and NO answer stored yet;
 *  - people first-emailed in the last `REFRESH_RECENT_DAYS` whose answer was
 *    last confirmed more than `REFRESH_EVERY_MS` ago: provenance is multi and
 *    GROWS after the serve (a later source finding a taken person adds a tag).
 * Batches of 1000 per (org, brand), at most `MAX_BATCHES_PER_PASS` per pass; the
 * next pass continues.
 *
 * A 502 (`provenance_source_unavailable`) or any failed call writes NOTHING for
 * the batch: those people stay `(unknown)` in the read and are asked again next
 * pass. Never an empty tag list in their name.
 */

import { sql } from "drizzle-orm";

import { db } from "../db";
import { normalizeLeadEmail } from "./lead-email";
import { iso } from "./domain-blocklist-read";

export const PROVENANCE_BATCH_SIZE = 1000;
export const MAX_BATCHES_PER_PASS = 120;
export const REFRESH_RECENT_DAYS = 30;
export const REFRESH_EVERY_MS = 24 * 60 * 60 * 1000;

function rowsOf<T>(result: unknown): T[] {
  if (Array.isArray(result)) return result as T[];
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as T[]) : [];
}

/** One served entry (lead-service `PersonProvenance`). Only `email` and `tags` are read; the rest is kept as payload. */
export interface PersonProvenance {
  email: string;
  tags: string[];
  [k: string]: unknown;
}

export type ProvenanceFetchResult =
  | { ok: true; people: PersonProvenance[] }
  | { ok: false; status: number; unavailable: boolean; error: string };

export type ProvenanceFetcher = (orgId: string, brandId: string, emails: string[]) => Promise<ProvenanceFetchResult>;

/** lead-service `POST /internal/brands/:brandId/lead-provenance/by-email`. Never throws: a failure is a result. */
export const fetchLeadProvenance: ProvenanceFetcher = async (orgId, brandId, emails) => {
  const url = process.env.LEAD_SERVICE_URL;
  const key = process.env.LEAD_SERVICE_API_KEY;
  if (!url || !key) return { ok: false, status: 0, unavailable: false, error: "LEAD_SERVICE_URL or LEAD_SERVICE_API_KEY is not set" };
  try {
    const response = await fetch(`${url}/internal/brands/${encodeURIComponent(brandId)}/lead-provenance/by-email`, {
      method: "POST",
      headers: { "content-type": "application/json", "x-api-key": key, "x-org-id": orgId },
      body: JSON.stringify({ emails }),
      signal: AbortSignal.timeout(60_000),
    });
    if (!response.ok) {
      const detail = (await response.text()).slice(0, 300);
      return { ok: false, status: response.status, unavailable: response.status === 502, error: detail };
    }
    const body = (await response.json()) as { people?: unknown };
    if (!Array.isArray(body.people)) {
      return { ok: false, status: response.status, unavailable: false, error: "response has no people[]" };
    }
    return { ok: true, people: body.people as PersonProvenance[] };
  } catch (error: unknown) {
    return { ok: false, status: 0, unavailable: true, error: error instanceof Error ? error.message : String(error) };
  }
};

export interface ProvenanceTarget {
  orgId: string;
  brandId: string;
  email: string;
}

/**
 * Who to ask this pass, grouped into (org, brand) batches of at most `batchSize`.
 * Pure. Never-answered people come first, then the stale recent ones.
 */
export function planProvenanceBatches(
  missing: readonly ProvenanceTarget[],
  stale: readonly ProvenanceTarget[],
  batchSize: number = PROVENANCE_BATCH_SIZE,
  maxBatches: number = MAX_BATCHES_PER_PASS,
): Array<{ orgId: string; brandId: string; emails: string[] }> {
  const batches: Array<{ orgId: string; brandId: string; emails: string[] }> = [];
  for (const group of [missing, stale]) {
    const byPair = new Map<string, { orgId: string; brandId: string; emails: Set<string> }>();
    for (const t of group) {
      // lead-service 400s a whole batch on one non-string address.
      if (typeof t.email !== "string" || t.email.length === 0) continue;
      const k = `${t.orgId}|${t.brandId}`;
      const g = byPair.get(k) ?? { orgId: t.orgId, brandId: t.brandId, emails: new Set<string>() };
      g.emails.add(t.email);
      byPair.set(k, g);
    }
    for (const g of byPair.values()) {
      const emails = [...g.emails];
      for (let i = 0; i < emails.length; i += batchSize) {
        if (batches.length >= maxBatches) return batches;
        batches.push({ orgId: g.orgId, brandId: g.brandId, emails: emails.slice(i, i + batchSize) });
      }
    }
  }
  return batches;
}

/** Every (org, brand, address) with a first email: never answered, and recent ones due a re-check. */
async function loadTargets(now: Date): Promise<{ missing: ProvenanceTarget[]; stale: ProvenanceTarget[] }> {
  const refreshSince = new Date(now.getTime() - REFRESH_RECENT_DAYS * 86_400_000).toISOString();
  const staleBefore = new Date(now.getTime() - REFRESH_EVERY_MS).toISOString();
  const result = await db.execute(sql`
    WITH first_sends AS (
      -- index-only on instantly_events_stats_covering_idx (~0.6 s for 60k)
      SELECT e.campaign_id, e.lead_email, MIN(e."timestamp") AS first_at
      FROM instantly_events e
      WHERE e.event_type = 'email_sent' AND e.step = 1
      GROUP BY 1, 2
    ),
    people AS (
      -- The address is the SEND's (the read joins on it too); the campaign
      -- row's own lead_email can be null.
      SELECT c.org_id AS org_id, b.brand_id, lower(trim(f.lead_email)) AS email, MAX(f.first_at) AS last_first_at
      FROM first_sends f
      JOIN instantly_campaigns c ON c.instantly_campaign_id = f.campaign_id
      CROSS JOIN LATERAL unnest(c.brand_ids) AS b(brand_id)
      WHERE c.org_id IS NOT NULL AND f.lead_email IS NOT NULL AND trim(f.lead_email) <> ''
      GROUP BY 1, 2, 3
    ),
    latest AS (
      SELECT org_id, brand_id, email, MAX(confirmed_at) AS confirmed_at
      FROM lead_provenance_raw
      GROUP BY 1, 2, 3
    )
    SELECT p.org_id AS "orgId", p.brand_id AS "brandId", p.email,
           CASE WHEN l.email IS NULL THEN 'missing' ELSE 'stale' END AS "state"
    FROM people p
    LEFT JOIN latest l ON l.org_id = p.org_id AND l.brand_id = p.brand_id AND l.email = p.email
    WHERE l.email IS NULL
       OR (p.last_first_at >= ${refreshSince}::timestamptz AND l.confirmed_at < ${staleBefore}::timestamptz)
  `);
  const missing: ProvenanceTarget[] = [];
  const stale: ProvenanceTarget[] = [];
  for (const r of rowsOf<ProvenanceTarget & { state: string }>(result)) {
    (r.state === "missing" ? missing : stale).push({ orgId: r.orgId, brandId: r.brandId, email: r.email });
  }
  return { missing, stale };
}

/** Stable JSON for comparing two answers (key order independent). */
function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value as Record<string, unknown>)
      .sort()
      .map((k) => `${JSON.stringify(k)}:${canonical((value as Record<string, unknown>)[k])}`)
      .join(",")}}`;
  }
  return JSON.stringify(value ?? null);
}

/**
 * Store one batch's answers: an answer identical to the latest stored one moves
 * its `confirmed_at`; a new or changed answer is a new row. An asked address
 * missing from the answer is left unstored (unknown).
 */
async function storeAnswers(orgId: string, brandId: string, people: PersonProvenance[], at: Date): Promise<{ inserted: number; confirmed: number }> {
  const served = new Map<string, PersonProvenance>();
  for (const p of people) {
    if (typeof p?.email !== "string" || !Array.isArray(p.tags)) continue;
    served.set(normalizeLeadEmail(p.email), p);
  }
  if (served.size === 0) return { inserted: 0, confirmed: 0 };
  const emails = [...served.keys()];
  const latest = rowsOf<{ id: string; email: string; payload: unknown }>(
    await db.execute(sql`
      SELECT DISTINCT ON (email) id, email, payload
      FROM lead_provenance_raw
      WHERE org_id = ${orgId} AND brand_id = ${brandId} AND email = ANY(${sql.param(emails)}::text[])
      ORDER BY email, fetched_at DESC
    `),
  );
  const latestByEmail = new Map(latest.map((r) => [r.email, r]));
  const confirmIds: string[] = [];
  const inserts: Array<{ email: string; tags: string[]; payload: PersonProvenance }> = [];
  for (const [email, p] of served) {
    const prev = latestByEmail.get(email);
    if (prev && canonical(prev.payload) === canonical(p)) confirmIds.push(prev.id);
    else inserts.push({ email, tags: [...new Set(p.tags.map(String))].sort(), payload: p });
  }
  const iso = at.toISOString();
  if (confirmIds.length > 0) {
    await db.execute(sql`
      UPDATE lead_provenance_raw SET confirmed_at = ${iso}::timestamptz
      WHERE id = ANY(${sql.param(confirmIds)}::text[])
    `);
  }
  if (inserts.length > 0) {
    await db.execute(sql`
      INSERT INTO lead_provenance_raw (id, org_id, brand_id, email, tags, payload, fetched_at, confirmed_at)
      SELECT gen_random_uuid()::text, ${orgId}, ${brandId}, x.email,
             ARRAY(SELECT jsonb_array_elements_text(x.tags)), x.payload, ${iso}::timestamptz, ${iso}::timestamptz
      FROM jsonb_to_recordset(${JSON.stringify(inserts)}::jsonb) AS x(email text, tags jsonb, payload jsonb)
    `);
  }
  return { inserted: inserts.length, confirmed: confirmIds.length };
}

export interface ProvenanceSyncResult {
  missing: number;
  stale: number;
  batches: number;
  asked: number;
  inserted: number;
  confirmed: number;
  /** Batches lead-service answered 502 / could not be reached: retried next pass, nothing stored. */
  unavailableBatches: number;
  /** Batches refused with another status: logged loud, retried next pass. */
  failedBatches: number;
  errors: string[];
}

let inFlight: Promise<ProvenanceSyncResult> | null = null;

/** One pass. Joins a pass already running. */
export function syncLeadProvenance(opts: { now?: Date; fetcher?: ProvenanceFetcher } = {}): Promise<ProvenanceSyncResult> {
  if (inFlight) return inFlight;
  inFlight = runPass(opts).finally(() => {
    inFlight = null;
  });
  return inFlight;
}

export async function runPass(opts: { now?: Date; fetcher?: ProvenanceFetcher } = {}): Promise<ProvenanceSyncResult> {
  const now = opts.now ?? new Date();
  const fetcher = opts.fetcher ?? fetchLeadProvenance;
  const { missing, stale } = await loadTargets(now);
  const batches = planProvenanceBatches(missing, stale);
  const out: ProvenanceSyncResult = {
    missing: missing.length,
    stale: stale.length,
    batches: batches.length,
    asked: 0,
    inserted: 0,
    confirmed: 0,
    unavailableBatches: 0,
    failedBatches: 0,
    errors: [],
  };
  for (const b of batches) {
    out.asked += b.emails.length;
    const r = await fetcher(b.orgId, b.brandId, b.emails);
    if (!r.ok) {
      if (r.unavailable) out.unavailableBatches += 1;
      else out.failedBatches += 1;
      const msg = `org ${b.orgId} brand ${b.brandId}: ${r.status} ${r.error}`;
      if (out.errors.length < 10) out.errors.push(msg);
      console.error(`[instantly-service] lead-provenance-sync: batch NOT stored (people stay unknown) ${msg}`);
      continue;
    }
    const stored = await storeAnswers(b.orgId, b.brandId, r.people, new Date());
    out.inserted += stored.inserted;
    out.confirmed += stored.confirmed;
  }
  return out;
}

/** When the copy was last written to; null = never. */
export async function lastProvenanceSyncAt(): Promise<Date | null> {
  const r = rowsOf<{ at: Date | string | null }>(
    await db.execute(sql`SELECT MAX(confirmed_at) AS at FROM lead_provenance_raw`),
  );
  const at = iso(r[0]?.at ?? null);
  return at ? new Date(at) : null;
}
