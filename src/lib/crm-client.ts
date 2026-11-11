// crm-service: the people corrections a brand's user stated (DUALIST-WA-SILENCE-EMAIL).
//
// The one read this service needs from crm-service: "did the brand's user STATE
// that this person is not someone of the brand?" (a people correction
// `tag_add professional_not_to_brand`). crm-service owns that statement, its
// undo, and the person it is attached to (an email, a phone or a WhatsApp
// handle of the same built person all match), so this service ASKS, never
// stores a copy: an undone statement lifts on the next read.
//
// Env is read AT CALL TIME, never at boot: a missing `CRM_SERVICE_URL` must not
// crash the service, it fails every read loudly with the variable named.

import { normalizeLeadEmail } from "./lead-email";

/** The producer's caps (POST /internal/people/stated-not-to-brand). */
export const CRM_STATED_MAX_EMAILS = 1000;
export const CRM_STATED_MAX_BRANDS = 50;
const CRM_STATED_TIMEOUT_MS = 10_000;
export const CRM_STATED_TRANSPORT_RETRIES = 2;

function isTimeout(error: unknown): boolean {
  const name = (error as { name?: string } | null)?.name;
  return name === "TimeoutError" || name === "AbortError";
}

/** A statement crm-service holds for one asked address. */
export interface StatedNotToBrandRecord {
  brandId: string | null;
  correctionId: string | null;
}

/** crm-service could not answer: never read as "nobody stated anything". */
export class CrmStatedReadError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "CrmStatedReadError";
  }
}

function crmConfig(): { url: string; apiKey: string } {
  const url = process.env.CRM_SERVICE_URL;
  const apiKey = process.env.CRM_SERVICE_API_KEY;
  const missing = [!url ? "CRM_SERVICE_URL" : null, !apiKey ? "CRM_SERVICE_API_KEY" : null].filter(Boolean);
  if (missing.length > 0) {
    throw new CrmStatedReadError(
      `${missing.join(" and ")} ${missing.length > 1 ? "are" : "is"} not set: cannot read whether the brand stated this person is not theirs`,
    );
  }
  return { url: url!.replace(/\/+$/, ""), apiKey: apiKey! };
}

function chunk<T>(items: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}

/**
 * POST a read to crm-service. A read, so a TRANSPORT failure is retried (twice, short backoff) before it fails:
 * in prod the first hour showed bursts of bare `fetch failed` (a reused keep-alive socket the server had closed,
 * while this process ran near 100% CPU) on reads that succeed at once when asked again. An HTTP answer (any
 * status) is never retried; a timeout is not retried either (crm-service is slow, not gone).
 */
async function postCrmRead(
  config: { url: string; apiKey: string },
  routePath: string,
  body: unknown,
  runId: string | null,
): Promise<unknown> {
  const path = `POST ${routePath}`;
  const request = () =>
    fetch(`${config.url}${routePath}`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-api-key": config.apiKey,
        ...(runId ? { "x-run-id": runId } : {}),
      },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(CRM_STATED_TIMEOUT_MS),
    });
  let response: Response | null = null;
  let lastError: unknown = null;
  for (let attempt = 0; attempt <= CRM_STATED_TRANSPORT_RETRIES; attempt++) {
    if (attempt > 0) await new Promise((resolve) => setTimeout(resolve, 100 * attempt));
    try {
      response = await request();
      break;
    } catch (error: unknown) {
      lastError = error;
      if (isTimeout(error)) break;
    }
  }
  if (!response) {
    const cause = (lastError as { cause?: { code?: string; message?: string } } | null)?.cause;
    throw new CrmStatedReadError(
      `crm-service ${path} unreachable: ${lastError instanceof Error ? lastError.message : String(lastError)}` +
        `${cause ? ` (${cause.code ?? cause.message})` : ""}`,
    );
  }
  if (!response.ok) {
    const detail = await response.text().catch(() => "");
    throw new CrmStatedReadError(`crm-service ${path} failed: ${response.status} - ${detail.slice(0, 200)}`);
  }
  return response.json().catch(() => null);
}

async function readOneChunk(
  config: { url: string; apiKey: string },
  orgId: string,
  brandIds: string[],
  emails: string[],
  runId: string | null,
): Promise<Array<{ email: string; stated: boolean; brandId: string | null; correctionId: string | null }>> {
  const path = "POST /internal/people/stated-not-to-brand";
  const body = (await postCrmRead(config, "/internal/people/stated-not-to-brand", { orgId, brandIds, emails }, runId)) as {
    results?: unknown;
  } | null;
  const results = body?.results;
  if (!Array.isArray(results) || results.length !== emails.length) {
    throw new CrmStatedReadError(
      `crm-service ${path} returned ${Array.isArray(results) ? `${results.length} results for ${emails.length} emails` : "no results array"}`,
    );
  }
  return results.map((raw, i) => {
    const r = raw as { email?: unknown; stated?: unknown; brandId?: unknown; correctionId?: unknown };
    if (typeof r.stated !== "boolean") {
      throw new CrmStatedReadError(`crm-service ${path} result ${i} carries no boolean "stated"`);
    }
    return {
      email: typeof r.email === "string" ? r.email : emails[i],
      stated: r.stated,
      brandId: typeof r.brandId === "string" ? r.brandId : null,
      correctionId: typeof r.correctionId === "string" ? r.correctionId : null,
    };
  });
}

/**
 * For this org and these brands, which of these addresses the brand's user
 * stated is not someone of the brand. Keyed by NORMALIZED address; absent =
 * no statement. Chunked to the producer's caps. Throws `CrmStatedReadError` on
 * any failure (unset env, unreachable, non-2xx, malformed body).
 */
export async function readStatedNotToBrand(params: {
  orgId: string;
  brandIds: string[];
  emails: string[];
  runId?: string | null;
}): Promise<Map<string, StatedNotToBrandRecord>> {
  const out = new Map<string, StatedNotToBrandRecord>();
  const brandIds = [...new Set(params.brandIds.filter(Boolean))];
  const emails = [...new Set(params.emails.map(normalizeLeadEmail).filter(Boolean))];
  if (brandIds.length === 0 || emails.length === 0) return out;
  const config = crmConfig();

  // Both caps are chunked: emails within one brand chunk, sequentially (bounded
  // load on crm-service; the first failure aborts the read, which is the point).
  for (const brandChunk of chunk(brandIds, CRM_STATED_MAX_BRANDS)) {
    for (const emailChunk of chunk(emails, CRM_STATED_MAX_EMAILS)) {
      const results = await readOneChunk(config, params.orgId, brandChunk, emailChunk, params.runId ?? null);
      results.forEach((r, i) => {
        if (!r.stated) return;
        const key = emailChunk[i];
        if (!out.has(key)) out.set(key, { brandId: r.brandId, correctionId: r.correctionId });
      });
    }
  }
  return out;
}

// ─── Many addresses at once: the whole answer per (org, brands), read once (STATUS-CRM-TIMEOUT-1107) ──
//
// The status read asks for ~10 batches of 100 addresses a second, and a features-service restart fires hundreds
// at once: one crm-service request per batch timed out 461 times in 3 h (2026-11-07, Distribute.you brand) and
// every timeout failed the whole status read (500). crm-service serves the whole fact for (org, brands) on
// `/internal/people/stated-not-to-brand/email-sets` (every address of every stated person, the same match as the
// per-address read; 44 statements fleet-wide), so it is read ONCE per (org, brands) and matched here.
//
// Freshness: an answer is used for at most `STATED_SETS_TTL_MS`; past `STATED_SETS_REFRESH_AFTER_MS` it is
// reloaded in the background while still served (a new statement or an undo lands within the TTL, the status
// read's own per-address cache is 60 s). Concurrent misses join ONE request. Fail loud: an expired or absent
// answer that cannot be reloaded THROWS (never read as "nobody stated"); a failed background refresh only logs,
// and the answer it would have replaced expires on schedule.
//
// MON-DELIVERY-STATUS-500-1111: crm-service's answer costs ~1.5 s for an org holding statements (it opens a platform
// run per read) and, under box load, past our 10 s timeout: 26 status reads a day 500'd, and lead-service's
// `POST /orgs/buffer/next` failed the whole serve behind each. The STATUS read may opt into `maxStaleOnErrorMs`:
// when the reload fails, the last answer crm-service gave for (org, brands) keeps being served up to that age, with a
// loud warning. Safe because the status read only REPORTS the refusal: the send gate re-asks crm-service per
// address (`readStatedNotToBrand`) and refuses on its own, and the dispatch sweep never opts in (it holds instead).

export const STATED_SETS_TTL_MS = 30_000;
export const STATED_SETS_REFRESH_AFTER_MS = 15_000;
const STATED_SETS_MAX_ENTRIES = 5_000;

type StatedSets = Map<string, StatedNotToBrandRecord>;
const statedSetsCache = new Map<string, { loadedAt: number; value: StatedSets }>();
const statedSetsInFlight = new Map<string, Promise<StatedSets>>();

/** Tests only. */
export function clearStatedSetsCache(): void {
  statedSetsCache.clear();
  statedSetsInFlight.clear();
}

async function fetchStatedSets(orgId: string, brandIds: string[]): Promise<StatedSets> {
  const config = crmConfig();
  const path = "POST /internal/people/stated-not-to-brand/email-sets";
  const out: StatedSets = new Map();
  for (const brandChunk of chunk(brandIds, CRM_STATED_MAX_BRANDS)) {
    const body = (await postCrmRead(config, "/internal/people/stated-not-to-brand/email-sets", { orgId, brandIds: brandChunk }, null)) as {
      statements?: unknown;
    } | null;
    const statements = body?.statements;
    if (!Array.isArray(statements)) throw new CrmStatedReadError(`crm-service ${path} returned no statements array`);
    statements.forEach((raw, i) => {
      const s = raw as { correctionId?: unknown; brandId?: unknown; emails?: unknown };
      if (typeof s.correctionId !== "string" || typeof s.brandId !== "string" || !Array.isArray(s.emails)) {
        throw new CrmStatedReadError(`crm-service ${path} statement ${i} is malformed`);
      }
      for (const email of s.emails) {
        if (typeof email !== "string") throw new CrmStatedReadError(`crm-service ${path} statement ${i} carries a non-string address`);
        const key = normalizeLeadEmail(email);
        // Statements come oldest first: the first one covering an address answers, like the per-address read.
        if (key && !out.has(key)) out.set(key, { brandId: s.brandId, correctionId: s.correctionId });
      }
    });
  }
  return out;
}

function loadStatedSets(key: string, orgId: string, brandIds: string[]): Promise<StatedSets> {
  const pending = statedSetsInFlight.get(key);
  if (pending) return pending;
  const load = fetchStatedSets(orgId, brandIds)
    .then((value) => {
      statedSetsCache.delete(key);
      statedSetsCache.set(key, { loadedAt: Date.now(), value });
      while (statedSetsCache.size > STATED_SETS_MAX_ENTRIES) {
        const oldest = statedSetsCache.keys().next().value;
        if (oldest === undefined) break;
        statedSetsCache.delete(oldest);
      }
      return value;
    })
    .finally(() => statedSetsInFlight.delete(key));
  statedSetsInFlight.set(key, load);
  return load;
}

/**
 * Same answer as `readStatedNotToBrand`, for callers asking about many addresses (the status read, the dispatch
 * sweep): read from the (org, brands) answer cached above. Throws `CrmStatedReadError` when no answer younger than
 * the TTL exists and crm-service cannot give one.
 */
export async function readStatedNotToBrandMany(params: {
  orgId: string;
  brandIds: string[];
  emails: string[];
  /** Serve the last answer up to this age when crm-service cannot reload it (status read only; see above). */
  maxStaleOnErrorMs?: number;
}): Promise<Map<string, StatedNotToBrandRecord>> {
  const out = new Map<string, StatedNotToBrandRecord>();
  const brandIds = [...new Set(params.brandIds.filter(Boolean))].sort();
  const emails = [...new Set(params.emails.map(normalizeLeadEmail).filter(Boolean))];
  if (brandIds.length === 0 || emails.length === 0) return out;

  const key = `${params.orgId}\u0000${brandIds.join(",")}`;
  const now = Date.now();
  const cached = statedSetsCache.get(key);
  let sets: StatedSets;
  if (cached && now - cached.loadedAt < STATED_SETS_TTL_MS) {
    sets = cached.value;
    if (now - cached.loadedAt >= STATED_SETS_REFRESH_AFTER_MS && !statedSetsInFlight.has(key)) {
      loadStatedSets(key, params.orgId, brandIds).catch((error: unknown) =>
        console.warn(
          `[instantly-service] stated-not-to-brand: background refresh failed for org=${params.orgId} brands=${brandIds.join(",")} (answer expires in ${Math.max(0, STATED_SETS_TTL_MS - (now - cached.loadedAt))} ms) — ${error instanceof Error ? error.message : String(error)}`,
        ),
      );
    }
  } else {
    try {
      sets = await loadStatedSets(key, params.orgId, brandIds);
    } catch (error: unknown) {
      const age = cached ? now - cached.loadedAt : Infinity;
      if (!cached || params.maxStaleOnErrorMs === undefined || age >= params.maxStaleOnErrorMs) throw error;
      console.warn(
        `[instantly-service] stated-not-to-brand: crm-service could not reload org=${params.orgId} brands=${brandIds.join(",")}, serving its last answer (${Math.round(age / 1000)} s old, ${cached.value.size} stated address(es)) — ${error instanceof Error ? error.message : String(error)}`,
      );
      sets = cached.value;
    }
  }
  for (const email of emails) {
    const hit = sets.get(email);
    if (hit) out.set(email, hit);
  }
  return out;
}
