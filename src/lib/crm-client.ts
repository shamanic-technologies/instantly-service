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

async function readOneChunk(
  config: { url: string; apiKey: string },
  orgId: string,
  brandIds: string[],
  emails: string[],
  runId: string | null,
): Promise<Array<{ email: string; stated: boolean; brandId: string | null; correctionId: string | null }>> {
  const path = "POST /internal/people/stated-not-to-brand";
  let response: Response;
  try {
    response = await fetch(`${config.url}/internal/people/stated-not-to-brand`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-api-key": config.apiKey,
        ...(runId ? { "x-run-id": runId } : {}),
      },
      body: JSON.stringify({ orgId, brandIds, emails }),
      signal: AbortSignal.timeout(CRM_STATED_TIMEOUT_MS),
    });
  } catch (error: unknown) {
    throw new CrmStatedReadError(
      `crm-service ${path} unreachable: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
  if (!response.ok) {
    const detail = await response.text().catch(() => "");
    throw new CrmStatedReadError(`crm-service ${path} failed: ${response.status} - ${detail.slice(0, 200)}`);
  }
  const body = (await response.json().catch(() => null)) as { results?: unknown } | null;
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
