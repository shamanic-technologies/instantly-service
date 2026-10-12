/**
 * Is this brand still working with us? Read from features-service, which owns
 * the per-brand account status (billing + campaign-service composed there:
 * active, out_of_credit, reactive_only, paused, inactive, payment_declined,
 * no_payment_method, unknown). Never re-derived here.
 *
 * Used by the staff check-in on an old unanswered question (owner 2026-10-12:
 * ObraCam churned, so the check-in must not keep the conversation going for it).
 *
 * The fleet audit it reads is served from features-service's cache (one row per
 * org x brand, ~60 rows), and the check-in is rare (an old question, once per
 * thread), so reading the whole list for one brand costs nothing measurable.
 *
 * Throws on a failed read: the caller decides what an unknown status means.
 */

/** Where the brand stands with us, as the check-in needs it. */
export type BrandRelationship = "working_with_us" | "not_working_with_us" | "unknown";

export interface BrandAccountStatus {
  /** features-service's own status word for this brand ("active", "paused", ...). */
  status: string;
  relationship: BrandRelationship;
}

/**
 * Pure: the producer's status word, read as "does this brand still work with us".
 * Something still runs for it (proactive, reactive, or proactive short of credit)
 * = working with us. Paused, inactive, no card, declined card = not.
 */
export function relationshipForStatus(status: string | null | undefined): BrandRelationship {
  switch (status) {
    case "active":
    case "out_of_credit":
    case "reactive_only":
      return "working_with_us";
    case "paused":
    case "inactive":
    case "payment_declined":
    case "no_payment_method":
      return "not_working_with_us";
    default:
      return "unknown";
  }
}

/** The brand's status in this org, or null when features-service holds no row for it. */
export async function fetchBrandAccountStatus(orgId: string, brandId: string): Promise<BrandAccountStatus | null> {
  const url = process.env.FEATURES_SERVICE_URL;
  const apiKey = process.env.FEATURES_SERVICE_API_KEY;
  if (!url || !apiKey) throw new Error("FEATURES_SERVICE_URL / FEATURES_SERVICE_API_KEY not set");
  const response = await fetch(`${url}/internal/stats/accounts`, {
    headers: { "x-api-key": apiKey },
    signal: AbortSignal.timeout(15_000),
  });
  if (!response.ok) throw new Error(`features-service /internal/stats/accounts ${response.status}`);
  const body = (await response.json()) as { rows?: Array<{ orgId?: unknown; brandId?: unknown; status?: unknown }> };
  if (!Array.isArray(body.rows)) throw new Error("features-service /internal/stats/accounts: no rows");
  // Keyed on org AND brand: a transferred brand has a row under each org.
  const row = body.rows.find((r) => r.orgId === orgId && r.brandId === brandId);
  if (!row || typeof row.status !== "string") return null;
  return { status: row.status, relationship: relationshipForStatus(row.status) };
}
