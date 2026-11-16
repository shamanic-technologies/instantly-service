/**
 * brand-service client — the ONE person to reach when a sales interest lands on
 * a brand, and the two facts about them.
 *
 * brand-service owns per-brand configuration, and it states this the way it
 * states the click destination and the WhatsApp link: one row per (org, brand),
 * reused across every campaign and channel of that brand, normalized to strict
 * E.164 at write so a consumer can hand it straight to a telephony provider.
 *
 * ⚠️ ABSENCE IS A FIRST-CLASS ANSWER, NOT AN ERROR. A brand that never stated a
 * number has nobody to ring, which is the overwhelmingly common case: brands buy
 * cold email, and only some of them want a phone to ring on a hot reply. `null`
 * is that answer, and the caller no-ops silently on it. A brand-service that is
 * unreachable is a DIFFERENT fact and throws — the caller is fail-soft and
 * swallows it, but it must swallow it knowing the difference.
 *
 * This file was deleted once, when stop-on-click was repointed at
 * campaign-service and brand-service lost its last consumer here. It is back for
 * exactly one read; do not grow it into a general brand mirror. The rep is that
 * one read — two facts about one person, never two reads.
 */

const BRAND_SERVICE_URL = process.env.BRAND_SERVICE_URL;
const BRAND_SERVICE_API_KEY = process.env.BRAND_SERVICE_API_KEY;

/**
 * What a hand-over needs to say who the prospect is being handed to: the
 * brand's display name and its sales rep.
 *
 * ONE read (the internal brand read, org-scoped) rather than two, because the
 * name and the rep are used in the same sentence ("I've copied Marie, Head of
 * Partnerships at Doc Dinners") and two reads are two chances to disagree.
 *
 * `firstName` / `role` are the rep's own name and job title when the brand
 * stated them; null means "not stated", and the hand-over then names the
 * brand's team instead of a person. A name is NEVER inferred from the email
 * address. A brand we cannot see (404/403) is `null`, the same absence as a
 * brand that named nobody; any other failure throws.
 */
export interface BrandHandoffContext {
  name: string | null;
  /** The brand's own website domain (`docdinners.com`), lowercased; null for a no-website brand. */
  domain: string | null;
  rep: {
    email: string | null;
    firstName: string | null;
    role: string | null;
  };
}

function nonEmpty(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

export async function getBrandHandoffContext(
  brandId: string,
  orgId: string,
): Promise<BrandHandoffContext | null> {
  if (!BRAND_SERVICE_URL || !BRAND_SERVICE_API_KEY) {
    throw new Error("BRAND_SERVICE_URL or BRAND_SERVICE_API_KEY is not set");
  }

  const response = await fetch(
    `${BRAND_SERVICE_URL}/internal/brands/${encodeURIComponent(brandId)}`,
    { headers: { "x-api-key": BRAND_SERVICE_API_KEY, "x-org-id": orgId } },
  );

  if (response.status === 404 || response.status === 403) return null;

  if (!response.ok) {
    const body = await response.text();
    throw new Error(
      `brand-service GET /internal/brands/{brandId} failed: ${response.status} - ${body.slice(0, 200)}`,
    );
  }

  const body = (await response.json()) as { brand?: Record<string, unknown> };
  const brand = body.brand ?? {};
  return {
    name: nonEmpty(brand.name),
    domain: nonEmpty(brand.domain)?.toLowerCase() ?? null,
    rep: {
      email: nonEmpty(brand.salesRepEmail),
      firstName: nonEmpty(brand.salesRepFirstName),
      role: nonEmpty(brand.salesRepRole),
    },
  };
}
