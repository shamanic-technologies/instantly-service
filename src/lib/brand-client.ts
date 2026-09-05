/**
 * brand-service client — the ONE number to ring when a sales interest lands on
 * a brand.
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
 * exactly one read; do not grow it into a general brand mirror.
 */

const BRAND_SERVICE_URL = process.env.BRAND_SERVICE_URL;
const BRAND_SERVICE_API_KEY = process.env.BRAND_SERVICE_API_KEY;

/**
 * The number to ring for this brand, or null when the brand never stated one.
 *
 * Org-scoped exactly like brand-service's other per-brand config reads, so a
 * brand belonging to another org answers 404 — which is read here as "no number
 * to ring" rather than as a failure: from this side, a brand we cannot see and a
 * brand that named nobody are the same absence, and both mean no call.
 *
 * Any OTHER non-2xx throws with its status and body intact. A read that degraded
 * to null on a 500 would report "this brand wants no call" about a brand that
 * may well want one, and nothing anywhere would say so.
 */
export async function getSalesRepPhone(
  brandId: string,
  orgId: string,
): Promise<string | null> {
  if (!BRAND_SERVICE_URL || !BRAND_SERVICE_API_KEY) {
    throw new Error("BRAND_SERVICE_URL or BRAND_SERVICE_API_KEY is not set");
  }

  const response = await fetch(
    `${BRAND_SERVICE_URL}/orgs/brands/${encodeURIComponent(brandId)}/sales-rep-phone`,
    {
      headers: {
        "x-api-key": BRAND_SERVICE_API_KEY,
        "x-org-id": orgId,
      },
    },
  );

  if (response.status === 404 || response.status === 403) return null;

  if (!response.ok) {
    const body = await response.text();
    throw new Error(
      `brand-service GET /orgs/brands/{brandId}/sales-rep-phone failed: ${response.status} - ${body.slice(0, 200)}`,
    );
  }

  const body = (await response.json()) as { salesRepPhone?: string | null };
  return body.salesRepPhone ?? null;
}
