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
 * one read — two facts about one person, never two reads. The second read,
 * the offer's sales path, answers whether the customer AUTHORIZED the instant
 * call at all (see `ring-rep-on-sales-interest.ts`).
 */

const BRAND_SERVICE_URL = process.env.BRAND_SERVICE_URL;
const BRAND_SERVICE_API_KEY = process.env.BRAND_SERVICE_API_KEY;

/**
 * The sales rep a brand stated, or `null` when it never stated one.
 *
 * Two facts about ONE person, read together in ONE request, because they are
 * used within seconds of each other on the same signal: the rep is emailed the
 * thread the prospect wrote, and then their phone rings. Two reads would be two
 * chances for the two halves to disagree about who the rep is.
 *
 * Either field may be null on its own. brand-service refuses a NEW rep that
 * states a phone with no email, but rows written before that rule existed carry
 * a phone and no email — that is the true record of a fact we were never told,
 * not a defect, and both consumers here degrade silently on it.
 */
export interface SalesRep {
  /** Where the rep is copied. Null on a rep stated before the email existed. */
  email: string | null;
  /** Strict E.164, ready to hand to a telephony provider. */
  phone: string | null;
}

/** Nobody stated, or a brand we cannot see. Both mean "reach nobody". */
const NO_REP: SalesRep = { email: null, phone: null };

/**
 * The rep to reach for this brand, or null when the brand never stated one.
 *
 * Org-scoped exactly like brand-service's other per-brand config reads, so a
 * brand belonging to another org answers 404 — which is read here as "nobody to
 * reach" rather than as a failure: from this side, a brand we cannot see and a
 * brand that named nobody are the same absence.
 *
 * Any OTHER non-2xx throws with its status and body intact. A read that degraded
 * to null on a 500 would report "this brand wants nobody reached" about a brand
 * that may well want somebody reached, and nothing anywhere would say so.
 */
export async function getSalesRep(
  brandId: string,
  orgId: string,
): Promise<SalesRep> {
  if (!BRAND_SERVICE_URL || !BRAND_SERVICE_API_KEY) {
    throw new Error("BRAND_SERVICE_URL or BRAND_SERVICE_API_KEY is not set");
  }

  const response = await fetch(
    `${BRAND_SERVICE_URL}/orgs/brands/${encodeURIComponent(brandId)}/sales-rep`,
    {
      headers: {
        "x-api-key": BRAND_SERVICE_API_KEY,
        "x-org-id": orgId,
      },
    },
  );

  if (response.status === 404 || response.status === 403) return NO_REP;

  if (!response.ok) {
    const body = await response.text();
    throw new Error(
      `brand-service GET /orgs/brands/{brandId}/sales-rep failed: ${response.status} - ${body.slice(0, 200)}`,
    );
  }

  const body = (await response.json()) as {
    salesRepEmail?: string | null;
    salesRepPhone?: string | null;
  };
  return {
    email: body.salesRepEmail ?? null,
    phone: body.salesRepPhone ?? null,
  };
}

/**
 * How an offer sells — the legs its customer ticked on the offer's sales path.
 *
 * `stated: false` is "the offer never stated a path", which is a DIFFERENT answer
 * from `stated: true` with an empty `legKeys` ("stated, and ticked nothing").
 * The instant call keys on that difference: a path never stated keeps the
 * behaviour that predates the path, a stated one decides.
 */
export interface OfferSalesPath {
  stated: boolean;
  legKeys: string[] | null;
}

/**
 * The sales path an offer's customer stated, or `null` when brand-service does
 * not know the offer (404).
 *
 * brand-service's INTERNAL read keyed on the offer alone — the caller holds the
 * offer id off campaign-service's row and nothing else. Any other non-2xx
 * throws with its status and body, so the caller can tell "unreadable" from
 * "never stated".
 */
export async function getOfferSalesPath(offerId: string): Promise<OfferSalesPath | null> {
  if (!BRAND_SERVICE_URL || !BRAND_SERVICE_API_KEY) {
    throw new Error("BRAND_SERVICE_URL or BRAND_SERVICE_API_KEY is not set");
  }

  const response = await fetch(
    `${BRAND_SERVICE_URL}/internal/offers/${encodeURIComponent(offerId)}/sales-path`,
    { headers: { "x-api-key": BRAND_SERVICE_API_KEY } },
  );

  if (response.status === 404) return null;

  if (!response.ok) {
    const body = await response.text();
    throw new Error(
      `brand-service GET /internal/offers/{offerId}/sales-path failed: ${response.status} - ${body.slice(0, 200)}`,
    );
  }

  const body = (await response.json()) as { stated?: unknown; legKeys?: unknown };
  if (typeof body.stated !== "boolean") {
    throw new Error("brand-service GET /internal/offers/{offerId}/sales-path returned no boolean `stated`");
  }
  return {
    stated: body.stated,
    legKeys: Array.isArray(body.legKeys)
      ? body.legKeys.filter((k): k is string => typeof k === "string")
      : null,
  };
}

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
    rep: {
      email: nonEmpty(brand.salesRepEmail),
      firstName: nonEmpty(brand.salesRepFirstName),
      role: nonEmpty(brand.salesRepRole),
    },
  };
}
