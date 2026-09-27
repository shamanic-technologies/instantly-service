/**
 * features-service leg catalogue — where a leg LANDS the prospect.
 *
 * A campaign is bought for a leg (campaign-service `legKey`), and a leg is a
 * transition between two steps that features-service publishes on
 * `GET /public/channels` → `channels[].stepTransitions[]` as
 * `{ legKey, from, to: { key } }`. The arrival step is that statement, read
 * verbatim — the leg key itself is never parsed, since a key is a name and a
 * name is a guess about how the next rename will spell things.
 *
 * This replaced reading the campaign's sales funnel (retired fleet-wide): the
 * question stop-on-click asks — "does a click mean the prospect already arrived
 * where the conversion happens?" — is a property of the LEG the campaign is
 * bought for, not of a funnel.
 */

import { getOrSetCachedStats } from "./stats-cache";

/** The step a click on one of our links lands the prospect on. */
export const WEBSITE_VISIT_STEP_KEY = "website_visit";

/** Catalogue freshness: the leg vocabulary moves on deploys, not per request. */
const LEG_CATALOGUE_TTL_MS = 5 * 60_000;

export interface CatalogueStepTransition {
  legKey: string;
  from: { key: string } | null;
  to: { key: string } | null;
}

export interface CatalogueChannel {
  slug: string;
  stepTransitions?: CatalogueStepTransition[] | null;
}

/**
 * The step a leg arrives at, as the catalogue states it — null when the
 * catalogue does not know the leg.
 *
 * Read on the campaign's OWN channel first. A channel absent from the catalogue
 * (or silent about this leg) falls back to every channel stating the leg, and
 * answers only when they all AGREE on the arrival step — a disagreement is not
 * resolved by picking one.
 */
export function legArrivalStep(
  channels: CatalogueChannel[],
  featureSlug: string | null,
  legKey: string,
): string | null {
  const arrivalsIn = (list: CatalogueChannel[]): string[] =>
    list.flatMap((c) =>
      (c.stepTransitions ?? [])
        .filter((t) => t.legKey === legKey && typeof t.to?.key === "string")
        .map((t) => t.to!.key),
    );

  const own = featureSlug ? arrivalsIn(channels.filter((c) => c.slug === featureSlug)) : [];
  const candidates = own.length > 0 ? own : arrivalsIn(channels);
  const distinct = new Set(candidates);
  return distinct.size === 1 ? [...distinct][0] : null;
}

/** Fetch the published channel catalogue. Throws on any failure — the caller decides. */
async function fetchChannels(): Promise<CatalogueChannel[]> {
  const url = process.env.FEATURES_SERVICE_URL;
  if (!url) throw new Error("FEATURES_SERVICE_URL is not set");

  const headers: Record<string, string> = {};
  if (process.env.FEATURES_SERVICE_API_KEY) headers["x-api-key"] = process.env.FEATURES_SERVICE_API_KEY;

  const response = await fetch(`${url}/public/channels`, { headers });
  if (!response.ok) {
    const body = await response.text();
    throw new Error(`features-service GET /public/channels failed: ${response.status} - ${body.slice(0, 200)}`);
  }
  const body = (await response.json()) as { channels?: CatalogueChannel[] };
  if (!Array.isArray(body.channels)) {
    throw new Error("features-service GET /public/channels returned no channels array");
  }
  return body.channels;
}

/** The catalogue, cached per replica. A failed read caches nothing and throws. */
export function getChannelCatalogue(): Promise<CatalogueChannel[]> {
  return getOrSetCachedStats("leg-catalogue:channels", fetchChannels, LEG_CATALOGUE_TTL_MS);
}
