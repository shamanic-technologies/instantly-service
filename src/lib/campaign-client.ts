/**
 * campaign-service client — reads the FUNNEL a campaign runs, and which stored
 * rows are ONE campaign.
 *
 * A funnel is a property of the CAMPAIGN, not of the brand: two campaigns of the
 * same brand can legitimately run different funnels, so reading this at brand
 * level answers a question nobody asked. campaign-service owns `funnel_key` and
 * is the only place it is a fact rather than an inference.
 */

import {
  familyOf,
  identityKeyOf,
  type CampaignIdentityRow,
} from "./campaign-identity";

const CAMPAIGN_SERVICE_URL = process.env.CAMPAIGN_SERVICE_URL;
const CAMPAIGN_SERVICE_API_KEY = process.env.CAMPAIGN_SERVICE_API_KEY;

/**
 * The sales-funnel vocabulary, mirroring campaign-service's `sales-funnel-vocabulary.ts`.
 *
 * These four ARE the funnels a campaign can state; campaign-service stores nothing else. The
 * previous gate here tested a `visit_` PREFIX, which was true of the pre-rename spellings and is
 * true of NONE of these — so every visit-led campaign silently stopped stopping the day the rename
 * landed. A prefix is a guess about how a vocabulary will be spelled next; an explicit map is not,
 * and it cannot rot the same way: adding a fifth funnel below is a type error until someone says
 * whether a click on it means the prospect has arrived.
 */
export const SALES_FUNNEL_KEYS = [
  "sales_meetings_from_conversation",
  "sales_meetings_from_website",
  "website_purchases",
  "form_magnet",
] as const;

export type SalesFunnelKey = (typeof SALES_FUNNEL_KEYS)[number];

/**
 * The pre-rename spelling of each funnel, as campaign rows written before campaign-service's
 * migration 0043 still carry it. Accepted forever on the way in, never emitted — dropping an entry
 * would silently un-stop every campaign a producer still names the old way, which is the failure
 * this map exists to prevent. Byte-identical to campaign-service's own `LEGACY_FUNNEL_KEYS`.
 */
export const LEGACY_FUNNEL_KEYS: Readonly<Record<string, SalesFunnelKey>> = Object.freeze({
  reply_meeting: "sales_meetings_from_conversation",
  visit_meeting: "sales_meetings_from_website",
  visit_signup: "website_purchases",
  visit_form: "form_magnet",
});

const CANONICAL_FUNNEL_KEYS: ReadonlySet<string> = new Set(SALES_FUNNEL_KEYS);

/**
 * The canonical funnel a value names, under any spelling — null when it names none.
 *
 * Never guesses: a token neither catalogue lists is "no funnel", not a fifth funnel we quietly
 * work. Mirrors campaign-service's `toFunnelKey`.
 */
export function toFunnelKey(value: string | null | undefined): SalesFunnelKey | null {
  if (!value) return null;
  if (CANONICAL_FUNNEL_KEYS.has(value)) return value as SalesFunnelKey;
  return LEGACY_FUNNEL_KEYS[value] ?? null;
}

/**
 * Does a click on this funnel mean the prospect has ALREADY arrived where the conversion happens?
 *
 * Exhaustive over the vocabulary on purpose — `Record<SalesFunnelKey, boolean>` makes a new funnel
 * a compile error rather than a silent `false`, so the next rename cannot re-create the outage this
 * replaces. Three of the four open on a website visit: a click puts the prospect on the landing
 * page, so more cold email only distracts. `sales_meetings_from_conversation` opens on a REPLY, so
 * a click says nothing about whether to keep sending.
 */
const FUNNEL_OPENS_ON_VISIT: Readonly<Record<SalesFunnelKey, boolean>> = Object.freeze({
  sales_meetings_from_conversation: false,
  sales_meetings_from_website: true,
  website_purchases: true,
  form_magnet: true,
});

/**
 * True when this campaign's funnel opens on a website visit.
 *
 * A NULL funnel does NOT stop. campaign-service's own rule is that "a funnel is a fact, never a
 * guess" — a campaign that stated none keeps a null one, and pausing a live sequence on an unknown
 * is the wrong direction to be wrong in. An UNRECOGNISED token is the same absence, and it is the
 * one worth being loud about: it is exactly what a vocabulary rename looks like from here, so the
 * caller logs it rather than letting the fleet go quiet unobserved.
 */
export function funnelStopsOnClick(funnelKey: string | null | undefined): boolean {
  const canonical = toFunnelKey(funnelKey);
  return canonical !== null && FUNNEL_OPENS_ON_VISIT[canonical];
}

/** True for a non-empty funnel token this service's vocabulary does not know. */
export function isUnrecognisedFunnelKey(funnelKey: string | null | undefined): boolean {
  return typeof funnelKey === "string" && funnelKey !== "" && toFunnelKey(funnelKey) === null;
}

interface CampaignRecord {
  id?: string | null;
  orgId?: string | null;
  brandId?: string | null;
  brandIds?: string[] | null;
  offerId?: string | null;
  funnelKey?: string | null;
  legKey?: string | null;
  acquisitionChannel?: string | null;
}

/**
 * Read one campaign from campaign-service.
 *
 * Org-scoped: campaign-service filters by `x-org-id`, so a campaign belonging to
 * another org simply 404s. Returns null for a 404 — an absent campaign is a
 * legitimate answer rather than an error. Any OTHER failure throws, so each
 * caller decides (both of ours fail soft: the sequence continues).
 */
async function fetchCampaign(campaignId: string, orgId: string): Promise<CampaignRecord | null> {
  if (!CAMPAIGN_SERVICE_URL || !CAMPAIGN_SERVICE_API_KEY) {
    throw new Error("CAMPAIGN_SERVICE_URL or CAMPAIGN_SERVICE_API_KEY is not set");
  }

  const response = await fetch(`${CAMPAIGN_SERVICE_URL}/campaigns/${campaignId}`, {
    headers: {
      "x-api-key": CAMPAIGN_SERVICE_API_KEY,
      "x-org-id": orgId,
    },
  });

  if (response.status === 404) return null;

  if (!response.ok) {
    const body = await response.text();
    throw new Error(
      `campaign-service GET /campaigns/${campaignId} failed: ${response.status} - ${body.slice(0, 200)}`,
    );
  }

  const body = (await response.json()) as { campaign?: CampaignRecord };
  return body.campaign ?? null;
}

/** A campaign's funnel key, null when it states none or the campaign is absent. */
export async function getCampaignFunnelKey(
  campaignId: string,
  orgId: string,
): Promise<string | null> {
  const campaign = await fetchCampaign(campaignId, orgId);
  return campaign?.funnelKey ?? null;
}

/**
 * The scope a step trigger names: the (brand, offer, funnel) the lead is on.
 *
 * All three come off the CAMPAIGN row campaign-service already owns — nothing
 * here is inferred. A campaign that states none of them is not a campaign a leg
 * can be resolved for, which is an ordinary absence rather than an error.
 */
export interface CampaignTriggerScope {
  brandId: string | null;
  offerId: string | null;
  funnelKey: string | null;
}

/**
 * Fetch the (brand, offer, funnel) a campaign runs.
 *
 * Same read as {@link getCampaignFunnelKey} — one campaign-service call answers
 * both questions, so there is no second endpoint and no second convention. A 404
 * is null (an absent campaign has no scope); any OTHER failure throws so the
 * caller decides.
 */
export async function getCampaignTriggerScope(
  campaignId: string,
  orgId: string,
): Promise<CampaignTriggerScope | null> {
  const campaign = await fetchCampaign(campaignId, orgId);
  if (!campaign) return null;

  return {
    // One campaign is one brand for every outbound send this service performs;
    // the column is an array, so the first entry is the brand the leg is for.
    brandId: campaign.brandIds?.[0] ?? null,
    offerId: campaign.offerId ?? null,
    funnelKey: campaign.funnelKey ?? null,
  };
}

/** One campaign campaign-service DID run for the leg out of the step. */
export interface StepTriggerTriggered {
  campaignId: string;
  legKey: string | null;
  workflowSlug: string;
}

/** One campaign it deliberately did NOT run, and the named business reason why. */
export interface StepTriggerSkipped {
  campaignId: string;
  legKey: string | null;
  reason: string;
  detail: string;
}

export interface StepTriggerOutcome {
  funnelKey: string;
  step: string;
  legKeys: string[];
  triggered: StepTriggerTriggered[];
  skipped: StepTriggerSkipped[];
}

/**
 * Ask campaign-service to run the campaign bought for the leg OUT of a step a
 * lead just reached.
 *
 * campaign-service owns every decision here: which leg leaves the step (it reads
 * features-service's published catalogue), which campaign states that leg, and
 * whether that campaign may spend. This is the ASK and nothing else — no leg is
 * resolved on this side, no funnel is parsed, and no campaign is selected.
 *
 * An empty `legKeys` is the COMMON answer: most brands buy one leg of one funnel,
 * so a step nobody bought the leg out of is an ordinary 200 with nothing in it.
 * Anything that is not a 200 throws — the caller decides how loudly to fail.
 */
export async function triggerCampaignForStep(params: {
  orgId: string;
  brandId: string;
  offerId: string;
  funnelKey: string;
  step: string;
}): Promise<StepTriggerOutcome> {
  if (!CAMPAIGN_SERVICE_URL || !CAMPAIGN_SERVICE_API_KEY) {
    throw new Error("CAMPAIGN_SERVICE_URL or CAMPAIGN_SERVICE_API_KEY is not set");
  }

  const { orgId, ...body } = params;
  const response = await fetch(`${CAMPAIGN_SERVICE_URL}/internal/campaigns/trigger-for-step`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": CAMPAIGN_SERVICE_API_KEY,
      "x-org-id": orgId,
    },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    const detail = await response.text();
    throw new Error(
      `campaign-service POST /internal/campaigns/trigger-for-step failed: ${response.status} - ${detail.slice(0, 200)}`,
    );
  }

  return (await response.json()) as StepTriggerOutcome;
}

/**
 * Every campaign id that answers to the same campaign as `campaignId`, ascending.
 *
 * campaign-service keeps an ancestor row per workflow change, so a customer's
 * one campaign is routinely dozens of stored rows. It owns the identity, so it
 * is asked: one read for the campaign itself, one for its brand's campaigns,
 * then the rows sharing its identity key (see `campaign-identity.ts`).
 *
 * ⚠️ FAILS LOUD. A caller uses this to widen a read across the whole campaign;
 * degrading to the asked row alone would hand back a fraction of the answer
 * looking exactly like the whole of it. Two cases are NOT failures and return
 * `[campaignId]`: campaign-service does not know the campaign (404), and the
 * campaign states too little to be pooled with anything — both are genuinely a
 * campaign of one.
 *
 * The brand read is deliberately NOT narrowed by `featureSlug`: the feature is
 * no part of the identity, so narrowing by the asked row's own slug could drop a
 * sibling that states another one.
 */
export async function getCampaignFamily(
  campaignId: string,
  orgId: string,
): Promise<string[]> {
  const self = await fetchCampaign(campaignId, orgId);
  if (!self) return [campaignId];

  const selfRow: CampaignIdentityRow = { ...self, id: campaignId };
  if (identityKeyOf(selfRow) === null) return [campaignId];

  const brandId = selfRow.brandId ?? selfRow.brandIds?.[0];
  // identityKeyOf already proved a brand is stated; this is the type narrowing.
  if (!brandId) return [campaignId];

  const rows = await fetchBrandCampaigns(brandId, orgId);
  // The asked row is authoritative for its own identity — it was read directly,
  // and the brand list is only how its siblings are found.
  const merged = [selfRow, ...rows.filter((r) => r.id !== campaignId)];
  return familyOf(merged, campaignId);
}

/** Every campaign campaign-service holds for one brand of this org. Fails loud. */
async function fetchBrandCampaigns(
  brandId: string,
  orgId: string,
): Promise<CampaignIdentityRow[]> {
  if (!CAMPAIGN_SERVICE_URL || !CAMPAIGN_SERVICE_API_KEY) {
    throw new Error("CAMPAIGN_SERVICE_URL or CAMPAIGN_SERVICE_API_KEY is not set");
  }

  const params = new URLSearchParams({ brandId });
  const response = await fetch(`${CAMPAIGN_SERVICE_URL}/campaigns?${params}`, {
    headers: {
      "x-api-key": CAMPAIGN_SERVICE_API_KEY,
      "x-org-id": orgId,
      "x-brand-id": brandId,
    },
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(
      `campaign-service GET /campaigns?brandId=${brandId} failed: ${response.status} - ${body.slice(0, 200)}`,
    );
  }

  const body = (await response.json()) as { campaigns?: CampaignIdentityRow[] };
  if (!Array.isArray(body.campaigns)) {
    throw new Error(
      "campaign-service GET /campaigns returned no campaigns array",
    );
  }
  return body.campaigns;
}
