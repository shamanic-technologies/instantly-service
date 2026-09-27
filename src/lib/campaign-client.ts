/**
 * campaign-service client — reads the LEG a campaign is bought for, the scope a
 * step trigger names, and which stored rows are ONE campaign.
 *
 * A campaign is (offer x leg x channel): campaign-service owns `offerId`,
 * `legKey` and `featureSlug`, and they are the only place those are facts
 * rather than inferences. What a leg MEANS (where it lands the prospect) is
 * features-service's statement, read through `leg-catalogue.ts`.
 */

import {
  familyOf,
  identityKeyOf,
  type CampaignIdentityRow,
} from "./campaign-identity";

const CAMPAIGN_SERVICE_URL = process.env.CAMPAIGN_SERVICE_URL;
const CAMPAIGN_SERVICE_API_KEY = process.env.CAMPAIGN_SERVICE_API_KEY;

interface CampaignRecord {
  id?: string | null;
  orgId?: string | null;
  brandId?: string | null;
  brandIds?: string[] | null;
  offerId?: string | null;
  legKey?: string | null;
  featureSlug?: string | null;
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

/** The leg a campaign is bought for, on the channel that performs it. */
export interface CampaignLeg {
  /** features-service's leg key, verbatim. NULL is a real state (a campaign bought for no leg). */
  legKey: string | null;
  /** The channel (feature) the campaign runs on — the catalogue entry the leg is read from. */
  featureSlug: string | null;
}

/** A campaign's leg, null when the campaign is absent. */
export async function getCampaignLeg(
  campaignId: string,
  orgId: string,
): Promise<CampaignLeg | null> {
  const campaign = await fetchCampaign(campaignId, orgId);
  if (!campaign) return null;
  return { legKey: campaign.legKey ?? null, featureSlug: campaign.featureSlug ?? null };
}

/**
 * The scope a step trigger names: the (brand, offer) the lead is on.
 *
 * Both come off the CAMPAIGN row campaign-service already owns — nothing here is
 * inferred. A campaign that states neither is not a campaign a leg can be
 * resolved for, which is an ordinary absence rather than an error.
 */
export interface CampaignTriggerScope {
  brandId: string | null;
  offerId: string | null;
}

/**
 * Fetch the (brand, offer) a campaign runs.
 *
 * Same read as {@link getCampaignLeg} — one campaign-service call answers
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
 * resolved on this side and no campaign is selected.
 *
 * An empty `legKeys` is the COMMON answer: most brands buy one leg of one offer,
 * so a step nobody bought the leg out of is an ordinary 200 with nothing in it.
 * Anything that is not a 200 throws — the caller decides how loudly to fail.
 */
export async function triggerCampaignForStep(params: {
  orgId: string;
  brandId: string;
  offerId: string;
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
