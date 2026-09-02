/**
 * campaign-service client — reads the FUNNEL a campaign runs.
 *
 * A funnel is a property of the CAMPAIGN, not of the brand: two campaigns of the
 * same brand can legitimately run different funnels, so reading this at brand
 * level answers a question nobody asked. campaign-service owns `funnel_key` and
 * is the only place it is a fact rather than an inference.
 */

const CAMPAIGN_SERVICE_URL = process.env.CAMPAIGN_SERVICE_URL;
const CAMPAIGN_SERVICE_API_KEY = process.env.CAMPAIGN_SERVICE_API_KEY;

/**
 * Funnel keys whose FIRST leg is a visit to the website.
 *
 * The key encodes the sales funnel the campaign runs, and its prefix is the leg that
 * opens it — so `visit_*` means the conversion starts on the site. That is
 * exactly when a click matters: the prospect is on the landing page and the
 * conversion happens there, so more cold email only distracts.
 *
 * `reply_meeting` deliberately does NOT stop: its conversion starts with a
 * REPLY, so a click says nothing about whether the sequence should continue.
 */
export const VISIT_FIRST_FUNNEL_PREFIX = "visit_";

/**
 * True when this campaign's funnel opens on a website visit.
 *
 * A NULL funnel does NOT stop. campaign-service's own rule is that "a funnel is
 * a fact, never a guess" — a campaign whose stated goal named no single funnel
 * keeps a null one, and pausing a live sequence on an unknown is the wrong
 * direction to be wrong in.
 */
export function funnelStopsOnClick(funnelKey: string | null | undefined): boolean {
  return typeof funnelKey === "string" && funnelKey.startsWith(VISIT_FIRST_FUNNEL_PREFIX);
}

interface CampaignRecord {
  brandIds?: string[] | null;
  offerId?: string | null;
  funnelKey?: string | null;
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
