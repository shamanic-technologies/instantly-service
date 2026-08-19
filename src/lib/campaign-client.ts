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
 * The key encodes the chain the campaign runs, and its prefix is the leg that
 * opens it — so `visit_*` means the conversion starts on the site. That is
 * exactly when a click matters: the prospect is on the landing page and the
 * conversion happens there, so more cold email only distracts.
 *
 * `reply_meeting` deliberately does NOT stop: its conversion starts with a
 * REPLY, so a click says nothing about whether the sequence should continue.
 */
export const VISIT_FIRST_FUNNEL_PREFIX = "visit_";

export interface CampaignFunnel {
  funnelKey: string | null;
}

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

/**
 * Fetch a campaign's funnel key.
 *
 * Org-scoped: campaign-service filters by `x-org-id`, so a campaign belonging to
 * another org simply 404s. Returns null for a 404 — an absent campaign has no
 * funnel, which is a legitimate answer rather than an error. Any OTHER failure
 * throws, so the caller can decide (here: fail soft, sequence continues).
 */
export async function getCampaignFunnelKey(
  campaignId: string,
  orgId: string,
): Promise<string | null> {
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

  const body = (await response.json()) as { campaign?: CampaignFunnel };
  return body.campaign?.funnelKey ?? null;
}
