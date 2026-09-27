/**
 * Stop-on-click for campaigns bought for a leg that lands on a website visit.
 *
 * When a prospect CLICKS a link in a cold email (`email_link_clicked`) AND the
 * campaign is bought for a leg whose ARRIVAL step is `website_visit` (today:
 * `start_to_website_visit`), the lead is on the brand's site — the conversion
 * happens there, so continuing the cold sequence only distracts. We PAUSE the
 * lead's Instantly campaign.
 *
 * ⚠️ The gate is the CAMPAIGN's leg, read from campaign-service (`legKey`), and
 * what the leg means is read from features-service's published leg catalogue
 * (`GET /public/channels` → `stepTransitions[].to.key`). The leg key is never
 * parsed. This replaced a gate on the campaign's sales funnel, a concept the
 * fleet retired; before that it read the brand's `current_goal`, which was the
 * wrong GRAIN (two campaigns of one brand can be bought for different legs).
 *
 * A leg landing on `conversation` deliberately does NOT stop: its conversion
 * starts with a REPLY, so a click says nothing about whether to keep sending. A
 * NULL leg does not stop either — pausing a live sequence on an unknown is the
 * wrong direction to be wrong in. A leg the catalogue does not know is WARNED
 * (that is what a vocabulary rename looks like from here) and does not stop.
 *
 * A reply, by contrast, ALWAYS stops the sequence whatever its sentiment — that
 * is `reply_received` in `SEQUENCE_STOP_EVENTS`, entirely separate from this and
 * not conditioned on any leg. This side effect is only about clicks.
 *
 * Placement: fired as a fail-soft side effect from `promoteEvent` in
 * silver-promote.ts, on REAL (non-inferred) click events only.
 *
 * Minimal by design — this only PAUSES on Instantly. The existing machinery
 * handles everything downstream, for free:
 *   - retry-stuck's live-status preflight sees the paused campaign and SKIPS
 *     redispatch (never resurrects it).
 *   - the nightly reconcile discovers the paused Instantly status → its finish
 *     closure cancels the lead's remaining provisioned holds (credit refund),
 *     deletes the contact (quota reclaim, if enabled) and marks the local row
 *     terminal.
 * So no local status write, no cost cancel, no contact delete is duplicated here.
 */

import { resolveInstantlyApiKey } from "./key-client";
import { updateCampaignStatus } from "./instantly-client";
import { getCampaignLeg } from "./campaign-client";
import { WEBSITE_VISIT_STEP_KEY, getChannelCatalogue, legArrivalStep } from "./leg-catalogue";
import { isSelfSendCampaignId } from "./self-send/transport";
import { stopSelfSendSequence } from "./self-send/stop-sequence";

/** The subset of a campaign row this side effect needs. */
export interface StopOnClickCampaign {
  instantlyCampaignId: string;
  /** The CALLER campaign id — the one campaign-service owns. Null on a platform send. */
  campaignId: string | null;
  orgId: string | null;
  userId: string | null;
  runId: string | null;
}

/**
 * Pause the lead's Instantly campaign iff its leg lands on a website visit.
 *
 * Fully fail-soft: any error (campaign-service or features-service down, key
 * resolution, Instantly pause) is swallowed and logged — the sequence simply
 * continues. NEVER throws into the webhook promote path (a 5xx would make
 * Instantly auto-pause the webhook).
 */
export async function maybeStopOnClickForLeg(
  campaign: StopOnClickCampaign,
  leadEmail: string,
): Promise<void> {
  if (!campaign.orgId) return;
  // A platform send belongs to no caller campaign, so it is bought for no leg and
  // there is nothing to read. Not an error — simply out of scope.
  if (!campaign.campaignId) return;

  try {
    const leg = await getCampaignLeg(campaign.campaignId, campaign.orgId);
    if (!leg || !leg.legKey) return;

    const arrival = legArrivalStep(await getChannelCatalogue(), leg.featureSlug, leg.legKey);

    // A leg the catalogue does not know is treated as no leg (we never guess), but it is NOT the
    // same fact as a campaign bought for none — it is what a vocabulary rename looks like from
    // here. Say so, once per click, so the next rename shows up the day it lands.
    if (arrival === null) {
      console.warn(
        `[instantly-service] stop-on-click: leg "${leg.legKey}" (feature=${leg.featureSlug}) on campaign=${campaign.campaignId} ` +
          `is not in features-service's leg catalogue — treating as no leg; the sequence continues`,
      );
      return;
    }

    if (arrival !== WEBSITE_VISIT_STEP_KEY) return;

    // A sequence WE dispatch has no Instantly campaign to pause, and reconcile
    // skips a `self:` row outright — so the stop has to be performed locally,
    // holds included, or it would not happen at all.
    if (isSelfSendCampaignId(campaign.instantlyCampaignId)) {
      await stopSelfSendSequence(campaign, leadEmail, `stop-on-click leg=${leg.legKey}`);
      return;
    }

    const { key } = await resolveInstantlyApiKey(campaign.orgId, "system", {
      method: "POST",
      path: "/internal/stop-on-click",
    });
    await updateCampaignStatus(key, campaign.instantlyCampaignId, "paused");

    console.log(
      `[instantly-service] stop-on-click: paused campaign=${campaign.instantlyCampaignId} lead=${leadEmail} (leg=${leg.legKey})`,
    );
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.warn(
      `[instantly-service] stop-on-click: no-op for campaign=${campaign.instantlyCampaignId} lead=${leadEmail} — ${message}; sequence continues`,
    );
  }
}
