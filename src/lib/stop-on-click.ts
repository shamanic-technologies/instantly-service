/**
 * Stop-on-click for campaigns whose funnel opens on a website visit.
 *
 * When a prospect CLICKS a link in a cold email (`email_link_clicked`) AND the
 * campaign runs a funnel whose first leg is a visit (`form_magnet`,
 * `website_purchases`, `sales_meetings_from_website`), the lead is on the landing page — the conversion happens
 * there, so continuing the cold sequence only distracts. We PAUSE the lead's
 * Instantly campaign.
 *
 * ⚠️ The gate is the CAMPAIGN's funnel, not the brand's goal. It used to read
 * `brands.current_goal === 'signup'` from brand-service, which was wrong on both
 * axes:
 *   - GRAIN. A funnel belongs to a campaign. Two campaigns of the same brand can
 *     run different funnels, so a brand-level answer is not an answer to the
 *     question being asked — it pauses sequences that should keep running, and
 *     misses ones that should stop.
 *   - SIGNAL. `current_goal` is a goal in the middle of being retired (see
 *     campaign-service migration 0042, which rewrites stated goals into stored
 *     funnels). `campaigns.funnel_key` is the fact that replaced it.
 *
 * `sales_meetings_from_conversation` deliberately does NOT stop: its conversion starts
 * with a REPLY, so a click says nothing about whether to keep sending. A NULL
 * funnel does not stop either — campaign-service's own rule is "a funnel is a
 * fact, never a guess", and pausing a live sequence on an unknown is the wrong
 * direction to be wrong in.
 *
 * A reply, by contrast, ALWAYS stops the sequence whatever its sentiment — that
 * is `reply_received` in `SEQUENCE_STOP_EVENTS`, entirely separate from this and
 * not conditioned on any funnel or goal. This side effect is only about clicks.
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
import { funnelStopsOnClick, getCampaignFunnelKey, isUnrecognisedFunnelKey } from "./campaign-client";
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
 * Pause the lead's Instantly campaign iff its funnel opens on a website visit.
 *
 * Fully fail-soft: any error (campaign-service down, key resolution, Instantly
 * pause) is swallowed and logged — the sequence simply continues. NEVER throws
 * into the webhook promote path (a 5xx would make Instantly auto-pause the
 * webhook).
 */
export async function maybeStopOnClickForFunnel(
  campaign: StopOnClickCampaign,
  leadEmail: string,
): Promise<void> {
  if (!campaign.orgId) return;
  // A platform send belongs to no caller campaign, so it runs no funnel and
  // there is nothing to read. Not an error — simply out of scope.
  if (!campaign.campaignId) return;

  try {
    const funnelKey = await getCampaignFunnelKey(campaign.campaignId, campaign.orgId);

    // A funnel we do not recognise is treated as no funnel (we never guess), but it is NOT the
    // same fact as a campaign that stated none — it is what a vocabulary rename looks like from
    // here, and the last one took the whole fleet's stop-on-click silent for weeks with nothing in
    // the logs to see. Say so, once per click, and the next rename shows up the day it lands.
    if (isUnrecognisedFunnelKey(funnelKey)) {
      console.warn(
        `[instantly-service] stop-on-click: unrecognised funnel key "${funnelKey}" on campaign=${campaign.campaignId} ` +
          `— treating as no funnel; if campaign-service renamed the vocabulary, funnelStopsOnClick is now blind to it`,
      );
      return;
    }

    if (!funnelStopsOnClick(funnelKey)) return;

    // A sequence WE dispatch has no Instantly campaign to pause, and reconcile
    // skips a `self:` row outright — so the stop has to be performed locally,
    // holds included, or it would not happen at all.
    if (isSelfSendCampaignId(campaign.instantlyCampaignId)) {
      await stopSelfSendSequence(campaign, leadEmail, `stop-on-click funnel=${funnelKey}`);
      return;
    }

    const { key } = await resolveInstantlyApiKey(campaign.orgId, "system", {
      method: "POST",
      path: "/internal/stop-on-click",
    });
    await updateCampaignStatus(key, campaign.instantlyCampaignId, "paused");

    console.log(
      `[instantly-service] stop-on-click: paused campaign=${campaign.instantlyCampaignId} lead=${leadEmail} (funnel=${funnelKey})`,
    );
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.warn(
      `[instantly-service] stop-on-click: no-op for campaign=${campaign.instantlyCampaignId} lead=${leadEmail} — ${message}; sequence continues`,
    );
  }
}
