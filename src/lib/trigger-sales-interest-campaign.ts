/**
 * A reply was qualified as a sales interest — ask campaign-service to run the
 * campaign bought for the leg OUT of that step, now.
 *
 * Until this existed, a reply was classified and then nothing ran. campaign-service
 * schedules everything on a clock, so the campaign a customer funded to answer an
 * interested buyer waited for its next tick: a prospect who says "yes, interested"
 * heard nothing for a day, which is precisely the problem that leg was bought to
 * solve. campaign-service shipped the entry point (`POST
 * /internal/campaigns/trigger-for-step`); nothing in the fleet was asking it.
 *
 * This service is where the reply ARRIVES and where it is QUALIFIED, so this is
 * where the ask belongs.
 *
 * ── WHAT IS ASKED, AND WHAT IS NOT DECIDED HERE ─────────────────────────────────
 *
 * The ask names a scope — (brand, offer, funnel) — plus the STEP the lead reached,
 * and nothing else. Every decision downstream of that is campaign-service's:
 * which leg leaves the step (it reads features-service's published catalogue),
 * which campaign states that leg, and whether that campaign may spend (the run
 * starts at `gate-check` like any other, so the affordability gate is untouched).
 * No leg is resolved here, no funnel is parsed, and no campaign is selected.
 *
 * The scope is read off the CALLER campaign — campaign-service's own row, via
 * `getCampaignTriggerScope`. Nothing is inferred: a campaign that states no brand,
 * no offer or no funnel cannot have a leg resolved for it, so the ask is simply
 * not made. That is an ordinary absence (a platform send has no caller campaign at
 * all), not a failure.
 *
 * ── WHICH REPLIES ───────────────────────────────────────────────────────────────
 *
 * `conversation` — features-service's step key, labelled "Sales interest": *a buyer
 * answers and a conversation opens*. The reply kinds that mean exactly that are the
 * ones `REPLY_KIND_CLASSIFICATION` reports as `positive`, which is the SAME signal
 * the fleet already prices and counts as the brand's sales interest.
 *
 * ⚠️ That is deliberately NOT `POSITIVE_REPLY_KINDS` (the forward-to-the-agency-inbox
 * set), which additionally contains `lead_referral`. The two answer different
 * questions and the divergence is documented on `REPLY_KIND_CLASSIFICATION`:
 * forwarding asks "is this worth a human's eyes" — a name to talk to obviously is —
 * while this asks "did this buyer open a conversation", which "not me, but talk to
 * X" did not. Firing a funded meeting-booking campaign at someone who just told us
 * they are the wrong person is the exact mistake that map exists to prevent. Do NOT
 * restore a lockstep with the forward set.
 *
 * ── FAIL SOFT, AND LOUDLY ───────────────────────────────────────────────────────
 *
 * Qualifying the reply is the primary job; the trigger is a consequence of it, so
 * it can NEVER change the qualification's outcome. Every failure is swallowed and
 * logged — a throw here would also 5xx the webhook, and Instantly auto-pauses a
 * webhook that keeps failing. Nothing is retried on this side: sales-lead-service
 * already holds a due-date follow-up queue a scheduled sweep drains, and the
 * campaign will reach the lead on its own next tick regardless — a missed trigger
 * costs latency, never the answer itself.
 *
 * A brand with no such campaign is the COMMON case, not an error: most brands buy
 * one leg of one funnel, so campaign-service answers 200 with an empty `legKeys`
 * or a NAMED skip (unfunded, run already in flight, channel operated by the
 * customer's own team). Those are logged as the ordinary answers they are. Only an
 * unexpected failure warns.
 */

import {
  getCampaignTriggerScope,
  triggerCampaignForStep,
  type StepTriggerOutcome,
} from "./campaign-client";
import { REPLY_KIND_CLASSIFICATION, isReplyKind } from "./reply-kind";

/**
 * features-service's step key for "Sales interest" — *a buyer answers and a
 * conversation opens, on whatever medium the channel runs on*
 * (`GET /public/channels` → `steps[]`). Carried VERBATIM: campaign-service matches
 * it against the catalogue and refuses a step nobody publishes.
 */
export const SALES_INTEREST_STEP_KEY = "conversation";

/**
 * True iff this event says a buyer opened a conversation — i.e. its reply kind is
 * what the fleet reports as the brand's sales interest.
 */
export function isSalesInterestQualification(eventType: string): boolean {
  return isReplyKind(eventType) && REPLY_KIND_CLASSIFICATION[eventType] === "positive";
}

/** The subset of a campaign row this side effect needs. */
export interface SalesInterestTriggerCampaign {
  instantlyCampaignId: string;
  /** The CALLER campaign id — the one campaign-service owns. Null on a platform send. */
  campaignId: string | null;
  orgId: string | null;
}

/** Log what campaign-service did, so a quiet no-op stays distinguishable from a bug. */
function logOutcome(
  campaign: SalesInterestTriggerCampaign,
  leadEmail: string,
  outcome: StepTriggerOutcome,
): void {
  const triggered = outcome.triggered.map((t) => `${t.campaignId}(${t.workflowSlug})`).join(", ");
  const skipped = outcome.skipped.map((s) => `${s.campaignId}:${s.reason}`).join(", ");
  console.log(
    `[instantly-service] sales-interest-trigger: campaign=${campaign.instantlyCampaignId} ` +
      `lead=${leadEmail} funnel=${outcome.funnelKey} legs=[${outcome.legKeys.join(", ")}] ` +
      `triggered=[${triggered}] skipped=[${skipped}]`,
  );
}

/**
 * Ask campaign-service to run the campaign responsible for the leg out of the
 * sales-interest step. No-op unless the event is a sales-interest qualification and
 * the caller campaign states a resolvable scope. Fully fail-soft — never throws.
 */
export async function maybeTriggerSalesInterestCampaign(
  campaign: SalesInterestTriggerCampaign,
  leadEmail: string,
  eventType: string,
): Promise<void> {
  if (!isSalesInterestQualification(eventType)) return;
  if (!campaign.orgId) return;
  // A platform send belongs to no caller campaign, so it is on no funnel and no
  // offer. There is nothing to name, so nothing is asked.
  if (!campaign.campaignId) return;

  try {
    const scope = await getCampaignTriggerScope(campaign.campaignId, campaign.orgId);
    // An absent campaign, or one stating no brand / offer / funnel, cannot have a
    // leg resolved for it. Naming a scope we do not hold would be a guess.
    if (!scope || !scope.brandId || !scope.offerId || !scope.funnelKey) return;

    const outcome = await triggerCampaignForStep({
      orgId: campaign.orgId,
      brandId: scope.brandId,
      offerId: scope.offerId,
      funnelKey: scope.funnelKey,
      step: SALES_INTEREST_STEP_KEY,
    });

    logOutcome(campaign, leadEmail, outcome);
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.warn(
      `[instantly-service] sales-interest-trigger: no-op for campaign=${campaign.instantlyCampaignId} ` +
        `lead=${leadEmail} — ${message}; the qualification stands and the campaign runs on its next tick`,
    );
  }
}
