/**
 * Stop-on-click for signup-maximizing brands.
 *
 * When a prospect CLICKS a link in a cold email (`email_link_clicked`) AND the
 * campaign's brand is currently maximizing signups (`current_goal === 'signup'`),
 * the lead is on the landing page — the conversion happens there, so continuing
 * the cold sequence only distracts. We PAUSE the lead's Instantly campaign.
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
 *
 * Always-on: gated only by the brand's runtime goal (`current_goal === 'signup'`)
 * and by fail-soft availability of brand-service. No env kill-switch — a click on
 * a non-signup brand is a natural no-op, and any error leaves the sequence running.
 */

import { resolveInstantlyApiKey } from "./key-client";
import { updateCampaignStatus } from "./instantly-client";
import { getCurrentGoals } from "./brand-client";

/** The `current_goal` value that means "maximize signups". */
export const SIGNUP_GOAL = "signup";

/**
 * True iff ANY brand in the set is currently maximizing signups. Multi-brand
 * campaigns stop if any member is in signup mode (signup-max is the aggressive
 * stop; most sends are single-brand anyway).
 */
export function anyGoalIsSignup(goals: string[]): boolean {
  return goals.some((g) => g === SIGNUP_GOAL);
}

/** The subset of a campaign row this side effect needs. */
export interface StopOnClickCampaign {
  instantlyCampaignId: string;
  orgId: string | null;
  brandIds?: string[] | null;
}

/**
 * Pause the lead's Instantly campaign iff the campaign's brand is maximizing
 * signups. Fully fail-soft: any error (brand-service down, key resolution,
 * Instantly pause) is swallowed and logged — the sequence simply continues.
 * NEVER throws into the webhook promote path (a 5xx would make Instantly
 * auto-pause the webhook).
 */
export async function maybeStopOnClickForSignup(
  campaign: StopOnClickCampaign,
  leadEmail: string,
): Promise<void> {
  if (!campaign.orgId) return;
  if (!campaign.brandIds || campaign.brandIds.length === 0) return;

  try {
    const goals = await getCurrentGoals(campaign.brandIds);
    if (!anyGoalIsSignup(goals)) return;

    const { key } = await resolveInstantlyApiKey(campaign.orgId, "system", {
      method: "POST",
      path: "/internal/stop-on-click",
    });
    await updateCampaignStatus(key, campaign.instantlyCampaignId, "paused");

    console.log(
      `[instantly-service] stop-on-click: paused campaign=${campaign.instantlyCampaignId} lead=${leadEmail} (brand goal=signup)`,
    );
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.warn(
      `[instantly-service] stop-on-click: no-op for campaign=${campaign.instantlyCampaignId} lead=${leadEmail} — ${message}; sequence continues`,
    );
  }
}
