/**
 * A reply that is not a reason to keep answering — stop that person's pending
 * follow-ups in lead-service.
 *
 * The mirror of `enqueue-followup-on-interest.ts`. That side effect ENTERS a
 * buyer into lead-service's follow-up queue the moment they say yes; nothing
 * ever took them back out when they then said no. lead-service's own contract
 * names the stop — a person stops being due when "they answered again: the
 * observer of that reply says so" — and this service is that observer.
 *
 * Prod 2026-09-24 (cynthia@springspine.net, Doc Dinners): "We are interested"
 * at 17:04 enqueued her; the responder answered with slots and scheduled the
 * next rung; "my previous email was sent in error, we are not interested" at
 * 17:22 re-classified the campaign row negative and left the schedule alone.
 * On 09-28 lead-service handed her out again, the drafter had nothing to say,
 * and an escalation reached the agency for a message that asked nothing.
 *
 * ── WHICH REPLIES STOP ──────────────────────────────────────────────────────────
 *
 * Every NEGATIVE reply kind (not interested, wrong person, changed job, asked to
 * stop), `lead_referral` ("not me, talk to X" — not this person opening a
 * conversation, the same reason it is excluded from the enqueue gate), and a
 * real `lead_unsubscribed` (a clicked link, or a recorded opt-out, which
 * promotes that same event). NOT `lead_neutral` — "let me check with my team"
 * inside an interested thread is a reason to keep answering — and NOT the
 * automated kinds: an out-of-office says nothing about the conversation.
 *
 * ── ONLY WHAT IS PENDING ────────────────────────────────────────────────────────
 *
 * The state is READ first and the stop is posted only when a due date or a claim
 * stands. lead-service records a stop reason on the row whatever it held, and
 * the customer's lead timeline renders it — stopping an empty schedule would put
 * "follow-ups stopped" on the page of every lead who ever declined, which never
 * happened. A stop is not a tombstone: if the person writes again with interest,
 * the enqueue side effect re-enters them.
 *
 * ── FAIL SOFT, AND LOUDLY ───────────────────────────────────────────────────────
 *
 * Same contract as the enqueue: the classification is the primary job and
 * stands whatever this does, and a throw here would 5xx Instantly's webhook.
 * Every failure is swallowed and WARNED with its reason — a silent failure is
 * exactly the state that let a declining prospect be chased.
 */

import { findLeadOnCampaignByEmail, readFollowupState, stopFollowups } from "./lead-client";
import { REPLY_KIND_CLASSIFICATION, isReplyKind } from "./reply-kind";
import type { SalesInterestTriggerCampaign } from "./trigger-sales-interest-campaign";

export type FollowupStopCampaign = SalesInterestTriggerCampaign;

/** Does this real event mean nobody should keep answering this person? */
export function isFollowupStoppingEvent(eventType: string): boolean {
  if (eventType === "lead_unsubscribed") return true;
  if (eventType === "lead_referral") return true;
  return isReplyKind(eventType) && REPLY_KIND_CLASSIFICATION[eventType] === "negative";
}

/**
 * Stop the person's pending follow-ups on this campaign, if any stand.
 *
 * No-op unless the event stops follow-ups on an org-scoped send naming a caller
 * campaign. Fully fail-soft — never throws.
 */
export async function maybeStopFollowupsOnDecline(
  campaign: FollowupStopCampaign,
  leadEmail: string,
  eventType: string,
): Promise<void> {
  if (!isFollowupStoppingEvent(eventType)) return;
  if (!campaign.orgId) return;
  // A platform send belongs to no caller campaign, so nothing was ever enqueued.
  if (!campaign.campaignId) return;

  const tag =
    `[instantly-service] followup-stop: campaign=${campaign.instantlyCampaignId} ` +
    `lead=${leadEmail} event=${eventType}`;

  try {
    const lead = await findLeadOnCampaignByEmail({
      orgId: campaign.orgId,
      campaignId: campaign.campaignId,
      email: leadEmail,
    });
    if (!lead) {
      console.warn(
        `${tag} — lead-service holds no single row for this address on campaign ` +
          `${campaign.campaignId}; nothing stopped`,
      );
      return;
    }

    const state = await readFollowupState({ orgId: campaign.orgId, leadRowId: lead.id });
    if (!state || (state.dueAt === null && state.claimedAt === null)) return;

    await stopFollowups({
      orgId: campaign.orgId,
      leadRowId: lead.id,
      reason: `reply:${eventType}`,
    });
    console.log(`${tag} leadRow=${lead.id} — stopped (was due ${state.dueAt ?? "claimed"})`);
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.warn(
      `${tag} — FAILED: ${message}; the classification stands, but this person may ` +
        `still be handed out for a follow-up`,
    );
  }
}
