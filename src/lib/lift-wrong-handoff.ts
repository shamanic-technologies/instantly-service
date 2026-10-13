/**
 * A reply judged a sales interest lifts the hand-over a WRONG verdict on it wrote.
 *
 * The off-topic / referral escalation (`escalate-off-topic-reply.ts`) states
 * `handed` in lead-service, and a handed row is permanent: no responder claims
 * it again, a later `scheduled` does not re-enter it. That is right while the
 * verdict holds. When the same thread is re-judged a sales interest (a fixed
 * classifier, a person correcting the kind), the hand-over rests on a verdict
 * that no longer exists, and the buyer is never answered.
 *
 * Prod 2026-10-13, DefiniteJobs: Sam's "yes we are hiring, a free listing
 * sounds good, can we have a quick call?" was called off topic and handed; once
 * re-judged a meeting request, the AI Meeting Booking responder claimed and got
 * nothing due, so Sam stayed unanswered.
 *
 * What is lifted, and what never is:
 *   - ONLY a hand-over this service's escalation wrote, named by the reason it
 *     wrote (`ESCALATION_HANDOFF_REASONS`). lead-service lifts nothing else: a
 *     person's own hand-over, its pre-responder debt hand-over, a hand-over
 *     because a person answered.
 *   - NEVER when a person has answered on the thread (`findHumanTakeover`): the
 *     thread is theirs, whatever the verdict became (the Shockwavecenters
 *     guarantee).
 *
 * Runs before the follow-up is scheduled, so the `scheduled` that follows
 * re-enters the person. Fail-soft and loud, like the enqueue it precedes.
 */

import { findHumanTakeover } from "./human-takeover";
import { findLeadOnCampaignByEmail, withdrawHandoff } from "./lead-client";
import { ESCALATION_HANDOFF_REASONS } from "./escalation-handoff-reasons";

/** Stored by lead-service as the withdrawal's history. */
export const WITHDRAWAL_REASON =
  "The reply was re-judged a sales interest; the off-topic or referral verdict that handed it to a person was wrong";

export interface LiftHandoffCampaign {
  instantlyCampaignId: string;
  campaignId: string | null;
  orgId: string | null;
}

export type LiftHandoffOutcome = "lifted" | "nothing_to_lift" | "person_answered" | "no_lead_row";

export async function liftWrongEscalationHandoff(
  campaign: LiftHandoffCampaign,
  leadEmail: string,
): Promise<LiftHandoffOutcome> {
  if (!campaign.orgId || !campaign.campaignId) return "no_lead_row";

  if (await findHumanTakeover(campaign.instantlyCampaignId)) return "person_answered";

  const lead = await findLeadOnCampaignByEmail({
    orgId: campaign.orgId,
    campaignId: campaign.campaignId,
    email: leadEmail,
  });
  if (!lead) return "no_lead_row";

  for (const handedReason of ESCALATION_HANDOFF_REASONS) {
    const lifted = await withdrawHandoff({
      orgId: campaign.orgId,
      leadRowId: lead.id,
      handedReason,
      reason: WITHDRAWAL_REASON,
    });
    if (lifted) return "lifted";
  }
  return "nothing_to_lift";
}
