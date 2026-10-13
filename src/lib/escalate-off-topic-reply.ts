/**
 * A reply about something other than the offer goes to a person.
 *
 * The automated responder only handles SALES conversations: it is fed by the
 * sales-interest gate (`isSalesInterestQualification`), which a `lead_off_topic`
 * reply never passes, so it will not answer one. Before this, nothing else did
 * either — a partnership proposal, a job enquiry, an investor, a vendor pitching
 * us, a journalist all landed as `lead_neutral`, entered no queue and reached no
 * human. Measured 2026-09-28: jakub@marktize.com asked "can you explain?" on a
 * partnership thread and was never answered.
 *
 * The follow-up ladder is stopped so nothing automated writes to them
 * meanwhile (`handThreadToHuman`, lib/escalate-reply); the prospect is sent
 * nothing. Who is told follows the owner's model (2026-10-13): a brand that
 * pays for AI answers (a responder runs on its offer) gets the reply in its
 * team's inbox (lib/notify-brand-team), like any reply the AI cannot answer; a
 * brand that does not answers it by hand in the Unibox, and nobody is emailed.
 * Staff are never emailed.
 *
 * Fired from `promoteEvent` on the FIRST promotion of a real `lead_off_topic`
 * event — so a webhook retry or a re-read does not escalate twice. Fail-soft:
 * the classification is recorded whatever happens here, and a throw would 5xx
 * the path that promoted it.
 */

import { findOngoingResponderCampaigns, getCampaignTriggerScope } from "./campaign-client";
import { handThreadToHuman } from "./escalate-reply";
import type { TeamEscalationReason } from "./notify-brand-team";
import { isEscalatedReplyKind } from "./reply-kind";
import { OFF_TOPIC_HANDOFF_REASON, REFERRAL_HANDOFF_REASON } from "./escalation-handoff-reasons";

/** The subset of a campaign row this needs. */
export interface OffTopicCampaign {
  instantlyCampaignId: string;
  campaignId: string | null;
  orgId: string | null;
  userId: string | null;
  runId: string | null;
  brandIds?: string[] | null;
}

/** Why the ladder stops, and what the brand's team is told, per escalated kind. */
const WHY: Record<string, { reason: Exclude<TeamEscalationReason, "question">; stopReason: string }> = {
  lead_off_topic: {
    reason: "off_topic",
    stopReason: OFF_TOPIC_HANDOFF_REASON,
  },
  lead_referral: {
    reason: "referral",
    stopReason: REFERRAL_HANDOFF_REASON,
  },
};

/**
 * Does the brand pay for AI answers on this thread's offer (an ongoing
 * responder campaign on its brand and offer)? Null when it cannot be said (no
 * caller campaign, no brand). Throws when campaign-service cannot be read.
 */
export async function responderRuns(orgId: string, callerCampaignId: string | null): Promise<boolean | null> {
  if (!callerCampaignId) return null;
  const scope = await getCampaignTriggerScope(callerCampaignId, orgId);
  if (!scope || !scope.brandId) return null;
  if (!scope.offerId) return false;
  const responders = await findOngoingResponderCampaigns({ orgId, brandId: scope.brandId, offerId: scope.offerId });
  return responders.length > 0;
}

/**
 * Hand an escalated reply (off-topic, referral — `ESCALATED_REPLY_KINDS`) to a
 * person. No-op on any other event.
 *
 * Works for ANY brand: stopping the follow-up ladder is skipped when
 * lead-service holds no row; the team email needs a responder on the offer.
 *
 * Needs an org-scoped campaign carrying its run and user: the notification is a
 * child of that run (org-billed, the same identity the positive-reply forward
 * uses). A row without them is logged loudly and left, never escalated on a
 * made-up identity.
 */
export async function maybeEscalateOffTopicReply(
  campaign: OffTopicCampaign,
  leadEmail: string,
  eventType: string,
): Promise<void> {
  if (!isEscalatedReplyKind(eventType)) return;

  if (!campaign.orgId || !campaign.runId || !campaign.userId) {
    console.warn(
      `[instantly-service] off-topic-escalation: campaign=${campaign.instantlyCampaignId} lead=${leadEmail} has no org/run/user — NOT escalated, nobody was told`,
    );
    return;
  }

  try {
    const why = WHY[eventType] ?? WHY.lead_off_topic;
    const paysForAiAnswers = await responderRuns(campaign.orgId, campaign.campaignId).catch((error: unknown) => {
      console.error(
        `[instantly-service] off-topic-escalation: responder read FAILED for campaign=${campaign.instantlyCampaignId} lead=${leadEmail}, the brand's team is NOT emailed — ${error instanceof Error ? error.message : String(error)}`,
      );
      return null;
    });
    const result = await handThreadToHuman({
      instantlyCampaignId: campaign.instantlyCampaignId,
      campaignId: campaign.campaignId,
      leadEmail,
      brandId: campaign.brandIds?.[0] ?? null,
      orgId: campaign.orgId,
      userId: campaign.userId,
      runId: campaign.runId,
      notifyTeam: paysForAiAnswers ? why.reason : null,
      stopReason: why.stopReason,
    });
    console.log(
      `[instantly-service] off-topic-escalation: campaign=${campaign.instantlyCampaignId} lead=${leadEmail} handed to a human (team told: ${result.handedTo ?? "nobody"}, followupsStopped=${result.followupsStopped})`,
    );
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(
      `[instantly-service] off-topic-escalation FAILED for campaign=${campaign.instantlyCampaignId} lead=${leadEmail} — ${message}; the reply is classified, its ladder may still run`,
    );
  }
}
