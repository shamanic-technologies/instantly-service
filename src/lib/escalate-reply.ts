/**
 * The responder cannot answer this one, so it stops and a human is told.
 *
 * A prospect asks for something we do not hold — a price, a spec, a reference,
 * a date nobody has committed to. The drafting model's response schema REQUIRES
 * a reply body, so it has no way to say "I cannot answer this": the only move
 * left to it is a deflection that re-invites to the call, and the next rung of
 * the follow-up ladder does it again. That is the insisting.
 *
 * This is the exit. It sends the prospect NOTHING, forwards the exchange to the
 * agency inbox naming the question that could not be answered, and empties the
 * follow-up schedule so the ladder stops. A human then answers, or does not.
 *
 * ⚠️ WHAT THIS DOES *NOT* DO IS DECIDE. Whether a question is answerable is a
 * judgement about the draft, and it can only be made where the draft is made —
 * in workflow-service's DAG, which holds the prompt, the brand facts and the
 * model's own answer. This service owns the two halves that judgement needs and
 * the drafter does not have: the mailbox and the thread (so it can forward the
 * conversation) and the campaign identity (so it can name the lead row whose
 * schedule stops). Do NOT add a content heuristic here; a caller declares.
 *
 * ⚠️ NOTHING IS RETROACTIVE AND NOTHING IS PERMANENT. A stop is not a tombstone
 * by lead-service's own model: if the prospect writes again, the reply side
 * effects re-enqueue them and qualification re-decides. That is correct — a new
 * message is a new decision.
 */

import { sendThreadForward } from "./forward-positive-reply";
import { findLeadOnCampaignByEmail, stopFollowups } from "./lead-client";
import { loadCampaign } from "./reply-to-lead";

/**
 * A refusal a caller can branch on, mirroring `ReplyToLeadError`'s shape so the
 * route handles both the same way.
 *
 * Its own class rather than a borrowed one: a `question_required` reported under
 * `campaign_not_found` would send a caller to look for a campaign that is
 * perfectly fine.
 */
export class EscalateReplyError extends Error {
  constructor(
    public readonly code: "campaign_not_found" | "question_required",
    public readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = "EscalateReplyError";
  }
}

export interface EscalateReplyInput {
  orgId: string;
  userId: string;
  /** Logical campaign id — the same key the reply route takes. */
  campaignId: string;
  leadEmail: string;
  /**
   * What they asked that we could not answer, in their own words.
   *
   * Required and non-empty. It is the whole point of the notification — "the
   * responder gave up on this thread" is not actionable, and lead-service
   * refuses a stop with no reason for the same reason.
   */
  question: string;
}

export interface EscalateReplyResult {
  instantlyCampaignId: string;
  leadEmail: string;
  /** How many messages of the exchange went to the agency inbox. */
  threadMessages: number;
  /**
   * Whether the follow-up ladder was stopped.
   *
   * False when lead-service holds no row for this person on this campaign —
   * which is a real state (a platform send, a lead registered elsewhere), not a
   * failure. The human was still told, which is the part that cannot be missed.
   */
  followupsStopped: boolean;
}

/**
 * Hand a thread to a human and stop the automated ladder.
 *
 * Order is deliberate: FORWARD first, stop second. A crash between the two
 * leaves a human informed on a thread still scheduled, which somebody can see
 * and undo; the reverse leaves a silently stopped ladder nobody was told about,
 * and the prospect simply never hears from anyone again.
 *
 * FAILS LOUD on the forward. The notification IS the deliverable — an
 * escalation that stopped the ladder and told nobody is strictly worse than not
 * escalating, because the sequence was at least still talking to them.
 */
export async function escalateReply(
  input: EscalateReplyInput,
): Promise<EscalateReplyResult> {
  const question = input.question.trim();
  if (!question) {
    throw new EscalateReplyError(
      "question_required",
      400,
      "question is required — an escalation with nothing to answer is not actionable",
    );
  }

  const campaign = await loadCampaign(
    input.orgId,
    input.campaignId,
    input.leadEmail,
  );
  if (!campaign) {
    throw new EscalateReplyError(
      "campaign_not_found",
      404,
      `No campaign ${input.campaignId} in this org for ${input.leadEmail}`,
    );
  }

  const threadMessages = await sendThreadForward(
    {
      instantlyCampaignId: campaign.instantlyCampaignId,
      campaignId: campaign.campaignId,
      orgId: input.orgId,
      userId: input.userId,
      runId: null,
      brandIds: campaign.brandId ? [campaign.brandId] : null,
    },
    campaign.leadEmail,
    {
      eventType: "reply-escalation",
      metadata: { leadEmail: campaign.leadEmail, question },
    },
  );

  // The ladder stops only if lead-service holds this person on this campaign.
  // It is the SAME narrowing-plus-exact-match the rep call uses, so the two
  // cannot disagree about which row a lead is; anything other than exactly one
  // match resolves to null and the schedule is left alone rather than a
  // stranger's being emptied.
  const lead = await findLeadOnCampaignByEmail({
    orgId: input.orgId,
    campaignId: campaign.campaignId,
    email: campaign.leadEmail,
  });

  if (!lead) {
    console.warn(
      `[instantly-service] reply-escalation: forwarded campaign=${campaign.instantlyCampaignId} lead=${campaign.leadEmail} but lead-service holds no row for them, so the follow-up ladder is UNCHANGED`,
    );
    return {
      instantlyCampaignId: campaign.instantlyCampaignId,
      leadEmail: campaign.leadEmail,
      threadMessages,
      followupsStopped: false,
    };
  }

  await stopFollowups({
    orgId: input.orgId,
    leadRowId: lead.id,
    reason: `The automated responder could not answer: ${question}`,
  });

  console.log(
    `[instantly-service] reply-escalation: campaign=${campaign.instantlyCampaignId} lead=${campaign.leadEmail} handed to a human, follow-up ladder stopped`,
  );

  return {
    instantlyCampaignId: campaign.instantlyCampaignId,
    leadEmail: campaign.leadEmail,
    threadMessages,
    followupsStopped: true,
  };
}
