/**
 * A thread became a PERSON's: tell lead-service so no automated responder ever
 * claims it again (owner 2026-10-11, Shockwavecenters).
 *
 * Every place this service hands a thread to a human states it here: the client
 * is asked to answer (lib/ask-client-to-answer), the responder escalates
 * (lib/escalate-reply), and a person already answered in the thread
 * (`human_took_over` on POST /orgs/replies). lead-service owns the queue and the
 * claim; we own the fact, so we state it (the queue's model: whoever OBSERVES
 * says so). The 409 gates in lib/reply-to-lead stay as the last line.
 *
 * Fail-soft and loud: the hand-over itself already happened (an email went
 * out, a refusal was answered); a failed statement must not undo or 5xx it.
 * The campaign is the caller (outreach) campaign that holds the person.
 */

import { findLeadOnCampaignByEmail, handFollowupsToHuman } from "./lead-client";

export async function handThreadFollowupsToHuman(input: {
  orgId: string | null | undefined;
  campaignId: string | null | undefined;
  leadEmail: string;
  reason: string;
  /** Log prefix naming the caller. */
  tag: string;
}): Promise<boolean> {
  if (!input.orgId || !input.campaignId) {
    console.warn(
      `[instantly-service] ${input.tag}: no org/campaign for ${input.leadEmail}, so the thread could not be marked handed in lead-service`,
    );
    return false;
  }
  try {
    const lead = await findLeadOnCampaignByEmail({
      orgId: input.orgId,
      campaignId: input.campaignId,
      email: input.leadEmail,
    });
    if (!lead) {
      console.warn(
        `[instantly-service] ${input.tag}: lead-service holds no row for ${input.leadEmail} on campaign=${input.campaignId}; nothing to mark handed`,
      );
      return false;
    }
    await handFollowupsToHuman({ orgId: input.orgId, leadRowId: lead.id, reason: input.reason });
    console.log(
      `[instantly-service] ${input.tag}: thread ${input.leadEmail} on campaign=${input.campaignId} marked HANDED in lead-service (${input.reason})`,
    );
    return true;
  } catch (error) {
    console.error(
      `[instantly-service] ${input.tag}: marking ${input.leadEmail} on campaign=${input.campaignId} HANDED FAILED — ${error instanceof Error ? error.message : String(error)}; an automated responder could still claim this thread`,
    );
    return false;
  }
}
