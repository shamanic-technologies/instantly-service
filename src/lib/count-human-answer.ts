/**
 * A message the brand's own person typed by hand on the thread is one of OUR
 * touches: the follow-ups go on (owner 2026-10-15, REPLY-ANY-LEAD-1015).
 *
 * Before, a `409 human_took_over` refusal on `POST /orgs/replies` handed the
 * thread to a person FOR GOOD (lead-service `kind:"handed"`, #1070): a founder
 * answering a prospect by hand ended every later follow-up. Now the refusal still
 * stops the responder from answering what the person already answered, and the
 * person's answer is stated as `acted` (count + 1, next due date), so the
 * prospect is followed up at the usual next date.
 *
 * The next date is the SAME growing ladder the responder applies after its own
 * answers (workflow-service `ai-meeting-booking` DAG, `compose-prompt`:
 * 3, 7, 21, 60, 180 days by follow-ups already sent), counted from the moment
 * the person answered, never earlier than a minute from now. Keep the two
 * ladders equal.
 *
 * Once stated, the answer is stamped on the thread
 * (`instantly_campaigns.human_answer_counted_at`) so the takeover gate stops
 * refusing on it (lib/human-takeover) and the follow-up due after it goes out.
 *
 * What is NOT counted, and stays a hand-over (lib/hand-followups-to-human):
 * a conversation held in a person's OWN mailbox (their Gmail: nothing automated
 * can write there) and `prospect_wrote_elsewhere`. A hand-over the responder
 * DECIDES (escalation) is untouched. A row with no live debt (stopped: opt-out,
 * decline, booked; handed; never due) is left alone: counting it would revive
 * follow-ups that ended for a reason.
 *
 * Fail-soft and loud: the refusal is already answered.
 */

import { sql } from "drizzle-orm";

import { db } from "../db";
import { findLeadOnCampaignByEmail, readFollowupState, recordFollowupActed } from "./lead-client";

/** Days to the next follow-up by follow-ups already sent (workflow-service's ladder). */
export const FOLLOWUP_LADDER_DAYS = [3, 7, 21, 60, 180] as const;

const DAY_MS = 86_400_000;
/** lead-service refuses a due date in the past: never earlier than this from now. */
const MIN_LEAD_MS = 60_000;

/** The next follow-up after a hand-typed answer at `answeredAt`. Pure. */
export function nextDueAfterHumanAnswer(
  answeredAt: string,
  followupCount: number,
  nowMs: number,
): string {
  const rung = FOLLOWUP_LADDER_DAYS[Math.min(Math.max(followupCount, 0), FOLLOWUP_LADDER_DAYS.length - 1)];
  const answeredMs = Date.parse(answeredAt);
  if (!Number.isFinite(answeredMs)) throw new Error(`unparseable answer time ${answeredAt}`);
  return new Date(Math.max(answeredMs + rung * DAY_MS, nowMs + MIN_LEAD_MS)).toISOString();
}

export type CountHumanAnswerOutcome =
  | "counted"
  | "no_lead_row"
  | "no_live_debt"
  | "failed";

export async function countHumanAnswerAsOurs(input: {
  orgId: string | null | undefined;
  /** The caller (outreach) campaign that holds the person. */
  campaignId: string | null | undefined;
  leadEmail: string;
  instantlyCampaignId: string;
  /** When the person answered, ISO 8601 UTC (from the takeover gate). */
  answeredAt: string;
  nowMs?: number;
}): Promise<CountHumanAnswerOutcome> {
  const tag = "[instantly-service] count-human-answer";
  if (!input.orgId || !input.campaignId) {
    console.warn(`${tag}: no org/campaign for ${input.leadEmail}; the hand-typed answer could not be counted`);
    return "no_lead_row";
  }
  try {
    const lead = await findLeadOnCampaignByEmail({
      orgId: input.orgId,
      campaignId: input.campaignId,
      email: input.leadEmail,
    });
    if (!lead) {
      console.warn(`${tag}: lead-service holds no row for ${input.leadEmail} on campaign=${input.campaignId}; nothing to count`);
      return "no_lead_row";
    }
    const state = await readFollowupState({ orgId: input.orgId, leadRowId: lead.id });
    if (!state || state.dueAt === null) {
      console.log(
        `${tag}: ${input.leadEmail} on campaign=${input.campaignId} holds no live follow-up (stopped=${state?.stoppedReason ?? "none"}); left alone`,
      );
      return "no_live_debt";
    }
    const nextDueAt = nextDueAfterHumanAnswer(input.answeredAt, state.followupCount, input.nowMs ?? Date.now());
    await recordFollowupActed({ orgId: input.orgId, leadRowId: lead.id, nextDueAt });
    await db.execute(sql`
      UPDATE instantly_campaigns
         SET human_answer_counted_at = GREATEST(
               COALESCE(human_answer_counted_at, ${input.answeredAt}::timestamptz),
               ${input.answeredAt}::timestamptz
             ),
             updated_at = now()
       WHERE instantly_campaign_id = ${input.instantlyCampaignId}
    `);
    console.log(
      `${tag}: a person answered ${input.leadEmail} by hand at ${input.answeredAt} (sequence ${input.instantlyCampaignId}); counted as our touch #${state.followupCount + 1}, next follow-up ${nextDueAt}`,
    );
    return "counted";
  } catch (error) {
    console.error(
      `${tag}: counting the hand-typed answer to ${input.leadEmail} on campaign=${input.campaignId} FAILED — ${error instanceof Error ? error.message : String(error)}; the responder will be refused again at its next claim`,
    );
    return "failed";
  }
}
