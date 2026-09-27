/**
 * Draining what is left of the waiting room — IO glue around the pure plan in
 * `scheduled-replies.ts`.
 *
 * Nothing is enqueued any more (answers go out immediately, see
 * `replyToLead`); this empties the rows held before that change. It rides the
 * existing self-send dispatch run, after the same mailbox poll, so a prospect
 * who has meanwhile written again is read before we answer them.
 *
 * ⚠️ It drains replies on BOTH transports, because a reply is dispatched by
 * `replyToLead`, which branches on the campaign's frozen transport — and so it
 * rides `SELF_SEND_DISPATCH_ENABLED`, the switch that gates the whole sweep.
 */

import {
  loadPendingScheduledReplies,
  markScheduledReplyFailed,
  markScheduledReplySent,
  markScheduledReplySuperseded,
  planScheduledReplies,
  type ScheduledReply,
} from "./scheduled-replies";
import { replyToLead } from "./reply-to-lead";
import { scheduleFollowupByEmail } from "./lead-client";
import { triggerSalesInterestLeg } from "./trigger-sales-interest-campaign";

export interface ScheduledReplySummary {
  /** Waiting replies read. */
  pending: number;
  /** Of those, the ones whose words were still true and were sent now. */
  due: number;
  sent: number;
  /** Drafts too old to send, superseded and handed back for a fresh answer. */
  redrafted: number;
  failed: number;
}

/**
 * Hand a stale automated draft back to the responder.
 *
 * Re-queue FIRST (fail-loud), mark superseded SECOND: if lead-service cannot take
 * the lead back, the row stays pending with the error and is retried — marking
 * it first would drop a buyer who is owed an answer with nothing left to answer
 * them. The trigger is fail-soft: without it the responder's campaign reaches
 * the queue on its own next tick.
 */
async function redraft(reply: ScheduledReply, asOf: Date): Promise<void> {
  await scheduleFollowupByEmail({
    orgId: reply.orgId,
    campaignId: reply.campaignId,
    email: reply.leadEmail,
    dueAt: asOf.toISOString(),
  });
  await triggerSalesInterestLeg(
    {
      instantlyCampaignId: reply.instantlyCampaignId,
      campaignId: reply.campaignId,
      orgId: reply.orgId,
    },
    reply.leadEmail,
  );
  await markScheduledReplySuperseded(reply.id);
}

/**
 * Send every waiting reply whose words are still true; hand the rest back.
 *
 * Fail-loud PER REPLY: one dead mailbox must not stop the rest of the queue,
 * and every failure is recorded on its own row with the error that caused it.
 */
export async function dispatchScheduledReplies(
  asOf: Date,
): Promise<ScheduledReplySummary> {
  const pending = await loadPendingScheduledReplies();
  const plan = planScheduledReplies(pending, asOf);

  const summary: ScheduledReplySummary = {
    pending: pending.length,
    due: plan.send.length,
    sent: 0,
    redrafted: 0,
    failed: 0,
  };

  const fail = async (reply: ScheduledReply, error: unknown): Promise<void> => {
    await markScheduledReplyFailed(reply.id, reply.attempts, error).catch(() => {});
    console.error(
      `[instantly-service] scheduled-reply: campaign=${reply.instantlyCampaignId} lead=${reply.leadEmail} failed: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
    summary.failed += 1;
  };

  for (const reply of plan.send) {
    try {
      await replyToLead({
        orgId: reply.orgId,
        userId: reply.userId,
        campaignId: reply.campaignId,
        leadEmail: reply.leadEmail,
        bodyHtml: reply.bodyHtml,
        sentBy: reply.sentBy,
      });
      await markScheduledReplySent(reply.id);
      summary.sent += 1;
    } catch (error: unknown) {
      await fail(reply, error);
    }
  }

  for (const reply of plan.redraft) {
    try {
      await redraft(reply, asOf);
      console.log(
        `[instantly-service] scheduled-reply: campaign=${reply.instantlyCampaignId} lead=${reply.leadEmail} draft from ${reply.createdAt.toISOString()} superseded — re-queued for a fresh answer`,
      );
      summary.redrafted += 1;
    } catch (error: unknown) {
      await fail(reply, error);
    }
  }

  return summary;
}
