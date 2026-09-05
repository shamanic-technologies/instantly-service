/**
 * Draining the waiting room — IO glue around the pure selection in
 * `scheduled-replies.ts`.
 *
 * ⚠️ THIS RIDES THE EXISTING HOURLY WORKER, IT IS NOT A NEW ONE. `runDispatch`
 * calls it in the same run that sends the sequence steps, after the same
 * mailbox poll, so a prospect who has meanwhile written again is read before we
 * answer them. There is no second cron, no second endpoint and no second set of
 * scheduling rules.
 *
 * ⚠️ It drains replies on BOTH transports, because a reply is dispatched by
 * `replyToLead`, which branches on the campaign's frozen transport. That does
 * couple an Instantly-transport reply to `SELF_SEND_DISPATCH_ENABLED` — the
 * switch that gates the whole sweep. That is the price of having ONE queue and
 * ONE drain rather than two, and it is the right trade: the alternative is a
 * second scheduler whose behaviour would drift from this one. If the switch is
 * ever disarmed, waiting replies stop draining and stay `pending`; nothing is
 * lost and they go out on the next armed run.
 */

import {
  loadPendingScheduledReplies,
  markScheduledReplyFailed,
  markScheduledReplySent,
  selectDueScheduledReplies,
} from "./scheduled-replies";
import { replyToLead } from "./reply-to-lead";

export interface ScheduledReplySummary {
  /** Waiting replies read, whatever their due date. */
  pending: number;
  /** Of those, the ones whose prospect's window is open right now. */
  due: number;
  sent: number;
  failed: number;
}

/**
 * Send every waiting reply whose prospect's window is open.
 *
 * Fail-loud PER REPLY: one dead mailbox must not stop the rest of the queue,
 * and every failure is recorded on its own row with the error that caused it.
 *
 * `deferOutsideWindow: false` on the dispatch is deliberate — the window was
 * already checked by the selection above, and re-checking inside `replyToLead`
 * would put a reply selected at 16:59 straight back into the queue it was just
 * taken out of.
 */
export async function dispatchScheduledReplies(
  asOf: Date,
): Promise<ScheduledReplySummary> {
  const pending = await loadPendingScheduledReplies();
  const due = selectDueScheduledReplies(pending, asOf);

  const summary: ScheduledReplySummary = {
    pending: pending.length,
    due: due.length,
    sent: 0,
    failed: 0,
  };

  for (const reply of due) {
    try {
      await replyToLead(
        {
          orgId: reply.orgId,
          userId: reply.userId,
          campaignId: reply.campaignId,
          leadEmail: reply.leadEmail,
          bodyHtml: reply.bodyHtml,
        },
        { asOf, deferOutsideWindow: false },
      );

      await markScheduledReplySent(reply.id);
      summary.sent += 1;
    } catch (error: unknown) {
      await markScheduledReplyFailed(reply.id, reply.attempts, error).catch(
        () => {},
      );
      console.error(
        `[instantly-service] scheduled-reply: campaign=${reply.instantlyCampaignId} lead=${reply.leadEmail} failed: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      summary.failed += 1;
    }
  }

  return summary;
}
