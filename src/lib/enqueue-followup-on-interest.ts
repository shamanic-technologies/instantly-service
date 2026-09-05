/**
 * A reply was qualified as a sales interest — enter that person into lead-service's
 * follow-up queue, owed an answer NOW.
 *
 * This is the missing half of the sibling side effect in
 * `trigger-sales-interest-campaign.ts`. That one asks campaign-service to RUN the
 * campaign bought to answer an interested buyer; the worker that campaign runs then
 * claims the next person OWED an answer out of lead-service's queue. Nobody ever
 * entered anybody into that queue — measured in production, ZERO rows across the
 * whole fleet carried a due date — so the campaign ran, found nobody, and no
 * interested prospect was ever answered. Both sides assumed the other did it: the
 * sibling's own doc comment asserts "sales-lead-service already holds a due-date
 * follow-up queue a scheduled sweep drains", which is true, and was never fed.
 *
 * Concretely on one production brand: 27 interested replies since July, an ongoing
 * funded campaign, ~8,600 workflow runs a day, and zero answers sent.
 *
 * ── THE GATE IS THE SIBLING'S GATE ──────────────────────────────────────────────
 *
 * `isSalesInterestQualification` — the same predicate that decides whether to ask
 * campaign-service to run. Deliberately imported rather than re-derived: the two
 * halves answer ONE question ("did a buyer open a conversation"), and a queue fed by
 * a wider set than the campaign that drains it is a queue with rows nothing ever
 * claims. It is likewise NOT `POSITIVE_REPLY_KINDS` (the forward-to-the-agency-inbox
 * set, which also contains `lead_referral`) — see the divergence documented on
 * `REPLY_KIND_CLASSIFICATION`.
 *
 * ── NOW, AND NOT RETROACTIVE ────────────────────────────────────────────────────
 *
 * `dueAt` is the moment the reply arrived: a buyer who just said "yes" is owed an
 * answer now, and the queue's ordering key is the due date. There is deliberately NO
 * backfill of historical replies — the brand above has interested replies going back
 * to July, and answering those now would reply into conversations that are dead.
 * Side effects only fire on the FIRST promotion of an event, so this reaches replies
 * arriving from here on and nothing before.
 *
 * ── FAIL SOFT, AND LOUDLY ───────────────────────────────────────────────────────
 *
 * Qualifying the reply is the primary job and its outcome can NEVER change because
 * the enqueue failed — a throw here would also 5xx the webhook, and Instantly
 * auto-pauses a webhook that keeps failing. Every failure is swallowed and WARNED
 * with its reason: lead-service refuses by name (404 `lead_not_found` for an address
 * it does not hold, 409 `ambiguous_lead` for one matching two rows), and a silently
 * dropped refusal would leave us believing the debt was recorded — which is exactly
 * the state this whole file exists to end.
 *
 * NO retry and NO claim column here. The queue is durable once written, and a missed
 * write costs latency on one prospect, never the answer: the person is still in
 * every other surface, and a later reply from them enqueues again.
 */

import { scheduleFollowupByEmail } from "./lead-client";
import {
  isSalesInterestQualification,
  type SalesInterestTriggerCampaign,
} from "./trigger-sales-interest-campaign";

/** The subset of a campaign row this side effect needs — identical to the sibling's. */
export type FollowupEnqueueCampaign = SalesInterestTriggerCampaign;

/**
 * Enter the lead into lead-service's follow-up queue, owed an answer now.
 *
 * No-op unless the event is a sales-interest qualification on an org-scoped send
 * that names a caller campaign. Fully fail-soft — never throws.
 */
export async function maybeEnqueueFollowupOnInterest(
  campaign: FollowupEnqueueCampaign,
  leadEmail: string,
  eventType: string,
  now: Date = new Date(),
): Promise<void> {
  if (!isSalesInterestQualification(eventType)) return;
  if (!campaign.orgId) return;
  // A platform send belongs to no caller campaign, so there is no campaign for a
  // debt to be owed on and no worker that would ever claim it.
  if (!campaign.campaignId) return;

  try {
    const result = await scheduleFollowupByEmail({
      orgId: campaign.orgId,
      campaignId: campaign.campaignId,
      email: leadEmail,
      dueAt: now.toISOString(),
    });

    console.log(
      `[instantly-service] followup-enqueue: campaign=${campaign.instantlyCampaignId} ` +
        `lead=${leadEmail} leadId=${result.leadId} dueAt=${result.followup.dueAt} ` +
        `followupCount=${result.followup.followupCount}`,
    );
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.warn(
      `[instantly-service] followup-enqueue: FAILED for campaign=${campaign.instantlyCampaignId} ` +
        `lead=${leadEmail} — ${message}; the qualification stands, but nobody is owed an answer ` +
        `for this reply until it is entered by hand`,
    );
  }
}
