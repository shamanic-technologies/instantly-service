/**
 * A reply was qualified as a sales interest: report the occurrence to
 * campaign-service's generic trigger door, `positive_reply_received`, naming the
 * person and carrying what they wrote (AIC-COMMON-1015).
 *
 * This service is where the reply ARRIVES and where it is QUALIFIED, so this is
 * where the occurrence is reported. What answers it is NOT decided here: every
 * pipe whose leg answers `positive_reply_received` (features-service's published
 * catalogue: AI Meeting Booking writes back by email, AI Instant Call rings the
 * brand's rep...) is fired by campaign-service through the same dispatch as every
 * other pipe, under that campaign's own funding gate, and receives `person` and
 * `item` on `/execute` `trigger`. Nothing channel-specific is performed here:
 * the rep's call used to be placed in this service (`ring-rep-on-sales-interest`,
 * deleted), a hard-coded side effect the owner forbade (« IL NE FAUT JAMAIS FAIRE
 * ÇA »); the AI Instant Call workflow now places it off this occurrence.
 *
 * ── WHAT IS SENT ────────────────────────────────────────────────────────────────
 *
 * - scope (brand, offer): read off the CALLER campaign, campaign-service's own
 *   row (`getCampaignTriggerScope`). A campaign stating no brand or no offer can
 *   have no pipe resolved for it, so nothing is reported (a platform send has no
 *   caller campaign at all): an ordinary absence, not a failure.
 * - `person` {email, name?}: who wrote. The name is lead-service's, best effort.
 * - `leadId`: lead-service's id for them on the sending campaign, when we hold it.
 * - `item` {replyText, priorMessages}: `positive-reply-item.ts`, shape LOCKED with
 *   the AI Instant Call workflow.
 * - `idempotencyKey`: ONE per reply (`positiveReplyKey`): a webhook retry, a
 *   re-poll, a re-qualification of the same words records one occurrence, so a
 *   rep is never rung twice about the same reply.
 *
 * ── WHICH REPLIES ───────────────────────────────────────────────────────────────
 *
 * The reply kinds `REPLY_KIND_CLASSIFICATION` reports as `positive`: the SAME
 * signal the fleet prices and counts as the brand's sales interest.
 *
 * ⚠️ That is deliberately NOT `POSITIVE_REPLY_KINDS` (the forward-to-the-agency-inbox
 * set), which additionally contains `lead_referral`. Forwarding asks "is this worth
 * a human's eyes"; this asks "did this buyer open a conversation", which "not me,
 * but talk to X" did not. Do NOT restore a lockstep with the forward set.
 *
 * ── FAIL SOFT, AND LOUDLY ───────────────────────────────────────────────────────
 *
 * Qualifying the reply is the primary job; the occurrence is a consequence of it,
 * so it can NEVER change the qualification's outcome. Every failure is swallowed
 * and logged. Nothing is retried on this side: campaign-service owns retries of
 * an occurrence it recorded (a person not known yet, a campaign out of money),
 * and lead-service's follow-up queue reaches the person on its own regardless.
 */

import {
  findOngoingResponderCampaigns,
  getCampaignTriggerScope,
  recordTriggerEvent,
  type RecordedTriggerEvent,
} from "./campaign-client";
import { findLeadOnCampaignByEmail } from "./lead-client";
import {
  buildPositiveReplyItem,
  emptyPositiveReplyItem,
  positiveReplyKey,
  readReplyThread,
  type PositiveReplyItem,
} from "./positive-reply-item";
import type { ThreadMessage } from "./forward-positive-reply";
import { REPLY_KIND_CLASSIFICATION, isReplyKind } from "./reply-kind";

/**
 * features-service's trigger for "a prospect answers with interest"
 * (`GET /public/channels` → `triggers[].id`). Carried VERBATIM: campaign-service
 * refuses a trigger nobody declares (400 `unknown_trigger`).
 */
export const POSITIVE_REPLY_TRIGGER_ID = "positive_reply_received";

/**
 * True iff this event says a buyer opened a conversation — i.e. its reply kind is
 * what the fleet reports as the brand's sales interest.
 */
export function isSalesInterestQualification(eventType: string): boolean {
  return isReplyKind(eventType) && REPLY_KIND_CLASSIFICATION[eventType] === "positive";
}

/** The subset of a campaign row this side effect needs. */
export interface SalesInterestTriggerCampaign {
  instantlyCampaignId: string;
  /** The CALLER campaign id — the one campaign-service owns. Null on a platform send. */
  campaignId: string | null;
  orgId: string | null;
  /** lead-service's id for the person on the sending campaign, when held. */
  leadId?: string | null;
}

/** Log what campaign-service did, so a quiet no-op stays distinguishable from a bug. */
function logOutcome(
  campaign: SalesInterestTriggerCampaign,
  leadEmail: string,
  event: RecordedTriggerEvent,
  replayed: boolean,
): void {
  const triggered = (event.detail?.triggered ?? []).map((t) => `${t.campaignId}(${t.workflowSlug})`).join(", ");
  const skipped = (event.detail?.skipped ?? []).map((s) => `${s.campaignId}:${s.reason}`).join(", ");
  console.log(
    `[instantly-service] sales-interest-trigger: campaign=${campaign.instantlyCampaignId} ` +
      `lead=${leadEmail} trigger=${POSITIVE_REPLY_TRIGGER_ID} event=${event.id}${replayed ? " (replayed)" : ""} ` +
      `status=${event.status} outcome=${event.outcome ?? "-"} skip=${event.skipReason ?? "-"} ` +
      `triggered=[${triggered}] skipped=[${skipped}]`,
  );
}

/**
 * Will an AI responder answer this reply by EMAIL? Read off campaign-service's
 * own answer to the occurrence, never off a campaign's `ongoing` status (owner
 * 2026-10-11: a responder that is on but out of money answered nobody, and
 * nobody was told).
 *
 * - `answers`: a responder campaign took the run, or one of its runs is already
 *   in flight (campaign-service: "the event is already being answered").
 * - `nobody`: anything else. No responder bought, no offer on the thread, the
 *   client's money is out (the occurrence stays owed at campaign-service until
 *   room returns, but nobody answers NOW), run by the client's own team,
 *   refused. A run taken by a PHONE pipe (AI Instant Call) answers no email, so
 *   it is not counted.
 * - `unknown`: the question could not be asked or read, or campaign-service has
 *   not asked any campaign yet (the person is not known to the brand yet).
 *   Nothing is decided on it.
 */
export type ResponderVerdict = "answers" | "nobody" | "unknown";

/** Pure: the campaigns that took the occurrence (ran, or one of their runs already in flight). */
export function campaignsTakingEvent(event: RecordedTriggerEvent): string[] {
  return [
    ...new Set([
      ...(event.ranCampaignIds ?? []),
      ...(event.detail?.triggered ?? []).map((t) => t.campaignId),
      ...(event.detail?.skipped ?? []).filter((s) => s.reason === "run_in_flight").map((s) => s.campaignId),
    ]),
  ];
}

/** Pure: has campaign-service asked any campaign about this occurrence yet? */
function campaignsAsked(event: RecordedTriggerEvent): boolean {
  if (event.status === "done") return true;
  const d = event.detail;
  return (d?.triggered?.length ?? 0) > 0 || (d?.skipped?.length ?? 0) > 0;
}

/**
 * Report the occurrence and say whether an email responder took it. Undefined
 * when the event is not a sales-interest qualification. Fully fail-soft:
 * never throws.
 */
export async function maybeTriggerSalesInterestCampaign(
  campaign: SalesInterestTriggerCampaign,
  leadEmail: string,
  eventType: string,
): Promise<ResponderVerdict | undefined> {
  if (!isSalesInterestQualification(eventType)) return undefined;
  return triggerSalesInterestLeg(campaign, leadEmail);
}

/**
 * The report itself, without the reply-kind gate — for a caller that already
 * knows a buyer is owed an answer. `idempotencyKey` overrides the per-reply key
 * (a stale drafted answer being redrafted is a NEW occurrence of the same reply:
 * the per-reply key would only replay the first). Same scope rules, same
 * fail-soft contract: never throws.
 */
export async function triggerSalesInterestLeg(
  campaign: SalesInterestTriggerCampaign,
  leadEmail: string,
  opts: { idempotencyKey?: string } = {},
): Promise<ResponderVerdict> {
  // A platform send belongs to no org and no caller campaign: nobody to ask.
  if (!campaign.orgId) return "unknown";
  if (!campaign.campaignId) return "unknown";
  const orgId = campaign.orgId;
  const campaignId = campaign.campaignId;

  try {
    const scope = await getCampaignTriggerScope(campaignId, orgId);
    if (!scope || !scope.brandId) return "unknown";
    // A thread on no offer can have no pipe answering it: pipes are bought per
    // (brand, offer), so nothing will answer it.
    if (!scope.offerId) return "nobody";
    const brandId = scope.brandId;
    const offerId = scope.offerId;

    // What they wrote and the emails it answers, out of ONE read of the thread.
    // A failure costs the words, never the occurrence: the pipes still run and
    // the call says the words are unavailable.
    let thread: ThreadMessage[] = [];
    let item: PositiveReplyItem = emptyPositiveReplyItem();
    try {
      thread = await readReplyThread(campaign.instantlyCampaignId);
      item = buildPositiveReplyItem(thread);
    } catch (error: unknown) {
      console.warn(
        `[instantly-service] sales-interest-trigger: could not read the thread for campaign=${campaign.instantlyCampaignId} ` +
          `lead=${leadEmail} — ${describe(error)}; reported without the words`,
      );
    }

    // Their name, best effort: the workflow reads the full identity off
    // lead-service itself, the name only labels the occurrence.
    let name: string | undefined;
    try {
      const lead = await findLeadOnCampaignByEmail({ orgId, campaignId, email: leadEmail });
      name = lead?.name?.trim() || undefined;
    } catch (error: unknown) {
      console.warn(
        `[instantly-service] sales-interest-trigger: lead lookup failed for campaign=${campaign.instantlyCampaignId} ` +
          `lead=${leadEmail} — ${describe(error)}; reported without a name`,
      );
    }

    const { event, replayed } = await recordTriggerEvent({
      orgId,
      triggerId: POSITIVE_REPLY_TRIGGER_ID,
      brandId,
      offerId,
      leadId: campaign.leadId ?? null,
      requestedByCampaignId: campaignId,
      idempotencyKey:
        opts.idempotencyKey ?? positiveReplyKey(campaign.instantlyCampaignId, leadEmail, thread),
      person: { email: leadEmail.trim(), ...(name ? { name } : {}) },
      item,
    });

    logOutcome(campaign, leadEmail, event, replayed);

    const taking = campaignsTakingEvent(event);
    if (taking.length === 0) return campaignsAsked(event) ? "nobody" : "unknown";
    const emailResponders = new Set(
      await findOngoingResponderCampaigns({ orgId, brandId, offerId }),
    );
    return taking.some((id) => emailResponders.has(id)) ? "answers" : "nobody";
  } catch (error: unknown) {
    console.warn(
      `[instantly-service] sales-interest-trigger: no-op for campaign=${campaign.instantlyCampaignId} ` +
        `lead=${leadEmail} — ${describe(error)}; the qualification stands and the campaign runs on its next tick`,
    );
    return "unknown";
  }
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
