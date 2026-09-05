/**
 * A reply was qualified as a sales interest — ring the brand's sales rep, and
 * offer to connect them to the prospect right now.
 *
 * Three side effects already fire at this one choke point: the thread is emailed
 * to the agency inbox, campaign-service is asked to run the leg out of the step,
 * and the person is entered into lead-service's follow-up queue. All three end in
 * something a human reads LATER. This is the one that reaches a human WHILE the
 * prospect is still at their desk: within a minute or two of "yes, interested",
 * the rep's phone rings, they hear who it is and what they wrote, and — when we
 * have the prospect's number — they press a key and are on the call.
 *
 * ── THE GATE IS THE SIBLINGS' GATE ──────────────────────────────────────────────
 *
 * `isSalesInterestQualification`, IMPORTED from the campaign trigger rather than
 * re-derived, exactly as the follow-up enqueue imports it. The three answer ONE
 * question ("did a buyer open a conversation") and must answer it identically. It
 * is deliberately NOT `POSITIVE_REPLY_KINDS` (the forward-to-the-agency-inbox
 * set, which also contains `lead_referral`): "not me, but talk to X" is worth a
 * human's eyes, which is why it forwards, and it is emphatically not a buyer
 * opening a conversation — ringing a rep to say a prospect is interested when
 * that prospect just said they are the wrong person is the exact mistake the
 * divergence on `REPLY_KIND_CLASSIFICATION` exists to prevent.
 *
 * ── AT MOST ONE CALL PER LEAD ───────────────────────────────────────────────────
 *
 * The same sales-interest signal legitimately arrives more than once — a webhook
 * retry, a reconcile re-poll, a re-qualification (interested → meeting booked →
 * closed each promote a distinct positive event) — so "we already rang" cannot be
 * inferred from the event stream. It is CLAIMED atomically on
 * `instantly_campaigns.sales_interest_call_at` (migration 0049) BEFORE anything
 * external happens, and released back to NULL when the call could not be placed
 * so a later signal re-attempts. Same shape and same reasoning as the
 * positive-reply forward's claim; two at-most-once side effects should not claim
 * two different ways.
 *
 * ── THE NINETY SECONDS ──────────────────────────────────────────────────────────
 *
 * Apollo's phone reveal is asynchronous: it answers WITHOUT the number and
 * delivers it to apollo-service's callback minutes later. So there is a real
 * trade, and it was made deliberately: a call that rings NOW to say "somebody is
 * interested but I cannot connect you" is worth much less than one ninety seconds
 * later that connects, and ninety seconds is still immediate measured against
 * what happened before this existed, which is nothing at all. The wait is bounded
 * and the rep is rung either way when it runs out.
 *
 * ⚠️ WHICH IS WHY THIS IS NOT AWAITED BY ITS CALLER. `promoteEvent` runs inside
 * Instantly's webhook, and Instantly counts a slow or failed delivery toward
 * DISABLING the whole subscription — that has already cost this service a six-day
 * outage once. A ninety-second await there would be an outage waiting to happen,
 * so the caller launches this detached. The cost of that is honest and small: a
 * deploy that recreates the container mid-wait leaves a lead claimed and never
 * rung, which errs toward the failure worth erring toward (nobody is rung twice).
 *
 * ── WHAT IS NEVER DONE ──────────────────────────────────────────────────────────
 *
 * A number flagged do-not-call is NEVER dialled: the call still happens and says
 * we have no number for them. No number is ever invented and there is no fallback
 * to any other number — a company switchboard is not the person who replied. A
 * brand that stated no number does nothing at all, silently, which is the
 * overwhelmingly common case and not an error. And nothing is retroactive: side
 * effects fire only on the FIRST promotion of an event, so this reaches replies
 * arriving from here on and no historical one.
 *
 * There is no quiet-hours window. The rep may be rung at any hour, by decision.
 */

import { and, eq, isNull } from "drizzle-orm";
import { db } from "../db";
import { instantlyCampaigns } from "../db/schema";
import { getSalesRepPhone } from "./brand-client";
import {
  readPhoneReveal,
  requestPhoneReveal,
  type PhoneReveal,
  type RevealIdentity,
} from "./apollo-client";
import { findLeadOnCampaignByEmail, type LeadForCall } from "./lead-client";
import { placeCall, type CallReply } from "./twilio-client";
import { isSalesInterestQualification } from "./trigger-sales-interest-campaign";
import { fetchMirroredEmailRecords } from "./mirror-emails";
import { selectThreadMessages, type ThreadMessage } from "./forward-positive-reply";
import { isSelfSendCampaignId } from "./self-send/transport";
import { fetchSelfSendThread } from "./self-send/thread";

/** How long we wait for Apollo to deliver the number before ringing anyway. */
export const PHONE_REVEAL_WAIT_MS = 90_000;
/** How often the reveal is re-read while waiting. */
export const PHONE_REVEAL_POLL_INTERVAL_MS = 5_000;

/**
 * Spoken in place of the reply when its words cannot be read.
 *
 * The rep is still rung — knowing a buyer is interested is the point — and the
 * absence is STATED rather than papered over with invented text.
 */
export const REPLY_TEXT_UNAVAILABLE =
  "their reply is not available to read out on this call.";

/** The campaign row this side effect needs. */
export interface RingRepCampaign {
  instantlyCampaignId: string;
  /** The CALLER campaign id — campaign-service's own row. Null on a platform send. */
  campaignId: string | null;
  orgId: string | null;
  userId: string | null;
  runId: string | null;
  brandIds?: string[] | null;
}

/** Injected clock and sleep, so the bounded wait is deterministic in tests. */
export interface RingRepDeps {
  now?: () => number;
  sleep?: (ms: number) => Promise<void>;
}

/** A reveal that will not change again by waiting longer. */
export function isRevealSettled(status: PhoneReveal["status"]): boolean {
  return status !== "pending";
}

/**
 * The number to bridge the rep to, or null.
 *
 * Three refusals, all of them null: nothing arrived in time (`pending`), Apollo
 * has none (`not_found` / `failed`), or the number is flagged DO NOT CALL. The
 * last is the one worth being explicit about — a DNC number is announced, never
 * dialled, and there is no second-choice number to fall back to.
 */
export function connectNumberFor(reveal: PhoneReveal | null): string | null {
  if (!reveal) return null;
  if (reveal.status !== "found") return null;
  if (reveal.doNotCall) return null;
  const number = reveal.mobilePhone?.trim();
  return number ? number : null;
}

/** What the prospect last wrote, out of the thread, or null when we hold none. */
export function latestInboundText(messages: ThreadMessage[]): string | null {
  for (let i = messages.length - 1; i >= 0; i--) {
    const message = messages[i];
    if (message.direction !== "inbound") continue;
    const text = message.bodyText?.trim();
    if (text && text !== "(no body)") return text;
  }
  return null;
}

/**
 * How the prospect is named out loud. Their name when lead-service holds one,
 * otherwise the address they replied from — which identifies them, which is the
 * whole point of the call.
 */
export function spokenName(leadEmail: string, lead: LeadForCall | null): string {
  const name = lead?.name?.trim();
  return name && name.length > 0 ? name : leadEmail;
}

/** Assemble what the call says about the reply. */
export function buildCallReply(
  leadEmail: string,
  lead: LeadForCall | null,
  message: string | null,
): CallReply {
  const reply: CallReply = {
    name: spokenName(leadEmail, lead),
    message: message?.trim() || REPLY_TEXT_UNAVAILABLE,
  };
  const company = lead?.company?.trim();
  if (company) reply.company = company;
  return reply;
}

const defaultSleep = (ms: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, ms));

/**
 * Ask for the number and wait, bounded, for it to arrive.
 *
 * Returns the last reveal seen — settled or still pending. A pending one carries
 * no usable number (see {@link connectNumberFor}) but is returned rather than
 * discarded so the caller can log WHY nobody was connected.
 */
export async function revealPhoneWithinBudget(
  apolloPersonId: string,
  identity: RevealIdentity,
  deps: RingRepDeps = {},
): Promise<PhoneReveal> {
  const now = deps.now ?? (() => Date.now());
  const sleep = deps.sleep ?? defaultSleep;

  const first = await requestPhoneReveal(apolloPersonId, identity);
  if (isRevealSettled(first.status)) return first;

  const deadline = now() + PHONE_REVEAL_WAIT_MS;
  let latest = first;
  while (now() < deadline) {
    await sleep(PHONE_REVEAL_POLL_INTERVAL_MS);
    latest = await readPhoneReveal(apolloPersonId, identity);
    if (isRevealSettled(latest.status)) return latest;
  }
  return latest;
}

/** Atomically claim the call for this lead. True iff THIS call won it. */
async function claimCall(instantlyCampaignId: string): Promise<boolean> {
  const claimed = await db
    .update(instantlyCampaigns)
    .set({ salesInterestCallAt: new Date(), updatedAt: new Date() })
    .where(
      and(
        eq(instantlyCampaigns.instantlyCampaignId, instantlyCampaignId),
        isNull(instantlyCampaigns.salesInterestCallAt),
      ),
    )
    .returning({ id: instantlyCampaigns.id });
  return claimed.length > 0;
}

/** Release the claim (the call could not be placed) so a later signal re-attempts. */
async function releaseCall(instantlyCampaignId: string): Promise<void> {
  await db
    .update(instantlyCampaigns)
    .set({ salesInterestCallAt: null, updatedAt: new Date() })
    .where(eq(instantlyCampaigns.instantlyCampaignId, instantlyCampaignId));
}

/** The prospect's own words, from whichever side of the transport holds them. */
async function readReplyText(campaign: RingRepCampaign): Promise<string | null> {
  const messages = isSelfSendCampaignId(campaign.instantlyCampaignId)
    ? await fetchSelfSendThread(campaign.instantlyCampaignId)
    : selectThreadMessages(
        await fetchMirroredEmailRecords(campaign.instantlyCampaignId),
      );
  return latestInboundText(messages);
}

/**
 * Ring the brand's sales rep about a buyer who just opened a conversation.
 *
 * No-op unless the event is a sales-interest qualification on an org-scoped send
 * whose brand states a number to ring. Fully fail-soft — never throws, and every
 * failure is warned with its reason.
 */
export async function maybeRingRepOnSalesInterest(
  campaign: RingRepCampaign,
  leadEmail: string,
  eventType: string,
  deps: RingRepDeps = {},
): Promise<void> {
  if (!isSalesInterestQualification(eventType)) return;
  if (!campaign.orgId) return;

  // One campaign is one brand for every outbound send this service performs. No
  // brand means no per-brand configuration to read, so there is nobody to ring.
  const brandId = campaign.brandIds?.[0];
  if (!brandId) return;

  let salesRepPhone: string | null;
  try {
    salesRepPhone = await getSalesRepPhone(brandId, campaign.orgId);
  } catch (error: unknown) {
    console.warn(
      `[instantly-service] ring-rep: could not read the number to ring for brand=${brandId} ` +
        `campaign=${campaign.instantlyCampaignId} lead=${leadEmail} — ${describe(error)}; nobody was rung`,
    );
    return;
  }

  // A brand that stated no number wants no call. The common case, and not an error.
  if (!salesRepPhone) return;

  // Claim BEFORE anything external: a losing caller (a retry, a re-poll, a
  // re-qualification) stops here having rung nobody.
  let claimed: boolean;
  try {
    claimed = await claimCall(campaign.instantlyCampaignId);
  } catch (error: unknown) {
    console.warn(
      `[instantly-service] ring-rep: claim failed for campaign=${campaign.instantlyCampaignId} ` +
        `lead=${leadEmail} — ${describe(error)}; will retry on the next positive signal`,
    );
    return;
  }
  if (!claimed) return;

  try {
    // Who they are, and Apollo's id for them. Best effort by design: a rep is
    // rung about an unidentified buyer rather than not rung at all.
    let lead: LeadForCall | null = null;
    try {
      // A platform send belongs to no caller campaign, so there is no campaign
      // to scope the lookup to. The rep is still rung, unconnected.
      if (campaign.campaignId) {
        lead = await findLeadOnCampaignByEmail({
          orgId: campaign.orgId,
          campaignId: campaign.campaignId,
          email: leadEmail,
        });
      }
    } catch (error: unknown) {
      console.warn(
        `[instantly-service] ring-rep: lead lookup failed for campaign=${campaign.instantlyCampaignId} ` +
          `lead=${leadEmail} — ${describe(error)}; ringing without a number to connect`,
      );
    }

    // The number, if Apollo can deliver one inside the budget. Every reason it
    // may not — no Apollo id, no run to declare the spend against, a reveal that
    // failed, a wait that ran out, a do-not-call flag — ends the same way: the
    // call happens and says we have no number.
    let reveal: PhoneReveal | null = null;
    if (lead?.apolloPersonId && campaign.userId && campaign.runId) {
      try {
        reveal = await revealPhoneWithinBudget(
          lead.apolloPersonId,
          {
            orgId: campaign.orgId,
            userId: campaign.userId,
            runId: campaign.runId,
            brandId,
            campaignId: campaign.campaignId,
          },
          deps,
        );
      } catch (error: unknown) {
        console.warn(
          `[instantly-service] ring-rep: phone reveal failed for campaign=${campaign.instantlyCampaignId} ` +
            `lead=${leadEmail} — ${describe(error)}; ringing without a number to connect`,
        );
      }
    }

    let replyText: string | null = null;
    try {
      replyText = await readReplyText(campaign);
    } catch (error: unknown) {
      console.warn(
        `[instantly-service] ring-rep: could not read the reply text for campaign=${campaign.instantlyCampaignId} ` +
          `lead=${leadEmail} — ${describe(error)}; the call states the words are unavailable`,
      );
    }

    const connectTo = connectNumberFor(reveal);
    const placed = await placeCall({
      orgId: campaign.orgId,
      // twilio-service scopes the call's run to a user; a send that names none
      // still gets its call, attributed to the org.
      userId: campaign.userId || "00000000-0000-0000-0000-000000000000",
      to: salesRepPhone,
      reply: buildCallReply(leadEmail, lead, replyText),
      ...(connectTo ? { connectTo } : {}),
      ...(campaign.runId ? { parentRunId: campaign.runId } : {}),
      brandId,
      ...(campaign.campaignId ? { campaignId: campaign.campaignId } : {}),
    });

    console.log(
      `[instantly-service] ring-rep: called ${salesRepPhone} about campaign=${campaign.instantlyCampaignId} ` +
        `lead=${leadEmail} callId=${placed.callId} connectOffered=${placed.connectOffered} ` +
        `reveal=${reveal?.status ?? "not-requested"}${reveal?.doNotCall ? " (do-not-call)" : ""}`,
    );
  } catch (error: unknown) {
    await releaseCall(campaign.instantlyCampaignId).catch(() => {});
    console.warn(
      `[instantly-service] ring-rep: no call placed for campaign=${campaign.instantlyCampaignId} ` +
        `lead=${leadEmail} — ${describe(error)}; claim released, will retry on the next positive signal`,
    );
  }
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
