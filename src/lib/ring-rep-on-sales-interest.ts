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
 * ── WHAT THE CALL HAS TO SAY WITH ────────────────────────────────────────────────
 *
 * A rep deciding whether to take a live call needs two things this used to
 * withhold. WHO the person is — spelled out: first name, last name, title,
 * company, city, state, country, every one of which already rode the
 * `?view=basic` projection lead-service serves and was being thrown away here.
 * And WHAT THEY ARE ANSWERING — the reply alone is half a conversation, so the
 * rest of the thread is handed over newest-first, entry 0 being the email the
 * reply responds to, for the call to walk back through one keypress at a time.
 *
 * Both are gathered from what is already in hand: the lead read that was already
 * being made, and the thread that was already being fetched for the reply's own
 * words. Neither adds a call.
 *
 * Everything spoken is CLEANED first — quoted history and our own signature plus
 * its opt-out footer are cut. Those are our words, not theirs, and reading them
 * back at a rep costs billed minutes to say nothing.
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
import { getSalesRep } from "./brand-client";
import {
  readPhoneReveal,
  requestPhoneReveal,
  type PhoneReveal,
  type RevealIdentity,
} from "./apollo-client";
import { findLeadOnCampaignByEmail, type LeadForCall } from "./lead-client";
import { placeCall, type CallReply, type PriorMessage } from "./twilio-client";
import { isSalesInterestQualification } from "./trigger-sales-interest-campaign";
import { fetchMirroredEmailRecords } from "./mirror-emails";
import { selectThreadMessages, type ThreadMessage } from "./forward-positive-reply";
import { stripQuotedHistory } from "./self-send/qualify-reply";
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

/**
 * How many earlier emails the call offers to walk back through.
 *
 * A cold sequence is three steps, so in practice the walk is one or two hops and
 * this never binds. It exists because a thread that has been going back and
 * forth for weeks would otherwise put a rep on a keypress treadmill while the
 * prospect waits — and every started minute of the call is billed.
 */
export const MAX_PRIOR_MESSAGES = 5;

/**
 * A line carrying nothing but the RFC 3676 signature delimiter.
 *
 * ⚠️ This is NOT `stripAccountSignature` and must not be replaced by it. That
 * function owns the WIRE form (`<p>--</p>`, `<br>--<br>`) and is protected by two
 * production incidents; by the time a body reaches here it has been through
 * `htmlToText`, which collapses `</p><p>` to a SINGLE newline — so the plain
 * marker that function looks for (`\n\n--\n`) does not match and it would
 * silently strip nothing. What survives the conversion is a line that is exactly
 * `--`, which is what this matches.
 */
const SPOKEN_SIGNATURE_LINE = /^[ \t]*--[ \t]*$/;

/** Cut a plain-text body at the signature delimiter, and everything below it. */
export function stripSpokenSignature(text: string): string {
  const lines = text.split(/\r?\n/);
  const kept: string[] = [];
  for (const line of lines) {
    if (SPOKEN_SIGNATURE_LINE.test(line)) break;
    kept.push(line);
  }
  return kept.join("\n").trim();
}

/**
 * What a human should hear, out of a stored body.
 *
 * Two cuts, both of them things WE put there and neither of them worth a second
 * of a billed call: the quoted history a client staples under a reply (which is
 * our own previous email read back at the rep), and our signature block with the
 * opt-out footer under it ("Don't want to hear from me again? unsubscribe" is a
 * strange thing to read to your own sales rep). Both are truncate-at-first-marker
 * cuts, so applying them in either order gives the same answer.
 *
 * A body carrying neither marker comes back unchanged — nothing is invented and
 * nothing is summarised.
 */
export function cleanForSpeech(text: string): string {
  return stripSpokenSignature(stripQuotedHistory(text)).trim();
}

/** True when a thread entry carries words rather than a placeholder. */
function hasWords(message: ThreadMessage): boolean {
  const text = message.bodyText?.trim();
  return Boolean(text) && text !== "(no body)";
}

/**
 * The rest of the conversation, newest-first, for the rep to walk back through.
 *
 * ⚠️ IT STOPS AT THE PROSPECT'S REPLY, and that boundary is the point. Entry 0 is
 * the email the reply ANSWERS, and each keypress goes one hop further back —
 * which is how a person reconstructs a conversation. Anything AFTER that reply
 * (an answer we have already sent) is excluded: it is not context for the reply,
 * it is what happened next, and reading it in this order would be confusing.
 *
 * With no inbound at all the whole thread is offered. That case means we could
 * not read what they wrote (the call says so in words), so our own last emails
 * are the only context there is, and withholding them would leave the rep with
 * nothing.
 *
 * Every entry is cleaned and a body-less one is DROPPED rather than spoken as an
 * empty pause.
 */
export function buildPriorMessages(messages: ThreadMessage[]): PriorMessage[] {
  let lastInbound = -1;
  for (let i = messages.length - 1; i >= 0; i--) {
    if (messages[i].direction === "inbound" && hasWords(messages[i])) {
      lastInbound = i;
      break;
    }
  }

  const earlier = lastInbound >= 0 ? messages.slice(0, lastInbound) : messages.slice();

  const walked: PriorMessage[] = [];
  for (let i = earlier.length - 1; i >= 0 && walked.length < MAX_PRIOR_MESSAGES; i--) {
    const message = earlier[i];
    if (!hasWords(message)) continue;
    const text = cleanForSpeech(message.bodyText);
    if (!text) continue;
    walked.push({ direction: message.direction, text });
  }
  return walked;
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

/**
 * Assemble what the call says about the reply.
 *
 * ⚠️ AN ABSENT FIELD IS OMITTED, NEVER SENT EMPTY. The call reads these aloud to
 * build a sentence, so a blank string becomes a gap a human hears as a fault —
 * and a placeholder ("unknown", "N/A") asserts we looked and found nothing when
 * the truth is usually that lead-service never held it. Saying less is the
 * honest shape.
 */
export function buildCallReply(
  leadEmail: string,
  lead: LeadForCall | null,
  message: string | null,
): CallReply {
  const reply: CallReply = {
    name: spokenName(leadEmail, lead),
    message: message?.trim() || REPLY_TEXT_UNAVAILABLE,
  };
  const said = (value: string | null | undefined): string | undefined =>
    value?.trim() || undefined;

  const company = said(lead?.company);
  if (company) reply.company = company;
  const firstName = said(lead?.firstName);
  if (firstName) reply.firstName = firstName;
  const lastName = said(lead?.lastName);
  if (lastName) reply.lastName = lastName;
  const title = said(lead?.title);
  if (title) reply.title = title;
  const city = said(lead?.city);
  if (city) reply.city = city;
  const state = said(lead?.state);
  if (state) reply.state = state;
  const country = said(lead?.country);
  if (country) reply.country = country;

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

/**
 * The conversation, from whichever side of the transport holds it.
 *
 * Read ONCE and returned whole: the reply the call is about and the emails it
 * answers come out of the same thread, and fetching it twice would let the two
 * halves of one call disagree about the same exchange.
 */
async function readThread(campaign: RingRepCampaign): Promise<ThreadMessage[]> {
  return isSelfSendCampaignId(campaign.instantlyCampaignId)
    ? await fetchSelfSendThread(campaign.instantlyCampaignId)
    : selectThreadMessages(
        await fetchMirroredEmailRecords(campaign.instantlyCampaignId),
      );
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

  // The whole rep, in one read. The forward that fired seconds ago copied this
  // same person by email; reading the two facts together is what stops the two
  // side effects naming different people for one brand on one conversation.
  let salesRepPhone: string | null;
  try {
    salesRepPhone = (await getSalesRep(brandId, campaign.orgId)).phone;
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

    // The reply and the emails it answers, out of ONE read of the thread. A
    // failure here costs the words, never the call: the rep is still told a
    // buyer is interested, and the call says the reply could not be read.
    let replyText: string | null = null;
    let priorMessages: PriorMessage[] = [];
    try {
      const thread = await readThread(campaign);
      const latest = latestInboundText(thread);
      replyText = latest ? cleanForSpeech(latest) || null : null;
      priorMessages = buildPriorMessages(thread);
    } catch (error: unknown) {
      console.warn(
        `[instantly-service] ring-rep: could not read the thread for campaign=${campaign.instantlyCampaignId} ` +
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
      ...(priorMessages.length > 0 ? { priorMessages } : {}),
      ...(connectTo ? { connectTo } : {}),
      ...(campaign.runId ? { parentRunId: campaign.runId } : {}),
      brandId,
      ...(campaign.campaignId ? { campaignId: campaign.campaignId } : {}),
    });

    console.log(
      `[instantly-service] ring-rep: called ${salesRepPhone} about campaign=${campaign.instantlyCampaignId} ` +
        `lead=${leadEmail} callId=${placed.callId} connectOffered=${placed.connectOffered} ` +
        `priorMessages=${priorMessages.length} ` +
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
