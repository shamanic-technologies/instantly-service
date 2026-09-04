/**
 * Reading the conversation we had with a prospect.
 *
 * `POST /orgs/replies` can already ANSWER someone who wrote back. Nothing could
 * READ what they wrote — a caller could learn THAT a lead replied (delivery
 * status, reply classification, reply kind) and nothing about WHAT they said. So
 * a worker drafting the answer was reduced to a template, which is the exact
 * failure the reply path exists to avoid: an answer that ignores the prospect's
 * question is worse than no answer.
 *
 * The words were already here. This exposes them.
 *
 * ⚠️ THE THREAD IS RESOLVED THE SAME WAY THE REPLY IS SENT — same identity in
 * (logical campaign id + lead email), same `loadCampaign` lookup, same transport
 * branch. A caller that can send a reply can read the thread it is about to
 * answer, with no extra knowledge. Do NOT introduce a second lookup: two answers
 * to "which sequence is this" is how a worker ends up reading one conversation
 * and answering into another.
 *
 * Both transports are covered because the consumer cannot know which pipe
 * carried a given prospect — exactly as `POST /orgs/replies` cannot. On the
 * Instantly transport the messages live in Instantly's Unibox (`GET /emails`);
 * on ours both halves are in bronze and `fetchSelfSendThread` interleaves them.
 * Both produce the SAME `ThreadMessage` shape, which is what makes one response
 * shape honest for both.
 *
 * ⚠️ NO SILENT FALLBACK. A conversation nobody has on record is a 404, never an
 * empty list: "we never emailed this person" and "we emailed them and they never
 * answered" are different facts, and a worker that cannot tell them apart will
 * happily draft a reply to nobody. A thread we hold but cannot FETCH is a 502,
 * not an empty list either.
 *
 * ⚠️ IT READS OUR OWN MIRROR FIRST, AND THAT IS WHAT MAKES IT SURVIVE THE PLAN
 * BEING CANCELLED. Cancelling an Instantly plan permanently deletes every
 * conversation those mailboxes carried, so a read that asks Instantly live goes
 * blank for every lead at once on cancellation day — and the words are gone at
 * that point, not merely unreachable. The bronze mirror (`instantly_emails_raw`,
 * kept current by the side effect in lib/mirror-emails) is therefore the source,
 * and it costs no Instantly quota per page view.
 *
 * The live provider is consulted in exactly ONE case: the mirror holds nothing
 * for a sequence our own event log says exchanged mail. That is a mirror we know
 * to be incomplete, it is rare, and what it fetches is stored so the next read
 * is local. Once the plan is gone that call fails, which is a 502 — never an
 * empty conversation.
 *
 * Declares NO cost and sends nothing — it is a read of what already happened.
 */

import {
  selectThreadMessages,
  type ThreadMessage,
} from "./forward-positive-reply";
import { insertEmailsBatch } from "./bronze";
import { listEmails, type EmailRecord } from "./instantly-client";
import {
  fetchMirroredEmailRecords,
  hasExchangedMailEvidence,
} from "./mirror-emails";
import { resolveInstantlyApiKey, type CallerInfo } from "./key-client";
import { loadCampaign, type CampaignRow } from "./reply-to-lead";
import { fetchSelfSendThread } from "./self-send/thread";
import { SEND_TRANSPORT_SMTP, type SendTransport } from "./self-send/transport";

const CALLER: CallerInfo = { method: "GET", path: "/orgs/conversations" };

/** A refusal a caller can branch on, rather than a bare 500. */
export class LeadConversationError extends Error {
  constructor(
    public readonly code: "campaign_not_found" | "thread_unavailable",
    public readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = "LeadConversationError";
  }
}

/**
 * Fail loud. An unreadable thread returned as an empty one would tell the caller
 * the prospect said nothing, which is a claim we cannot make.
 */
function unreadable(
  campaign: CampaignRow,
  which: string,
  error: unknown,
): LeadConversationError {
  return new LeadConversationError(
    "thread_unavailable",
    502,
    `Could not read ${which} thread for ${campaign.leadEmail} on ${campaign.instantlyCampaignId}: ${
      error instanceof Error ? error.message : String(error)
    }`,
  );
}

/**
 * Where the messages came from. `mirror` = our bronze copy of the Instantly
 * Unibox (the normal case, and the one that survives the plan being cancelled);
 * `self_send` = the sequence we dispatched ourselves; `provider` = read live
 * from Instantly because the mirror was incomplete.
 */
export type ConversationSource = "mirror" | "self_send" | "provider";

/** One message of the exchange, in the order it happened. */
export interface ConversationMessage {
  /** 'inbound' = the prospect wrote it; 'outbound' = we did. */
  direction: "inbound" | "outbound";
  from: string;
  to: string;
  /** ISO 8601, UTC. Empty only when the source carried no timestamp at all. */
  at: string;
  subject: string;
  /** The message as readable TEXT — markup stripped, never HTML. */
  text: string;
}

export interface LeadConversationInput {
  orgId: string;
  userId: string;
  /** Logical campaign id — the same key `POST /orgs/replies` takes. */
  campaignId: string;
  leadEmail: string;
}

export interface LeadConversation {
  campaignId: string;
  instantlyCampaignId: string;
  leadEmail: string;
  /** The mailbox that carried the outreach. Null on a row predating migration 0025. */
  accountEmail: string | null;
  /** Which pipe carried it — the caller does not need to know this to ask. */
  transport: SendTransport;
  /** Where these messages were read from — see ConversationSource. */
  source: ConversationSource;
  messageCount: number;
  /** Oldest first. Empty when the sequence exists but nothing has been exchanged. */
  messages: ConversationMessage[];
}

/**
 * The API shape for one message.
 *
 * `bodyText` is renamed to `text` deliberately: the consumer passes this
 * straight into an LLM prompt, and what matters there is that the field reads as
 * the words themselves. The stripping is the SAME `htmlToText` the forwarded
 * thread uses, so a message reads identically wherever it surfaces.
 */
function toConversationMessage(m: ThreadMessage): ConversationMessage {
  return {
    direction: m.direction,
    from: m.from,
    to: m.to,
    at: m.date,
    subject: m.subject,
    text: m.bodyText,
  };
}

/**
 * The Instantly-transport thread, out of our own mirror wherever possible.
 *
 * Unlike the positive-reply forward, this does NOT start at the prospect's first
 * reply. The forward is for a human who only needs the newest part of the
 * conversation (the reply quotes the rest beneath it); a worker drafting an
 * answer needs what WE said too, because half of what the prospect is responding
 * to is our own words.
 */
async function fetchInstantlyConversation(
  campaign: CampaignRow,
  input: LeadConversationInput,
): Promise<{ thread: ThreadMessage[]; source: ConversationSource }> {
  let mirrored: EmailRecord[];
  try {
    mirrored = await fetchMirroredEmailRecords(campaign.instantlyCampaignId);
  } catch (error: unknown) {
    throw unreadable(campaign, "our mirror of the", error);
  }
  if (mirrored.length > 0) {
    return { thread: selectThreadMessages(mirrored), source: "mirror" };
  }

  // An empty mirror is ambiguous on its own, and the two readings are different
  // facts a caller must be able to tell apart: a sequence that has exchanged
  // nothing, and one whose words we hold no copy of. Our own event log settles
  // it — see `hasExchangedMailEvidence`.
  let exchanged: boolean;
  try {
    exchanged = await hasExchangedMailEvidence(campaign.instantlyCampaignId);
  } catch (error: unknown) {
    throw unreadable(campaign, "our mirror of the", error);
  }
  if (!exchanged) return { thread: [], source: "mirror" };

  // The mirror is INCOMPLETE for a sequence that did exchange mail. Ask the
  // provider once — and store what comes back, so this costs nothing next time.
  // After the plan is cancelled this throws, which is exactly right: an
  // unreadable thread must never be returned as an empty one.
  let records: EmailRecord[];
  try {
    const { key } = await resolveInstantlyApiKey(input.orgId, input.userId, CALLER);
    records = await listEmails(key, { campaignId: campaign.instantlyCampaignId });
  } catch (error: unknown) {
    throw unreadable(campaign, "the Instantly", error);
  }

  // Fail-soft: failing to widen the mirror must not fail the read the caller
  // asked for, and the next inbound event will try again.
  await insertEmailsBatch(
    campaign.instantlyCampaignId,
    // The lookup is org-scoped, so this campaign belongs to the caller's org.
    input.orgId,
    records,
  ).catch((error: unknown) => {
    console.warn(
      `[instantly-service] lead-conversation: could not mirror the thread it just read for campaign=${campaign.instantlyCampaignId}: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
    return [];
  });

  return { thread: selectThreadMessages(records), source: "provider" };
}

/** We hold the thread; read both halves out of bronze. */
async function fetchSelfSendConversation(
  campaign: CampaignRow,
): Promise<ThreadMessage[]> {
  try {
    return await fetchSelfSendThread(campaign.instantlyCampaignId);
  } catch (error: unknown) {
    throw unreadable(campaign, "the stored", error);
  }
}

/**
 * The messages exchanged with one prospect on one campaign, oldest first.
 *
 * Throws `LeadConversationError('campaign_not_found', 404)` when this org holds
 * no such sequence — distinct from a sequence that exists and has nothing in it,
 * which answers 200 with an empty `messages`.
 */
export async function fetchLeadConversation(
  input: LeadConversationInput,
): Promise<LeadConversation> {
  const campaign = await loadCampaign(
    input.orgId,
    input.campaignId,
    input.leadEmail,
  );
  if (!campaign) {
    throw new LeadConversationError(
      "campaign_not_found",
      404,
      `No campaign ${input.campaignId} in this org for ${input.leadEmail}`,
    );
  }

  const { thread, source } =
    campaign.sendTransport === SEND_TRANSPORT_SMTP
      ? {
          thread: await fetchSelfSendConversation(campaign),
          source: "self_send" as ConversationSource,
        }
      : await fetchInstantlyConversation(campaign, input);

  const messages = thread.map(toConversationMessage);

  return {
    campaignId: input.campaignId,
    instantlyCampaignId: campaign.instantlyCampaignId,
    // The stored casing, not the caller's — the lookup is case-insensitive on
    // purpose, and echoing the caller's spelling back would hide that.
    leadEmail: campaign.leadEmail,
    accountEmail: campaign.accountEmail,
    transport: campaign.sendTransport,
    source,
    messageCount: messages.length,
    messages,
  };
}
