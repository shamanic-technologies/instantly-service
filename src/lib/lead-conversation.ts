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
 * Declares NO cost and sends nothing — it is a read of what already happened.
 */

import {
  selectThreadMessages,
  type ThreadMessage,
} from "./forward-positive-reply";
import { listEmails } from "./instantly-client";
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
 * Instantly holds the thread; ask it for the whole exchange.
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
): Promise<ThreadMessage[]> {
  try {
    const { key } = await resolveInstantlyApiKey(input.orgId, input.userId, CALLER);
    const records = await listEmails(key, {
      campaignId: campaign.instantlyCampaignId,
    });
    return selectThreadMessages(records);
  } catch (error: unknown) {
    // Fail loud. An unreadable thread returned as an empty one would tell the
    // caller the prospect said nothing, which is a claim we cannot make.
    throw new LeadConversationError(
      "thread_unavailable",
      502,
      `Could not read the Instantly thread for ${campaign.leadEmail} on ${campaign.instantlyCampaignId}: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }
}

/** We hold the thread; read both halves out of bronze. */
async function fetchSelfSendConversation(
  campaign: CampaignRow,
): Promise<ThreadMessage[]> {
  try {
    return await fetchSelfSendThread(campaign.instantlyCampaignId);
  } catch (error: unknown) {
    throw new LeadConversationError(
      "thread_unavailable",
      502,
      `Could not read the stored thread for ${campaign.leadEmail} on ${campaign.instantlyCampaignId}: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
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

  const thread =
    campaign.sendTransport === SEND_TRANSPORT_SMTP
      ? await fetchSelfSendConversation(campaign)
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
    messageCount: messages.length,
    messages,
  };
}
