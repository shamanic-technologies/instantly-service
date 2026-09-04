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
 * ⚠️ IT ANSWERS FOR THE WHOLE CAMPAIGN, NOT ONE STORED ROW. campaign-service
 * mints a fresh campaign row every time the campaign's workflow changes and
 * keeps the ancestors, so one campaign as the customer knows it is routinely
 * dozens of rows — 46 for one production brand — and a prospect emailed over
 * three months sits in several of them. Reading a single row therefore showed a
 * FRACTION of the exchange looking exactly like the whole of it: one measured
 * lead had its first three emails (May, May, June) under a sibling row and its
 * reply placed ABOVE the email it answered, because the only send the panel
 * could see was a July one. The identity comes from campaign-service, which owns
 * it (see `campaign-identity.ts`); this module never re-derives it.
 *
 * ⚠️ THE THREAD IS RESOLVED THE SAME WAY THE REPLY IS SENT — same key in
 * (logical campaign id + lead email), same `loadCampaignSequences` lookup, same
 * transport branch per sequence. A caller that can send a reply can read the
 * thread it is about to answer, with no extra knowledge. Do NOT introduce a
 * second lookup: two answers to "which sequences are these" is how a worker ends
 * up reading one conversation and answering into another.
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
import { getCampaignFamily } from "./campaign-client";
import { listEmails, type EmailRecord } from "./instantly-client";
import {
  fetchMirroredEmailRecords,
  hasExchangedMailEvidence,
} from "./mirror-emails";
import { resolveInstantlyApiKey, type CallerInfo } from "./key-client";
import { loadCampaignSequences, type CampaignRow } from "./reply-to-lead";
import { fetchSelfSendThread } from "./self-send/thread";
import { SEND_TRANSPORT_SMTP, type SendTransport } from "./self-send/transport";

const CALLER: CallerInfo = { method: "GET", path: "/orgs/conversations" };

/**
 * How many sequences one lead may hold across one campaign before the read
 * refuses rather than fans out.
 *
 * Measured against production: the busiest (org, lead) pair in the fleet sits in
 * 23 campaign rows across ALL its campaigns, 99.9% of leads sit in 3 or fewer,
 * and narrowing to one campaign can only be smaller. 40 is headroom over that
 * and still a bound, so a lead panel can never open an unbounded fan-out.
 *
 * Refusing is deliberate: silently reading the first N would hand back part of a
 * conversation looking exactly like all of it, which is the failure this whole
 * read exists to remove.
 */
export const MAX_CONVERSATION_SEQUENCES = 40;

/**
 * How many sequence threads are read at once.
 *
 * Normally each is one indexed mirror read, but a sequence whose mirror is
 * incomplete costs a paginated live `GET /emails`. A lead panel is opened
 * interactively, so the fan-out is capped rather than left to the family's size.
 */
const THREAD_FETCH_CONCURRENCY = 4;

/** A refusal a caller can branch on, rather than a bare 500. */
export class LeadConversationError extends Error {
  constructor(
    public readonly code:
      | "campaign_not_found"
      | "thread_unavailable"
      | "campaign_identity_unavailable"
      | "too_many_sequences",
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
  /** The stored campaign row this message was exchanged under. */
  campaignId: string;
  /** That row's Instantly (or `self:`) sequence id. */
  instantlyCampaignId: string;
}

/** One stored campaign row that contributed to the exchange. */
export interface ConversationSequence {
  campaignId: string;
  instantlyCampaignId: string;
  /** The mailbox that carried it. Null on a row predating migration 0025. */
  accountEmail: string | null;
  transport: SendTransport;
  /** Where THIS row's messages were read from — see ConversationSource. */
  source: ConversationSource;
  messageCount: number;
}

export interface LeadConversationInput {
  orgId: string;
  userId: string;
  /** Logical campaign id — the same key `POST /orgs/replies` takes. */
  campaignId: string;
  leadEmail: string;
}

export interface LeadConversation {
  /** The campaign id asked for. Unchanged, whatever else it turned out to be part of. */
  campaignId: string;
  /**
   * Every stored campaign row of this campaign that holds this lead, oldest
   * first — one entry when the campaign is a single row.
   */
  campaignIds: string[];
  /** The asked row's sequence id. Each message says which row it came from. */
  instantlyCampaignId: string;
  leadEmail: string;
  /** The asked row's mailbox. Null on a row predating migration 0025. */
  accountEmail: string | null;
  /** The asked row's pipe — the caller does not need to know this to ask. */
  transport: SendTransport;
  /** Where the ASKED row's messages were read from; `sequences` says it per row. */
  source: ConversationSource;
  messageCount: number;
  /** Oldest first, across every contributing row. Empty when nothing was exchanged. */
  messages: ConversationMessage[];
  /** What each contributing row carried, oldest first. */
  sequences: ConversationSequence[];
}

/**
 * The API shape for one message.
 *
 * `bodyText` is renamed to `text` deliberately: the consumer passes this
 * straight into an LLM prompt, and what matters there is that the field reads as
 * the words themselves. The stripping is the SAME `htmlToText` the forwarded
 * thread uses, so a message reads identically wherever it surfaces.
 */
function toConversationMessage(
  m: ThreadMessage,
  sequence: CampaignRow,
): ConversationMessage {
  return {
    direction: m.direction,
    from: m.from,
    to: m.to,
    at: m.date,
    subject: m.subject,
    text: m.bodyText,
    campaignId: sequence.campaignId,
    instantlyCampaignId: sequence.instantlyCampaignId,
  };
}

/**
 * Merge the sequences' threads into ONE exchange, oldest first.
 *
 * The sort key is the message's own timestamp. Ties, and messages whose source
 * carried no timestamp at all, fall back to the order the sequences happened in
 * and then to each thread's own order — never to a fabricated time. An undated
 * message sorts LAST rather than being dropped: we hold it, we just cannot place
 * it, and dropping it would hide a message the prospect really wrote.
 */
export function mergeConversationMessages(
  threads: { sequence: CampaignRow; messages: ThreadMessage[] }[],
): ConversationMessage[] {
  const entries = threads.flatMap(({ sequence, messages }, sequenceIndex) =>
    messages.map((m, messageIndex) => ({
      sequenceIndex,
      messageIndex,
      time: Date.parse(m.date),
      message: toConversationMessage(m, sequence),
    })),
  );

  entries.sort((a, b) => {
    const aDated = Number.isFinite(a.time);
    const bDated = Number.isFinite(b.time);
    if (aDated && bDated && a.time !== b.time) return a.time - b.time;
    if (aDated !== bDated) return aDated ? -1 : 1;
    if (a.sequenceIndex !== b.sequenceIndex) return a.sequenceIndex - b.sequenceIndex;
    return a.messageIndex - b.messageIndex;
  });

  return entries.map((e) => e.message);
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
 * Every campaign id this campaign is made of, as campaign-service defines it.
 *
 * FAILS LOUD. Falling back to the asked row alone on an outage would return a
 * fraction of the conversation looking exactly like all of it — the precise
 * failure this read exists to remove, so it must not be the degraded mode.
 */
async function resolveFamily(input: LeadConversationInput): Promise<string[]> {
  try {
    return await getCampaignFamily(input.campaignId, input.orgId);
  } catch (error: unknown) {
    throw new LeadConversationError(
      "campaign_identity_unavailable",
      502,
      `Could not resolve which campaigns ${input.campaignId} is part of: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }
}

/** Run `task` over `items`, at most `limit` at a time, preserving order. */
async function mapWithConcurrency<T, R>(
  items: T[],
  limit: number,
  task: (item: T) => Promise<R>,
): Promise<R[]> {
  const results = new Array<R>(items.length);
  let next = 0;
  const workers = Array.from(
    { length: Math.min(limit, items.length) },
    async () => {
      for (;;) {
        const index = next++;
        if (index >= items.length) return;
        results[index] = await task(items[index]);
      }
    },
  );
  await Promise.all(workers);
  return results;
}

/** One sequence's thread, on whichever pipe carried it. */
async function fetchSequenceThread(
  sequence: CampaignRow,
  input: LeadConversationInput,
): Promise<{ thread: ThreadMessage[]; source: ConversationSource }> {
  if (sequence.sendTransport === SEND_TRANSPORT_SMTP) {
    return {
      thread: await fetchSelfSendConversation(sequence),
      source: "self_send",
    };
  }
  return fetchInstantlyConversation(sequence, input);
}

/**
 * The messages exchanged with one prospect on one campaign, oldest first, across
 * every stored row that campaign is made of.
 *
 * Throws `LeadConversationError('campaign_not_found', 404)` when this org holds
 * no such exchange — distinct from an exchange that exists and has nothing in
 * it, which answers 200 with an empty `messages`, and from one we hold but could
 * not read, which is a 502. A reader acts differently on each of the three.
 */
export async function fetchLeadConversation(
  input: LeadConversationInput,
): Promise<LeadConversation> {
  const campaignIds = await resolveFamily(input);
  const sequences = await loadCampaignSequences(
    input.orgId,
    campaignIds,
    input.leadEmail,
  );

  if (sequences.length === 0) {
    throw new LeadConversationError(
      "campaign_not_found",
      404,
      `No campaign ${input.campaignId} in this org for ${input.leadEmail}`,
    );
  }

  if (sequences.length > MAX_CONVERSATION_SEQUENCES) {
    throw new LeadConversationError(
      "too_many_sequences",
      502,
      `${input.leadEmail} sits in ${sequences.length} sequences of campaign ${input.campaignId}, over the ${MAX_CONVERSATION_SEQUENCES} this read will fan out to`,
    );
  }

  // Any sequence failing takes the whole read down. Half a conversation
  // presented as the whole one is worse than saying it could not be read.
  const threads = await mapWithConcurrency(
    sequences,
    THREAD_FETCH_CONCURRENCY,
    async (sequence) => ({
      sequence,
      ...(await fetchSequenceThread(sequence, input)),
    }),
  );

  const messages = mergeConversationMessages(
    threads.map(({ sequence, thread }) => ({ sequence, messages: thread })),
  );

  // The asked row still describes itself, so a single-row campaign answers byte
  // for byte as it did before this read learned about families.
  const askedIndex = Math.max(
    threads.findIndex((t) => t.sequence.campaignId === input.campaignId),
    0,
  );
  const asked = threads[askedIndex];

  return {
    campaignId: input.campaignId,
    campaignIds: sequences.map((s) => s.campaignId),
    instantlyCampaignId: asked.sequence.instantlyCampaignId,
    // The stored casing, not the caller's — the lookup is case-insensitive on
    // purpose, and echoing the caller's spelling back would hide that.
    leadEmail: asked.sequence.leadEmail,
    accountEmail: asked.sequence.accountEmail,
    transport: asked.sequence.sendTransport,
    source: asked.source,
    messageCount: messages.length,
    messages,
    sequences: threads.map(({ sequence, thread, source }) => ({
      campaignId: sequence.campaignId,
      instantlyCampaignId: sequence.instantlyCampaignId,
      accountEmail: sequence.accountEmail,
      transport: sequence.sendTransport,
      source,
      messageCount: thread.length,
    })),
  };
}
