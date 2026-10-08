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

import { sql } from "drizzle-orm";

import { db } from "../db";
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
import {
  fetchOwnDispatchedMessages,
  fetchSelfSendThread,
  type OwnDispatchedMessage,
} from "./self-send/thread";
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
  /**
   * WHICH `email_sent` outreach fact this message is (`GET
   * /internal/outreach-facts`), so a consumer pairs message and fact by
   * identity, never by comparing their clocks — see `loadSendFactIndex`.
   * Null on every inbound message, and on an outbound one no served fact
   * recorded (a manual answer, a send the event stream never saw, or a send
   * younger than the feed's next emission tick).
   */
  outreachFact: ConversationSendFact | null;
}

/** The served `email_sent` fact an outbound message is, in the feed's own words. */
export interface ConversationSendFact {
  /** The fact's `subjectKey`, byte for byte — the id lead-service keeps for it. */
  subjectKey: string;
  /** The fact's step (1 = first email). Null where the feed serves none (poll-only sends). */
  step: number | null;
  /** The fact's position: always set, by step or by order. */
  position: "first" | "followup";
}

/**
 * The served `email_sent` facts of one lead's sequences, keyed by the stored
 * copy each one recorded: Instantly's email id (webhook `email_id`, or the
 * mirror row a poll event was promoted from) or our own dispatch row id.
 */
export interface SendFactIndex {
  byInstantlyEmailId: Map<string, ConversationSendFact>;
  byDispatchId: Map<string, ConversationSendFact>;
}

const EMPTY_SEND_FACTS: SendFactIndex = {
  byInstantlyEmailId: new Map(),
  byDispatchId: new Map(),
};

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
  facts: SendFactIndex,
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
    outreachFact: sendFactOf(m, facts),
  };
}

/** The fact this message IS, by its stored copy's id. Inbound never is one. */
function sendFactOf(
  m: ThreadMessage,
  facts: SendFactIndex,
): ConversationSendFact | null {
  if (m.direction !== "outbound" || !m.sourceRef) return null;
  const { instantlyEmailId, dispatchId } = m.sourceRef;
  return (
    (instantlyEmailId ? facts.byInstantlyEmailId.get(instantlyEmailId) : undefined) ??
    (dispatchId ? facts.byDispatchId.get(dispatchId) : undefined) ??
    null
  );
}

/**
 * Every served `email_sent` fact of this lead on these sequences, indexed by
 * the stored copy of the email it recorded.
 *
 * ⚠️ IDENTITY, NEVER TIME. The fact is stamped with the EVENT's time (the
 * webhook's), the message with the EMAIL's own time, and the two routinely sit
 * a minute apart (measured: 13:05:44 vs 13:06:45 for one step-1 send), so a
 * consumer pairing them by clock pairs nothing — or, with a window, pairs two
 * close emails wrongly. Each event already names its email:
 *  - webhook: `raw_payload.email_id` = Instantly's email id (every webhook
 *    send in prod carries one);
 *  - poll:    `source_row_id` = the `instantly_emails_raw` row it came from;
 *  - self-send: `source_row_id` = our `smtp_dispatch_raw` row.
 * Read from the SERVED facts (not raw events) so the subject key handed out is
 * one lead-service holds: an unstepped poll copy of a webhook send is never a
 * fact, and the webhook fact claims the email instead.
 *
 * Fail loud: an unreadable index would silently strip every pairing.
 */
export async function loadSendFactIndex(
  leadEmail: string,
  instantlyCampaignIds: string[],
): Promise<SendFactIndex> {
  if (instantlyCampaignIds.length === 0) return EMPTY_SEND_FACTS;
  let rows: Record<string, unknown>[];
  try {
    const result = await db.execute(sql`
      SELECT f.subject_key AS "subjectKey",
             f.payload->>'step' AS "step",
             f.payload->>'position' AS "position",
             COALESCE(e.raw_payload->>'email_id', r.instantly_email_id) AS "instantlyEmailId",
             CASE WHEN e.source = 'self_send' THEN e.source_row_id END AS "dispatchId"
      FROM outreach_facts f
      JOIN instantly_events e ON e.id = substr(f.subject_key, 6)
      LEFT JOIN instantly_emails_raw r ON e.source = 'poll_emails' AND r.id = e.source_row_id
      WHERE f.type = 'email_sent'
        AND f.subject_key LIKE 'ievt:%'
        AND lower(f.lead_email) = ${leadEmail.trim().toLowerCase()}
        AND f.instantly_campaign_id = ANY(${sql.param(instantlyCampaignIds)}::text[])
      ORDER BY f.seq
    `);
    rows = (result as { rows: Record<string, unknown>[] }).rows;
  } catch (error: unknown) {
    throw new LeadConversationError(
      "thread_unavailable",
      502,
      `Could not read the send facts for ${leadEmail}: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }

  const index: SendFactIndex = { byInstantlyEmailId: new Map(), byDispatchId: new Map() };
  for (const row of rows) {
    const fact: ConversationSendFact = {
      subjectKey: String(row.subjectKey),
      step: row.step == null ? null : Number(row.step),
      position: row.position === "followup" ? "followup" : "first",
    };
    // First served fact wins: the feed emits one per event, oldest first.
    const emailId = row.instantlyEmailId;
    if (typeof emailId === "string" && emailId && !index.byInstantlyEmailId.has(emailId)) {
      index.byInstantlyEmailId.set(emailId, fact);
    }
    const dispatchId = row.dispatchId;
    if (typeof dispatchId === "string" && dispatchId && !index.byDispatchId.has(dispatchId)) {
      index.byDispatchId.set(dispatchId, fact);
    }
  }
  return index;
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
  facts: SendFactIndex = EMPTY_SEND_FACTS,
): ConversationMessage[] {
  const entries = threads.flatMap(({ sequence, messages }, sequenceIndex) =>
    messages.map((m, messageIndex) => ({
      sequenceIndex,
      messageIndex,
      time: Date.parse(m.date),
      message: toConversationMessage(m, sequence, facts),
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
  // What WE answered, which no provider told us about. `POST /orgs/replies`
  // records it in bronze on this transport too, and Instantly's mirror only
  // learns of it when the prospect writes back and the thread is re-mirrored —
  // so between the two it exists nowhere a reader looks. Fetched FIRST because
  // it is also evidence the sequence exchanged mail (see below).
  let own: OwnDispatchedMessage[];
  try {
    own = await fetchOwnDispatchedMessages(campaign.instantlyCampaignId);
  } catch (error: unknown) {
    throw unreadable(campaign, "our record of the", error);
  }

  let mirrored: EmailRecord[];
  try {
    mirrored = await fetchMirroredEmailRecords(campaign.instantlyCampaignId);
  } catch (error: unknown) {
    throw unreadable(campaign, "our mirror of the", error);
  }
  if (mirrored.length > 0) {
    return {
      thread: withOwnReplies(selectThreadMessages(mirrored), mirrored, own),
      source: "mirror",
    };
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
  // An answer we dispatched is itself proof the sequence exchanged mail, so a
  // reply sent on a sequence whose events we somehow hold none of is still a
  // conversation and must not read as an empty one.
  if (!exchanged && own.length === 0) return { thread: [], source: "mirror" };
  if (!exchanged) {
    return { thread: withOwnReplies([], [], own), source: "mirror" };
  }

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

  return {
    thread: withOwnReplies(selectThreadMessages(records), records, own),
    source: "provider",
  };
}

/**
 * Fold the answers we dispatched into a thread read from the provider's copy.
 *
 * ⚠️ DEDUP ON THE PROVIDER'S OWN ID, never on time or on the body. Once the
 * prospect writes back, the re-mirrored thread contains our answer as well
 * (Instantly returns it as `ue_type: 3`, manual-sent), and rendering both
 * copies would show the customer the same message twice. A message the
 * provider already carries is therefore DROPPED from our side: the provider's
 * copy is the one that was actually delivered, so it wins.
 *
 * A message with no provider id was never given to a provider (the self-send
 * transport), so nothing can duplicate it and it always survives.
 */
function withOwnReplies(
  thread: ThreadMessage[],
  records: EmailRecord[],
  own: OwnDispatchedMessage[],
): ThreadMessage[] {
  if (own.length === 0) return thread;
  const alreadyRendered = new Set(records.map((r) => r.id));
  const missing = own
    .filter((o) => o.instantlyEmailId === null || !alreadyRendered.has(o.instantlyEmailId))
    .map((o) => o.message);
  if (missing.length === 0) return thread;

  // Oldest first, the order every reader of this shape already relies on. An
  // undatable message sorts last rather than being dropped — we know we sent it.
  return [...thread, ...missing].sort((a, b) => {
    const at = Date.parse(a.date);
    const bt = Date.parse(b.date);
    if (Number.isNaN(at) !== Number.isNaN(bt)) return Number.isNaN(at) ? 1 : -1;
    if (Number.isNaN(at)) return 0;
    return at - bt;
  });
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

  const facts = await loadSendFactIndex(
    input.leadEmail,
    sequences.map((s) => s.instantlyCampaignId),
  );
  const messages = mergeConversationMessages(
    threads.map(({ sequence, thread }) => ({ sequence, messages: thread })),
    facts,
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
