/**
 * What a positive reply CARRIES when it is reported to campaign-service's
 * trigger door (`positive_reply_received`, AIC-COMMON-1015): the prospect's own
 * words and the emails those words answer.
 *
 * Every pipe answering that trigger receives this verbatim as `/execute`
 * `trigger.item`. The AI Instant Call workflow (workflow-service, dynasty
 * `ai-instant-call-*`) reads it to tell the rep what the prospect wrote and to
 * walk back through the thread one keypress at a time. Its shape is LOCKED with
 * that workflow:
 *
 *   { channel: "email",
 *     replyText: string | null,
 *     priorMessages: [{ direction: "outbound" | "inbound", text }] }   // newest-first, <= 5
 *
 * `channel` is the field name crm-service stamps on its own reply occurrences
 * (`item.channel`: "whatsapp" for messaging, "email" for the Gmail mirror), so
 * one reader serves both producers. Every reply this service reports came by
 * email. The instant-call workflow rings ONLY on an email reply and treats a
 * missing channel as not-email, so dropping this field silences every call
 * (INSTANT-CALL-EMAIL-ONLY-1016).
 *
 * A reply read as an occurrence with no `answers`/`formName` (that is a form).
 * Identity (name, title, company...) and the phone reveal are NOT here: the
 * workflow reads them off lead-service itself.
 *
 * Everything is CLEANED first: quoted history and our own signature plus its
 * opt-out footer are cut. Those are our words, not theirs, and reading them back
 * at a rep costs billed minutes to say nothing.
 */

import { createHash } from "node:crypto";
import { fetchMirroredEmailRecords } from "./mirror-emails";
import { selectThreadMessages, type ThreadMessage } from "./forward-positive-reply";
import { stripQuotedHistory } from "./self-send/qualify-reply";
import { isSelfSendCampaignId } from "./self-send/transport";
import { fetchSelfSendThread } from "./self-send/thread";

/** One earlier email of the thread, as the call walks back through it. */
export interface PriorMessage {
  direction: "outbound" | "inbound";
  text: string;
}

/** The reply channel of every occurrence this service reports: a cold-email reply. */
export const POSITIVE_REPLY_CHANNEL = "email" as const;

/** The `item` of a `positive_reply_received` occurrence. */
export interface PositiveReplyItem {
  /** How the reply reached us. Byte-equal to crm-service's `item.channel`. */
  channel: typeof POSITIVE_REPLY_CHANNEL;
  /** What the prospect wrote, cleaned. Null when we hold no words (the call says so). */
  replyText: string | null;
  /** The emails the reply answers, newest-first, entry 0 = the one it responds to. */
  priorMessages: PriorMessage[];
}

/**
 * How many earlier emails the call offers to walk back through.
 *
 * A cold sequence is three steps, so in practice the walk is one or two hops and
 * this never binds. It exists because a thread that has been going back and
 * forth for weeks would otherwise put a rep on a keypress treadmill while the
 * prospect waits, and every started minute of the call is billed.
 */
export const MAX_PRIOR_MESSAGES = 5;

/**
 * A line carrying nothing but the RFC 3676 signature delimiter.
 *
 * ⚠️ This is NOT `stripAccountSignature` and must not be replaced by it. That
 * function owns the WIRE form (`<p>--</p>`, `<br>--<br>`) and is protected by two
 * production incidents; by the time a body reaches here it has been through
 * `htmlToText`, and whether the plain marker that function looks for
 * (`\n\n--\n`) survives depends on how the delimiter was wrapped (`<p>--</p>`
 * yields an empty line before it, `<br>--<br>` does not). What survives the
 * conversion in EVERY case is a line that is exactly `--`, which is what this
 * matches.
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
 * What a human should hear, out of a stored body: the quoted history a client
 * staples under a reply and our signature block (with the opt-out footer under
 * it) are cut. Both are truncate-at-first-marker cuts, so their order does not
 * matter. A body carrying neither comes back unchanged.
 */
export function cleanForSpeech(text: string): string {
  return stripSpokenSignature(stripQuotedHistory(text)).trim();
}

/** True when a thread entry carries words rather than a placeholder. */
function hasWords(message: ThreadMessage): boolean {
  const text = message.bodyText?.trim();
  return Boolean(text) && text !== "(no body)";
}

/** Index of the prospect's last message with words, or -1. */
function lastInboundIndex(messages: ThreadMessage[]): number {
  for (let i = messages.length - 1; i >= 0; i--) {
    if (messages[i].direction === "inbound" && hasWords(messages[i])) return i;
  }
  return -1;
}

/**
 * The rest of the conversation, newest-first, for the rep to walk back through.
 *
 * ⚠️ IT STOPS AT THE PROSPECT'S REPLY. Entry 0 is the email the reply ANSWERS;
 * anything after the reply (an answer we already sent) is what happened next,
 * not context. With no inbound at all the whole thread is offered: we could not
 * read what they wrote, so our own last emails are the only context there is.
 * Every entry is cleaned and a body-less one is DROPPED.
 */
export function buildPriorMessages(messages: ThreadMessage[]): PriorMessage[] {
  const lastInbound = lastInboundIndex(messages);
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
  const i = lastInboundIndex(messages);
  return i >= 0 ? messages[i].bodyText.trim() : null;
}

/** The item when the thread could not be read: no words, still an email reply. */
export function emptyPositiveReplyItem(): PositiveReplyItem {
  return { channel: POSITIVE_REPLY_CHANNEL, replyText: null, priorMessages: [] };
}

/** Pure: the item, out of one read of the thread. */
export function buildPositiveReplyItem(thread: ThreadMessage[]): PositiveReplyItem {
  const latest = latestInboundText(thread);
  return {
    channel: POSITIVE_REPLY_CHANNEL,
    replyText: latest ? cleanForSpeech(latest) || null : null,
    priorMessages: buildPriorMessages(thread),
  };
}

/**
 * The idempotency key of ONE reply: a webhook retry, a re-poll, or a
 * re-qualification of the same words (interested, then meeting booked) is the
 * same occurrence and records once. A NEW reply later in the thread is a new
 * occurrence. Keyed on the reply's own date and words; when we hold no words
 * (unreadable thread) the thread itself is the key, so an unreadable reply
 * still records once. Hashed: campaign-service caps the key at 200 characters.
 */
export function positiveReplyKey(
  instantlyCampaignId: string,
  leadEmail: string,
  thread: ThreadMessage[],
): string {
  const i = lastInboundIndex(thread);
  const reply = i >= 0 ? `${thread[i].date}\n${thread[i].bodyText.trim()}` : "unread";
  const digest = createHash("sha256")
    .update(`${instantlyCampaignId}\n${leadEmail.trim().toLowerCase()}\n${reply}`)
    .digest("hex");
  return `positive_reply_received:${digest}`;
}

/**
 * The conversation, from whichever side of the transport holds it. Read ONCE:
 * the reply and the emails it answers come out of the same read, so the two
 * halves cannot disagree about the same exchange.
 */
export async function readReplyThread(instantlyCampaignId: string): Promise<ThreadMessage[]> {
  return isSelfSendCampaignId(instantlyCampaignId)
    ? await fetchSelfSendThread(instantlyCampaignId)
    : selectThreadMessages(await fetchMirroredEmailRecords(instantlyCampaignId));
}
