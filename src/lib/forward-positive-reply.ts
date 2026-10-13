/**
 * A positive reply is CELEBRATED to the client (lib/celebrate-positive-reply):
 * `maybeForwardPositiveReply` delegates there, exactly once per thread. This
 * module also keeps the thread helpers (reading and rendering the exchange)
 * other modules share.
 *
 * The bare thread forward to the agency inbox, the "answer it yourself" email
 * and the staff check-ins are GONE (owner 2026-10-13): a message sent because
 * of a fact is a campaign on a trigger, never a staff email from this service.
 * A client answers a reply by hand in the Unibox; a client paying for AI
 * answers is told by the AI's own escalation (lib/notify-brand-team).
 *
 * Fail-soft: never throws into the webhook promote path (a 5xx would make
 * Instantly auto-pause the webhook).
 */

import type { EmailRecord } from "./instantly-client";
import { isEscalatedReplyKind, POSITIVE_REPLY_KINDS } from "./reply-kind";
import { isStaffSender } from "./staff-senders";

/**
 * The reply kinds that mean "worth forwarding to the agency inbox". We forward
 * ONLY on these; negative / automated replies never trigger a forward.
 *
 * All four positive distinctions forward: someone who wants to know more, or
 * points us at the right buyer, is exactly as worth reading as someone asking
 * for a call. Deal progress is absent because it is no longer a reply kind at
 * all — a booked meeting is recorded by the lead-outcomes service, and it is
 * not evidence that a reply just arrived.
 *
 * ⚠️ `lead_referral` is NOT forwarded here any more: it is ESCALATED instead
 * (`ESCALATED_REPLY_KINDS`, lib/escalate-off-topic-reply), which forwards the
 * same thread with a "a person must follow this up" lead — one email per reply.
 * Historical note: it used to forward while reporting `neutral`. Forwarding answers "is this
 * worth a human's eyes"; the coarse map answers "was this a buying signal we
 * should price and count". A unit test asserts exactly that relationship — do
 * not restore an equality assertion between the two.
 */
export const POSITIVE_QUALIFICATION_EVENT_TYPES = new Set<string>(
  POSITIVE_REPLY_KINDS.filter((k) => !isEscalatedReplyKind(k)),
);

/** True iff this event is a positive reply kind. */
export function isPositiveQualification(eventType: string): boolean {
  return POSITIVE_QUALIFICATION_EVENT_TYPES.has(eventType);
}

/** The subset of a campaign row this side effect needs. */
export interface ForwardPositiveReplyCampaign {
  instantlyCampaignId: string;
  campaignId: string | null;
  orgId: string | null;
  userId: string | null;
  runId: string | null;
  brandIds?: string[] | null;
  /**
   * The caller campaign whose WHOLE conversation the email tells. Defaults to
   * `campaignId`. The escalation passes it separately because it deliberately
   * leaves `campaignId` null (run attribution, see escalate-reply).
   */
  conversationCampaignId?: string | null;
}

/** One rendered message in the conversation thread. */
export interface ThreadMessage {
  /** 'outbound' = sent by us (ue_type 1/3), 'inbound' = reply from the lead (2). */
  direction: "outbound" | "inbound";
  from: string;
  to: string;
  date: string;
  subject: string;
  bodyText: string;
  /**
   * Which stored copy this message IS — Instantly's email id (mirror / live
   * read) or our own `smtp_dispatch_raw` row id. Absent where the source has
   * none (an inbound IMAP row). Lets a reader tie the message to the
   * `email_sent` event that recorded it by IDENTITY, never by time.
   */
  sourceRef?: { instantlyEmailId?: string; dispatchId?: string };
  /**
   * An automated step of the cold sequence (Instantly `ue_type 1`, our dispatch
   * at step > 0), never a person writing. A follow-up that went out after the
   * prospect wrote because nobody saw their reply is NOT an answer to it.
   */
  sequenceStep?: boolean;
}

/**
 * Collapse an Instantly email HTML body to readable plain text. Deliberately
 * light — enough to strip markup for an internal ops email, not a full parser.
 * Removes <style>/<script>, turns <br> and block-close tags into newlines,
 * strips all remaining tags, decodes the few common entities, and collapses
 * runaway blank lines.
 *
 * ⚠️ A PARAGRAPH ends in an EMPTY LINE (`</p>`, `</h1-6>`, `</table>` → "\n\n"),
 * a `<br>` / `</div>` / `</li>` / `</tr>` in ONE newline. The text this returns
 * is what a customer reads as "the email we sent" (lead timeline), and our mail
 * is one `<p>` per paragraph, so the prospect sees blank lines between them. A
 * single newline per `</p>` made every sent email read as one dense block.
 * `</div>` stays single on purpose: mail clients (Gmail) write one `<div>` per
 * LINE, so doubling it would space out every line of a prospect's reply.
 *
 * ⚠️ A `<blockquote>` comes out as `> `-prefixed lines (one `>` per nesting
 * level), the plain-text quoting every client uses. Without the prefix the
 * quoted thread reads as the prospect's own words, and `stripQuotedHistory`
 * cannot tell where a quote ENDS, so a bottom-posted answer below it ("STOP!")
 * was cut away with the quote.
 */
export function htmlToText(html: string): string {
  return quoteBlockquotes(flattenHtml(
    html
      .replace(/<\s*blockquote\b[^>]*>/gi, `\n${QUOTE_OPEN}\n`)
      .replace(/<\s*\/\s*blockquote\s*>/gi, `\n${QUOTE_CLOSE}\n`),
  ));
}

const QUOTE_OPEN = "\u0001";
const QUOTE_CLOSE = "\u0002";

/** Prefix the lines between the blockquote sentinels; drop the sentinels. */
function quoteBlockquotes(text: string): string {
  if (!text.includes(QUOTE_OPEN)) return text;
  let depth = 0;
  const out: string[] = [];
  // Blank lines touching a quote boundary are the markup's own spacing.
  let afterBoundary = false;
  const atBoundary = () => {
    while (out.length && /^[> ]*$/.test(out[out.length - 1])) out.pop();
    afterBoundary = true;
  };
  for (const line of text.split("\n")) {
    const trimmed = line.trim();
    if (trimmed === QUOTE_OPEN) { atBoundary(); depth += 1; continue; }
    if (trimmed === QUOTE_CLOSE) { atBoundary(); depth = Math.max(0, depth - 1); continue; }
    if (afterBoundary && !trimmed) continue;
    afterBoundary = false;
    out.push(depth > 0 ? `${"> ".repeat(depth)}${line}`.trimEnd() : line);
  }
  return out
    .join("\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function flattenHtml(html: string): string {
  return html
    .replace(/<\s*(style|script)[^>]*>[\s\S]*?<\s*\/\s*\1\s*>/gi, "")
    .replace(/<\s*br\s*\/?\s*>/gi, "\n")
    .replace(/<\s*\/\s*(p|h[1-6]|table)\s*>/gi, "\n\n")
    .replace(/<\s*\/\s*(div|tr|li)\s*>/gi, "\n")
    .replace(/<[^>]+>/g, "")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

/** Prefer the plain-text body; fall back to a stripped HTML body. */
function bodyToText(record: EmailRecord): string {
  const text = record.body?.text?.trim();
  if (text) return text;
  const html = record.body?.html?.trim();
  if (html) return htmlToText(html);
  return "(no body)";
}

/**
 * Normalize the raw Instantly email records of one campaign into an ordered
 * conversation thread. 1 Instantly campaign = 1 lead, so `GET /emails?campaign_id`
 * returns exactly this lead's thread. Includes real messages (ue_type 1 sent,
 * 2 received, 3 manual-sent); skips scheduled-but-unsent (ue_type 4). Sorted
 * oldest → newest by the email timestamp so the reader follows the exchange in
 * order.
 */
export function selectThreadMessages(records: EmailRecord[]): ThreadMessage[] {
  return records
    .filter((r) => r.ue_type === 1 || r.ue_type === 2 || r.ue_type === 3)
    .slice()
    .sort(
      (a, b) =>
        new Date(a.timestamp_email).getTime() -
        new Date(b.timestamp_email).getTime(),
    )
    .map((r) => ({
      // An inbound message one of our own people wrote (a staff answer CC'd to
      // the sending mailbox) is OUR side of the conversation — see staff-senders.
      direction:
        r.ue_type === 2 && !isStaffSender(r.from_address_email, r.lead) ? "inbound" : "outbound",
      from: r.from_address_email || r.eaccount || "(unknown)",
      to: r.to_address_email_list || r.lead || "(unknown)",
      date: r.timestamp_email,
      subject: r.subject || "(no subject)",
      bodyText: bodyToText(r),
      ...(r.id ? { sourceRef: { instantlyEmailId: r.id } } : {}),
      ...(r.ue_type === 1 ? { sequenceStep: true } : {}),
    }));
}

/** One message our IMAP poller filed for a sequence, beside its RFC Message-ID. */
export interface ImapThreadMessage {
  /** The RFC 5322 Message-ID: the same id Instantly's mirror stores as `message_id`. */
  messageId: string;
  message: ThreadMessage;
}

/**
 * Oldest first, the order every reader of this shape already relies on. An
 * undatable message sorts last rather than being dropped: we hold it.
 */
export function byDate(messages: ThreadMessage[]): ThreadMessage[] {
  return messages.sort((a, b) => {
    const at = Date.parse(a.date);
    const bt = Date.parse(b.date);
    if (Number.isNaN(at) !== Number.isNaN(bt)) return Number.isNaN(at) ? 1 : -1;
    if (Number.isNaN(at)) return 0;
    return at - bt;
  });
}

function normalizedMessageId(id: unknown): string | null {
  if (typeof id !== "string") return null;
  const bare = id.trim().replace(/^<|>$/g, "").toLowerCase();
  return bare || null;
}

/**
 * Pure: fold the messages our IMAP poller filed into a thread read from
 * Instantly's copy. The prospect's newest message counts wherever it was
 * ingested (owner 2026-10-12, Dr. Gallego's Sep 14 reply).
 *
 * ⚠️ DEDUP ON THE RFC Message-ID, never on time or body. A reply both pipes saw
 * (5 of 7 in prod on 2026-10-12) is in the mirror as `message_id` and in
 * `imap_messages_raw.message_id`; Instantly's copy wins. An IMAP row with no
 * Message-ID cannot be matched, so it is kept: we hold it.
 */
export function withImapMessages(
  thread: ThreadMessage[],
  records: EmailRecord[],
  imap: ImapThreadMessage[],
): ThreadMessage[] {
  if (imap.length === 0) return thread;
  const mirrored = new Set(
    records.map((r) => normalizedMessageId(r.message_id)).filter((id): id is string => id !== null),
  );
  const missing = imap
    .filter((m) => {
      const id = normalizedMessageId(m.messageId);
      return id === null || !mirrored.has(id);
    })
    .map((m) => m.message);
  if (missing.length === 0) return thread;
  return byDate([...thread, ...missing]);
}

/** Format an ISO timestamp as a readable email date, e.g. "Jul 13, 2026, 5:57 PM UTC". */
export function formatThreadDate(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return (
    d.toLocaleString("en-US", {
      month: "short",
      day: "numeric",
      year: "numeric",
      hour: "numeric",
      minute: "2-digit",
      hour12: true,
      timeZone: "UTC",
    }) + " UTC"
  );
}

/** The conversation subject = the newest message's subject (what a reply carries). */
export function threadSubject(messages: ThreadMessage[]): string {
  for (let i = messages.length - 1; i >= 0; i--) {
    if (messages[i].subject && messages[i].subject !== "(no subject)") {
      return messages[i].subject;
    }
  }
  return messages.length > 0 ? messages[messages.length - 1].subject : "(no subject)";
}


/**
 * Celebrate a positively-qualified reply to the client — exactly once per
 * thread (lib/celebrate-positive-reply). No-op unless `eventType` is a positive
 * qualification and the campaign is org-scoped. Never throws.
 *
 * ⚠️ BACKGROUND BY DEFAULT. This runs inside `promoteEvent`, i.e. inside
 * Instantly's webhook, and the celebration waits (minutes, bounded) until the
 * prospect's words are readable. The claim is taken before this returns; the
 * send finishes on its own. `background: false` awaits it (tests).
 *
 * Platform sends (orgId null) are out of scope, as before.
 */
export async function maybeForwardPositiveReply(
  campaign: ForwardPositiveReplyCampaign,
  leadEmail: string,
  eventType: string,
  options: { background?: boolean; waitsMs?: number[] } = {},
): Promise<void> {
  if (!isPositiveQualification(eventType)) return;
  if (!campaign.orgId) return;

  const { celebrateOnce } = await import("./celebrate-positive-reply");
  const run = celebrateOnce(campaign, leadEmail, { waitsMs: options.waitsMs, kind: eventType });
  if (options.background === false) {
    await run;
  } else {
    void run;
  }
}
