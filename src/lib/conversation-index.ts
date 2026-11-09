/**
 * A conversation read in two steps (AGENT-READS-THREAD-1105, owner 2026-11-05).
 *
 * `GET /orgs/conversations` returns every message in full, oldest first. An LLM
 * agent whose tool caps the answer's size then loses the END of a long thread:
 * on 2026-11-04 Sophie read Edmund Bartlett's 5-message thread cut after his
 * positive reply, never saw our 3 answers, and told the client three times that
 * nobody had answered him.
 *
 * So the same conversation is also served as:
 *   1. an INDEX: every message in a short form (id, when, direction, from/to,
 *      subject, a one-line preview, its length), NEWEST FIRST, so a size cap
 *      ever only drops the oldest lines, never the latest;
 *   2. a MESSAGES read: one message, or a chosen list, in full, by id.
 *
 * Both are projections of `fetchLeadConversation` (one lookup, one thread
 * resolution, never a second one). A message id is DERIVED from what the
 * message is (its sequence, direction, time, sender, subject, stored-copy id,
 * words), so the same message carries the same id on every read, with nothing
 * stored. Pure: no IO here.
 */
import { createHash } from "crypto";

import type {
  ConversationMessage,
  LeadConversation,
} from "./lead-conversation";

/** How long a preview may be, in characters: one line an agent can scan. */
export const PREVIEW_MAX_CHARS = 140;

/** How many messages one `messages` read returns at most. */
export const MAX_MESSAGE_IDS = 50;

export interface ConversationIndexEntry {
  id: string;
  at: string;
  direction: ConversationMessage["direction"];
  from: string;
  to: string;
  subject: string;
  /** The message's own words on one line, quoted history cut, ≤ PREVIEW_MAX_CHARS. */
  preview: string;
  /** Length of the full `text`, in characters. */
  chars: number;
  campaignId: string;
  hasAttachments: boolean;
  /** RFC Message-ID, for `POST /orgs/replies` `in_reply_to`. Absent when unknown. */
  messageId?: string;
  /** Every address on the message with its role. Absent when unknown. */
  participants?: ConversationMessage["participants"];
}

export type ConversationIndex = Omit<LeadConversation, "messages"> & {
  order: "newest_first";
  inboundCount: number;
  outboundCount: number;
  lastMessageAt: string | null;
  lastInboundAt: string | null;
  lastOutboundAt: string | null;
  messages: ConversationIndexEntry[];
};

export type IdentifiedConversationMessage = ConversationMessage & { id: string };

/** The id of one message: stable across reads, derived, never stored. */
export function conversationMessageId(m: ConversationMessage): string {
  const hash = createHash("sha256")
    .update(
      JSON.stringify([
        m.instantlyCampaignId,
        m.direction,
        m.at,
        m.from,
        m.to,
        m.subject,
        m.text,
      ]),
    )
    .digest("hex");
  return `msg_${hash.slice(0, 20)}`;
}

/** Lines where quoted history starts in a reply (every client's own marker). */
const QUOTE_START_PATTERNS: RegExp[] = [
  /^>/,
  /^On .+wrote:\s*$/i,
  // Gmail wraps a long header: "On Mon, Nov 2, 2026 at 8:24 AM Kevin Lourd" / "wrote:".
  /^On (Mon|Tue|Wed|Thu|Fri|Sat|Sun), .+\d{4}/,
  /^Le .+a écrit\s*:\s*$/i,
  /^-{2,}\s*Original Message\s*-{2,}/i,
  /^From: .+/,
  /^De : .+/,
];

/** The message's own words on one line: quoted history cut, whitespace folded. */
export function previewOf(text: string): string {
  const own: string[] = [];
  for (const line of text.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (QUOTE_START_PATTERNS.some((p) => p.test(trimmed))) break;
    own.push(trimmed);
  }
  const ownText = own.join(" ").replace(/\s+/g, " ").trim();
  // A message that is ONLY a quote still previews as its words, never empty.
  const oneLine = ownText || text.replace(/\s+/g, " ").trim();
  return oneLine.length <= PREVIEW_MAX_CHARS
    ? oneLine
    : `${oneLine.slice(0, PREVIEW_MAX_CHARS - 1).trimEnd()}…`;
}

function newestAt(messages: ConversationMessage[], direction?: string): string | null {
  let best: string | null = null;
  let bestTime = -Infinity;
  for (const m of messages) {
    if (direction && m.direction !== direction) continue;
    const t = Date.parse(m.at);
    if (!Number.isFinite(t)) continue;
    if (t > bestTime) {
      bestTime = t;
      best = m.at;
    }
  }
  return best;
}

/** Every message, short, newest first. Undated messages (merged last) stay last. */
export function toConversationIndex(c: LeadConversation): ConversationIndex {
  const { messages, ...rest } = c;
  const dated = messages.filter((m) => Number.isFinite(Date.parse(m.at)));
  const undated = messages.filter((m) => !Number.isFinite(Date.parse(m.at)));
  const ordered = [...dated.reverse(), ...undated];
  return {
    ...rest,
    order: "newest_first",
    inboundCount: messages.filter((m) => m.direction === "inbound").length,
    outboundCount: messages.filter((m) => m.direction === "outbound").length,
    lastMessageAt: newestAt(messages),
    lastInboundAt: newestAt(messages, "inbound"),
    lastOutboundAt: newestAt(messages, "outbound"),
    messages: ordered.map((m) => ({
      id: conversationMessageId(m),
      at: m.at,
      direction: m.direction,
      from: m.from,
      to: m.to,
      subject: m.subject,
      preview: previewOf(m.text),
      chars: m.text.length,
      campaignId: m.campaignId,
      hasAttachments: (m.attachments?.length ?? 0) > 0,
      ...(m.messageId ? { messageId: m.messageId } : {}),
      ...(m.participants ? { participants: m.participants } : {}),
    })),
  };
}

/**
 * The asked messages in full, in the order asked. `missing` names every id the
 * conversation does not hold (the caller decides; the route refuses with 404).
 */
export function pickConversationMessages(
  c: LeadConversation,
  ids: string[],
): { messages: IdentifiedConversationMessage[]; missing: string[] } {
  const byId = new Map<string, IdentifiedConversationMessage>();
  for (const m of c.messages) {
    const id = conversationMessageId(m);
    if (!byId.has(id)) byId.set(id, { id, ...m });
  }
  const messages: IdentifiedConversationMessage[] = [];
  const missing: string[] = [];
  for (const id of ids) {
    const m = byId.get(id);
    if (m) messages.push(m);
    else missing.push(id);
  }
  return { messages, missing };
}
