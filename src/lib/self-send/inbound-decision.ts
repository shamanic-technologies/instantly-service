/**
 * MIGRATE-1105 (reply_received): what each message that landed in one of our mailboxes IS, decided for one poll
 * batch of one mailbox, asked of a stored factory Script through the shared service switch (`service-script.ts`).
 * ONE decision per poll batch (never per message: the poller reads thousands a day), only over messages not filed
 * yet.
 *
 * The DECISION (pure, `decideInbound`), per message, in the poll's order:
 *   - `classifyInbound` (DSN / auto-reply / reply / unrelated, the Message-Ids of ours it references),
 *   - `correlateSend` (matched / ambiguous / none),
 *   - `refileStaffReply` with the staff sender rule and the PREFETCHED our-side fact (`senderOurSide`),
 *   - the `mailto:` unsubscribe rule (`mailtoUnsubscribeSends`),
 *   - the orphan-candidate rule (`orphanReplyExclusion` with our own mail domains).
 * Everything around it stays in the poller: the IMAP fetches (a DSN's body is fetched BEFORE the decision, it is
 * an input), the our-side lookup (IO, `senderIsOurSide`, asked only where the rule reads it), bronze, promotions.
 */
import { bareAddress, isStaffSender, STAFF_ADDRESSES, staffDomains } from "../staff-senders";
import {
  classifyInbound,
  correlateSend,
  refileStaffReply,
  type CorrelatedSend,
  type InboundHeaders,
  type InboundKind,
} from "./inbound";
import { isMailtoUnsubscribeSubject, mailtoUnsubscribeSends } from "./opt-out-variant";
import { orphanReplyExclusion } from "./orphan-reply";
import { decideThroughScript, type DecideDeps, type ServiceScriptEngine } from "../service-script";

/** The Engine the inbound decision's script makes (MIGRATE-1105 S5: an email that came back; was reply_received). */
export const REPLY_RECEIVED_OUTCOME = "email_reply_received";
export const INBOUND_DECISION_TASK = "inbound-decision";

/** One message as the decision reads it. */
export interface InboundMessageFacts {
  messageId: string;
  /** Header names lowercased (`normalizeHeaders`). */
  headers: InboundHeaders;
  /** A DSN's text + html (the only message whose body is read before the decision); "" otherwise. */
  body: string;
  /** The parsed `From` display text (null when absent: the `from` header is read instead). */
  from: string | null;
  subject: string | null;
  /**
   * Whether the sender is on OUR side of the thread (`senderIsOurSide`): present only where the rule asks it
   * (`needsOurSideLookup`): a correlated reply from someone neither the lead nor staff.
   */
  senderOurSide?: boolean;
}

export interface InboundDecisionInput {
  accountEmail: string;
  /** Sends from this mailbox, keyed by the Message-Id that went on the wire. */
  knownSends: ReadonlyMap<string, CorrelatedSend>;
  ownMailDomains: ReadonlySet<string>;
  messages: readonly InboundMessageFacts[];
}

export type InboundCorrelationWords =
  | { outcome: "matched"; instantlyCampaignId: string; leadEmail: string; step: number }
  | { outcome: "ambiguous"; campaignIds: string[] }
  | { outcome: "none" };

export interface InboundMessageDecision {
  messageId: string;
  kind: InboundKind;
  referencedMessageIds: string[];
  correlation: InboundCorrelationWords;
  /** The sequences a `mailto:` unsubscribe stops (empty = none). */
  mailtoUnsubscribe: Array<{ instantlyCampaignId: string; leadEmail: string; step: number }>;
  /** A prospect possibly answering from another address: keeps its words, judged by the orphan sweep. */
  orphanCandidate: boolean;
}

const senderOf = (m: InboundMessageFacts): string | undefined => m.from ?? m.headers["from"];

function classifyAndCorrelate(m: InboundMessageFacts, knownSends: ReadonlyMap<string, CorrelatedSend>) {
  const classified = classifyInbound(m.headers, m.body, new Set(knownSends.keys()));
  const correlation = correlateSend(classified.referencedMessageIds, knownSends);
  return { classified, correlation };
}

/** The send to ask `senderIsOurSide` about for this message, or null when the rule does not read it. */
export function needsOurSideLookup<T extends CorrelatedSend>(m: InboundMessageFacts, knownSends: ReadonlyMap<string, T>): T | null {
  const { classified, correlation } = classifyAndCorrelate(m, knownSends);
  if (correlation.outcome !== "matched" || classified.kind !== "reply") return null;
  const send = correlation.send as T;
  return isStaffSender(senderOf(m), send.leadEmail) ? null : send;
}

/** Our own decision for one batch (pure). Throws when a message the rule asks about carries no our-side fact. */
export function decideInbound(input: InboundDecisionInput): InboundMessageDecision[] {
  return input.messages.map((m) => {
    const { classified, correlation } = classifyAndCorrelate(m, input.knownSends);
    const send = correlation.outcome === "matched" ? correlation.send : undefined;
    const sender = senderOf(m);
    let ourSide = isStaffSender(sender, send?.leadEmail);
    if (!ourSide && classified.kind === "reply" && send !== undefined) {
      if (m.senderOurSide === undefined) throw new Error(`reply_received: message ${m.messageId} carries no our-side fact`);
      ourSide = m.senderOurSide;
    }
    const kind = refileStaffReply(classified.kind, ourSide);
    const unsubscribe =
      kind === "unrelated" && correlation.outcome === "none" && isMailtoUnsubscribeSubject(m.subject)
        ? mailtoUnsubscribeSends(bareAddress(sender), input.knownSends.values())
        : [];
    const orphanCandidate =
      unsubscribe.length === 0 &&
      kind === "unrelated" &&
      correlation.outcome === "none" &&
      orphanReplyExclusion({ fromAddress: m.from, subject: m.subject, headers: m.headers }, input.ownMailDomains) === null;
    return {
      messageId: m.messageId,
      kind,
      referencedMessageIds: classified.referencedMessageIds,
      correlation:
        correlation.outcome === "matched"
          ? { outcome: "matched", instantlyCampaignId: correlation.send.instantlyCampaignId, leadEmail: correlation.send.leadEmail, step: correlation.send.step }
          : correlation.outcome === "ambiguous"
            ? { outcome: "ambiguous", campaignIds: correlation.campaignIds }
            : { outcome: "none" },
      mailtoUnsubscribe: unsubscribe.map((s) => ({ instantlyCampaignId: s.instantlyCampaignId, leadEmail: s.leadEmail, step: s.step })),
      orphanCandidate,
    };
  });
}

/**
 * The known sends a batch can reach: those whose Message-Id appears in a message's headers or DSN body, or whose
 * lead is a message's sender (the `mailto:` rule). Case-insensitive text match, a superset of anything the rules
 * can reference; the rest of the mailbox's history is left out of the material.
 */
export function reachableKnownSends(knownSends: ReadonlyMap<string, CorrelatedSend>, messages: readonly InboundMessageFacts[]): Array<{ messageId: string } & CorrelatedSend> {
  const hay = messages.map((m) => `${JSON.stringify(m.headers)}\n${m.body}\n${m.from ?? ""}`.toLowerCase()).join("\n");
  const senders = new Set(messages.map((m) => bareAddress(senderOf(m))).filter((a): a is string => a !== null));
  const out: Array<{ messageId: string } & CorrelatedSend> = [];
  for (const [messageId, s] of knownSends) {
    const bare = messageId.replace(/^<|>$/g, "").toLowerCase();
    if ((bare && hay.includes(bare)) || senders.has(s.leadEmail.trim().toLowerCase())) {
      out.push({ messageId, instantlyCampaignId: s.instantlyCampaignId, leadEmail: s.leadEmail, step: s.step });
    }
  }
  return out.sort((a, b) => (a.messageId < b.messageId ? -1 : a.messageId > b.messageId ? 1 : 0));
}

/** The decision's inputs as the script reads them. */
export function inboundMaterial(input: InboundDecisionInput): Record<string, unknown> {
  return {
    accountEmail: input.accountEmail,
    staffAddresses: [...STAFF_ADDRESSES].sort(),
    staffDomains: [...staffDomains()].sort(),
    ownMailDomains: [...input.ownMailDomains].sort(),
    knownSends: reachableKnownSends(input.knownSends, input.messages),
    messages: input.messages.map((m) => ({
      messageId: m.messageId,
      headers: m.headers,
      body: m.body,
      from: m.from,
      subject: m.subject,
      ...(m.senderOurSide !== undefined ? { senderOurSide: m.senderOurSide } : {}),
    })),
  };
}

export function inboundWords(decisions: readonly InboundMessageDecision[]): Record<string, unknown> {
  return { decisions: decisions.map((d) => ({ ...d })) };
}

const KINDS: ReadonlySet<string> = new Set(["reply", "auto_reply", "bounce", "delay", "unrelated", "staff_reply"]);

/** The script's answer as our own decisions (checked: the poller acts on it). */
export function inboundFromWords(words: Record<string, unknown>): InboundMessageDecision[] {
  if (!Array.isArray(words.decisions)) throw new Error("the reply_received script answered no decisions");
  return (words.decisions as Array<Record<string, unknown>>).map((d) => {
    if (typeof d.messageId !== "string" || typeof d.kind !== "string" || !KINDS.has(d.kind)) throw new Error("the reply_received script answered a decision without messageId/kind");
    const c = (d.correlation ?? {}) as Record<string, unknown>;
    const correlation: InboundCorrelationWords =
      c.outcome === "matched"
        ? { outcome: "matched", instantlyCampaignId: String(c.instantlyCampaignId), leadEmail: String(c.leadEmail), step: Number(c.step) }
        : c.outcome === "ambiguous"
          ? { outcome: "ambiguous", campaignIds: Array.isArray(c.campaignIds) ? c.campaignIds.map(String) : [] }
          : c.outcome === "none"
            ? { outcome: "none" }
            : (() => {
                throw new Error(`the reply_received script answered correlation ${String(c.outcome)}`);
              })();
    return {
      messageId: d.messageId,
      kind: d.kind as InboundKind,
      referencedMessageIds: Array.isArray(d.referencedMessageIds) ? d.referencedMessageIds.map(String) : [],
      correlation,
      mailtoUnsubscribe: Array.isArray(d.mailtoUnsubscribe)
        ? (d.mailtoUnsubscribe as Array<Record<string, unknown>>).map((s) => ({ instantlyCampaignId: String(s.instantlyCampaignId), leadEmail: String(s.leadEmail), step: Number(s.step) }))
        : [],
      orphanCandidate: d.orphanCandidate === true,
    };
  });
}

export const REPLY_RECEIVED_ENGINE: ServiceScriptEngine<InboundMessageDecision[]> = {
  outcome: REPLY_RECEIVED_OUTCOME,
  task: INBOUND_DECISION_TASK,
  consumes: "message_sent",
  fromWords: inboundFromWords,
};

/** The batch's decisions to act on: ours, or the stored script's while switched. Recorded either way. */
export async function inboundDecisionsToAct(input: InboundDecisionInput, deps?: DecideDeps): Promise<InboundMessageDecision[]> {
  const ours = decideInbound(input);
  return decideThroughScript(REPLY_RECEIVED_ENGINE, ours, () => ({ material: inboundMaterial(input), oursWords: inboundWords(ours) }), deps);
}
