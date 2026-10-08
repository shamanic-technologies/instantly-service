/**
 * Replies that arrive from ANOTHER address — pure rules, no IO.
 *
 * The poller ties a reply to a send through our own Message-Id (`inbound.ts`).
 * That key is absent whenever the prospect answers from a different account as
 * a NEW email (their personal address, a colleague they forwarded our pitch to
 * who writes back fresh). Prod 2026-09-24, Doc Dinners: we emailed
 * stacy.blecher@twinhealth.com, she answered two hours later from
 * drblecher@chsmetabolismdoc.com with no In-Reply-To / References, the poll
 * filed it `unrelated`, and nobody ever read it while the sequence kept going.
 *
 * Three stages, and only the LAST one is a verdict:
 *   1. `orphanReplyExclusion` — STRUCTURAL noise that must never reach a paid
 *      judgment: our own fleet and staff, mailing lists, automated mail,
 *      Instantly's warmup pool. Measured on prod 2026-10-08 over 30 days:
 *      716,030 `unrelated` rows, 3,128 survive (~104/day), most of them cold
 *      pitches and spam addressed TO our mailboxes.
 *   2. `rankOrphanReplyLeads` — which of the leads this mailbox wrote to share
 *      a distinctive token with the message (their name, their company domain,
 *      our subject). A message sharing nothing with any lead is not judged: there
 *      is no lead to attach it to. Only a pre-filter on WHICH leads to offer.
 *   3. A Jev `choice` judgment (chat-service) picks one offered lead or `none`,
 *      with a probability. Below `ORPHAN_REPLY_MIN_PROBABILITY` nothing happens.
 *
 * Owner rule: an "is X a Y?" decision is a judgment, judged once and persisted.
 * No regex or header is the verdict here — stages 1 and 2 only decide what is
 * worth asking about.
 */

import { bareAddress, isStaffSender } from "../staff-senders";
import type { JudgmentChoiceAnswer, JudgmentChoiceQuestion } from "../chat-client";
import { isAutoReply, isDeliveryStatusNotification, type InboundHeaders } from "./inbound";

/**
 * The filter tag Instantly's warmup pool stamps on every warmup email of OUR
 * workspace (subject `... | 3849WSQ WNT6JJB`, sometimes buried inside a random
 * token in the subject or body). Those emails come from thousands of strangers'
 * domains, so no address rule can catch them: 19,968 of the 21,376 non-fleet
 * `unrelated` messages of one prod week carried it (2026-10-08). The tag is set
 * once per Instantly workspace; a new workspace means a new entry here.
 */
export const INSTANTLY_WARMUP_TAGS: readonly string[] = ["WNT6JJB"];

/**
 * A sender whose local part says no human is behind it. Deliberately NOT
 * `info@` / `hello@` / `office@`: a small practice answers from exactly those.
 */
const SYSTEM_LOCAL_PART =
  /^(mailer-daemon|postmaster|no-?reply[\w.-]*|do-?not-?reply|notifications?|bounces?[\w.+-]*|dmarc[\w.-]*)$/i;

/** Headers only a mailing list, a bulk sender or a report carries. */
const LIST_HEADERS = ["list", "list-id", "list-unsubscribe", "list-unsubscribe-post", "feedback-id"];

export type OrphanReplyExclusion =
  | "no_sender"
  | "own_domain"
  | "staff"
  | "automated"
  | "mailing_list"
  | "system_sender"
  | "instantly_warmup";

export interface OrphanReplyMessage {
  fromAddress: string | null;
  subject: string | null;
  headers: InboundHeaders;
  /** The body, when we hold it. The warmup tag may live only there. */
  text?: string | null;
}

function domainOf(address: string): string {
  return address.split("@")[1] ?? "";
}

/** Does this text carry one of our Instantly warmup tags? */
export function carriesWarmupTag(text: string | null | undefined): boolean {
  if (!text) return false;
  const upper = text.toUpperCase();
  return INSTANTLY_WARMUP_TAGS.some((tag) => upper.includes(tag));
}

/**
 * Why this message can never be a prospect's answer, or null when it might be.
 *
 * Every reason is a fact about WHO sent it or HOW (a list, an autoresponder, a
 * warmup engine) — never about what it says. What it says is the judgment's.
 */
export function orphanReplyExclusion(
  message: OrphanReplyMessage,
  ownDomains: ReadonlySet<string>,
): OrphanReplyExclusion | null {
  const address = bareAddress(message.fromAddress ?? message.headers["from"]);
  if (!address) return "no_sender";

  const domain = domainOf(address);
  if (ownDomains.has(domain)) return "own_domain";
  if (isStaffSender(address)) return "staff";

  if (isDeliveryStatusNotification(message.headers) || isAutoReply(message.headers)) {
    return "automated";
  }
  const contentType = message.headers["content-type"]?.toLowerCase() ?? "";
  if (contentType.includes("multipart/report")) return "automated";

  if (LIST_HEADERS.some((name) => message.headers[name] !== undefined)) return "mailing_list";
  const precedence = message.headers["precedence"]?.trim().toLowerCase();
  if (precedence === "list") return "mailing_list";

  if (SYSTEM_LOCAL_PART.test(address.split("@")[0] ?? "")) return "system_sender";

  if (carriesWarmupTag(message.subject) || carriesWarmupTag(message.text)) {
    return "instantly_warmup";
  }
  return null;
}

/** One lead this mailbox wrote to, offered to the judgment as a possible match. */
export interface OrphanReplyLead {
  instantlyCampaignId: string;
  leadEmail: string;
  firstName: string | null;
  lastName: string | null;
  companyName: string | null;
  /** The subject of the first email we sent them. */
  subject: string | null;
  /** The start of the first email we sent them, as text. */
  excerpt: string | null;
  /** The latest step that actually went out — the step a reply is filed on. */
  lastStep: number;
  firstSentAt: Date;
  lastSentAt: Date;
  orgId: string | null;
}

export interface RankedOrphanReplyLead {
  lead: OrphanReplyLead;
  score: number;
}

/** Mail providers whose domain says nothing about a company. */
const FREEMAIL = new Set([
  "gmail", "googlemail", "yahoo", "hotmail", "outlook", "live", "msn", "aol",
  "icloud", "me", "mac", "proton", "protonmail", "gmx", "mail", "yandex", "zoho",
  "comcast", "verizon", "att", "sbcglobal", "bellsouth",
]);

/** Words too common to tie a message to one lead. */
const STOPWORDS = new Set([
  "the", "and", "for", "you", "your", "with", "that", "this", "from", "have",
  "are", "our", "can", "will", "about", "quick", "question", "re", "fwd", "fw",
  "hello", "thanks", "thank", "regards", "best", "just", "what", "when", "how",
  "into", "more", "help", "team", "company", "group", "inc", "llc", "ltd", "corp",
  "practice", "business", "growing", "idea", "ideas", "call", "time", "today",
]);

function words(text: string | null | undefined, minLength: number): Set<string> {
  const out = new Set<string>();
  if (!text) return out;
  for (const token of text.toLowerCase().split(/[^a-z0-9à-ÿ]+/)) {
    if (token.length >= minLength && !STOPWORDS.has(token)) out.add(token);
  }
  return out;
}

function domainRoot(address: string): string | null {
  const labels = domainOf(address).split(".");
  const root = labels.length >= 2 ? labels[labels.length - 2] : labels[0];
  if (!root || root.length < 3 || FREEMAIL.has(root)) return null;
  return root;
}

/** Signals tying a lead to a message, with how much each one is worth. */
function leadSignals(lead: OrphanReplyLead): Array<{ token: string; weight: number }> {
  const signals = new Map<string, number>();
  const add = (token: string | null | undefined, weight: number) => {
    if (!token) return;
    const key = token.toLowerCase();
    if (key.length < 2 || STOPWORDS.has(key)) return;
    signals.set(key, Math.max(signals.get(key) ?? 0, weight));
  };

  // A surname is rarer than a first name; both are what a person signs with.
  for (const w of words(lead.firstName, 2)) add(w, 2);
  for (const w of words(lead.lastName, 2)) add(w, 3);
  for (const w of words(bareAddress(lead.leadEmail)?.split("@")[0] ?? "", 3)) add(w, 2);
  add(domainRoot(lead.leadEmail), 3);
  for (const w of words(lead.companyName, 4)) add(w, 2);
  // Our subject words are what a forward or a quoted answer carries.
  for (const w of words(lead.subject, 5)) add(w, 1);

  return [...signals].map(([token, weight]) => ({ token, weight }));
}

/** The leads worth offering to the judgment: at least this much in common. */
export const ORPHAN_REPLY_MIN_SCORE = 2;

/** How many leads one judgment is offered. */
export const ORPHAN_REPLY_MAX_LEADS = 8;

/**
 * Rank the leads this mailbox wrote to by what they share with the message.
 *
 * A pre-filter on WHICH leads the judgment sees, never the verdict: a display
 * name shared by two people, or a colleague answering with nothing but our
 * quoted pitch, is exactly what the judgment is for. Only leads we had written
 * to BEFORE the message arrived are eligible.
 */
export function rankOrphanReplyLeads(
  message: { fromAddress: string | null; subject: string | null; text: string | null; receivedAt: Date },
  leads: readonly OrphanReplyLead[],
): RankedOrphanReplyLead[] {
  const address = bareAddress(message.fromAddress) ?? "";
  const tokens = new Set<string>([
    ...words(message.fromAddress, 2),
    ...words(address.split("@")[0] ?? "", 2),
    ...words(message.subject, 2),
    ...words(message.text, 2),
  ]);
  const senderRoot = domainRoot(address);
  if (senderRoot) tokens.add(senderRoot);

  const ranked: RankedOrphanReplyLead[] = [];
  for (const lead of leads) {
    if (lead.firstSentAt.getTime() > message.receivedAt.getTime()) continue;
    let score = 0;
    for (const { token, weight } of leadSignals(lead)) {
      if (tokens.has(token)) score += weight;
    }
    if (score >= ORPHAN_REPLY_MIN_SCORE) ranked.push({ lead, score });
  }

  ranked.sort(
    (a, b) =>
      b.score - a.score || b.lead.lastSentAt.getTime() - a.lead.lastSentAt.getTime(),
  );
  return ranked.slice(0, ORPHAN_REPLY_MAX_LEADS);
}

/** The judgment's answer key for "this answers none of them". */
export const ORPHAN_REPLY_NONE = "none";

/** The caller-chosen key the answer comes back under. */
export const ORPHAN_REPLY_QUESTION_KEY = "orphan_reply";

/**
 * How sure the judgment must be before a stranger's email becomes a lead's
 * reply. A wrong attribution fabricates a reply in the customer's stats AND
 * stops a live sequence; a missed one leaves the email in bronze, stored with
 * its words, where it can still be read.
 */
export const ORPHAN_REPLY_MIN_PROBABILITY = 0.75;

function leadKey(index: number): string {
  return `lead_${index + 1}`;
}

function describeLead(lead: OrphanReplyLead): string {
  const name = [lead.firstName, lead.lastName].filter(Boolean).join(" ") || "(no name)";
  const company = lead.companyName ? ` at ${lead.companyName}` : "";
  const subject = lead.subject ? `"${lead.subject}"` : "(no subject)";
  const excerpt = lead.excerpt ? ` It began: "${lead.excerpt}"` : "";
  return `We cold-emailed ${name}${company} <${lead.leadEmail}> from this mailbox, first on ${lead.firstSentAt.toISOString().slice(0, 10)}, subject ${subject}.${excerpt}`;
}

/** The choice question: one key per offered lead, plus `none`. */
export function buildOrphanReplyQuestion(
  ranked: readonly RankedOrphanReplyLead[],
): JudgmentChoiceQuestion {
  const criteria: JudgmentChoiceQuestion["criteria"] = {};
  ranked.forEach(({ lead }, index) => {
    criteria[leadKey(index)] = {
      what: `A person answering our email to this lead. ${describeLead(lead)}`,
      examples: [
        "The lead writing back from a personal or other work address",
        "A colleague or assistant the lead forwarded our email to, answering us",
      ],
    };
  });
  criteria[ORPHAN_REPLY_NONE] = {
    what: "Not an answer to any of the emails above.",
    examples: [
      "Someone pitching their own product or service to us",
      "A newsletter, promotion, notification or automated message",
      "A warmup or test email",
      "A person writing about something none of our emails were about",
    ],
  };
  return {
    type: "choice",
    instructions:
      "This email arrived in a mailbox we use for cold outreach. It is NOT threaded onto any email we sent, so it may be: a prospect answering our pitch from ANOTHER address (their personal account, another job), a colleague they forwarded our email to who now writes to us, or ordinary unrelated mail (pitches to us, spam, newsletters, warmup traffic). Pick the lead it answers only when the email itself shows it is that person, or someone acting for them, responding to our email: their name or signature, a reference to what we wrote, a quoted copy of our email. A shared first name alone is not enough. When in doubt, answer none.",
    criteria,
  };
}

/** The state the judgment reads: the inbound email itself. */
export function buildOrphanReplyState(message: {
  fromAddress: string | null;
  toAddress: string;
  subject: string | null;
  receivedAt: Date;
  text: string | null;
}): string {
  const body = message.text?.trim()
    ? message.text.trim().slice(0, 3000)
    : "(the body of this email could not be read; judge from the sender and subject)";
  return [
    `From: ${message.fromAddress ?? "(unknown)"}`,
    `To: ${message.toAddress}`,
    `Date: ${message.receivedAt.toISOString()}`,
    `Subject: ${message.subject ?? "(none)"}`,
    "",
    body,
  ].join("\n");
}

export type OrphanReplyVerdict =
  | { outcome: "matched"; lead: OrphanReplyLead; probability: number }
  | { outcome: "none" | "low_confidence"; probability: number };

/**
 * Read the judgment. Only a lead chosen at or above the bar is a match; an
 * unknown key is a failure, never a guess.
 */
export function readOrphanReplyVerdict(
  answer: JudgmentChoiceAnswer,
  ranked: readonly RankedOrphanReplyLead[],
): OrphanReplyVerdict {
  const probability = answer.probabilities[answer.choice] ?? 0;
  if (answer.choice === ORPHAN_REPLY_NONE) return { outcome: "none", probability };

  const index = ranked.findIndex((_, i) => leadKey(i) === answer.choice);
  if (index < 0) {
    throw new Error(`orphan-reply judgment answered an unknown option "${answer.choice}"`);
  }
  if (probability < ORPHAN_REPLY_MIN_PROBABILITY) {
    return { outcome: "low_confidence", probability };
  }
  return { outcome: "matched", lead: ranked[index]!.lead, probability };
}
