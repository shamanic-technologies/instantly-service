/**
 * A positive reply nobody will answer: tell the client to answer it, from their
 * own mailbox, in one click.
 *
 * The celebration (lib/celebrate-positive-reply) tells the client "you have
 * nothing to do: we handle the reply". That is true only when an AI responder
 * campaign runs on the brand's offer. When none does, the prospect waits on the
 * client, the client waits on us, and nobody writes back (Shockwavecenters,
 * 2026-10-08: drdoug@prohealthdoc.com asked a question and got nothing).
 *
 * Owner 2026-10-11: whether a responder answers is read off what campaign-service
 * ANSWERED when asked to take the reply (lib/trigger-sales-interest-campaign),
 * never off a campaign's `ongoing` status: a responder that was on but out of
 * money answered nobody. So when campaign-service says no email responder took
 * it (none bought, client's money out, own team, refused), or one took it and
 * sent nothing in 24 h (lib/unanswered-reply-deadline), every member of the
 * client org gets a plain email (copy locked word for word by the owner
 * 2026-10-08, only the placeholders vary). Covers questions (info requests,
 * which get no celebration) as well as interested replies (after the
 * celebration):
 *
 *   - Subject `Re: <thread subject>`, Reply-To = the PROSPECT, so hitting Reply
 *     answers them under the same subject with the conversation quoted below.
 *   - One model-written line saying what the prospect wrote. Factual, never
 *     invented; when it cannot be produced it is DROPPED, never made up.
 *   - Below `---`, the whole conversation, newest first: their reply verbatim on
 *     top, then every email of the thread with sender and date.
 *
 * The celebration is untouched. Agency inbox in Cc, visible (owner 2026-10-11,
 * "the staff in cc"; on the first member's copy only, one copy is enough); an
 * org with no member reachable gets it at the agency inbox, somebody must answer.
 *
 * ONCE PER THREAD: claim `client_answer_requested_at` (migration 0068), taken
 * before the send, released only when NO member could be emailed. A failed
 * responder read sends nothing and takes no claim (an unreadable answer is not
 * "nobody answers"). Never throws.
 */

import { and, eq, sql } from "drizzle-orm";
import { db } from "../db";
import { instantlyCampaigns } from "../db/schema";
import { agencyInbox } from "./agency-inbox";
import { findOngoingResponderCampaigns, getCampaignTriggerScope } from "./campaign-client";
import { orgComplete } from "./chat-client";
import { listOrgMembers, type OrgMember } from "./client-org-client";
import { brandContextOrNull, escapeHtml, prospectLabel } from "./celebrate-positive-reply";
import { sendEmail } from "./email-client";
import type { ForwardPositiveReplyCampaign, ThreadMessage } from "./forward-positive-reply";
import { formatThreadDate } from "./forward-positive-reply";
import { findLeadOnCampaignByEmail } from "./lead-client";
import { handThreadFollowupsToHuman } from "./hand-followups-to-human";
import type { HistoryItem } from "./prospect-history";
import { stripQuotedHistory } from "./self-send/qualify-reply";
import { bareAddress } from "./staff-senders";
import { createRun, getRun } from "./runs-client";
import { fetchBrandAccountStatus, type BrandAccountStatus, type BrandRelationship } from "./brand-account-status";

/** The transactional-email template this module sends (deployed at startup). */
export const CLIENT_ANSWER_EVENT_TYPE = "positive-reply-answer-request";
/** The staff-only check-in this module sends for an OLD question (deployed at startup). */
export const STAFF_CHECK_IN_EVENT_TYPE = "unanswered-question-check-in";

/**
 * A question older than this may have been answered where we cannot see it (a
 * call, the client's own mailbox). Asking the client to answer it reads wrong,
 * so staff get a ready check-in to send instead (owner 2026-10-11, Dr. Gallego:
 * asked Aug 2, relabeled Oct 8).
 */
export const QUESTION_STALE_AFTER_MS = 3 * 24 * 60 * 60 * 1000;

// ─── Pure ────────────────────────────────────────────────────────────────────

/** Pure: the thread subject with every leading Re:/Fwd: removed, then `Re: ` once. */
export function replySubject(subject: string): string {
  let base = subject.trim();
  for (;;) {
    const next = base.replace(/^\s*(re|fw|fwd|aw|tr|sv)\s*(\[\d+\])?\s*:\s*/i, "");
    if (next === base) break;
    base = next;
  }
  return base ? `Re: ${base}` : "Re: your outreach";
}

/** Pure: a model's summary line made safe to send, or null to drop it. */
export function cleanSummaryLine(raw: string | null | undefined): string | null {
  if (!raw) return null;
  let line = raw.trim().replace(/^["'“]+|["'”]+$/g, "").trim();
  if (!line || /\n/.test(line)) return null;
  if (/^none\.?$/i.test(line)) return null;
  // No dashes as punctuation in client copy.
  line = line.replace(/\s*[—–]\s*/g, ", ");
  if (line.length > 220) return null;
  if (!/[.!?]$/.test(line)) line = `${line}.`;
  return line;
}

/** Pure: every address WE sent from in this thread (any of our sending mailboxes). */
export function ourAddresses(messages: ThreadMessage[]): Set<string> {
  const ours = new Set<string>();
  for (const m of messages) {
    if (m.direction !== "outbound") continue;
    const a = bareAddress(m.from);
    if (a) ours.add(a);
  }
  return ours;
}

function timeOf(m: ThreadMessage): number {
  const t = Date.parse(m.date);
  return Number.isFinite(t) ? t : Number.NEGATIVE_INFINITY;
}

/**
 * Pure: the latest message the PROSPECT wrote: inbound and not sent from one of
 * our own mailboxes. Never one of ours (owner 2026-10-11: an alert quoted
 * Fallon's Sep 4 bump as Dr. Gallego's question). Null when there is none.
 */
export function latestProspectMessage(messages: ThreadMessage[]): ThreadMessage | null {
  const ours = ourAddresses(messages);
  let best: ThreadMessage | null = null;
  for (const m of messages) {
    if (m.direction !== "inbound") continue;
    const from = bareAddress(m.from);
    if (from && ours.has(from)) continue;
    if (!best || timeOf(m) >= timeOf(best)) best = m;
  }
  return best;
}

/**
 * Pure: our first message sent AFTER the prospect's question, or null. Any
 * message a PERSON on our side wrote counts (never an automated sequence step:
 * recovered May replies were each followed by our own scheduled follow-ups), a manual reply from the Instantly Unibox included,
 * and one of our mailboxes filed inbound (a copy landing in the thread): a
 * person handled the thread, so nobody is asked to answer it.
 */
export function ourAnswerAfter(messages: ThreadMessage[], question: ThreadMessage): ThreadMessage | null {
  const asked = timeOf(question);
  if (!Number.isFinite(asked)) return null;
  const ours = ourAddresses(messages);
  const fromUs = (m: ThreadMessage) => {
    // An automated follow-up of the sequence is not a person answering them.
    if (m.direction === "outbound") return !m.sequenceStep;
    const from = bareAddress(m.from);
    return Boolean(from && ours.has(from));
  };
  const after = messages.filter((m) => fromUs(m) && timeOf(m) > asked).sort((a, b) => timeOf(a) - timeOf(b));
  return after[0] ?? null;
}

/** Pure: the mailbox the thread was sent from (our latest message), and the first name it signs with. */
export function threadSender(messages: ThreadMessage[]): { address: string | null; firstName: string | null } {
  const ours = messages.filter((m) => m.direction === "outbound").sort((a, b) => timeOf(b) - timeOf(a))[0];
  if (!ours) return { address: null, firstName: null };
  const address = bareAddress(ours.from);
  const display = ours.from.match(/^\s*"?([^"<]*?)"?\s*<[^>]+>\s*$/)?.[1]?.trim();
  if (display && !display.includes("@")) return { address, firstName: display.split(/\s+/)[0] };
  const local = address?.split("@")[0] ?? "";
  const word = local.split(/[._+-]/)[0];
  return { address, firstName: /^[a-z]{2,}$/i.test(word) ? word.charAt(0).toUpperCase() + word.slice(1).toLowerCase() : null };
}

/**
 * Pure: does this reply kind ASK something? The thread's persisted reply kind
 * is the judgment (`lead_info_requested` = a question); unknown (a staff hand
 * run, a live caller) reads as a question, the wording these emails always had.
 * Any other positive kind ("I'll take a look at it. Thanks", Robert, Sep 18)
 * is a reply, not a question: never told it "asked a question".
 */
export function isQuestionKind(replyKind: string | null | undefined): boolean {
  return !replyKind || replyKind === "lead_info_requested";
}

/**
 * Pure: the `x-brand-id` of every call this thread makes: ALL its brands, CSV,
 * the fleet's multi-brand form. The first brand alone 409'd against a run that
 * carries two (runs-service parent-child conflict on brandIds, 2026-10-13).
 */
export function brandHeader(campaign: { brandIds?: string[] | null }): string | undefined {
  const ids = (campaign.brandIds ?? []).filter((id) => id && id.trim());
  return ids.length > 0 ? ids.join(",") : undefined;
}

/** Pure: "Aug 2, 2026" (UTC). */
export function shortDate(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric", timeZone: "UTC" });
}

/** Pure: the first line, naming both sides. "Client: Doc Dinners · Prospect: Dr. Gallego (Shin Wellness)". */
export function whoLine(input: { brandName: string | null; leadEmail: string; leadFullName: string | null; company: string | null }): string {
  const name = input.leadFullName?.trim();
  const detail = input.company?.trim() || input.leadEmail;
  const prospect = name ? `${name} (${detail})` : input.leadEmail;
  return `Client: ${input.brandName?.trim() || "unknown brand"} · Prospect: ${prospect}`;
}

export interface AnswerRequestInput {
  /** The member's first name; null = "Hi,". */
  clientFirstName: string | null;
  leadEmail: string;
  /** "Doug Arvanitis"; null = the address. */
  leadFullName: string | null;
  /** "Doug"; null = "their". */
  leadFirstName: string | null;
  company: string | null;
  /** The one line on what they wrote; null = dropped. */
  summaryLine: string | null;
  /** The brand the thread is for; null = "unknown brand". */
  brandName: string | null;
  /** The prospect's reply (their latest message, never ours), verbatim. */
  reply: ThreadMessage;
  /** Every other email of the thread (any order). */
  earlier: ThreadMessage[];
}

export interface AnswerRequestContent {
  subject: string;
  text: string;
  html: string;
}

/** Pure: a body as written, with runs of blank lines (mail-client spacing) folded to one. */
export function tidyBody(body: string): string {
  return body
    .replace(/[ \t\u00A0\u200B]+$/gm, "")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

/**
 * Pure: the prospect's own words, without the thread their client quoted under
 * them (that thread is listed in full below, once). Cuts at the shared quote
 * markers (`stripQuotedHistory`: `>`, "On ... wrote:", "Original Message") and
 * at an Outlook header block (`From:` followed by `Sent:`/`Date:`/`To:`/
 * `Subject:` within 4 lines) or its `____` rule. Nothing quoted = unchanged;
 * nothing left = the body as written.
 */
export function replyWordsOnly(body: string, stripShared: (t: string) => string): string {
  const shared = stripShared(body);
  const lines = shared.split(/\r?\n/);
  let cut = lines.length;
  for (let i = 0; i < lines.length; i++) {
    if (/^\s*_{8,}\s*$/.test(lines[i])) { cut = i; break; }
    if (/^\s*From:\s*\S/i.test(lines[i]) && lines.slice(i + 1, i + 5).some((l) => /^\s*(Sent|Date|To|Subject):/i.test(l))) {
      cut = i;
      break;
    }
  }
  const words = lines.slice(0, cut).join("\n").trim();
  return words || body;
}

/**
 * The quoted conversation stays well under the email service's body limit
 * (a long thread 413'd the staff check-in, 2026-10-13): each email is cut at
 * QUOTED_BODY_MAX_CHARS with a note, and at most QUOTED_EARLIER_MAX earlier
 * emails are quoted, the rest counted.
 */
export const QUOTED_BODY_MAX_CHARS = 3000;
export const QUOTED_EARLIER_MAX = 12;

/** Pure: a body cut to `max` characters, saying how much was left out. */
export function capBody(body: string, max: number = QUOTED_BODY_MAX_CHARS): string {
  if (body.length <= max) return body;
  return `${body.slice(0, max).trimEnd()}\n[... ${body.length - max} more characters not shown]`;
}

function messageBlock(m: ThreadMessage): string {
  return [`From: ${m.from}`, `Date: ${formatThreadDate(m.date)}`, `Subject: ${m.subject}`, ``, capBody(tidyBody(m.bodyText))].join(
    "\n",
  );
}

/** Pure: the newest earlier emails, capped, plus a line counting the older ones left out. */
function earlierBlocks(earlier: ThreadMessage[]): string[] {
  const sorted = [...earlier].sort(byDateDesc);
  const shown = sorted.slice(0, QUOTED_EARLIER_MAX).map(messageBlock);
  const left = sorted.length - QUOTED_EARLIER_MAX;
  return left > 0 ? [...shown, `[${left} older email${left === 1 ? "" : "s"} in this thread not shown]`] : shown;
}

function byDateDesc(a: ThreadMessage, b: ThreadMessage): number {
  return (Date.parse(b.date) || 0) - (Date.parse(a.date) || 0);
}

function conversationBelow(reply: ThreadMessage, earlier: ThreadMessage[]): string {
  const words = { ...reply, bodyText: replyWordsOnly(reply.bodyText, stripQuotedHistory) };
  return [messageBlock(words), ...earlierBlocks(earlier.filter((m) => m !== reply))].join("\n\n");
}

/**
 * Pure: the email (owner copy 2026-10-08, plus the who line and the date of
 * their message, owner 2026-10-11). Plain text; the HTML is that text, escaped.
 */
export function renderAnswerRequest(input: AnswerRequestInput): AnswerRequestContent {
  const who = input.leadFullName?.trim() || input.leadEmail;
  const company = input.company?.trim();
  const possessive = input.leadFirstName?.trim() ? `${input.leadFirstName.trim()}'s` : "their";
  const lines = [
    whoLine(input),
    ``,
    input.clientFirstName?.trim() ? `Hi ${input.clientFirstName.trim()},` : "Hi,",
    ``,
    `Good news: ${who}${company ? ` at ${company}` : ""} is interested and is waiting for an answer.`,
    `They wrote on ${shortDate(input.reply.date)}.`,
    ...(input.summaryLine ? [input.summaryLine] : []),
    ``,
    `You can answer by hitting Reply on this email. It adds ${possessive} email in To:, under the same subject, with the conversation below.`,
    ``,
    `Thanks,`,
    `Kevin`,
    ``,
    `---`,
  ];
  const text = `${lines.join("\n")}\n${conversationBelow(input.reply, input.earlier)}`;
  const html = `<div style="white-space:pre-wrap;word-break:break-word;">${escapeHtml(text)}</div>`;
  return { subject: replySubject(input.reply.subject), text, html };
}

export interface StaffCheckInInput {
  brandName: string | null;
  leadEmail: string;
  leadFullName: string | null;
  company: string | null;
  /** Their latest message, never ours. Null = their words could not be read (stated, never invented). */
  question: ThreadMessage | null;
  /** When they wrote; absent = their message's date. Required when `question` is null. */
  repliedAt?: string;
  /** False = a positive reply that asks nothing ("I'll take a look"): worded as a reply. Default true. */
  isQuestion?: boolean;
  earlier: ThreadMessage[];
  /** The mailbox the thread was sent from; the check-in goes from there. */
  senderAddress: string | null;
  /** The check-in to send, ready as is. */
  checkIn: string;
}

/**
 * Pure: the staff-only email for an OLD question. Someone may have answered it
 * where we cannot see (a call, the client's mailbox), so staff get a check-in
 * to send in the thread, never a request to answer the original question.
 */
export function renderStaffCheckIn(input: StaffCheckInInput): AnswerRequestContent {
  const brand = input.brandName?.trim() || "the client";
  const who = input.leadFullName?.trim() || input.leadEmail;
  const repliedAt = input.repliedAt ?? input.question?.date ?? "";
  const lines = [
    whoLine(input),
    ``,
    input.isQuestion === false
      ? `${who} replied on ${shortDate(repliedAt)}.`
      : `${who} asked a question on ${shortDate(repliedAt)}.`,
    ...(input.question ? [] : [`Their words could not be read, only that they replied.`]),
    `Nobody answered in this thread.`,
    `Someone may have answered another way, so check in first.`,
    ``,
    `Send this in the thread${input.senderAddress ? `, from ${input.senderAddress}` : ""}:`,
    ``,
    input.checkIn,
    ``,
    `${brand.charAt(0).toUpperCase()}${brand.slice(1)} was not emailed about this.`,
    ``,
    `---`,
  ];
  const below = input.question
    ? conversationBelow(input.question, input.earlier)
    : earlierBlocks(input.earlier).join("\n\n");
  const text = `${lines.join("\n")}\n${below}`;
  const html = `<div style="white-space:pre-wrap;word-break:break-word;">${escapeHtml(text)}</div>`;
  return { subject: `Check in with ${who} (${input.brandName?.trim() || "unknown brand"})`, text, html };
}

/**
 * Pure: the check-in written from a template (English), when the model cannot
 * write one. Promises nothing: we hold neither the brand's documents, prices nor
 * calendar (owner 2026-10-12). A brand still working with us can follow up
 * itself; otherwise (or unknown) the prospect is pointed to the brand directly.
 */
export function templateCheckIn(input: {
  leadFirstName: string | null;
  brandName: string | null;
  senderFirstName: string | null;
  relationship: BrandRelationship;
  /** False = they replied without asking: "Following up on your reply." Default true. */
  isQuestion?: boolean;
}): string {
  const brand = input.brandName?.trim() || "the team";
  const opener = input.isQuestion === false ? "Following up on your reply." : "Following up on your question.";
  const body =
    input.relationship === "working_with_us"
      ? `${opener} Did you get what you needed from ${brand}? They can follow up with you directly.`
      : `${opener} Did you get what you needed? For anything else, please reach ${brand} directly.`;
  return [
    input.leadFirstName?.trim() ? `Hi ${input.leadFirstName.trim()},` : "Hi,",
    ``,
    body,
    ``,
    input.senderFirstName?.trim() || "Thanks",
  ].join("\n");
}

/**
 * Wording that promises a deliverable or a time frame only the brand could keep
 * ("Sinon je te les fais parvenir cette semaine", owner 2026-10-12: never legit
 * from us). A last guard on the model's output; the prompt does the real work.
 */
export const CHECK_IN_PROMISE_WORDING =
  /\b(this week|next week|tomorrow|I will send|I'll send|I can send|I can set up|I'll set up|I will set up|I can arrange|I'll arrange|esta semana|la pr[oó]xima semana|ma[nñ]ana|te (lo|la|los|las) (hago llegar|env[ií]o|mando)|puedo organizar|puedo enviar|cette semaine|la semaine prochaine|demain|je (te|vous) (les |le |la )?(fais parvenir|envoie|enverrai)|je peux organiser)\b/i;

/** Pure: a model-written check-in made safe to send, or null to use the template. */
export function cleanCheckIn(raw: string | null | undefined): string | null {
  if (!raw) return null;
  const text = raw
    .trim()
    .replace(/^["'“]+|["'”]+$/g, "")
    .replace(/\s*[—–]\s*/g, ", ")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
  if (!text || /^none\.?$/i.test(text) || text.length > 600) return null;
  if (CHECK_IN_PROMISE_WORDING.test(text)) return null;
  return text;
}

/** Pure: who receives it. Members with an address; none = the agency inbox alone. */
export function answerRequestRecipients(
  members: OrgMember[],
  agency: string,
): Array<{ email: string; firstName: string | null; cc: string[] }> {
  if (members.length === 0) return [{ email: agency, firstName: null, cc: [] }];
  return members.map((m, i) => ({
    email: m.email,
    firstName: m.firstName,
    cc: i === 0 && m.email.toLowerCase() !== agency.toLowerCase() ? [agency] : [],
  }));
}

// ─── The summary line (chat-service, org-billed on the campaign row's run) ───

const SUMMARY_SYSTEM_PROMPT = [
  "You write ONE short sentence for a business owner, telling them what a prospect wrote in reply to their sales email.",
  "Rules:",
  "- Say only what the prospect actually wrote. Never add a fact, a guess, an intent or a feeling they did not state.",
  "- Address the business owner: their offer, method or product is 'yours'.",
  "- Start with the prospect's first name when you are given one, else 'They'.",
  "- Do not use gendered pronouns (he, she, his, her) unless the prospect's own words state them; rephrase instead.",
  "- Plain words, past tense, at most 20 words, one line, no quotes, no dashes, no greeting.",
  "- If the reply says nothing beyond being interested, answer exactly NONE.",
  "Example: Doug asked how the clinical protocols at Pro Health compare with yours.",
].join("\n");

async function summaryLineOrNull(
  campaign: ForwardPositiveReplyCampaign,
  reply: ThreadMessage,
  leadFirstName: string | null,
  company: string | null,
): Promise<string | null> {
  if (!campaign.orgId || !campaign.runId || !campaign.userId) return null;
  try {
    const words = replyWordsOnly(reply.bodyText, stripQuotedHistory);
    const result = await orgComplete(
      {
        message: [
          `Prospect first name: ${leadFirstName ?? "unknown"}`,
          `Prospect company: ${company ?? "unknown"}`,
          `Their reply, verbatim:`,
          `"""`,
          words,
          `"""`,
        ].join("\n"),
        systemPrompt: SUMMARY_SYSTEM_PROMPT,
        provider: "anthropic",
        model: "sonnet",
        // No `temperature`: chat-service refuses sampling parameters on
        // reasoning-first models (400), which silently dropped the line.
        maxTokens: 120,
      },
      { orgId: campaign.orgId, userId: campaign.userId, runId: campaign.runId, brandId: brandHeader(campaign) },
    );
    return cleanSummaryLine(result.content);
  } catch (error) {
    console.warn(
      `[instantly-service] ask-client-to-answer: summary line unavailable for campaign=${campaign.instantlyCampaignId}, sent without it — ${error instanceof Error ? error.message : String(error)}`,
    );
    return null;
  }
}

const QUESTION_SITUATION =
  "The prospect asked the brand something a while ago. Someone may have answered them another way (a call, the brand's own inbox). So the rep does not answer the question and does not promise anything. The rep only checks whether they got what they needed, and makes sure they know who to turn to.";
const REPLY_SITUATION =
  "The prospect replied to the brand a while ago and showed interest, without asking anything. Someone may have followed up with them another way (a call, the brand's own inbox). So the rep does not speak for the brand and does not promise anything. The rep only checks whether they got what they needed, and makes sure they know who to turn to.";

const CHECK_IN_SYSTEM_PROMPT = [
  "You write a short check-in email that a sales rep sends in an old email thread.",
  "The situation: the rep found this prospect and introduced them to a business (the brand). The rep is only the go-between. The rep holds none of the brand's documents, reports, prices or calendar, so the rep can deliver nothing for the brand and cannot speak for its timing.",
  QUESTION_SITUATION,
  "You are told whether the brand still works with us. Use it to decide who the prospect should turn to:",
  "- Still working with us: the brand can follow up with them; leave that door open.",
  "- No longer working with us: do not keep the conversation going for the brand; point the prospect to the brand directly for anything they still need.",
  "- Unknown: treat it like no longer working with us.",
  "If the prospect raised a doubt or an objection, acknowledge it in a few plain words, without arguing or making a claim about the brand.",
  "What you are told about the brand is for you, not for the prospect: never say whether the brand works with us, never mention our arrangement with it, and never explain what the rep does or does not hold. Just say who they should turn to.",
  "Rules:",
  "- Write in the language the prospect wrote in.",
  "- First line: a greeting with the prospect's first name when given, else a plain greeting.",
  "- Then two or three short sentences. One idea per sentence. Plain words. Name the brand.",
  "- Last line: the rep's first name alone.",
  "- Never promise a document, a price, a call, an intro, a meeting, a date or a time frame, and never say the rep will send or set up anything. Only the brand can deliver those.",
  "- No dashes, no quotes, no subject line, no placeholders.",
].join("\n");

/** The system prompt for a question (unchanged) or for a positive reply that asks nothing. */
export function checkInSystemPrompt(isQuestion: boolean): string {
  return isQuestion ? CHECK_IN_SYSTEM_PROMPT : CHECK_IN_SYSTEM_PROMPT.replace(QUESTION_SITUATION, REPLY_SITUATION);
}

/** Exported for staff render scripts (prints a draft, sends nothing). */
export async function checkInText(
  campaign: ForwardPositiveReplyCampaign,
  question: ThreadMessage,
  input: {
    leadFirstName: string | null;
    brandName: string | null;
    senderFirstName: string | null;
    brandStatus: BrandAccountStatus | null;
    /** Default true. */
    isQuestion?: boolean;
  },
): Promise<string> {
  const relationship = input.brandStatus?.relationship ?? "unknown";
  const isQuestion = input.isQuestion !== false;
  if (campaign.orgId && campaign.runId && campaign.userId) {
    try {
      const result = await orgComplete(
        {
          message: [
            `Prospect first name: ${input.leadFirstName ?? "unknown"}`,
            `Brand: ${input.brandName ?? "unknown"}`,
            `Brand still works with us: ${relationshipWords(relationship)}${input.brandStatus ? ` (account status: ${input.brandStatus.status})` : ""}`,
            `Rep first name: ${input.senderFirstName ?? "unknown"}`,
            `What the prospect wrote on ${shortDate(question.date)}, verbatim:`,
            `"""`,
            replyWordsOnly(question.bodyText, stripQuotedHistory),
            `"""`,
          ].join("\n"),
          systemPrompt: checkInSystemPrompt(isQuestion),
          provider: "anthropic",
          model: "sonnet",
          // Sonnet reasons before it writes: at 300 a long objection (Dr. Gallego,
          // 2026-10-12) came back EMPTY and the template went out instead.
          maxTokens: 1200,
        },
        { orgId: campaign.orgId, userId: campaign.userId, runId: campaign.runId, brandId: brandHeader(campaign) },
      );
      const cleaned = cleanCheckIn(result.content);
      if (cleaned) return cleaned;
      console.warn(
        `[instantly-service] ask-client-to-answer: check-in unusable for campaign=${campaign.instantlyCampaignId}, English template used — draft was: ${JSON.stringify((result.content ?? "").slice(0, 800))}`,
      );
    } catch (error) {
      console.warn(
        `[instantly-service] ask-client-to-answer: check-in unavailable for campaign=${campaign.instantlyCampaignId}, English template used — ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }
  return templateCheckIn({ ...input, relationship, isQuestion });
}

function relationshipWords(relationship: BrandRelationship): string {
  if (relationship === "working_with_us") return "yes";
  if (relationship === "not_working_with_us") return "no";
  return "unknown";
}

// ─── IO ──────────────────────────────────────────────────────────────────────

/** Pure: Instantly's webhook copy of a reply as a thread message, or null when it carries no words. */
export function webhookReplyFromRow(
  row: { reply_text?: unknown; reply_subject?: unknown; timestamp?: unknown } | undefined,
  leadEmail: string,
): ThreadMessage | null {
  if (!row) return null;
  const text = typeof row.reply_text === "string" ? row.reply_text.trim() : "";
  if (!text) return null;
  const at = row.timestamp instanceof Date ? row.timestamp : new Date(`${String(row.timestamp).replace(" ", "T")}Z`);
  if (Number.isNaN(at.getTime())) return null;
  return {
    direction: "inbound",
    from: leadEmail,
    to: "",
    date: at.toISOString(),
    subject: typeof row.reply_subject === "string" ? row.reply_subject : "",
    bodyText: text,
  };
}

/** The newest reply Instantly's webhook delivered on this sequence, words included, or null. Throws on a failed read. */
async function webhookReplyMessage(instantlyCampaignId: string, leadEmail: string): Promise<ThreadMessage | null> {
  const result = await db.execute(sql`
    SELECT e.raw_payload->>'reply_text' AS reply_text, e.raw_payload->>'reply_subject' AS reply_subject, e."timestamp"
    FROM instantly_events e
    WHERE e.campaign_id = ${instantlyCampaignId}
      AND e.inferred = false
      AND e.withdrawn_at IS NULL
      AND COALESCE(e.raw_payload->>'reply_text', '') <> ''
    ORDER BY e."timestamp" DESC
    LIMIT 1
  `);
  const rows = (result as { rows?: unknown }).rows;
  const row = Array.isArray(rows) ? (rows[0] as Record<string, unknown> | undefined) : undefined;
  return webhookReplyFromRow(row, leadEmail);
}

/**
 * The run this thread's calls hang under: its own run when that run belongs to
 * the thread's org and user, else a fresh root run there. A brand moved to
 * another org keeps its old runs (lib/transfer-brand moves history, runs-service
 * does not): runs-service refused every child of such a run with a 409
 * parent-child conflict, and the staff check-in never went out (2026-10-13).
 * Throws when runs-service cannot answer.
 */
export async function runInThreadOrg(campaign: ForwardPositiveReplyCampaign, orgId: string): Promise<string> {
  const identity = { orgId, userId: campaign.userId || "00000000-0000-0000-0000-000000000000" };
  if (campaign.runId) {
    const run = await getRun(campaign.runId, identity);
    if (run && run.organizationId === orgId && (!campaign.userId || !run.userId || run.userId === campaign.userId)) {
      return campaign.runId;
    }
  }
  const created = await createRun({ serviceName: "instantly-service", taskName: "ask-client-to-answer" }, identity);
  console.log(
    `[instantly-service] ask-client-to-answer: run ${campaign.runId ?? "(none)"} is not this thread's org ${orgId}; new run ${created.id}`,
  );
  return created.id;
}

/**
 * A claim whose send never finished is re-claimable after this long. A deploy
 * recreates the container mid-ask (a backlog pass runs for minutes): the
 * claim was kept and nobody was ever asked (jfigueroa, 2026-10-13).
 */
export const STALE_ANSWER_CLAIM_MS = 30 * 60 * 1000;
/** `metadata` key present while a claim's send is in flight. Claims taken before it existed never carry it. */
export const ANSWER_CLAIM_PENDING_KEY = "answerRequestPendingSince";

/**
 * SQL: this thread can be claimed: never claimed, or claimed by a send that
 * never finished (still pending past STALE_ANSWER_CLAIM_MS). Shared by the
 * claim and the sweep's read so both agree.
 */
export function answerClaimOpenSql(asOf: Date = new Date()) {
  const staleBefore = new Date(asOf.getTime() - STALE_ANSWER_CLAIM_MS).toISOString();
  return sql`(${instantlyCampaigns.clientAnswerRequestedAt} IS NULL OR (
    (${instantlyCampaigns.metadata}->>${ANSWER_CLAIM_PENDING_KEY}) IS NOT NULL
    AND ${instantlyCampaigns.clientAnswerRequestedAt} < ${staleBefore}::timestamp
  ))`;
}

async function claimAnswerRequest(instantlyCampaignId: string): Promise<boolean> {
  const now = new Date();
  const claimed = await db
    .update(instantlyCampaigns)
    .set({
      clientAnswerRequestedAt: now,
      metadata: sql`COALESCE(${instantlyCampaigns.metadata}, '{}'::jsonb) || jsonb_build_object(${ANSWER_CLAIM_PENDING_KEY}::text, ${now.toISOString()}::text)`,
      updatedAt: now,
    })
    .where(and(eq(instantlyCampaigns.instantlyCampaignId, instantlyCampaignId), answerClaimOpenSql(now)))
    .returning({ id: instantlyCampaigns.id });
  return claimed.length > 0;
}

async function settleAnswerRequest(instantlyCampaignId: string): Promise<void> {
  await db
    .update(instantlyCampaigns)
    .set({ metadata: sql`${instantlyCampaigns.metadata} - ${ANSWER_CLAIM_PENDING_KEY}::text`, updatedAt: new Date() })
    .where(eq(instantlyCampaigns.instantlyCampaignId, instantlyCampaignId));
}

async function releaseAnswerRequest(instantlyCampaignId: string): Promise<void> {
  await db
    .update(instantlyCampaigns)
    .set({
      clientAnswerRequestedAt: null,
      metadata: sql`${instantlyCampaigns.metadata} - ${ANSWER_CLAIM_PENDING_KEY}::text`,
      updatedAt: new Date(),
    })
    .where(eq(instantlyCampaigns.instantlyCampaignId, instantlyCampaignId));
}

/**
 * Is an AI responder running on this thread's (brand, offer) right now? Null
 * when it cannot be said (no caller campaign, no offer stated is "no": the
 * responder trigger asks on (brand, offer) and fires nothing without one).
 * Throws when campaign-service cannot be read.
 */
export async function responderRuns(campaign: ForwardPositiveReplyCampaign): Promise<boolean | null> {
  const callerId = campaign.conversationCampaignId ?? campaign.campaignId;
  if (!campaign.orgId || !callerId) return null;
  const scope = await getCampaignTriggerScope(callerId, campaign.orgId);
  if (!scope || !scope.brandId) return null;
  if (!scope.offerId) return false;
  const responders = await findOngoingResponderCampaigns({
    orgId: campaign.orgId,
    brandId: scope.brandId,
    offerId: scope.offerId,
  });
  return responders.length > 0;
}

export type AnswerRequestOutcome =
  | { sent: true; to: "client" | "staff"; recipients: string[]; cc: string[]; subject: string; text: string }
  | { sent: false; reason: string };

/**
 * Send the "answer it yourself" email for one thread when no responder runs.
 * Never throws; the outcome says what happened.
 */
export async function maybeAskClientToAnswer(
  campaign: ForwardPositiveReplyCampaign,
  leadEmail: string,
  options: {
    waitsMs?: number[];
    /**
     * Why we already know nobody answers: campaign-service said no responder
     * took the reply (`no_responder`), or one took it and sent nothing in 24 h
     * (`overdue`, lib/unanswered-reply-deadline). Absent (staff hand run) = the
     * responder's status is read here.
     */
    because?: "no_responder" | "overdue";
    /** The thread's reply kind (lib/unanswered-reply-deadline); absent = worded as a question, as always. */
    replyKind?: string;
    /**
     * When they wrote, as the sweep dated it. Lets an OLD reply whose words no
     * copy holds still reach staff (Instantly's plan ended 2026-10-12: its
     * threads can no longer be re-read); absent = an unreadable reply fails.
     */
    repliedAt?: Date;
    /** The clock (tests). */
    asOf?: Date;
  } = {},
): Promise<AnswerRequestOutcome> {
  const tag = `campaign=${campaign.instantlyCampaignId} lead=${leadEmail}`;
  if (!campaign.orgId) return { sent: false, reason: "no_org" };

  if (!options.because) {
    let runs: boolean | null;
    try {
      runs = await responderRuns(campaign);
    } catch (error) {
      console.error(
        `[instantly-service] ask-client-to-answer: responder read FAILED for ${tag}, nothing sent — ${error instanceof Error ? error.message : String(error)}`,
      );
      return { sent: false, reason: "responder_read_failed" };
    }
    if (runs === null) return { sent: false, reason: "no_caller_campaign" };
    if (runs) {
      console.log(`[instantly-service] ask-client-to-answer: a responder runs for ${tag}; nothing to ask`);
      return { sent: false, reason: "responder_running" };
    }
  }

  let claimed: boolean;
  try {
    claimed = await claimAnswerRequest(campaign.instantlyCampaignId);
  } catch (error) {
    console.warn(
      `[instantly-service] ask-client-to-answer: claim failed for ${tag} — ${error instanceof Error ? error.message : String(error)}`,
    );
    return { sent: false, reason: "claim_failed" };
  }
  if (!claimed) return { sent: false, reason: "already_asked" };

  const orgId: string = campaign.orgId;
  const outcome = await (async (): Promise<AnswerRequestOutcome> => {
    try {
      // Every call below (model, email) runs as a child of this run, so it
      // must belong to the thread's org now.
      campaign = { ...campaign, runId: await runInThreadOrg(campaign, orgId) };
      const { loadHistoryWithLatestReply, REPLY_WAIT_SHORT_MS } = await import("./prospect-history");
      const { history } = await loadHistoryWithLatestReply(campaign, leadEmail, {
        waitsMs: options.waitsMs ?? REPLY_WAIT_SHORT_MS,
      });
      const earlier = history.items
        .filter((i): i is Extract<HistoryItem, { type: "message" }> => i.type === "message")
        .map((i) => i.message);
      // Their own words, never one of ours (a manual bump is not their question).
      // No copy of the thread holds them: Instantly's own webhook copy of the
      // reply (its words as Instantly delivered them) is still theirs.
      const latestReply = latestProspectMessage(earlier) ?? (await webhookReplyMessage(campaign.instantlyCampaignId, leadEmail));
      const asOf = options.asOf ?? new Date();
      const isQuestion = isQuestionKind(options.replyKind);
      if (!latestReply) {
        // An OLD reply nobody can read any more still reaches a person: staff get
        // a check-in that says so. A fresh one may still land in a copy: retry.
        if (!options.repliedAt || asOf.getTime() - options.repliedAt.getTime() <= QUESTION_STALE_AFTER_MS) {
          throw new Error("the prospect's reply is not readable yet");
        }
        const brand = await brandContextOrNull(campaign.brandIds?.[0], orgId);
        return await sendStaffCheckIn({
          campaign,
          leadEmail,
          tag,
          callerId: campaign.conversationCampaignId ?? campaign.campaignId,
          brandName: brand?.name ?? null,
          fullName: null,
          firstName: null,
          company: null,
          question: null,
          repliedAt: options.repliedAt.toISOString(),
          isQuestion,
          earlier,
        });
      }

      // We wrote after their question (a manual reply from the Unibox, the
      // responder): a person handled the thread, nobody is asked.
      const answered = ourAnswerAfter(earlier, latestReply);
      if (answered) {
        await releaseAnswerRequest(campaign.instantlyCampaignId).catch(() => {});
        console.log(
          `[instantly-service] ask-client-to-answer: ${tag} answered in the thread on ${answered.date} by ${answered.from} (question ${latestReply.date}); nothing sent`,
        );
        return { sent: false, reason: "answered_in_thread" };
      }

      const callerId = campaign.conversationCampaignId ?? campaign.campaignId;
      const lead = callerId
        ? await findLeadOnCampaignByEmail({ orgId: orgId, campaignId: callerId, email: leadEmail }).catch(
            (error: unknown) => {
              console.warn(
                `[instantly-service] ask-client-to-answer: lead row unreadable for ${tag} — ${error instanceof Error ? error.message : String(error)}; naming them from their email`,
              );
              return null;
            },
          )
        : null;
      const fromName = prospectLabel(leadEmail, latestReply);
      const fullName =
        [lead?.firstName, lead?.lastName].filter((p) => p && p.trim()).join(" ") ||
        lead?.name?.trim() ||
        (fromName !== leadEmail ? fromName : null);
      const firstName = lead?.firstName?.trim() || (fullName ? fullName.split(/\s+/)[0] : null);
      const company = lead?.company?.trim() || null;
      const brand = await brandContextOrNull(campaign.brandIds?.[0], orgId);
      const brandName = brand?.name ?? null;

      if (asOf.getTime() - timeOf(latestReply) > QUESTION_STALE_AFTER_MS) {
        return await sendStaffCheckIn({
          campaign,
          leadEmail,
          tag,
          callerId,
          brandName,
          fullName,
          firstName,
          company,
          question: latestReply,
          repliedAt: latestReply.date,
          isQuestion,
          earlier,
        });
      }

      const summaryLine = await summaryLineOrNull(campaign, latestReply, firstName, company);

      const members = await listOrgMembers(orgId);
      const recipients = answerRequestRecipients(members, agencyInbox());

      const sentTo: string[] = [];
      const ccSent: string[] = [];
      let content: AnswerRequestContent | null = null;
      for (const r of recipients) {
        content = renderAnswerRequest({
          clientFirstName: r.firstName,
          leadEmail,
          leadFullName: fullName,
          leadFirstName: firstName,
          company,
          summaryLine,
          brandName,
          reply: latestReply,
          earlier,
        });
        try {
          await sendEmail(
            {
              appId: "instantly-service",
              eventType: CLIENT_ANSWER_EVENT_TYPE,
              recipientEmail: r.email,
              ...(r.cc.length > 0 ? { ccEmails: r.cc } : {}),
              replyToEmail: leadEmail,
              metadata: { subject: content.subject, html: content.html, text: content.text },
            },
            {
              orgId: orgId,
              userId: campaign.userId || "00000000-0000-0000-0000-000000000000",
              runId: campaign.runId || undefined,
              tracking: { campaignId: campaign.campaignId ?? undefined, brandId: brandHeader(campaign) },
            },
          );
          sentTo.push(r.email);
          ccSent.push(...r.cc);
        } catch (error) {
          console.error(
            `[instantly-service] ask-client-to-answer: send to ${r.email} FAILED for ${tag} — ${error instanceof Error ? error.message : String(error)}`,
          );
        }
      }
      if (sentTo.length === 0 || !content) throw new Error("no member could be emailed");

      console.log(
        `[instantly-service] ask-client-to-answer: sent for ${tag} → ${sentTo.join(",")}${ccSent.length ? ` cc=${ccSent.join(",")}` : ""} replyTo=${leadEmail} subject="${content.subject}" summary=${summaryLine ? "yes" : "dropped"} because=${options.because ?? "no_responder_running"}`,
      );
      // The client now owns this thread: no responder switched on later may answer it.
      await handThreadFollowupsToHuman({
        orgId: orgId,
        campaignId: callerId,
        leadEmail,
        reason: `Client asked to answer (${sentTo.join(", ")}): no AI responder ran when the reply arrived.`,
        tag: "ask-client-to-answer",
      });
      return { sent: true, to: "client", recipients: sentTo, cc: ccSent, subject: content.subject, text: content.text };
    } catch (error) {
      await releaseAnswerRequest(campaign.instantlyCampaignId).catch(() => {});
      console.error(
        `[instantly-service] ask-client-to-answer: FAILED for ${tag}, claim released — ${error instanceof Error ? error.message : String(error)}`,
      );
      return { sent: false, reason: "send_failed" };
    }
  })();
  // Done: the claim stops being pending (a pending claim is a send in flight,
  // re-claimable once stale). A failure here leaves it pending: at worst the
  // thread is asked twice, never zero times.
  if (outcome.sent) {
    await settleAnswerRequest(campaign.instantlyCampaignId).catch((error: unknown) => {
      console.warn(
        `[instantly-service] ask-client-to-answer: sent for ${tag} but the claim could not be settled — ${error instanceof Error ? error.message : String(error)}`,
      );
    });
  }
  return outcome;
}

/**
 * The OLD-question branch: staff only (the client is not emailed), a ready
 * check-in in the prospect's language, signed as the thread's sender. Same
 * once-per-thread claim; follow-ups handed to a person. Throws on a failed send
 * (the caller releases the claim).
 */
async function sendStaffCheckIn(args: {
  campaign: ForwardPositiveReplyCampaign;
  leadEmail: string;
  tag: string;
  callerId: string | null | undefined;
  brandName: string | null;
  fullName: string | null;
  firstName: string | null;
  company: string | null;
  question: ThreadMessage | null;
  repliedAt: string;
  isQuestion: boolean;
  earlier: ThreadMessage[];
}): Promise<AnswerRequestOutcome> {
  const { campaign, leadEmail, tag, question, earlier } = args;
  const sender = threadSender(earlier);
  const brandId = campaign.brandIds?.[0];
  const brandStatus = brandId
    ? await Promise.resolve()
        .then(() => fetchBrandAccountStatus(campaign.orgId!, brandId))
        .then((status) => status ?? null)
        .catch((error: unknown) => {
        console.warn(
          `[instantly-service] ask-client-to-answer: brand status unreadable for ${tag}, check-in written as if the brand no longer works with us — ${error instanceof Error ? error.message : String(error)}`,
        );
        return null;
      })
    : null;
  // No words to read: the model has nothing to work from, the template goes out.
  const checkIn = question
    ? await checkInText(campaign, question, {
        leadFirstName: args.firstName,
        brandName: args.brandName,
        senderFirstName: sender.firstName,
        brandStatus,
        isQuestion: args.isQuestion,
      })
    : templateCheckIn({
        leadFirstName: args.firstName,
        brandName: args.brandName,
        senderFirstName: sender.firstName,
        relationship: brandStatus?.relationship ?? "unknown",
        isQuestion: args.isQuestion,
      });
  const content = renderStaffCheckIn({
    brandName: args.brandName,
    leadEmail,
    leadFullName: args.fullName,
    company: args.company,
    question,
    repliedAt: args.repliedAt,
    isQuestion: args.isQuestion,
    earlier,
    senderAddress: sender.address,
    checkIn,
  });
  const agency = agencyInbox();
  await sendEmail(
    {
      appId: "instantly-service",
      eventType: STAFF_CHECK_IN_EVENT_TYPE,
      recipientEmail: agency,
      metadata: { subject: content.subject, html: content.html, text: content.text },
    },
    {
      orgId: campaign.orgId!,
      userId: campaign.userId || "00000000-0000-0000-0000-000000000000",
      runId: campaign.runId || undefined,
      tracking: { campaignId: campaign.campaignId ?? undefined, brandId: brandHeader(campaign) },
    },
  );
  console.log(
    `[instantly-service] ask-client-to-answer: ${args.isQuestion ? "question" : "reply"} from ${shortDate(args.repliedAt)} is old, staff check-in sent for ${tag} → ${agency} subject="${content.subject}"${question ? "" : " words=unreadable"}`,
  );
  await handThreadFollowupsToHuman({
    orgId: campaign.orgId!,
    campaignId: args.callerId,
    leadEmail,
    reason: `Staff asked to check in (${agency}): the ${args.isQuestion ? "question" : "reply"} dates from ${shortDate(args.repliedAt)}, it may have been answered elsewhere.`,
    tag: "ask-client-to-answer",
  });
  return { sent: true, to: "staff", recipients: [agency], cc: [], subject: content.subject, text: content.text };
}
