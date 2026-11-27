/**
 * A positive reply is good news for the CLIENT, so the client is told — in a
 * short email that shows the prospect's reply complete and word for word. This replaced the "positive-reply-forward" (a bare thread to the agency
 * inbox, the rep in Cc) on 2026-09-29.
 *
 * WHO: the brand's sales rep (Brand Settings, the same person the one-to-one
 * reply copies) in To, the agency inbox in Bcc. A brand that named no rep
 * celebrates to the agency inbox alone — somebody must still see it.
 *
 * ONLY SUCCESSES ARE ANNOUNCED (owner, 2026-10-05: "Only celebrate
 * successes."). An info request (`lead_info_requested`: questions, "send me
 * more") is NOT a success: it is handled silently by us (the responder answers,
 * stats still count it positive) and sends NO email, to the rep or anyone. It
 * takes no claim either, so if the same thread later turns into interest or a
 * meeting request, that success is still celebrated. Every path that can
 * celebrate goes through `celebrateOnce`, which holds this gate.
 *
 * ONCE PER THREAD: the claim is `positive_reply_forwarded_at` (migration 0028),
 * shared by every path that can celebrate — the Instantly qualification, a
 * hand-recorded qualification, and the escalation (it passes no kind: the
 * thread's recorded kind is read, so an info request it escalates stays silent
 * too). Whichever arrives first sends; the rest find the claim taken and send
 * nothing. A send that fails releases it.
 *
 * TWO EMAILS, ONE PER CELEBRATED KIND (owner, 2026-10-01): a meeting request
 * gets the full celebration and its OWN claim (`meeting_request_celebrated_at`,
 * migration 0063), so a call request that follows a plain interest on the same
 * thread is still announced; a plain interest is the calmer one. Every variant
 * keeps ALL the information (who, which outreach, what happens next, the reply
 * with its date and subject, the earlier thread, the dashboard link) in as few
 * words as possible (owner 2026-10-02).
 *
 * ⚠️ THE REPLY IS ANNOUNCED BEFORE IT IS READABLE. The history is waited on
 * (`loadHistoryWithLatestReply`) until it holds the prospect's message; one that
 * is still unreadable after the bounded wait is SAID in the email, never
 * replaced by a summary. A model paraphrase is never presented as their words.
 */

import { errorMessage } from "./db-error.js";
import { and, eq, isNull, sql } from "drizzle-orm";
import { db } from "../db";
import { instantlyCampaigns, instantlyLeadStatusCurrent } from "../db/schema";
import { agencyInbox } from "./agency-inbox";
import { getExternalOrgId } from "./client-org-client";
import { getBrandHandoffContext, type BrandHandoffContext } from "./brand-client";
import { sendEmail } from "./email-client";
import { findLeadOnCampaignByEmail } from "./lead-client";
import type { ForwardPositiveReplyCampaign, ThreadMessage } from "./forward-positive-reply";
import { formatThreadDate, threadSubject } from "./forward-positive-reply";
import { stripQuotedHistory } from "./self-send/qualify-reply";
import type { HistoryItem, ProspectHistory } from "./prospect-history";

/** The transactional-email template this module sends (deployed at startup). */
export const CELEBRATION_EVENT_TYPE = "positive-reply-celebration";

/** The plain-text thread email the rep answers with Reply (REP-REPLY-1127). */
export const REP_THREAD_EVENT_TYPE = "positive-reply-thread";

/** Where a client follows a conversation (the dashboard, v2). */
export const DASHBOARD_ORIGIN = "https://dashboard.distribute.you";

/**
 * Which email a positive reply gets. Keyed on the reply KIND, never on wording:
 * the kind is what `promoteEvent` recorded (Instantly's plain "interested" is
 * refined to the finer kind our classifier read, lib/refine-interest-kind).
 */
export type CelebrationVariant = "meeting_requested" | "interested";

export function celebrationVariantFor(kind: string | null | undefined): CelebrationVariant {
  if (kind === "lead_meeting_requested") return "meeting_requested";
  return "interested";
}

/**
 * Positive reply kinds that are NOT a success, so never announced (owner
 * 2026-10-05). The reply is still recorded and counted positive; only the email
 * is withheld.
 */
export const UNCELEBRATED_POSITIVE_KINDS: ReadonlySet<string> = new Set(["lead_info_requested"]);

export function isCelebratedKind(kind: string | null | undefined): boolean {
  return !(kind && UNCELEBRATED_POSITIVE_KINDS.has(kind));
}

/**
 * Pure: the dashboard page where the client reads the conversation. The person
 * page (`/people/{leads_campaigns row id}`, its "Conversation and activity"
 * timeline) when we know the row; the brand's People list when we only know the
 * brand; the dashboard home (which opens the client's own org) otherwise. The
 * org segment is the CLERK org id, never our internal UUID.
 */
export function conversationHref(params: {
  externalOrgId: string | null;
  brandId: string | null | undefined;
  leadRowId: string | null | undefined;
}): string {
  const { externalOrgId, brandId, leadRowId } = params;
  if (!externalOrgId || !brandId) return `${DASHBOARD_ORIGIN}/v2`;
  const base = `${DASHBOARD_ORIGIN}/v2/orgs/${encodeURIComponent(externalOrgId)}/brands/${encodeURIComponent(brandId)}/people`;
  return leadRowId ? `${base}/${encodeURIComponent(leadRowId)}` : base;
}

/** Charter blue. */
const BLUE = "#2563EB";
const INK = "#0F172A";
const MUTED = "#64748B";
const TINT = "#F5F8FF";
const TINT_RULE = "#D6E2FB";
const FONT =
  "-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif";

export function escapeHtml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

export interface CelebrationContent {
  subject: string;
  html: string;
  text: string;
}

export interface CelebrationInput {
  leadEmail: string;
  brandName: string | null;
  /**
   * The prospect's company, as lead-service holds it. Null when unknown: the
   * label then falls back to the person's name, then to the address.
   */
  company?: string | null;
  /** The prospect's latest reply, verbatim. Null = could not be read. */
  reply: ThreadMessage | null;
  /** Everything before the reply (emails both ways, actions), oldest first. */
  history: Pick<ProspectHistory, "items" | "notes">;
  /** The recorded reply kind. Decides the email; unknown reads as plain interest. */
  kind?: string | null;
  /** Where the "Follow the conversation" button goes (`conversationHref`). */
  conversationUrl: string;
}

/**
 * Pure: who wrote, in words a client reads. The display name of the reply's
 * From header (`"Andrew Kakishita" <dr.k@…>` → Andrew Kakishita) when it carries
 * one, else the address we emailed. Never a name guessed from an address.
 */
export function prospectLabel(leadEmail: string, reply: ThreadMessage | null): string {
  const from = reply?.from?.trim() ?? "";
  const match = from.match(/^\s*"?([^"<]*?)"?\s*<[^>]+>\s*$/);
  const name = match?.[1]?.trim();
  return name && !name.includes("@") ? name : leadEmail;
}

interface VariantCopy {
  emoji: string;
  headline: string;
}

/**
 * Pure: the title of each variant. A meeting request is the full celebration;
 * a plain interest is the calmer one.
 */
export function variantCopy(variant: CelebrationVariant, who: string): VariantCopy {
  if (variant === "meeting_requested") return { emoji: "\u{1F389}", headline: `${who} wants to book a call` };
  return { emoji: "\u{1F44F}", headline: `${who} is interested` };
}

export const FOLLOW_BUTTON_LABEL = "Follow the conversation";

const RULE = "#E2E8F0";

/** Pure: "Name · date · subject", the line above any message. */
function metaLine(from: string, date: string, subject: string): string {
  return [from, formatThreadDate(date), subject].filter((p) => p.trim()).join(" \u00B7 ");
}

function historyItemHtml(item: HistoryItem, leadEmail: string): string {
  if (item.type === "message") {
    const m = item.message;
    return [
      `<tr><td style="padding:12px 0 0 0;border-top:1px solid ${RULE};">`,
      `<div style="font:12px/1.5 ${FONT};color:${MUTED};">${escapeHtml(metaLine(m.from, m.date, m.subject))}</div>`,
      `<div style="padding:4px 0 0 0;font:14px/1.55 ${FONT};color:#334155;white-space:pre-wrap;word-break:break-word;">${escapeHtml(m.bodyText)}</div>`,
      `</td></tr>`,
    ].join("");
  }
  return `<tr><td style="padding:10px 0 0 0;border-top:1px solid ${RULE};font:13px/1.5 ${FONT};color:${MUTED};">${escapeHtml(`${formatThreadDate(item.action.at)} \u00B7 ${actionText(item, leadEmail)}`)}</td></tr>`;
}

function actionText(item: Extract<HistoryItem, { type: "action" }>, leadEmail: string): string {
  const a = item.action;
  if (a.kind === "click") return a.page ? `${leadEmail} visited ${a.page}` : `${leadEmail} clicked a link`;
  if (a.kind === "bounce") return `An email to ${leadEmail} bounced`;
  return `${leadEmail} unsubscribed`;
}

/**
 * Pure: the email. Owner 2026-10-02: every piece of information stays (who and
 * their company, which outreach, what happens next, their reply with its date
 * and subject, the earlier thread, the link), in as few words as possible; no
 * flourish, no process prose. Every string from the prospect is escaped (the
 * template engine interpolates raw).
 */
export function renderCelebration(input: CelebrationInput): CelebrationContent {
  const { leadEmail, brandName, reply, history } = input;
  const person = prospectLabel(leadEmail, reply);
  const company = input.company?.trim() || null;
  const who =
    person !== leadEmail ? (company ? `${person} (${company})` : person) : (company ?? leadEmail);
  const copy = variantCopy(celebrationVariantFor(input.kind), who);
  const title = `${copy.emoji} ${copy.headline}`;
  const subject = brandName ? `${title} (${brandName})` : title;
  // Names who acts in every sentence: "Reply to your outreach" read as an order
  // to the client (owner 2026-10-02).
  const intro = `They answered your ${brandName ? `${brandName} ` : ""}outreach. You have nothing to do: we handle the reply and will only reach out if we need something from you.`;
  const href = escapeHtml(input.conversationUrl);
  const unreadable = "Their reply could not be read when this email was sent. It is in the conversation.";

  const earlier = history.items.filter((item) => !(item.type === "message" && reply && item.message === reply));
  const notes = history.notes.filter((n) => !(reply === null && n.startsWith("the prospect's latest reply")));

  const replyHtml = reply
    ? [
        `<div style="font:12px/1.5 ${FONT};color:${MUTED};">${escapeHtml(metaLine(person, reply.date, reply.subject))}</div>`,
        `<div style="padding:6px 0 0 0;font:15px/1.6 ${FONT};color:${INK};white-space:pre-wrap;word-break:break-word;">${escapeHtml(reply.bodyText)}</div>`,
      ].join("")
    : `<div style="font:14px/1.6 ${FONT};color:${MUTED};">${unreadable}</div>`;

  const html = [
    `<!doctype html><html><body style="margin:0;padding:0;background:#FFFFFF;">`,
    `<table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr><td align="center" style="padding:32px 16px;">`,
    `<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="max-width:560px;">`,
    `<tr><td><h1 style="margin:0;font:700 22px/1.3 ${FONT};color:${INK};">${escapeHtml(title)}</h1></td></tr>`,
    `<tr><td style="padding:8px 0 0 0;font:15px/1.6 ${FONT};color:${MUTED};">${escapeHtml(intro)}</td></tr>`,
    `<tr><td style="padding:20px 0 0 0;"><div style="background:${TINT};border:1px solid ${TINT_RULE};border-radius:12px;padding:16px 18px;">${replyHtml}</div></td></tr>`,
    notes.length > 0
      ? `<tr><td style="padding:12px 0 0 0;font:13px/1.5 ${FONT};color:${MUTED};">${notes.map((n) => `Note: ${escapeHtml(n)}`).join("<br>")}</td></tr>`
      : "",
    `<tr><td style="padding:20px 0 0 0;"><table role="presentation" cellpadding="0" cellspacing="0"><tr><td style="border-radius:10px;background:${BLUE};">`,
    `<a href="${href}" style="display:inline-block;padding:12px 20px;font:600 14px/1 ${FONT};color:#FFFFFF;text-decoration:none;border-radius:10px;">${FOLLOW_BUTTON_LABEL}</a>`,
    `</td></tr></table></td></tr>`,
    earlier.length > 0
      ? `<tr><td style="padding:28px 0 0 0;"><div style="font:600 13px/1.4 ${FONT};color:${INK};">Earlier in the conversation</div><table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-top:6px;">${earlier.map((i) => historyItemHtml(i, leadEmail)).join("")}</table></td></tr>`
      : "",
    `</table></td></tr></table></body></html>`,
  ].join("");

  const text = [
    title,
    intro,
    ``,
    reply ? [metaLine(person, reply.date, reply.subject), reply.bodyText].join("\n") : unreadable,
    ...(notes.length > 0 ? ["", ...notes.map((n) => `Note: ${n}`)] : []),
    ``,
    `${FOLLOW_BUTTON_LABEL}: ${input.conversationUrl}`,
    ...(earlier.length > 0
      ? [
          ``,
          `Earlier in the conversation`,
          ...earlier.map((item) =>
            item.type === "message"
              ? `\n${metaLine(item.message.from, item.message.date, item.message.subject)}\n${item.message.bodyText}`
              : `\n${formatThreadDate(item.action.at)} \u00B7 ${actionText(item, leadEmail)}`,
          ),
        ]
      : []),
  ].join("\n");

  return { subject, html, text };
}

/**
 * Where the rep's REPLY goes (REP-REPLY-1127, owner 2026-11-27: the rep opens
 * the celebration, hits Reply and is writing to the prospect, carrying on the
 * conversation by hand).
 *
 * - Reply-To = the prospect (the address their latest reply came from, else the
 *   one we emailed) AND the thread's own mailbox. Every mail client puts each
 *   Reply-To address in To (RFC 5322 3.6.2): the rep writes to the prospect,
 *   and our mailbox gets the same email, so the poll files it on the thread as
 *   our side's answer (the rep is our side, `our-side-sender.ts`): the AI reads
 *   it and does not answer over the rep; the prospect's reply-all comes back to
 *   that mailbox too. No other copy is added (NO-DEFAULT-COPY-1116: these are
 *   the two parties of the conversation, named on THIS email only).
 * - In-Reply-To / References = the prospect's thread, so the rep's reply carries
 *   the chain: the prospect's client keeps it in their conversation, the poll
 *   correlates it to the sequence.
 * - Subject `Re: <their subject>`: Reply turns it into exactly the subject of
 *   the prospect's thread (Gmail threads on subject + References). A 🎉 subject
 *   would have reached the prospect as "Re: 🎉 Anna is interested".
 *
 * Pure. Null `thread` (the sequence names no mailbox) = Reply-To the prospect
 * alone, no threading, the celebration's own subject.
 */
export interface RepReplyThread {
  /** The thread's mailbox: who our emails in this thread left as. */
  mailbox: ReplyToAddress;
  inReplyTo: string | null;
  references: string[];
  /** `Re: <conversation subject>`; empty = unknown. */
  subject: string;
}

/** One Reply-To address, structured: transactional-email-service renders the header. */
export interface ReplyToAddress {
  name?: string;
  email: string;
}

export interface RepReplyPath {
  replyTo: ReplyToAddress[];
  inReplyTo?: string;
  references?: string[];
  subject?: string;
}

/** transactional-email-service's cap on `references` (1-50 Message-IDs). */
export const MAX_REFERENCES = 50;

/** A Message-ID as the wire wants it: one `<id@host>`. */
export function bracketMessageId(id: string): string {
  const bare = id.trim().replace(/^<+/, "").replace(/>+$/, "");
  return `<${bare}>`;
}

/**
 * A display name the header can carry: the producer refuses a double quote, a
 * backslash or a line break (400, nothing sent), so those are dropped here,
 * never a refusal (the email must still leave). Empty = no name.
 */
export function safeDisplayName(name: string | null | undefined): string | undefined {
  const cleaned = (name ?? "").replace(/["\\\r\n]+/g, " ").replace(/\s+/g, " ").trim();
  return cleaned || undefined;
}

/**
 * At most `MAX_REFERENCES` ids, keeping the thread's FIRST message and the most
 * recent ones (RFC 5322 3.6.4: the first and the latest are what clients thread on).
 */
export function capReferences(ids: string[]): string[] {
  const unique = [...new Set(ids.map(bracketMessageId))];
  if (unique.length <= MAX_REFERENCES) return unique;
  return [unique[0], ...unique.slice(unique.length - (MAX_REFERENCES - 1))];
}

export function repReplyPath(input: {
  leadEmail: string;
  reply: ThreadMessage | null;
  thread: RepReplyThread | null;
}): RepReplyPath {
  const fromAddress = input.reply?.from?.match(/<\s*([^<>\s]+@[^<>\s]+)\s*>/)?.[1] ??
    (input.reply?.from && /^[^\s<>"]+@[^\s<>"]+$/.test(input.reply.from.trim()) ? input.reply.from.trim() : null);
  const prospectAddress = fromAddress ?? input.leadEmail;
  const label = prospectLabel(prospectAddress, input.reply);
  const name = label !== prospectAddress ? safeDisplayName(label) : undefined;
  const prospect: ReplyToAddress = name ? { name, email: prospectAddress } : { email: prospectAddress };
  const { thread } = input;
  if (!thread) return { replyTo: [prospect] };
  const mailboxName = safeDisplayName(thread.mailbox.name);
  const mailbox: ReplyToAddress = mailboxName ? { name: mailboxName, email: thread.mailbox.email } : { email: thread.mailbox.email };
  const references = capReferences(thread.references);
  return {
    replyTo: [prospect, mailbox],
    ...(thread.inReplyTo ? { inReplyTo: bracketMessageId(thread.inReplyTo) } : {}),
    ...(references.length > 0 ? { references } : {}),
    ...(thread.subject.trim() ? { subject: thread.subject.trim() } : {}),
  };
}

/**
 * What a won claim took, so a failed send can give back exactly that.
 * `general` = `positive_reply_forwarded_at`; `meeting` = the meeting column.
 */
export interface CelebrationClaim {
  general: boolean;
  meeting: boolean;
}

/**
 * Atomically claim the celebration for a thread. Null iff another call holds it.
 *
 * A meeting request claims its OWN column (and the general one in the same
 * statement when still free, so a calmer reply after it sends nothing); every
 * other kind claims the general column only. `now()` is one value per
 * statement, so "the general column equals the meeting column" afterwards means
 * THIS statement set both.
 */
export async function claimCelebration(
  instantlyCampaignId: string,
  variant: CelebrationVariant = "interested",
): Promise<CelebrationClaim | null> {
  if (variant === "meeting_requested") {
    const claimed = await db
      .update(instantlyCampaigns)
      .set({
        meetingRequestCelebratedAt: sql`now()`,
        positiveReplyForwardedAt: sql`COALESCE(${instantlyCampaigns.positiveReplyForwardedAt}, now())`,
        updatedAt: new Date(),
      })
      .where(
        and(
          eq(instantlyCampaigns.instantlyCampaignId, instantlyCampaignId),
          isNull(instantlyCampaigns.meetingRequestCelebratedAt),
        ),
      )
      .returning({
        id: instantlyCampaigns.id,
        tookGeneral: sql<boolean>`${instantlyCampaigns.positiveReplyForwardedAt} = ${instantlyCampaigns.meetingRequestCelebratedAt}`,
      });
    if (claimed.length === 0) return null;
    return { general: claimed[0].tookGeneral === true, meeting: true };
  }

  const claimed = await db
    .update(instantlyCampaigns)
    .set({ positiveReplyForwardedAt: new Date(), updatedAt: new Date() })
    .where(
      and(
        eq(instantlyCampaigns.instantlyCampaignId, instantlyCampaignId),
        isNull(instantlyCampaigns.positiveReplyForwardedAt),
      ),
    )
    .returning({ id: instantlyCampaigns.id });
  return claimed.length > 0 ? { general: true, meeting: false } : null;
}

/** Release a claim (the send failed) so a later signal re-attempts. Only what it took. */
export async function releaseCelebration(
  instantlyCampaignId: string,
  claim: CelebrationClaim = { general: true, meeting: false },
): Promise<void> {
  if (!claim.general && !claim.meeting) return;
  await db
    .update(instantlyCampaigns)
    .set({
      ...(claim.general ? { positiveReplyForwardedAt: null } : {}),
      ...(claim.meeting ? { meetingRequestCelebratedAt: null } : {}),
      updatedAt: new Date(),
    })
    .where(eq(instantlyCampaigns.instantlyCampaignId, instantlyCampaignId));
}

/**
 * The thread's recorded reply kind (gold), for a caller that does not carry one
 * (the escalation). Null when unknown; the email then reads as plain interest.
 */
export async function recordedReplyKind(instantlyCampaignId: string, leadEmail: string): Promise<string | null> {
  const rows = await db
    .select({ replyKind: instantlyLeadStatusCurrent.replyKind })
    .from(instantlyLeadStatusCurrent)
    .where(
      and(
        eq(instantlyLeadStatusCurrent.instantlyCampaignId, instantlyCampaignId),
        sql`lower(${instantlyLeadStatusCurrent.leadEmail}) = lower(${leadEmail})`,
      ),
    )
    .limit(1);
  return rows[0]?.replyKind ?? null;
}

/** Brand name + rep, or null when brand-service cannot say (logged loudly). */
export async function brandContextOrNull(
  brandId: string | null | undefined,
  orgId: string,
): Promise<BrandHandoffContext | null> {
  if (!brandId) return null;
  try {
    return await getBrandHandoffContext(brandId, orgId);
  } catch (error) {
    console.error(
      `[instantly-service] celebrate: could not read brand=${brandId} org=${orgId}; treating it as naming no rep`,
      error,
    );
    return null;
  }
}

/**
 * The prospect's lead-service row (company, and the row id the dashboard's
 * person page is keyed on), or null. A celebration never waits on or fails over
 * these: an unreachable lead-service is logged, the subject names the person and
 * the button opens the brand's People list instead.
 */
async function leadOrNull(
  campaign: ForwardPositiveReplyCampaign,
  leadEmail: string,
): Promise<{ id: string; company: string | null } | null> {
  const campaignId = campaign.conversationCampaignId ?? campaign.campaignId;
  if (!campaign.orgId || !campaignId) return null;
  try {
    const lead = await findLeadOnCampaignByEmail({ orgId: campaign.orgId, campaignId, email: leadEmail });
    return lead ? { id: lead.id, company: lead.company } : null;
  } catch (error) {
    console.warn(
      `[instantly-service] celebrate: could not read the lead row of ${leadEmail} — ${errorMessage(error)}; naming the person and linking the People list instead`,
    );
    return null;
  }
}

/** The org's Clerk id for the dashboard link, or null (logged; the link degrades). */
export async function externalOrgIdOrNull(orgId: string): Promise<string | null> {
  try {
    return await getExternalOrgId(orgId);
  } catch (error) {
    console.warn(
      `[instantly-service] celebrate: could not resolve the dashboard org of ${orgId} — ${errorMessage(error)}; the button opens the dashboard home`,
    );
    return null;
  }
}

/**
 * Send the celebration for one thread (no claim — the caller holds it).
 * Throws on a send failure so the caller can release the claim.
 */
export async function sendCelebration(
  campaign: ForwardPositiveReplyCampaign,
  leadEmail: string,
  options: { waitsMs?: number[]; kind?: string | null } = {},
): Promise<{ recipient: string; bcc: string[]; replyRead: boolean; conversationUrl: string }> {
  if (!campaign.orgId) throw new Error("celebrate requires an org-scoped campaign (orgId is null)");
  const { loadHistoryWithLatestReply, REPLY_WAIT_BACKGROUND_MS } = await import("./prospect-history");
  const { history, latestReply } = await loadHistoryWithLatestReply(campaign, leadEmail, {
    waitsMs: options.waitsMs ?? REPLY_WAIT_BACKGROUND_MS,
  });

  const brand = await brandContextOrNull(campaign.brandIds?.[0], campaign.orgId);
  const agency = agencyInbox();
  const recipient = brand?.rep.email ?? agency;
  const bcc = recipient.toLowerCase() === agency.toLowerCase() ? [] : [agency];

  const brandId = campaign.brandIds?.[0] ?? null;
  const [lead, externalOrgId] = await Promise.all([
    leadOrNull(campaign, leadEmail),
    externalOrgIdOrNull(campaign.orgId),
  ]);
  const conversationUrl = conversationHref({ externalOrgId, brandId, leadRowId: lead?.id || null });
  const content = renderCelebration({
    leadEmail,
    brandName: brand?.name ?? null,
    company: lead?.company ?? null,
    reply: latestReply,
    history,
    kind: options.kind,
    conversationUrl,
  });

  const identity = {
    orgId: campaign.orgId,
    userId: campaign.userId || "00000000-0000-0000-0000-000000000000",
    runId: campaign.runId || undefined,
    tracking: {
      campaignId: campaign.campaignId ?? undefined,
      brandId: campaign.brandIds?.[0],
    },
  };

  // The thread first: the email the rep answers with Reply (REP-REPLY-1127).
  // A failure throws before the celebration, so a retry sends both once.
  const thread = await sendRepThreadEmail({ campaign, leadEmail, recipient, latestReply, messages: history.messages, identity });

  await sendEmail(
    {
      appId: "instantly-service",
      eventType: CELEBRATION_EVENT_TYPE,
      recipientEmail: recipient,
      ...(bcc.length > 0 ? { bccEmails: bcc } : {}),
      metadata: { subject: content.subject, html: content.html, text: content.text },
    },
    identity,
  );
  console.log(
    `[instantly-service] celebrate: sent for campaign=${campaign.instantlyCampaignId} lead=${leadEmail} → ${recipient}${bcc.length ? ` bcc=${bcc.join(",")}` : ""} replyRead=${latestReply !== null} variant=${celebrationVariantFor(options.kind)} link=${conversationUrl} thread="${thread.subject}" replyTo=${thread.replyTo.join(",")}`,
  );
  return { recipient, bcc, replyRead: latestReply !== null, conversationUrl };
}

/**
 * The conversation as a mail client quotes it in plain text, NEWEST first:
 * "On <date>, <from> wrote:" then each line under "> ". Each message keeps
 * only its own words (its client's quote of the earlier ones is dropped, they
 * are quoted on their own below). Nothing of ours: the prospect reads all of
 * it under the rep's answer (owner 2026-11-27: « tout doit être propre et
 * ressembler à un thread email normal »). Pure.
 */
export function renderThreadText(messages: ThreadMessage[], mailbox?: ReplyToAddress | null): string {
  return messages
    .map((m) => ({ ...m, bodyText: ownWordsOf(m.bodyText), from: namedSender(m.from, mailbox) }))
    .sort((a, b) => (Date.parse(b.date) || 0) - (Date.parse(a.date) || 0))
    .map((m) => {
      const body = m.bodyText
        .replace(/\r\n/g, "\n")
        .replace(/[ \t ​]+$/gm, "")
        .replace(/\n{3,}/g, "\n\n")
        .trim();
      const quoted = body
        .split("\n")
        .map((line) => (line ? `> ${line}` : ">"))
        .join("\n");
      return `On ${formatThreadDate(m.date)}, ${m.from} wrote:\n${quoted}`;
    })
    .join("\n\n");
}

/** An Outlook header block's first line ("From:", "Von:", "De:"...), in the languages our prospects write in. */
const QUOTE_FROM_LINE = /^\s*\*?(From|Von|De|Da|Van|Från|Fra|Od)\s*:\*?\s*\S/i;
/** A line of the same block ("Sent:", "Gesendet:", "Envoyé:", "Betreff:"...). */
const QUOTE_HEADER_LINE =
  /^\s*\*?(Sent|Date|To|Cc|Subject|Gesendet|Datum|An|Betreff|Envoyé|Date d'envoi|À|Objet|Inviato|Data|A|Oggetto|Enviado|Fecha|Para|Asunto|Verzonden|Aan|Onderwerp|Skickat|Till|Ämne)\s*:/i;

/**
 * Pure: a message's OWN words. The shared cut (`stripQuotedHistory`: `>`
 * blocks, "On ... wrote:", "Original Message"), then an Outlook header block
 * (a From-line followed within 4 lines by Sent/To/Subject, any language) or
 * its `____` rule: the earlier emails it quotes are quoted on their own in the
 * thread email, once. Nothing left = the body as written.
 */
export function ownWordsOf(body: string): string {
  const shared = stripQuotedHistory(body).trim() || body;
  const lines = shared.split(/\r?\n/);
  let cut = lines.length;
  for (let i = 0; i < lines.length; i++) {
    if (/^\s*_{8,}\s*$/.test(lines[i])) {
      cut = i;
      break;
    }
    if (QUOTE_FROM_LINE.test(lines[i]) && lines.slice(i + 1, i + 5).some((l) => QUOTE_HEADER_LINE.test(l))) {
      cut = i;
      break;
    }
  }
  const words = lines.slice(0, cut).join("\n").trim();
  return words || body.trim();
}

/** Pure: our own send's bare address shown as `Name <address>`, the way it left. */
function namedSender(from: string, mailbox?: ReplyToAddress | null): string {
  const bare = from.trim();
  if (!mailbox?.name || bare.toLowerCase() !== mailbox.email.toLowerCase()) return from;
  return `${mailbox.name} <${mailbox.email}>`;
}

/** The subject a reply in this conversation carries: `Re:` + its subject, once. */
export function threadReplySubject(messages: ThreadMessage[]): string {
  const base = threadSubject(messages).trim().replace(/^((re|aw|fwd?|wg|tr|sv)\s*:\s*)+/i, "");
  return base ? `Re: ${base}` : "";
}

/**
 * The SECOND email (REP-REPLY-1127, owner 2026-11-27: « deux emails au client:
 * la célébration en HTML, et l'email de thread en texte pour qu'il puisse
 * reply »). Plain text, nothing but the conversation; Reply-To = the prospect
 * + the thread's mailbox, In-Reply-To/References = their thread
 * (`repReplyPath`), so the rep hits Reply and writes to the prospect in their
 * own conversation, and our mailbox gets the same email (filed `staff_reply`,
 * the AI never answers over the rep). Same recipient as the celebration, no
 * copy. Throws on failure (the caller gives the claim back).
 */
async function sendRepThreadEmail(input: {
  campaign: ForwardPositiveReplyCampaign;
  leadEmail: string;
  recipient: string;
  latestReply: ThreadMessage | null;
  messages: ThreadMessage[];
  identity: Parameters<typeof sendEmail>[1];
}): Promise<{ subject: string; replyTo: string[] }> {
  const thread = await repReplyThreadOrNull(input.campaign.instantlyCampaignId);
  const path = repReplyPath({ leadEmail: input.leadEmail, reply: input.latestReply, thread });
  const subject = path.subject ?? threadReplySubject(input.messages);
  const text = renderThreadText(input.messages, thread?.mailbox);
  await sendEmail(
    {
      appId: "instantly-service",
      eventType: REP_THREAD_EVENT_TYPE,
      recipientEmail: input.recipient,
      replyToAddresses: path.replyTo,
      ...(path.inReplyTo ? { inReplyTo: path.inReplyTo } : {}),
      ...(path.references ? { references: path.references } : {}),
      metadata: { subject, text, html: "" },
    },
    input.identity,
  );
  return { subject, replyTo: path.replyTo.map((a) => a.email) };
}

/**
 * The thread the rep's Reply joins, or null when the sequence names no mailbox.
 * A failed read FAILS the send (the claim is given back, retried on the next
 * signal): a celebration whose Reply silently stopped reaching the prospect
 * is the bug REP-REPLY-1127 fixed.
 */
async function repReplyThreadOrNull(instantlyCampaignId: string): Promise<RepReplyThread | null> {
  const { loadThreadReplyPath } = await import("./reply-to-lead");
  const path = await loadThreadReplyPath(instantlyCampaignId);
  if (!path) return null;
  return {
    mailbox: { name: path.mailboxName, email: path.mailboxEmail },
    inReplyTo: path.anchor?.inReplyTo ?? null,
    references: path.anchor?.references ?? [],
    subject: path.subject,
  };
}

/**
 * Celebrate a thread exactly once (twice at most: a meeting request after an
 * earlier positive reply gets its own email). Never throws: claim, send, release
 * on failure. Returns true iff this call sent. An info request sends nothing and
 * claims nothing (`isCelebratedKind`).
 *
 * `kind` is the reply kind that fired it; a caller that does not carry one (the
 * escalation) leaves it undefined and the thread's recorded kind is read.
 */
export async function celebrateOnce(
  campaign: ForwardPositiveReplyCampaign,
  leadEmail: string,
  options: { waitsMs?: number[]; kind?: string | null } = {},
): Promise<boolean> {
  if (!campaign.orgId) return false;
  let kind: string | null;
  let claim: CelebrationClaim | null;
  try {
    kind =
      options.kind !== undefined ? options.kind : await recordedReplyKind(campaign.instantlyCampaignId, leadEmail);
    if (!isCelebratedKind(kind)) {
      // Not a success: handled silently, and NO claim, so a later success on
      // this thread is still celebrated (owner 2026-10-05).
      console.log(
        `[instantly-service] celebrate: skipped for campaign=${campaign.instantlyCampaignId} lead=${leadEmail} kind=${kind} (not a success, never announced)`,
      );
      return false;
    }
    claim = await claimCelebration(campaign.instantlyCampaignId, celebrationVariantFor(kind));
  } catch (error) {
    console.warn(
      `[instantly-service] celebrate: claim failed for campaign=${campaign.instantlyCampaignId} lead=${leadEmail} — ${errorMessage(error)}`,
    );
    return false;
  }
  if (!claim) return false;

  try {
    await sendCelebration(campaign, leadEmail, { waitsMs: options.waitsMs, kind });
    return true;
  } catch (error) {
    await releaseCelebration(campaign.instantlyCampaignId, claim).catch(() => {});
    console.error(
      `[instantly-service] celebrate: FAILED for campaign=${campaign.instantlyCampaignId} lead=${leadEmail} — ${errorMessage(error)}; claim released, will retry on the next signal`,
    );
    return false;
  }
}
