/**
 * A positive reply is good news for the CLIENT, so the client is told — in a
 * clean, branded email that shows the prospect's reply complete and word for
 * word. This replaced the "positive-reply-forward" (a bare thread to the agency
 * inbox, the rep in Cc) on 2026-09-29.
 *
 * WHO: the brand's sales rep (Brand Settings, the same person the one-to-one
 * reply copies) in To, the agency inbox in Bcc. A brand that named no rep
 * celebrates to the agency inbox alone — somebody must still see it.
 *
 * ONCE PER THREAD: the claim is `positive_reply_forwarded_at` (migration 0028),
 * shared by every path that can celebrate — the Instantly qualification, a
 * hand-recorded qualification, and the escalation (a reply the responder could
 * not answer is still a reply worth celebrating). Whichever arrives first sends;
 * the rest find the claim taken and send nothing. A send that fails releases it.
 *
 * THREE EMAILS, ONE PER POSITIVE KIND (owner, 2026-10-01): a meeting request
 * gets the full celebration and its OWN claim (`meeting_request_celebrated_at`,
 * migration 0063), so a call request that follows an info request on the same
 * thread is still announced; an info request is calm (no "Congratulations", a
 * different emoji), it is a mark of interest and not a booking; a plain interest
 * sits in between. Every variant says the client has nothing to do, that we
 * answer the prospect for them, that we come back to them if we need anything,
 * and links to the conversation in the dashboard.
 *
 * ⚠️ THE REPLY IS ANNOUNCED BEFORE IT IS READABLE. The history is waited on
 * (`loadHistoryWithLatestReply`) until it holds the prospect's message; one that
 * is still unreadable after the bounded wait is SAID in the email, never
 * replaced by a summary. A model paraphrase is never presented as their words.
 */

import { and, eq, isNull, sql } from "drizzle-orm";
import { db } from "../db";
import { instantlyCampaigns, instantlyLeadStatusCurrent } from "../db/schema";
import { agencyInbox } from "./agency-inbox";
import { getExternalOrgId } from "./client-org-client";
import { getBrandHandoffContext, type BrandHandoffContext } from "./brand-client";
import { sendEmail } from "./email-client";
import { findLeadOnCampaignByEmail } from "./lead-client";
import type { ForwardPositiveReplyCampaign, ThreadMessage } from "./forward-positive-reply";
import { formatThreadDate } from "./forward-positive-reply";
import type { HistoryItem, ProspectHistory } from "./prospect-history";

/** The transactional-email template this module sends (deployed at startup). */
export const CELEBRATION_EVENT_TYPE = "positive-reply-celebration";

/** Where a client follows a conversation (the dashboard, v2). */
export const DASHBOARD_ORIGIN = "https://dashboard.distribute.you";

/**
 * Which email a positive reply gets. Keyed on the reply KIND, never on wording:
 * the kind is what `promoteEvent` recorded (Instantly's plain "interested" is
 * refined to the finer kind our classifier read, lib/refine-interest-kind).
 */
export type CelebrationVariant = "meeting_requested" | "interested" | "info_requested";

export function celebrationVariantFor(kind: string | null | undefined): CelebrationVariant {
  if (kind === "lead_meeting_requested") return "meeting_requested";
  if (kind === "lead_info_requested") return "info_requested";
  return "interested";
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
const RULE = "#E2E8F0";
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
  history: Pick<ProspectHistory, "items" | "notes">;
  /** The recorded reply kind. Decides the email; unknown reads as plain interest. */
  kind?: string | null;
  /** Where the "Follow the conversation" button goes (`conversationHref`). */
  conversationUrl: string;
}

function messageBlockHtml(m: ThreadMessage): string {
  return [
    `<tr><td style="padding:16px 0 0 0;border-top:1px solid ${RULE};">`,
    `<div style="font:12px/1.5 ${FONT};color:${MUTED};">${escapeHtml(m.from)} &middot; ${escapeHtml(formatThreadDate(m.date))}</div>`,
    `<div style="font:13px/1.5 ${FONT};color:${INK};font-weight:600;padding:2px 0 6px 0;">${escapeHtml(m.subject)}</div>`,
    `<div style="font:14px/1.6 ${FONT};color:#334155;white-space:pre-wrap;word-break:break-word;">${escapeHtml(m.bodyText)}</div>`,
    `</td></tr>`,
  ].join("");
}

function actionBlockHtml(item: Extract<HistoryItem, { type: "action" }>, leadEmail: string): string {
  const a = item.action;
  const what =
    a.kind === "click"
      ? a.page
        ? `${leadEmail} visited ${a.page}`
        : `${leadEmail} clicked a link in one of the emails`
      : a.kind === "bounce"
        ? `An email to ${leadEmail} bounced`
        : `${leadEmail} unsubscribed`;
  return `<tr><td style="padding:12px 0 0 0;border-top:1px solid ${RULE};font:13px/1.5 ${FONT};color:${MUTED};">${escapeHtml(formatThreadDate(a.at))} &middot; ${escapeHtml(what)}</td></tr>`;
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
  subject: string;
  headline: string;
  lead: string;
  /** The banner: the full blue one is for the two buying signals, a tint for an info request. */
  banner: "blue" | "tint";
}

/**
 * Pure: the words of each variant. Owner rules (2026-10-01): an info request is
 * a mark of interest, NOT a booking request, so it is never congratulated and
 * never wears the party emoji; a meeting request is the full celebration.
 */
export function variantCopy(
  variant: CelebrationVariant,
  who: string,
  byline: string,
  campaignLabel: string,
): VariantCopy {
  if (variant === "meeting_requested") {
    return {
      emoji: "\u{1F389}",
      subject: `\u{1F389} ${who} wants to book a call`,
      headline: `${who} wants to book a call!`,
      lead: `${byline} replied to ${campaignLabel} and asked for a call. Congratulations, this is the moment the outreach is for.`,
      banner: "blue",
    };
  }
  if (variant === "info_requested") {
    return {
      emoji: "\u{1F4AC}",
      subject: `\u{1F4AC} ${who} asked for more information`,
      headline: `${who} asked for more information`,
      lead: `${byline} replied to ${campaignLabel} and wants to know more. That is a positive reply, and a good start.`,
      banner: "tint",
    };
  }
  return {
    emoji: "\u{1F44F}",
    subject: `\u{1F44F} ${who} replied with interest to ${campaignLabel}`,
    headline: `${who} is interested`,
    lead: `${byline} sent a positive reply to ${campaignLabel}. Good news.`,
    banner: "blue",
  };
}

/**
 * Pure: what happens next, in plain words, on every variant. The client has
 * nothing to do; we answer for them; we come back to them only if we need
 * something; the conversation is one click away.
 */
export function nextStepsLines(person: string): string[] {
  return [
    "You have nothing to do.",
    `We answer ${person} for you, in the same email thread.`,
    "If we need any information from you to answer, we will come back to you.",
    "You can follow the conversation in your dashboard at any time.",
  ];
}

export const FOLLOW_BUTTON_LABEL = "Follow the conversation";

/**
 * Pure: the celebration email. Every string from the prospect or the thread is
 * escaped (the template engine interpolates raw).
 */
export function renderCelebration(input: CelebrationInput): CelebrationContent {
  const { leadEmail, brandName, reply, history } = input;
  const campaignLabel = brandName ? `your ${brandName} outreach` : "your outreach";
  const person = prospectLabel(leadEmail, reply);
  const company = input.company?.trim() || null;
  const who = company ?? person;
  const byline =
    company && person !== leadEmail ? `${person} at ${company}` : company ? `Someone at ${company}` : person;
  const copy = variantCopy(celebrationVariantFor(input.kind), who, byline, campaignLabel);
  const steps = nextStepsLines(person === leadEmail ? "them" : person);
  const subject = copy.subject;

  const earlier = history.items.filter(
    (item) => !(item.type === "message" && reply && item.message === reply),
  );
  const notes = history.notes.filter((n) => !(reply === null && n.startsWith("the prospect's latest reply")));

  const replyCard = reply
    ? [
        `<div style="font:12px/1.5 ${FONT};color:${MUTED};">From ${escapeHtml(reply.from)} &middot; ${escapeHtml(formatThreadDate(reply.date))}</div>`,
        `<div style="font:13px/1.5 ${FONT};color:${INK};font-weight:600;padding:2px 0 10px 0;">${escapeHtml(reply.subject)}</div>`,
        `<div style="font:15px/1.65 ${FONT};color:${INK};white-space:pre-wrap;word-break:break-word;">${escapeHtml(reply.bodyText)}</div>`,
      ].join("")
    : `<div style="font:14px/1.6 ${FONT};color:${INK};">We could not read their reply when this email was sent. It is not summarized here: open the conversation in your dashboard to read it in full.</div>`;

  const conversation = earlier
    .map((item) => (item.type === "message" ? messageBlockHtml(item.message) : actionBlockHtml(item, leadEmail)))
    .join("");

  const blue = copy.banner === "blue";
  const bannerBg = blue ? BLUE : TINT;
  const bannerBrand = blue ? "#FFFFFF" : BLUE;
  const bannerTitle = blue ? "#FFFFFF" : INK;
  const bannerLead = blue ? "#DBEAFE" : "#334155";
  const bannerBorder = blue ? "" : `border-bottom:1px solid ${TINT_RULE};`;
  const href = escapeHtml(input.conversationUrl);

  const nextSteps = [
    `<tr><td style="padding:24px 32px 0 32px;">`,
    `<div style="border:1px solid ${RULE};border-radius:12px;padding:20px;">`,
    `<div style="font:600 13px/1.4 ${FONT};color:${INK};text-transform:uppercase;letter-spacing:.04em;">What happens next</div>`,
    ...steps.map(
      (line, i) =>
        `<p style="margin:${i === 0 ? "10px" : "6px"} 0 0 0;font:${i === 0 ? "600 " : ""}14px/1.6 ${FONT};color:${INK};">${escapeHtml(line)}</p>`,
    ),
    `<table role="presentation" cellpadding="0" cellspacing="0" style="margin-top:16px;"><tr><td style="border-radius:10px;background:${BLUE};">`,
    `<a href="${href}" style="display:inline-block;padding:12px 20px;font:600 14px/1 ${FONT};color:#FFFFFF;text-decoration:none;border-radius:10px;">${FOLLOW_BUTTON_LABEL}</a>`,
    `</td></tr></table>`,
    `</div></td></tr>`,
  ].join("");

  const html = [
    `<!doctype html><html><body style="margin:0;padding:0;background:#F8FAFC;">`,
    `<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#F8FAFC;"><tr><td align="center" style="padding:32px 16px;">`,
    `<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="max-width:600px;background:#FFFFFF;border:1px solid ${RULE};border-radius:16px;">`,
    `<tr><td style="padding:0;background:${bannerBg};border-radius:15px 15px 0 0;${bannerBorder}">`,
    `<table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr><td style="padding:28px 32px 30px 32px;">`,
    `<div style="font:700 15px/1 ${FONT};color:${bannerBrand};opacity:.9;">distribute.you</div>`,
    `<div style="margin:22px 0 0 0;font:40px/1 ${FONT};">${copy.emoji}</div>`,
    `<h1 style="margin:12px 0 0 0;font:700 26px/1.25 ${FONT};color:${bannerTitle};">${escapeHtml(copy.headline)}</h1>`,
    `<p style="margin:10px 0 0 0;font:15px/1.6 ${FONT};color:${bannerLead};">${escapeHtml(copy.lead)}</p>`,
    `</td></tr></table></td></tr>`,
    nextSteps,
    `<tr><td style="padding:28px 32px 0 32px;">`,
    `<span style="display:inline-block;padding:4px 10px;border-radius:999px;background:${TINT};border:1px solid ${TINT_RULE};font:600 12px/1.4 ${FONT};color:${BLUE};">Their reply, exactly as they wrote it</span>`,
    `</td></tr>`,
    `<tr><td style="padding:12px 32px 0 32px;"><div style="background:${TINT};border:1px solid ${TINT_RULE};border-radius:12px;padding:20px;">${replyCard}</div></td></tr>`,
    notes.length > 0
      ? `<tr><td style="padding:16px 32px 0 32px;font:13px/1.5 ${FONT};color:${MUTED};">${notes.map((n) => `Note: ${escapeHtml(n)}`).join("<br>")}</td></tr>`
      : "",
    conversation
      ? `<tr><td style="padding:28px 32px 0 32px;"><div style="font:600 13px/1.4 ${FONT};color:${INK};text-transform:uppercase;letter-spacing:.04em;">The conversation so far</div><table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="margin-top:8px;">${conversation}</table></td></tr>`
      : "",
    `<tr><td style="padding:28px 32px 28px 32px;font:12px/1.6 ${FONT};color:${MUTED};">Sent by distribute.you, the team running ${escapeHtml(campaignLabel)}.</td></tr>`,
    `</table></td></tr></table></body></html>`,
  ].join("");

  const textLines = [
    `${copy.emoji} ${copy.headline}`,
    ``,
    copy.lead,
    ``,
    `What happens next`,
    ...steps,
    `${FOLLOW_BUTTON_LABEL}: ${input.conversationUrl}`,
    ``,
    `Their reply, exactly as they wrote it:`,
    ``,
    reply
      ? [`From: ${reply.from}`, `Date: ${formatThreadDate(reply.date)}`, `Subject: ${reply.subject}`, ``, reply.bodyText].join("\n")
      : "We could not read their reply when this email was sent. It is not summarized here.",
    ...(notes.length > 0 ? ["", ...notes.map((n) => `Note: ${n}`)] : []),
    ``,
    `Sent by distribute.you, the team running ${campaignLabel}.`,
  ];

  return { subject, html, text: textLines.join("\n") };
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
      `[instantly-service] celebrate: could not read the lead row of ${leadEmail} — ${error instanceof Error ? error.message : String(error)}; naming the person and linking the People list instead`,
    );
    return null;
  }
}

/** The org's Clerk id for the dashboard link, or null (logged; the link degrades). */
async function externalOrgIdOrNull(orgId: string): Promise<string | null> {
  try {
    return await getExternalOrgId(orgId);
  } catch (error) {
    console.warn(
      `[instantly-service] celebrate: could not resolve the dashboard org of ${orgId} — ${error instanceof Error ? error.message : String(error)}; the button opens the dashboard home`,
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

  await sendEmail(
    {
      appId: "instantly-service",
      eventType: CELEBRATION_EVENT_TYPE,
      recipientEmail: recipient,
      ...(bcc.length > 0 ? { bccEmails: bcc } : {}),
      metadata: { subject: content.subject, html: content.html, text: content.text },
    },
    {
      orgId: campaign.orgId,
      userId: campaign.userId || "00000000-0000-0000-0000-000000000000",
      runId: campaign.runId || undefined,
      tracking: {
        campaignId: campaign.campaignId ?? undefined,
        brandId: campaign.brandIds?.[0],
      },
    },
  );
  console.log(
    `[instantly-service] celebrate: sent for campaign=${campaign.instantlyCampaignId} lead=${leadEmail} → ${recipient}${bcc.length ? ` bcc=${bcc.join(",")}` : ""} replyRead=${latestReply !== null} variant=${celebrationVariantFor(options.kind)} link=${conversationUrl}`,
  );
  return { recipient, bcc, replyRead: latestReply !== null, conversationUrl };
}

/**
 * Celebrate a thread exactly once (twice at most: a meeting request after an
 * earlier positive reply gets its own email). Never throws: claim, send, release
 * on failure. Returns true iff this call sent.
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
    claim = await claimCelebration(campaign.instantlyCampaignId, celebrationVariantFor(kind));
  } catch (error) {
    console.warn(
      `[instantly-service] celebrate: claim failed for campaign=${campaign.instantlyCampaignId} lead=${leadEmail} — ${error instanceof Error ? error.message : String(error)}`,
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
      `[instantly-service] celebrate: FAILED for campaign=${campaign.instantlyCampaignId} lead=${leadEmail} — ${error instanceof Error ? error.message : String(error)}; claim released, will retry on the next signal`,
    );
    return false;
  }
}
