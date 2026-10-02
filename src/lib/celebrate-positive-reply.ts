/**
 * A positive reply is good news for the CLIENT, so the client is told — in a
 * short email that shows the prospect's reply complete and word for word. This replaced the "positive-reply-forward" (a bare thread to the agency
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
 * sits in between. Every variant is SHORT (owner 2026-10-02): a title, "nothing
 * to do, we're answering them", their reply word for word, and a button to the
 * conversation in the dashboard.
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
 * Pure: the title of each variant. Owner rules (2026-10-01): an info request is
 * a mark of interest, NOT a booking request, so it never wears the party emoji;
 * a meeting request is the celebration.
 */
export function variantCopy(variant: CelebrationVariant, who: string): VariantCopy {
  if (variant === "meeting_requested") return { emoji: "\u{1F389}", headline: `${who} wants to book a call` };
  if (variant === "info_requested") return { emoji: "\u{1F4AC}", headline: `${who} asked for more information` };
  return { emoji: "\u{1F44F}", headline: `${who} is interested` };
}

/** The one line under the title, on every variant (owner 2026-10-02: less to read). */
export const NOTHING_TO_DO_LINE = "Nothing to do, we're answering them.";

export const FOLLOW_BUTTON_LABEL = "Follow the conversation";

/**
 * Pure: the email. A title, one line, their reply word for word, one button.
 * Nothing else (owner 2026-10-02: "trop de texte, trop de charge mentale"): the
 * earlier emails live behind the button. Every string from the prospect is
 * escaped (the template engine interpolates raw).
 */
export function renderCelebration(input: CelebrationInput): CelebrationContent {
  const { leadEmail, brandName, reply } = input;
  const person = prospectLabel(leadEmail, reply);
  const company = input.company?.trim() || null;
  const who = person !== leadEmail ? person : (company ?? leadEmail);
  const copy = variantCopy(celebrationVariantFor(input.kind), who);
  const title = `${copy.emoji} ${copy.headline}`;
  const subject = brandName ? `${title} (${brandName})` : title;
  const href = escapeHtml(input.conversationUrl);

  const replyHtml = reply
    ? `<div style="font:15px/1.6 ${FONT};color:${INK};white-space:pre-wrap;word-break:break-word;">${escapeHtml(reply.bodyText)}</div>`
    : `<div style="font:14px/1.6 ${FONT};color:${MUTED};">Their reply could not be read here. It is in the conversation.</div>`;

  const html = [
    `<!doctype html><html><body style="margin:0;padding:0;background:#FFFFFF;">`,
    `<table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr><td align="center" style="padding:32px 16px;">`,
    `<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="max-width:560px;">`,
    `<tr><td><h1 style="margin:0;font:700 22px/1.3 ${FONT};color:${INK};">${escapeHtml(title)}</h1></td></tr>`,
    `<tr><td style="padding:8px 0 0 0;font:15px/1.6 ${FONT};color:${MUTED};">${escapeHtml(NOTHING_TO_DO_LINE)}</td></tr>`,
    `<tr><td style="padding:20px 0 0 0;"><div style="background:${TINT};border:1px solid ${TINT_RULE};border-radius:12px;padding:16px 18px;">${replyHtml}</div></td></tr>`,
    `<tr><td style="padding:20px 0 0 0;"><table role="presentation" cellpadding="0" cellspacing="0"><tr><td style="border-radius:10px;background:${BLUE};">`,
    `<a href="${href}" style="display:inline-block;padding:12px 20px;font:600 14px/1 ${FONT};color:#FFFFFF;text-decoration:none;border-radius:10px;">${FOLLOW_BUTTON_LABEL}</a>`,
    `</td></tr></table></td></tr>`,
    `</table></td></tr></table></body></html>`,
  ].join("");

  const text = [
    title,
    NOTHING_TO_DO_LINE,
    ``,
    reply ? reply.bodyText : "Their reply could not be read here. It is in the conversation.",
    ``,
    `${FOLLOW_BUTTON_LABEL}: ${input.conversationUrl}`,
  ].join("\n");

  return { subject, html, text };
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
  const { latestReply } = await loadHistoryWithLatestReply(campaign, leadEmail, {
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
