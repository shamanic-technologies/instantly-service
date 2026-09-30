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
 * ⚠️ THE REPLY IS ANNOUNCED BEFORE IT IS READABLE. The history is waited on
 * (`loadHistoryWithLatestReply`) until it holds the prospect's message; one that
 * is still unreadable after the bounded wait is SAID in the email, never
 * replaced by a summary. A model paraphrase is never presented as their words.
 */

import { and, eq, isNull } from "drizzle-orm";
import { db } from "../db";
import { instantlyCampaigns } from "../db/schema";
import { agencyInbox } from "./agency-inbox";
import { getBrandHandoffContext, type BrandHandoffContext } from "./brand-client";
import { sendEmail } from "./email-client";
import type { ForwardPositiveReplyCampaign, ThreadMessage } from "./forward-positive-reply";
import { formatThreadDate } from "./forward-positive-reply";
import type { HistoryItem, ProspectHistory } from "./prospect-history";

/** The transactional-email template this module sends (deployed at startup). */
export const CELEBRATION_EVENT_TYPE = "positive-reply-celebration";

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
  /** The prospect's latest reply, verbatim. Null = could not be read. */
  reply: ThreadMessage | null;
  history: Pick<ProspectHistory, "items" | "notes">;
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
 * Pure: the celebration email. Every string from the prospect or the thread is
 * escaped — the template engine interpolates raw.
 */
export function renderCelebration(input: CelebrationInput): CelebrationContent {
  const { leadEmail, brandName, reply, history } = input;
  const campaignLabel = brandName ? `your ${brandName} outreach` : "your outreach";
  const subject = `Good news: ${leadEmail} replied to ${campaignLabel}`;

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
    : `<div style="font:14px/1.6 ${FONT};color:${INK};">We could not read their reply when this email was sent. It is not summarized here: open the thread in the sending mailbox to read it in full.</div>`;

  const conversation = earlier
    .map((item) => (item.type === "message" ? messageBlockHtml(item.message) : actionBlockHtml(item, leadEmail)))
    .join("");

  const html = [
    `<!doctype html><html><body style="margin:0;padding:0;background:#F8FAFC;">`,
    `<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#F8FAFC;"><tr><td align="center" style="padding:32px 16px;">`,
    `<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="max-width:600px;background:#FFFFFF;border:1px solid ${RULE};border-radius:16px;">`,
    `<tr><td style="padding:28px 32px 0 32px;font:700 16px/1 ${FONT};color:${BLUE};">distribute.you</td></tr>`,
    `<tr><td style="padding:24px 32px 0 32px;">`,
    `<span style="display:inline-block;padding:4px 10px;border-radius:999px;background:${TINT};border:1px solid ${TINT_RULE};font:600 12px/1.4 ${FONT};color:${BLUE};">Positive reply</span>`,
    `<h1 style="margin:14px 0 0 0;font:700 24px/1.3 ${FONT};color:${INK};">${escapeHtml(leadEmail)} wrote back</h1>`,
    `<p style="margin:10px 0 0 0;font:15px/1.6 ${FONT};color:#334155;">A prospect answered ${escapeHtml(campaignLabel)}. Here is their reply, exactly as they wrote it.</p>`,
    `</td></tr>`,
    `<tr><td style="padding:20px 32px 0 32px;"><div style="background:${TINT};border:1px solid ${TINT_RULE};border-radius:12px;padding:20px;">${replyCard}</div></td></tr>`,
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
    `${leadEmail} wrote back.`,
    ``,
    `A prospect answered ${campaignLabel}. Here is their reply, exactly as they wrote it.`,
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

/** Atomically claim the celebration for a thread. True iff THIS call won it. */
export async function claimCelebration(instantlyCampaignId: string): Promise<boolean> {
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
  return claimed.length > 0;
}

/** Release a claim (the send failed) so a later signal re-attempts. */
export async function releaseCelebration(instantlyCampaignId: string): Promise<void> {
  await db
    .update(instantlyCampaigns)
    .set({ positiveReplyForwardedAt: null, updatedAt: new Date() })
    .where(eq(instantlyCampaigns.instantlyCampaignId, instantlyCampaignId));
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
 * Send the celebration for one thread (no claim — the caller holds it).
 * Throws on a send failure so the caller can release the claim.
 */
export async function sendCelebration(
  campaign: ForwardPositiveReplyCampaign,
  leadEmail: string,
  options: { waitsMs?: number[] } = {},
): Promise<{ recipient: string; bcc: string[]; replyRead: boolean }> {
  if (!campaign.orgId) throw new Error("celebrate requires an org-scoped campaign (orgId is null)");
  const { loadHistoryWithLatestReply, REPLY_WAIT_BACKGROUND_MS } = await import("./prospect-history");
  const { history, latestReply } = await loadHistoryWithLatestReply(campaign, leadEmail, {
    waitsMs: options.waitsMs ?? REPLY_WAIT_BACKGROUND_MS,
  });

  const brand = await brandContextOrNull(campaign.brandIds?.[0], campaign.orgId);
  const agency = agencyInbox();
  const recipient = brand?.rep.email ?? agency;
  const bcc = recipient.toLowerCase() === agency.toLowerCase() ? [] : [agency];

  const content = renderCelebration({
    leadEmail,
    brandName: brand?.name ?? null,
    reply: latestReply,
    history,
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
    `[instantly-service] celebrate: sent for campaign=${campaign.instantlyCampaignId} lead=${leadEmail} → ${recipient}${bcc.length ? ` bcc=${bcc.join(",")}` : ""} replyRead=${latestReply !== null}`,
  );
  return { recipient, bcc, replyRead: latestReply !== null };
}

/**
 * Celebrate a thread exactly once. Never throws: claim, send, release on
 * failure. Returns true iff this call sent.
 */
export async function celebrateOnce(
  campaign: ForwardPositiveReplyCampaign,
  leadEmail: string,
  options: { waitsMs?: number[] } = {},
): Promise<boolean> {
  if (!campaign.orgId) return false;
  let claimed: boolean;
  try {
    claimed = await claimCelebration(campaign.instantlyCampaignId);
  } catch (error) {
    console.warn(
      `[instantly-service] celebrate: claim failed for campaign=${campaign.instantlyCampaignId} lead=${leadEmail} — ${error instanceof Error ? error.message : String(error)}`,
    );
    return false;
  }
  if (!claimed) return false;

  try {
    await sendCelebration(campaign, leadEmail, options);
    return true;
  } catch (error) {
    await releaseCelebration(campaign.instantlyCampaignId).catch(() => {});
    console.error(
      `[instantly-service] celebrate: FAILED for campaign=${campaign.instantlyCampaignId} lead=${leadEmail} — ${error instanceof Error ? error.message : String(error)}; claim released, will retry on the next signal`,
    );
    return false;
  }
}
