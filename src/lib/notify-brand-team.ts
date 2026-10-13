/**
 * A prospect's reply the AI cannot answer goes to the BRAND'S OWN TEAM.
 *
 * Owner 2026-10-13: "the AI reply workflow must escalate to the user when it
 * does not know how to answer. If they do not pay for it, they answer by hand
 * in the Unibox." So the people told are the members of the org that owns the
 * brand (client-service's member list), each by name, with the prospect's reply
 * word for word and a button into the brand's Unibox. Staff are NOT a recipient:
 * every escalation before this went to the agency inbox and the client never
 * learned the AI was stuck (11 of 11 in 30 days).
 *
 * Only reached from the AI-answers path (lib/escalate-reply). A brand that pays
 * for no AI responder gets nothing from here: its team answers in the Unibox.
 *
 * FAILS LOUD: an org with no reachable member, or a send that reached nobody,
 * throws, so the caller gives its claim back and the escalation is retried
 * instead of reported delivered.
 */

import { externalOrgIdOrNull, DASHBOARD_ORIGIN, escapeHtml, prospectLabel } from "./celebrate-positive-reply";
import { listOrgMembers } from "./client-org-client";
import { sendEmail } from "./email-client";
import { formatThreadDate, type ThreadMessage } from "./forward-positive-reply";
import type { ProspectHistory } from "./prospect-history";
import { renderProspectHistory } from "./prospect-history";
import { stripQuotedHistory } from "./self-send/qualify-reply";

/** The transactional-email template this module sends (deployed at startup). */
export const TEAM_ESCALATION_EVENT_TYPE = "reply-escalation";

export const UNIBOX_BUTTON_LABEL = "Answer in your Unibox";

/**
 * Pure: the brand's Unibox, where the client reads and answers every
 * conversation. The org segment is the CLERK org id; without it (or a brand)
 * the dashboard home, which opens the client's own org.
 */
export function uniboxHref(params: { externalOrgId: string | null; brandId: string | null | undefined }): string {
  const { externalOrgId, brandId } = params;
  if (!externalOrgId || !brandId) return `${DASHBOARD_ORIGIN}/v2`;
  return `${DASHBOARD_ORIGIN}/v2/orgs/${encodeURIComponent(externalOrgId)}/brands/${encodeURIComponent(brandId)}/unibox`;
}

const INK = "#0F172A";
const MUTED = "#64748B";
const BLUE = "#2563EB";
const TINT = "#F5F8FF";
const TINT_RULE = "#D6E2FB";
const FONT = "-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif";

/**
 * Why the team is told: the AI met a question it cannot answer, or the reply is
 * one the AI never answers (about something other than the offer, a referral).
 */
export type TeamEscalationReason = "question" | "off_topic" | "referral";

export interface TeamEscalationInput {
  reason: TeamEscalationReason;
  /** The member this copy is addressed to; null = no greeting name. */
  memberFirstName: string | null;
  leadEmail: string;
  brandName: string | null;
  /** The prospect's latest reply, verbatim. Null = could not be read. */
  reply: ThreadMessage | null;
  /** The whole exchange, for the block below the button. */
  history: Pick<ProspectHistory, "items" | "notes">;
  uniboxUrl: string;
}

export interface TeamEscalationContent {
  subject: string;
  html: string;
  text: string;
}

/**
 * Pure: the email. Short sentences, the person named in each, their words
 * verbatim on top, one button. Every string from the prospect is escaped (the
 * template engine interpolates raw).
 */
export function renderTeamEscalation(input: TeamEscalationInput): TeamEscalationContent {
  const who = prospectLabel(input.leadEmail, input.reply);
  const outreach = `your ${input.brandName ? `${input.brandName} ` : ""}outreach`;
  const subject =
    input.reason === "question"
      ? `${who} asked your ${input.brandName ? `${input.brandName} ` : ""}team a question`
      : `${who} replied to ${outreach}`;
  const greeting = input.memberFirstName ? `Hi ${input.memberFirstName},` : "Hi,";
  const lines =
    input.reason === "question"
      ? [`${who} replied to ${outreach} with a question for your team.`, `Our AI paused so you can answer it yourself.`]
      : input.reason === "referral"
        ? [`${who} replied to ${outreach} and pointed you to someone else.`, `Our AI does not write to a referred contact, so this reply is yours.`]
        : [`${who} replied to ${outreach} about something other than your offer.`, `Our AI only answers sales conversations, so this reply is yours.`];
  const words = input.reply ? stripQuotedHistory(input.reply.bodyText).trim() || input.reply.bodyText : null;
  const unreadable = "Their reply could not be read when this email was sent. It is in your Unibox.";
  const href = escapeHtml(input.uniboxUrl);
  const thread = renderProspectHistory(input.history, input.leadEmail);

  const replyHtml = input.reply
    ? [
        `<div style="font:12px/1.5 ${FONT};color:${MUTED};">${escapeHtml([who, formatThreadDate(input.reply.date), input.reply.subject].filter((p) => p.trim()).join(" · "))}</div>`,
        `<div style="padding:6px 0 0 0;font:15px/1.6 ${FONT};color:${INK};white-space:pre-wrap;word-break:break-word;">${escapeHtml(words ?? "")}</div>`,
      ].join("")
    : `<div style="font:14px/1.6 ${FONT};color:${MUTED};">${unreadable}</div>`;

  const html = [
    `<!doctype html><html><body style="margin:0;padding:0;background:#FFFFFF;">`,
    `<table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr><td align="center" style="padding:32px 16px;">`,
    `<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="max-width:560px;">`,
    `<tr><td style="font:15px/1.6 ${FONT};color:${INK};">${escapeHtml(greeting)}</td></tr>`,
    ...lines.map((l) => `<tr><td style="padding:8px 0 0 0;font:15px/1.6 ${FONT};color:${INK};">${escapeHtml(l)}</td></tr>`),
    `<tr><td style="padding:20px 0 0 0;"><div style="background:${TINT};border:1px solid ${TINT_RULE};border-radius:12px;padding:16px 18px;">${replyHtml}</div></td></tr>`,
    `<tr><td style="padding:20px 0 0 0;"><table role="presentation" cellpadding="0" cellspacing="0"><tr><td style="border-radius:10px;background:${BLUE};">`,
    `<a href="${href}" style="display:inline-block;padding:12px 20px;font:600 14px/1 ${FONT};color:#FFFFFF;text-decoration:none;border-radius:10px;">${UNIBOX_BUTTON_LABEL}</a>`,
    `</td></tr></table></td></tr>`,
    `<tr><td style="padding:28px 0 0 0;"><div style="font:600 13px/1.4 ${FONT};color:${INK};">The whole conversation</div>`,
    `<pre style="margin:6px 0 0 0;font:13px/1.5 ${FONT};color:#334155;white-space:pre-wrap;word-break:break-word;">${escapeHtml(thread)}</pre></td></tr>`,
    `</table></td></tr></table></body></html>`,
  ].join("");

  const text = [
    greeting,
    ``,
    ...lines,
    ``,
    input.reply ? words : unreadable,
    ``,
    `${UNIBOX_BUTTON_LABEL}: ${input.uniboxUrl}`,
    ``,
    `The whole conversation`,
    ``,
    thread,
  ].join("\n");

  return { subject, html, text };
}

export interface EmailBrandTeamInput {
  reason: TeamEscalationReason;
  orgId: string;
  userId: string;
  runId: string;
  brandId: string | null;
  brandName: string | null;
  leadEmail: string;
  history: Pick<ProspectHistory, "items" | "notes">;
  latestReply: ThreadMessage | null;
}

/**
 * Email every member of the brand's org. Returns the addresses reached; throws
 * when none was (no member with an address, or every send failed).
 */
export async function emailBrandTeam(input: EmailBrandTeamInput): Promise<string[]> {
  const members = await listOrgMembers(input.orgId);
  if (members.length === 0) {
    throw new Error(`org ${input.orgId} has no member with an email address: nobody of the brand's team can be told`);
  }
  const uniboxUrl = uniboxHref({ externalOrgId: await externalOrgIdOrNull(input.orgId), brandId: input.brandId });

  const sent: string[] = [];
  for (const member of members) {
    const content = renderTeamEscalation({
      reason: input.reason,
      memberFirstName: member.firstName,
      leadEmail: input.leadEmail,
      brandName: input.brandName,
      reply: input.latestReply,
      history: input.history,
      uniboxUrl,
    });
    try {
      await sendEmail(
        {
          appId: "instantly-service",
          eventType: TEAM_ESCALATION_EVENT_TYPE,
          recipientEmail: member.email,
          metadata: { subject: content.subject, html: content.html, text: content.text },
        },
        {
          orgId: input.orgId,
          userId: input.userId,
          runId: input.runId,
          tracking: { brandId: input.brandId ?? undefined },
        },
      );
      sent.push(member.email);
    } catch (error) {
      console.error(
        `[instantly-service] reply-escalation: send to team member ${member.email} FAILED for lead=${input.leadEmail} — ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }
  if (sent.length === 0) {
    throw new Error(`no member of org ${input.orgId} could be emailed about ${input.leadEmail}`);
  }
  return sent;
}
