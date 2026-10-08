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
 * So, right after a celebration was SENT, and only when no responder campaign
 * runs on (brand, offer) AT THAT MOMENT (`findOngoingResponderCampaigns`), every
 * member of the client org gets a SECOND, separate, plain email (owner
 * 2026-10-08, copy locked word for word, only the placeholders vary):
 *
 *   - Subject `Re: <thread subject>`, Reply-To = the PROSPECT, so hitting Reply
 *     answers them under the same subject with the conversation quoted below.
 *   - One model-written line saying what the prospect wrote. Factual, never
 *     invented; when it cannot be produced it is DROPPED, never made up.
 *   - Below `---`, the whole conversation, newest first: their reply verbatim on
 *     top, then every email of the thread with sender and date.
 *
 * The celebration is untouched. Agency inbox in Bcc (on the first member's copy
 * only, one copy is enough); an org with no member reachable gets it at the
 * agency inbox, somebody must answer.
 *
 * ONCE PER THREAD: claim `client_answer_requested_at` (migration 0068), taken
 * before the send, released only when NO member could be emailed. A failed
 * responder read sends nothing and takes no claim (an unreadable answer is not
 * "nobody answers"). Never throws.
 */

import { and, eq, isNull } from "drizzle-orm";
import { db } from "../db";
import { instantlyCampaigns } from "../db/schema";
import { agencyInbox } from "./agency-inbox";
import { findOngoingResponderCampaigns, getCampaignTriggerScope } from "./campaign-client";
import { orgComplete } from "./chat-client";
import { listOrgMembers, type OrgMember } from "./client-org-client";
import { escapeHtml, prospectLabel } from "./celebrate-positive-reply";
import { sendEmail } from "./email-client";
import type { ForwardPositiveReplyCampaign, ThreadMessage } from "./forward-positive-reply";
import { formatThreadDate } from "./forward-positive-reply";
import { findLeadOnCampaignByEmail } from "./lead-client";
import type { HistoryItem } from "./prospect-history";

/** The transactional-email template this module sends (deployed at startup). */
export const CLIENT_ANSWER_EVENT_TYPE = "positive-reply-answer-request";

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
  /** The prospect's reply, verbatim. */
  reply: ThreadMessage;
  /** Every other email of the thread (any order). */
  earlier: ThreadMessage[];
}

export interface AnswerRequestContent {
  subject: string;
  text: string;
  html: string;
}

function messageBlock(m: ThreadMessage): string {
  return [`From: ${m.from}`, `Date: ${formatThreadDate(m.date)}`, `Subject: ${m.subject}`, ``, m.bodyText.trim()].join(
    "\n",
  );
}

function byDateDesc(a: ThreadMessage, b: ThreadMessage): number {
  return (Date.parse(b.date) || 0) - (Date.parse(a.date) || 0);
}

/** Pure: the email (owner copy, locked 2026-10-08). Plain text; the HTML is that text, escaped. */
export function renderAnswerRequest(input: AnswerRequestInput): AnswerRequestContent {
  const who = input.leadFullName?.trim() || input.leadEmail;
  const company = input.company?.trim();
  const possessive = input.leadFirstName?.trim() ? `${input.leadFirstName.trim()}'s` : "their";
  const lines = [
    input.clientFirstName?.trim() ? `Hi ${input.clientFirstName.trim()},` : "Hi,",
    ``,
    `Good news: ${who}${company ? ` at ${company}` : ""} is interested and is waiting for an answer.`,
    ...(input.summaryLine ? [input.summaryLine] : []),
    ``,
    `You can answer by hitting Reply on this email. It adds ${possessive} email in To:, under the same subject, with the conversation below.`,
    ``,
    `Thanks,`,
    `Kevin`,
    ``,
    `---`,
  ];
  const conversation = [input.reply, ...input.earlier.filter((m) => m !== input.reply).sort(byDateDesc)]
    .map(messageBlock)
    .join("\n\n");
  const text = `${lines.join("\n")}\n${conversation}`;
  const html = `<div style="white-space:pre-wrap;word-break:break-word;">${escapeHtml(text)}</div>`;
  return { subject: replySubject(input.reply.subject), text, html };
}

/** Pure: who receives it. Members with an address; none = the agency inbox alone. */
export function answerRequestRecipients(
  members: OrgMember[],
  agency: string,
): Array<{ email: string; firstName: string | null; bcc: string[] }> {
  if (members.length === 0) return [{ email: agency, firstName: null, bcc: [] }];
  return members.map((m, i) => ({
    email: m.email,
    firstName: m.firstName,
    bcc: i === 0 && m.email.toLowerCase() !== agency.toLowerCase() ? [agency] : [],
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
    const { stripQuotedHistory } = await import("./self-send/qualify-reply");
    const words = stripQuotedHistory(reply.bodyText).trim() || reply.bodyText;
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
        maxTokens: 120,
        temperature: 0,
      },
      { orgId: campaign.orgId, userId: campaign.userId, runId: campaign.runId, brandId: campaign.brandIds?.[0] },
    );
    return cleanSummaryLine(result.content);
  } catch (error) {
    console.warn(
      `[instantly-service] ask-client-to-answer: summary line unavailable for campaign=${campaign.instantlyCampaignId}, sent without it — ${error instanceof Error ? error.message : String(error)}`,
    );
    return null;
  }
}

// ─── IO ──────────────────────────────────────────────────────────────────────

async function claimAnswerRequest(instantlyCampaignId: string): Promise<boolean> {
  const claimed = await db
    .update(instantlyCampaigns)
    .set({ clientAnswerRequestedAt: new Date(), updatedAt: new Date() })
    .where(
      and(
        eq(instantlyCampaigns.instantlyCampaignId, instantlyCampaignId),
        isNull(instantlyCampaigns.clientAnswerRequestedAt),
      ),
    )
    .returning({ id: instantlyCampaigns.id });
  return claimed.length > 0;
}

async function releaseAnswerRequest(instantlyCampaignId: string): Promise<void> {
  await db
    .update(instantlyCampaigns)
    .set({ clientAnswerRequestedAt: null, updatedAt: new Date() })
    .where(eq(instantlyCampaigns.instantlyCampaignId, instantlyCampaignId));
}

/**
 * Is an AI responder running on this thread's (brand, offer) right now? Null
 * when it cannot be said (no caller campaign, no offer stated is "no": the
 * responder trigger asks on (brand, offer) and fires nothing without one).
 * Throws when campaign-service cannot be read.
 */
async function responderRuns(campaign: ForwardPositiveReplyCampaign): Promise<boolean | null> {
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
  | { sent: true; recipients: string[]; bcc: string[]; subject: string; text: string }
  | { sent: false; reason: string };

/**
 * Send the "answer it yourself" email for one thread when no responder runs.
 * Never throws; the outcome says what happened.
 */
export async function maybeAskClientToAnswer(
  campaign: ForwardPositiveReplyCampaign,
  leadEmail: string,
  options: { waitsMs?: number[] } = {},
): Promise<AnswerRequestOutcome> {
  const tag = `campaign=${campaign.instantlyCampaignId} lead=${leadEmail}`;
  if (!campaign.orgId) return { sent: false, reason: "no_org" };

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

  try {
    const { loadHistoryWithLatestReply, REPLY_WAIT_SHORT_MS } = await import("./prospect-history");
    const { history, latestReply } = await loadHistoryWithLatestReply(campaign, leadEmail, {
      waitsMs: options.waitsMs ?? REPLY_WAIT_SHORT_MS,
    });
    if (!latestReply) throw new Error("the prospect's reply is not readable yet");

    const callerId = campaign.conversationCampaignId ?? campaign.campaignId;
    const lead = callerId
      ? await findLeadOnCampaignByEmail({ orgId: campaign.orgId, campaignId: callerId, email: leadEmail }).catch(
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
    const summaryLine = await summaryLineOrNull(campaign, latestReply, firstName, company);

    const earlier = history.items
      .filter((i): i is Extract<HistoryItem, { type: "message" }> => i.type === "message")
      .map((i) => i.message);
    const members = await listOrgMembers(campaign.orgId);
    const recipients = answerRequestRecipients(members, agencyInbox());

    const sentTo: string[] = [];
    const bccSent: string[] = [];
    let content: AnswerRequestContent | null = null;
    for (const r of recipients) {
      content = renderAnswerRequest({
        clientFirstName: r.firstName,
        leadEmail,
        leadFullName: fullName,
        leadFirstName: firstName,
        company,
        summaryLine,
        reply: latestReply,
        earlier,
      });
      try {
        await sendEmail(
          {
            appId: "instantly-service",
            eventType: CLIENT_ANSWER_EVENT_TYPE,
            recipientEmail: r.email,
            ...(r.bcc.length > 0 ? { bccEmails: r.bcc } : {}),
            replyToEmail: leadEmail,
            metadata: { subject: content.subject, html: content.html, text: content.text },
          },
          {
            orgId: campaign.orgId,
            userId: campaign.userId || "00000000-0000-0000-0000-000000000000",
            runId: campaign.runId || undefined,
            tracking: { campaignId: campaign.campaignId ?? undefined, brandId: campaign.brandIds?.[0] },
          },
        );
        sentTo.push(r.email);
        bccSent.push(...r.bcc);
      } catch (error) {
        console.error(
          `[instantly-service] ask-client-to-answer: send to ${r.email} FAILED for ${tag} — ${error instanceof Error ? error.message : String(error)}`,
        );
      }
    }
    if (sentTo.length === 0 || !content) throw new Error("no member could be emailed");

    console.log(
      `[instantly-service] ask-client-to-answer: sent for ${tag} → ${sentTo.join(",")}${bccSent.length ? ` bcc=${bccSent.join(",")}` : ""} replyTo=${leadEmail} subject="${content.subject}" summary=${summaryLine ? "yes" : "dropped"}`,
    );
    return { sent: true, recipients: sentTo, bcc: bccSent, subject: content.subject, text: content.text };
  } catch (error) {
    await releaseAnswerRequest(campaign.instantlyCampaignId).catch(() => {});
    console.error(
      `[instantly-service] ask-client-to-answer: FAILED for ${tag}, claim released — ${error instanceof Error ? error.message : String(error)}`,
    );
    return { sent: false, reason: "send_failed" };
  }
}
