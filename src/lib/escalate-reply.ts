/**
 * The responder cannot answer this one, so it stops and a human is told.
 *
 * A prospect asks for something we do not hold — a price, a spec, a reference,
 * a date nobody has committed to. The drafting model's response schema REQUIRES
 * a reply body, so it has no way to say "I cannot answer this": the only move
 * left to it is a deflection that re-invites to the call, and the next rung of
 * the follow-up ladder does it again. That is the insisting.
 *
 * This is the exit (owner-decided 2026-09-29). Exactly once per thread:
 *
 *   1. the CLIENT is told, in the positive-reply celebration (their rep, the
 *      agency inbox in Bcc) — a reply we cannot answer is still good news, and
 *      the celebration's own claim makes it one email however many paths fire;
 *   2. when the brand names a rep, the prospect gets an IMMEDIATE hand-over in
 *      their own thread, from the mailbox that wrote to them: it acknowledges
 *      what they asked, says the client's team is copied and will answer, and
 *      answers nothing. The rep is in Cc, the agency inbox in Bcc, the history
 *      quoted below. The rep replies-all from their own inbox;
 *   3. when the brand names NO rep, the prospect gets nothing and the agency
 *      inbox gets the thread under its own subject, quoted as a forward;
 *   4. the follow-up ladder stops, once.
 *
 * Our job stops at the hand-over: no reminder, no follow-up, and the responder
 * never writes on that thread again (`handed_over`, lib/reply-to-lead).
 *
 * ⚠️ NEVER A PARAPHRASE AS THEIR WORDS. The caller's `question` is the drafting
 * model's own reading; it is given to the hand-over model as a hint and shown
 * to nobody. Every email that shows the conversation carries the prospect's
 * reply verbatim, waited on until readable (lib/prospect-history).
 *
 * ⚠️ WHAT THIS DOES *NOT* DO IS DECIDE. Whether a question is answerable is a
 * judgement about the draft, and it can only be made where the draft is made —
 * in workflow-service's DAG, which holds the prompt, the brand facts and the
 * model's own answer. This service owns the two halves that judgement needs and
 * the drafter does not have: the mailbox and the thread (so it can forward the
 * conversation) and the campaign identity (so it can name the lead row whose
 * schedule stops). Do NOT add a content heuristic here; a caller declares.
 *
 * ⚠️ NOTHING IS RETROACTIVE AND NOTHING IS PERMANENT. A stop is not a tombstone
 * by lead-service's own model: if the prospect writes again, the reply side
 * effects re-enqueue them and qualification re-decides. That is correct — a new
 * message is a new decision.
 */

import { sql } from "drizzle-orm";
import { db } from "../db";
import { agencyInbox } from "./agency-inbox";
import { brandContextOrNull, celebrateOnce, escapeHtml } from "./celebrate-positive-reply";
import { orgComplete } from "./chat-client";
import { sendEmail } from "./email-client";
import {
  formatThreadDate,
  sendThreadForward,
  threadSubject,
  type ForwardPositiveReplyCampaign,
  type ThreadMessage,
} from "./forward-positive-reply";
import { findLeadOnCampaignByEmail, stopFollowups } from "./lead-client";
import {
  loadHistoryWithLatestReply,
  renderProspectHistory,
  REPLY_WAIT_SHORT_MS,
} from "./prospect-history";
import { loadCampaign, replyToLead, ReplyToLeadError } from "./reply-to-lead";
import { stripQuotedHistory } from "./self-send/qualify-reply";
import type { BrandHandoffContext } from "./brand-client";

/**
 * A refusal a caller can branch on, mirroring `ReplyToLeadError`'s shape so the
 * route handles both the same way.
 */
export class EscalateReplyError extends Error {
  constructor(
    public readonly code: "campaign_not_found" | "question_required",
    public readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = "EscalateReplyError";
  }
}

export interface EscalateReplyInput {
  orgId: string;
  userId: string;
  /**
   * The caller's run. Required: every send is a child of it (org-billed), and
   * transactional-email-service refuses a send without one.
   */
  runId: string;
  /** Logical campaign id — the same key the reply route takes. */
  campaignId: string;
  leadEmail: string;
  /**
   * The caller's reading of what could not be answered. Required and non-empty
   * (an escalation with nothing to answer is not actionable), but it is a MODEL
   * PARAPHRASE: it is a hint for the hand-over draft and is shown to nobody.
   */
  question: string;
}

export type EscalationHandoff = "rep" | "agency" | "already_escalated";

export interface EscalateReplyResult {
  instantlyCampaignId: string;
  leadEmail: string;
  /** How many messages of the exchange the hand-over carried (0 on a repeat). */
  threadMessages: number;
  /** Whether the follow-up ladder was stopped BY THIS CALL. */
  followupsStopped: boolean;
  /** Who now owns the thread. */
  handoff: EscalationHandoff;
  handedTo: string | null;
  /** Whether the prospect's reply could be read (and so is in what was sent). Null = not checked. */
  replyRead: boolean | null;
}

/** Atomically claim the escalation of one thread. True iff THIS call won. */
export async function claimEscalation(instantlyCampaignId: string): Promise<boolean> {
  const result = await db.execute(sql`
    UPDATE instantly_campaigns
    SET escalated_at = now(), updated_at = now()
    WHERE instantly_campaign_id = ${instantlyCampaignId}
      AND escalated_at IS NULL
    RETURNING id
  `);
  return ((result as { rows?: unknown[] }).rows ?? []).length > 0;
}

/** Release a claim when NOTHING was sent, so a retry can hand over. */
export async function releaseEscalation(instantlyCampaignId: string): Promise<void> {
  await db.execute(sql`
    UPDATE instantly_campaigns
    SET escalated_at = NULL, escalation_handed_to = NULL, updated_at = now()
    WHERE instantly_campaign_id = ${instantlyCampaignId}
  `);
}

export async function recordHandedTo(instantlyCampaignId: string, handedTo: string): Promise<void> {
  await db.execute(sql`
    UPDATE instantly_campaigns
    SET escalation_handed_to = ${handedTo}, updated_at = now()
    WHERE instantly_campaign_id = ${instantlyCampaignId}
  `);
}

async function readHandedTo(instantlyCampaignId: string): Promise<string | null> {
  const result = await db.execute(sql`
    SELECT escalation_handed_to AS "handedTo" FROM instantly_campaigns
    WHERE instantly_campaign_id = ${instantlyCampaignId} LIMIT 1
  `);
  const row = ((result as { rows?: Record<string, unknown>[] }).rows ?? [])[0];
  return row?.handedTo ? String(row.handedTo) : null;
}

// ─── The hand-over draft ────────────────────────────────────────────────────

/** Who the prospect is being handed to, in words. Never a guessed name. */
export function colleagueLine(brand: BrandHandoffContext | null): string {
  const team = brand?.name ? `the ${brand.name} team` : "the team";
  const first = brand?.rep.firstName;
  if (first) {
    const role = brand?.rep.role;
    const at = brand?.name ? ` at ${brand.name}` : "";
    return `Introduce the colleague as ${first}${role ? `, ${role}${at}` : at}. Say they are copied and will answer directly.`;
  }
  return `There is no individual name: say you have copied ${team}, who will answer their questions directly. Never invent or guess a person's name.`;
}

export const HANDOFF_SYSTEM_PROMPT = `You write one short email reply on behalf of a sales rep's assistant.
Situation: a prospect replied to our cold outreach with questions only the client's team can answer. You hand the conversation over to the client's team, who are copied on this email and will reply to everyone.
Write like a busy, competent person typing in Gmail. Plain, warm, specific.
- 40 to 80 words. No subject line. Plain text only.
- Start with "Hi <first name or how they signed>," on its own line.
- One sentence acknowledging what they asked, in your own words, specific to their message. No praise of their questions.
- One sentence introducing who is copied (as instructed below), saying they will answer directly.
- Do not answer any of their questions. Do not promise numbers, dates, documents or outcomes. Do not restate their list.
- End with "Best," on its own line and nothing after it: the sender's name and signature are added automatically.
Banned: em-dashes, "great question", "thoughtful", "exactly the right", "loop in", "looping in", "over to you", "reach out", "happy to", "don't hesitate", "I hope", "delve", "seamless", exclamation marks, bullet points.`;

/** Pure: the model's plain text → the reply's HTML. Em-dashes never survive. */
export function handoffTextToHtml(text: string): string {
  const cleaned = text
    .replace(/\s*\u2014\s*/g, ", ")
    .replace(/\u2013/g, "-")
    .trim();
  return cleaned
    .split(/\n\s*\n/)
    .map((p) => p.trim())
    .filter(Boolean)
    .map((p) => `<p>${escapeHtml(p).replace(/\n/g, "<br>")}</p>`)
    .join("");
}

/**
 * Pure: the conversation quoted under the hand-over, newest first, the way a
 * mail client quotes a thread. Messages only (a visit is not something anyone
 * wrote).
 */
export function renderQuotedHistory(messages: ThreadMessage[]): string {
  if (messages.length === 0) return "";
  const blocks = messages
    .slice()
    .reverse()
    .map(
      (m) =>
        `<div style="margin:0 0 12px 0;">On ${escapeHtml(formatThreadDate(m.date))}, ${escapeHtml(m.from)} wrote:</div>` +
        `<div style="margin:0 0 16px 0;white-space:pre-wrap;">${escapeHtml(m.bodyText)}</div>`,
    )
    .join("");
  return `<br><blockquote style="margin:0 0 0 .8ex;border-left:1px solid #ccc;padding-left:1ex;color:#555;">${blocks}</blockquote>`;
}

async function draftHandoff(input: {
  reply: ThreadMessage;
  brand: BrandHandoffContext | null;
  question: string;
  identity: { orgId: string; userId: string; runId: string };
}): Promise<string> {
  const words = stripQuotedHistory(input.reply.bodyText).trim() || input.reply.bodyText;
  const message = [
    `The prospect's latest reply, verbatim (from ${input.reply.from}):`,
    `"""`,
    words,
    `"""`,
    ``,
    `Who is copied: ${colleagueLine(input.brand)}`,
    ``,
    `Our assistant's note on what it could not answer (a hint, not their words): ${input.question}`,
  ].join("\n");

  const result = await orgComplete(
    {
      message,
      systemPrompt: HANDOFF_SYSTEM_PROMPT,
      provider: "anthropic",
      model: "opus",
      maxTokens: 600,
    },
    input.identity,
  );
  const text = result.content?.trim();
  if (!text) throw new Error("chat-service returned an empty hand-over draft");
  return text;
}

// ─── The escalation ─────────────────────────────────────────────────────────

/**
 * Hand a thread to a person, exactly once. See the module doc for what is sent.
 *
 * FAILS LOUD when nothing could be sent: the claim is released and the error
 * thrown, because an escalation that told nobody is worse than none. Once
 * something WAS sent the claim stands — a retry must not send it twice.
 */
export async function escalateReply(
  input: EscalateReplyInput,
): Promise<EscalateReplyResult> {
  const question = input.question.trim();
  if (!question) {
    throw new EscalateReplyError(
      "question_required",
      400,
      "question is required — an escalation with nothing to answer is not actionable",
    );
  }

  const campaign = await loadCampaign(input.orgId, input.campaignId, input.leadEmail);
  if (!campaign) {
    throw new EscalateReplyError(
      "campaign_not_found",
      404,
      `No campaign ${input.campaignId} in this org for ${input.leadEmail}`,
    );
  }

  if (!(await claimEscalation(campaign.instantlyCampaignId))) {
    const handedTo = await readHandedTo(campaign.instantlyCampaignId);
    console.log(
      `[instantly-service] reply-escalation: campaign=${campaign.instantlyCampaignId} lead=${campaign.leadEmail} already escalated (handed to ${handedTo ?? "in progress"}); nothing sent, nothing stopped`,
    );
    return {
      instantlyCampaignId: campaign.instantlyCampaignId,
      leadEmail: campaign.leadEmail,
      threadMessages: 0,
      followupsStopped: false,
      handoff: "already_escalated",
      handedTo,
      replyRead: false,
    };
  }

  // The caller's run owns the attribution: restating the lead row's own
  // campaign made runs-service 409 the child and nothing was sent. The history
  // the emails tell is still the lead's whole campaign.
  const thread: ForwardPositiveReplyCampaign = {
    instantlyCampaignId: campaign.instantlyCampaignId,
    campaignId: null,
    orgId: input.orgId,
    userId: input.userId,
    runId: input.runId,
    brandIds: campaign.brandId ? [campaign.brandId] : null,
    conversationCampaignId: campaign.campaignId,
  };

  let handoff: "rep" | "agency" | null = null;
  let handedTo: string | null = null;
  let threadMessages = 0;
  let replyRead = false;

  try {
    const brand = await brandContextOrNull(campaign.brandId, input.orgId);
    const { history, latestReply } = await loadHistoryWithLatestReply(thread, campaign.leadEmail, {
      waitsMs: REPLY_WAIT_SHORT_MS,
    });
    replyRead = latestReply !== null;
    threadMessages = history.messages.length;

    // The client hears about it: exactly once per thread, shared with the
    // Instantly qualification path. Background — it waits for the words.
    void celebrateOnce(thread, campaign.leadEmail);

    const repEmail = brand?.rep.email ?? null;
    if (repEmail && latestReply) {
      try {
        const text = await draftHandoff({
          reply: latestReply,
          brand,
          question,
          identity: { orgId: input.orgId, userId: input.userId, runId: input.runId },
        });
        const agency = agencyInbox();
        await replyToLead({
          orgId: input.orgId,
          userId: input.userId,
          campaignId: input.campaignId,
          leadEmail: campaign.leadEmail,
          bodyHtml: handoffTextToHtml(text),
          sentBy: "automation",
          handoff: true,
          copy: {
            cc: [repEmail],
            bcc: repEmail.toLowerCase() === agency.toLowerCase() ? [] : [agency],
          },
          quotedHtml: renderQuotedHistory(history.messages),
        });
        handoff = "rep";
        handedTo = repEmail;
      } catch (error) {
        // The hand-over could not go out (a person already answered, no thread,
        // the model or the mailbox refused). The agency inbox takes it instead:
        // somebody must still be told.
        const why =
          error instanceof ReplyToLeadError
            ? `${error.code}: ${error.message}`
            : error instanceof Error
              ? error.message
              : String(error);
        console.error(
          `[instantly-service] reply-escalation: hand-over to rep ${repEmail} FAILED for campaign=${campaign.instantlyCampaignId} lead=${campaign.leadEmail} — ${why}; handing to the agency inbox instead`,
        );
      }
    } else if (repEmail) {
      console.error(
        `[instantly-service] reply-escalation: brand names rep ${repEmail} but the reply from ${campaign.leadEmail} is unreadable; nothing can be drafted against it, so the agency inbox takes it`,
      );
    }

    if (!handoff) {
      await sendEmail(
        {
          appId: "instantly-service",
          eventType: "reply-escalation",
          recipientEmail: agencyInbox(),
          metadata: {
            subject: threadSubject(history.messages),
            leadEmail: campaign.leadEmail,
            brandName: escapeHtml(brand?.name ?? "This brand"),
            thread: escapeHtml(renderProspectHistory(history, campaign.leadEmail)),
          },
        },
        {
          orgId: input.orgId,
          userId: input.userId,
          runId: input.runId,
          tracking: { brandId: campaign.brandId ?? undefined },
        },
      );
      handoff = "agency";
      handedTo = agencyInbox();
    }
  } catch (error) {
    // Nothing reached anyone: give the claim back so a retry can.
    await releaseEscalation(campaign.instantlyCampaignId).catch(() => {});
    throw error;
  }

  await recordHandedTo(campaign.instantlyCampaignId, handedTo!).catch((error) =>
    console.error(
      `[instantly-service] reply-escalation: could not record the hand-over on campaign=${campaign.instantlyCampaignId} — ${error instanceof Error ? error.message : String(error)}`,
    ),
  );

  const followupsStopped = await stopLadderOnce({
    orgId: input.orgId,
    campaignId: campaign.campaignId,
    instantlyCampaignId: campaign.instantlyCampaignId,
    leadEmail: campaign.leadEmail,
    reason:
      handoff === "rep"
        ? `Handed to ${handedTo}: the automated responder could not answer this reply.`
        : "Handed to the agency inbox: the automated responder could not answer this reply.",
  });

  console.log(
    `[instantly-service] reply-escalation: campaign=${campaign.instantlyCampaignId} lead=${campaign.leadEmail} handed to ${handoff} (${handedTo}), replyRead=${replyRead}, followupsStopped=${followupsStopped}`,
  );

  return {
    instantlyCampaignId: campaign.instantlyCampaignId,
    leadEmail: campaign.leadEmail,
    threadMessages,
    followupsStopped,
    handoff: handoff!,
    handedTo,
    replyRead,
  };
}

/**
 * Stop the follow-up ladder. Never throws once the hand-over went out — a
 * retry would find the claim taken and could not send it again, so a failed
 * stop is logged loudly rather than turning a delivered hand-over into a 500.
 */
async function stopLadderOnce(input: {
  orgId: string;
  campaignId: string | null;
  instantlyCampaignId: string;
  leadEmail: string;
  reason: string;
}): Promise<boolean> {
  try {
    const lead = input.campaignId
      ? await findLeadOnCampaignByEmail({
          orgId: input.orgId,
          campaignId: input.campaignId,
          email: input.leadEmail,
        })
      : null;
    if (!lead) {
      console.warn(
        `[instantly-service] reply-escalation: lead-service holds no row for ${input.leadEmail} on campaign=${input.instantlyCampaignId}, so the follow-up ladder is UNCHANGED`,
      );
      return false;
    }
    await stopFollowups({ orgId: input.orgId, leadRowId: lead.id, reason: input.reason });
    return true;
  } catch (error) {
    console.error(
      `[instantly-service] reply-escalation: follow-up STOP FAILED for ${input.leadEmail} on campaign=${input.instantlyCampaignId} after the hand-over went out — ${error instanceof Error ? error.message : String(error)}`,
    );
    return false;
  }
}

/** One thread to hand to a person — the campaign row plus who is asking. */
export interface HandThreadToHumanInput {
  /** The per-lead thread id (an Instantly campaign id, or a `self:` id). */
  instantlyCampaignId: string;
  /** The logical campaign id lead-service keys its row on. */
  campaignId: string | null;
  leadEmail: string;
  brandId: string | null;
  orgId: string;
  userId: string;
  /** Forwarded as `x-run-id` — transactional-email-service refuses a send without it. */
  runId: string;
  /** What the human has to act on; rendered first in the notification. */
  question: string;
  /** Recorded on lead-service's stop, so the lead timeline says why. */
  stopReason: string;
}

/**
 * Forward the thread to the agency inbox, then stop the follow-up ladder.
 *
 * The shared core of every hand-over: the responder giving up on a question
 * (`escalateReply`) and a reply about something other than the offer
 * (`maybeEscalateOffTopicReply`). One path, so both reach the same inbox in the
 * same shape and stop the same row.
 */
export async function handThreadToHuman(
  input: HandThreadToHumanInput,
): Promise<EscalateReplyResult> {
  const campaign = input;
  const question = input.question;
  const threadMessages = await sendThreadForward(
    {
      instantlyCampaignId: campaign.instantlyCampaignId,
      // The notification is a child of the CALLER's run, and runs-service 409s a
      // child whose campaign differs from its parent's. The caller is the
      // responder campaign; the lead's row belongs to the outreach campaign that
      // first wrote to them — two different ids for the same thread. The parent
      // run already carries the right attribution, so none is restated here.
      campaignId: null,
      orgId: input.orgId,
      userId: input.userId,
      runId: input.runId,
      brandIds: campaign.brandId ? [campaign.brandId] : null,
      // The history the email tells IS the lead's row's campaign, whole.
      conversationCampaignId: campaign.campaignId,
    },
    campaign.leadEmail,
    {
      eventType: "reply-handover",
      metadata: { leadEmail: campaign.leadEmail, question },
    },
  );

  // The ladder stops only if lead-service holds this person on this campaign.
  // It is the SAME narrowing-plus-exact-match the rep call uses, so the two
  // cannot disagree about which row a lead is; anything other than exactly one
  // match resolves to null and the schedule is left alone rather than a
  // stranger's being emptied.
  const lead = campaign.campaignId
    ? await findLeadOnCampaignByEmail({
        orgId: input.orgId,
        campaignId: campaign.campaignId,
        email: campaign.leadEmail,
      })
    : null;

  if (!lead) {
    console.warn(
      `[instantly-service] reply-escalation: forwarded campaign=${campaign.instantlyCampaignId} lead=${campaign.leadEmail} but lead-service holds no row for them, so the follow-up ladder is UNCHANGED`,
    );
    return {
      instantlyCampaignId: campaign.instantlyCampaignId,
      leadEmail: campaign.leadEmail,
      threadMessages,
      followupsStopped: false,
      handoff: "agency",
      handedTo: agencyInbox(),
      replyRead: null,
    };
  }

  await stopFollowups({
    orgId: input.orgId,
    leadRowId: lead.id,
    reason: input.stopReason,
  });

  console.log(
    `[instantly-service] reply-escalation: campaign=${campaign.instantlyCampaignId} lead=${campaign.leadEmail} handed to a human, follow-up ladder stopped`,
  );

  return {
    instantlyCampaignId: campaign.instantlyCampaignId,
    leadEmail: campaign.leadEmail,
    threadMessages,
    followupsStopped: true,
    handoff: "agency",
    handedTo: agencyInbox(),
    replyRead: null,
  };
}
