/**
 * A reply about something other than the offer goes to a person.
 *
 * The automated responder only handles SALES conversations: it is fed by the
 * sales-interest gate (`isSalesInterestQualification`), which a `lead_off_topic`
 * reply never passes, so it will not answer one. Before this, nothing else did
 * either — a partnership proposal, a job enquiry, an investor, a vendor pitching
 * us, a journalist all landed as `lead_neutral`, entered no queue and reached no
 * human. Measured 2026-09-28: jakub@marktize.com asked "can you explain?" on a
 * partnership thread and was never answered.
 *
 * The exit is the one the responder itself uses when it cannot answer
 * (`handThreadToHuman`, lib/escalate-reply): the thread is forwarded to the
 * agency inbox with their words first, and the follow-up ladder is stopped so
 * nothing automated writes to them meanwhile. The prospect is sent nothing.
 *
 * Fired from `promoteEvent` on the FIRST promotion of a real `lead_off_topic`
 * event — so a webhook retry or a re-read does not escalate twice. Fail-soft:
 * the classification is recorded whatever happens here, and a throw would 5xx
 * the path that promoted it.
 */

import { sql } from "drizzle-orm";

import { db } from "../db";
import { handThreadToHuman } from "./escalate-reply";
import { htmlToText } from "./forward-positive-reply";
import { isOffTopicReplyKind } from "./reply-kind";
import { fetchLatestMirroredInbound } from "./reply-opt-out";
import { stripQuotedHistory } from "./self-send/qualify-reply";
import { isSelfSendCampaignId } from "./self-send/transport";

/** The subset of a campaign row this needs. */
export interface OffTopicCampaign {
  instantlyCampaignId: string;
  campaignId: string | null;
  orgId: string | null;
  userId: string | null;
  runId: string | null;
  brandIds?: string[] | null;
}

/** Stated when their words cannot be read — the forwarded thread still carries them. */
export const WORDS_UNAVAILABLE = "(their words could not be read here — see the thread below)";

/** What the notification leads with: why a person is needed, then their words. */
export function offTopicQuestion(replyText: string | null): string {
  const words = replyText ? stripQuotedHistory(replyText).slice(0, 2000) : "";
  return [
    "This reply is about something other than the offer (a partnership, hiring, investors, a vendor or the press). The automated responder only handles sales conversations, so it will not answer it.",
    "",
    `They wrote: ${words || WORDS_UNAVAILABLE}`,
  ].join("\n");
}

function rowsOf(result: unknown): Record<string, unknown>[] {
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

/** The prospect's latest reply on this thread, whichever pipe carried it. */
async function fetchLatestReplyText(instantlyCampaignId: string): Promise<string | null> {
  if (!isSelfSendCampaignId(instantlyCampaignId)) {
    return (await fetchLatestMirroredInbound(instantlyCampaignId))?.text ?? null;
  }
  const result = await db.execute(sql`
    SELECT m.payload->>'textSnippet' AS text
    FROM imap_messages_raw m
    WHERE m.instantly_campaign_id = ${instantlyCampaignId}
      AND m.kind = 'reply'
    ORDER BY COALESCE(m.received_at, m.polled_at) DESC
    LIMIT 1
  `);
  const text = rowsOf(result)[0]?.text;
  return typeof text === "string" && text.trim() ? htmlToText(text) : null;
}

/**
 * Hand an off-topic reply to a person. No-op on any other event.
 *
 * Needs an org-scoped campaign carrying its run and user: the notification is a
 * child of that run (org-billed, the same identity the positive-reply forward
 * uses). A row without them is logged loudly and left, never escalated on a
 * made-up identity.
 */
export async function maybeEscalateOffTopicReply(
  campaign: OffTopicCampaign,
  leadEmail: string,
  eventType: string,
): Promise<void> {
  if (!isOffTopicReplyKind(eventType)) return;

  if (!campaign.orgId || !campaign.runId || !campaign.userId) {
    console.warn(
      `[instantly-service] off-topic-escalation: campaign=${campaign.instantlyCampaignId} lead=${leadEmail} has no org/run/user — NOT escalated, nobody was told`,
    );
    return;
  }

  try {
    const replyText = await fetchLatestReplyText(campaign.instantlyCampaignId);
    const result = await handThreadToHuman({
      instantlyCampaignId: campaign.instantlyCampaignId,
      campaignId: campaign.campaignId,
      leadEmail,
      brandId: campaign.brandIds?.[0] ?? null,
      orgId: campaign.orgId,
      userId: campaign.userId,
      runId: campaign.runId,
      question: offTopicQuestion(replyText),
      stopReason:
        "The reply is about something other than the offer; handed to a person, the automated responder does not answer it",
    });
    console.log(
      `[instantly-service] off-topic-escalation: campaign=${campaign.instantlyCampaignId} lead=${leadEmail} handed to a human (${result.threadMessages} msg, followupsStopped=${result.followupsStopped})`,
    );
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(
      `[instantly-service] off-topic-escalation FAILED for campaign=${campaign.instantlyCampaignId} lead=${leadEmail} — ${message}; the reply is classified but NOBODY WAS TOLD`,
    );
  }
}
