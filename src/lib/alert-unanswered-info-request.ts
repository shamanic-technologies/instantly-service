/**
 * A prospect's QUESTION nobody will answer: tell staff.
 *
 * An info request (`lead_info_requested`) is never announced to the client
 * (owner 2026-10-05, "Only celebrate successes"): we answer it ourselves,
 * through the AI responder. That holds only while a responder campaign runs on
 * the brand's offer. When none does (never bought, stopped, payment hold), the
 * question reached NOBODY: no celebration (withheld by kind), so no "answer it
 * yourself" email either (that one rides on a SENT celebration). Prod
 * 2026-10-05 / 10-08, Shockwavecenters: Pamela Stewart and Dr. Steve Hruby
 * asked questions and sat unseen for 3 to 6 days.
 *
 * So when an info request lands and no responder runs on (brand, offer) AT
 * THAT MOMENT, the agency inbox gets ONE staff email per thread, template
 * `info-request-unanswered`: why it needs a person, their words, the whole
 * conversation. The client is NOT copied (it stays a question we handle). A
 * responder that cannot be read, or a thread whose (brand, offer) cannot be
 * resolved, ALSO alerts: "we could not tell whether anyone answers" is not
 * silence we can afford. A running responder sends nothing (as today).
 *
 * Once per thread: claim `metadata.infoRequestUnansweredAlert` on the campaign
 * row (no migration), released when the send fails so a later event retries.
 * Never throws; the outcome says what happened.
 */

import { sql } from "drizzle-orm";

import { db } from "../db";
import { responderRuns } from "./ask-client-to-answer";
import type { ForwardPositiveReplyCampaign } from "./forward-positive-reply";
import { sendThreadForward } from "./forward-positive-reply";

/** The transactional-email template this module sends (deployed at startup). */
export const INFO_REQUEST_ALERT_EVENT_TYPE = "info-request-unanswered";

/** The one reply kind this alert covers: a question, never announced to the client. */
export const INFO_REQUEST_KIND = "lead_info_requested";

export type InfoRequestAlertOutcome =
  | { sent: true; why: string }
  | { sent: false; reason: string };

function rowsOf(result: unknown): Record<string, unknown>[] {
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

/** Pure: why a person must answer, per responder reading. */
export function unansweredWhy(reading: "none" | "unknown_scope" | "unreadable", detail?: string): string {
  if (reading === "none") {
    return "They asked a question. No AI responder runs on this brand's offer, so nobody will answer it unless a person does. The client was not told (info requests are never announced to them).";
  }
  if (reading === "unknown_scope") {
    return "They asked a question. We could not tell which brand offer this thread belongs to, so we cannot say an AI responder will answer it. Please check it is answered. The client was not told.";
  }
  return `They asked a question. We could not read whether an AI responder runs on this brand's offer${detail ? ` (${detail})` : ""}, so please check it is answered. The client was not told.`;
}

async function claim(instantlyCampaignId: string, entry: string): Promise<boolean> {
  const claimed = rowsOf(
    await db.execute(sql`
      UPDATE instantly_campaigns
      SET metadata = jsonb_set(COALESCE(metadata, '{}'::jsonb), '{infoRequestUnansweredAlert}', ${entry}::jsonb)
      WHERE instantly_campaign_id = ${instantlyCampaignId}
        AND (metadata->'infoRequestUnansweredAlert') IS NULL
      RETURNING instantly_campaign_id
    `),
  );
  return claimed.length > 0;
}

async function release(instantlyCampaignId: string): Promise<void> {
  await db.execute(sql`
    UPDATE instantly_campaigns
    SET metadata = metadata - 'infoRequestUnansweredAlert'
    WHERE instantly_campaign_id = ${instantlyCampaignId}
  `);
}

/**
 * Alert staff about an info request when no AI responder will answer it.
 * No-op on any other kind. Never throws.
 */
export async function maybeAlertUnansweredInfoRequest(
  campaign: ForwardPositiveReplyCampaign,
  leadEmail: string,
  eventType: string,
): Promise<InfoRequestAlertOutcome> {
  if (eventType !== INFO_REQUEST_KIND) return { sent: false, reason: "not_info_request" };
  const tag = `campaign=${campaign.instantlyCampaignId} lead=${leadEmail}`;
  if (!campaign.orgId || !campaign.runId || !campaign.userId) {
    console.error(
      `[instantly-service] info-request-alert: ${tag} has no org/run/user — NOT alerted, nobody was told`,
    );
    return { sent: false, reason: "no_identity" };
  }

  let why: string;
  try {
    const runs = await responderRuns(campaign);
    if (runs === true) {
      console.log(`[instantly-service] info-request-alert: a responder runs for ${tag}; it answers, no alert`);
      return { sent: false, reason: "responder_running" };
    }
    why = unansweredWhy(runs === false ? "none" : "unknown_scope");
  } catch (error) {
    why = unansweredWhy("unreadable", error instanceof Error ? error.message.slice(0, 200) : String(error));
  }

  try {
    const entry = JSON.stringify({ alertedAt: new Date().toISOString(), eventType });
    if (!(await claim(campaign.instantlyCampaignId, entry))) return { sent: false, reason: "already_alerted" };
  } catch (error) {
    console.error(
      `[instantly-service] info-request-alert: claim FAILED for ${tag} — ${error instanceof Error ? error.message : String(error)}; nobody was told`,
    );
    return { sent: false, reason: "claim_failed" };
  }

  try {
    const { fetchLatestReplyText, WORDS_UNAVAILABLE } = await import("./escalate-off-topic-reply");
    const { stripQuotedHistory } = await import("./self-send/qualify-reply");
    const replyText = await fetchLatestReplyText(campaign.instantlyCampaignId);
    const words = replyText ? stripQuotedHistory(replyText).slice(0, 2000) : "";
    const question = [why, "", `They wrote: ${words || WORDS_UNAVAILABLE}`].join("\n");
    const messages = await sendThreadForward(campaign, leadEmail, {
      eventType: INFO_REQUEST_ALERT_EVENT_TYPE,
      metadata: { leadEmail, question },
      copySalesRep: false,
    });
    console.log(`[instantly-service] info-request-alert: SENT for ${tag} (${messages} msg) — ${why}`);
    return { sent: true, why };
  } catch (error) {
    await release(campaign.instantlyCampaignId).catch(() => {});
    console.error(
      `[instantly-service] info-request-alert: send FAILED for ${tag}, claim released — ${error instanceof Error ? error.message : String(error)}; nobody was told`,
    );
    return { sent: false, reason: "send_failed" };
  }
}
