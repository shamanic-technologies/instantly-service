/**
 * An email that ANSWERS a person's own action leaves at once, like a reply
 * (owner 2026-10-14: "every person who fills your Meta Ads form gets our AI
 * answer within minutes"; people answered within minutes book, a day later they
 * have gone elsewhere).
 *
 * WHICH sends: every sequence whose campaign runs a REACTIVE leg, in
 * campaign-service's own vocabulary (`getCampaignKind`): it runs because a
 * person did something — a form fill, an email a client pushed for someone who
 * asked, a prospect's reply. A PROACTIVE campaign (cold email: it goes and
 * contacts people on its own budget) is untouched: its first emails keep waiting
 * for the sweep, its business hours and its Sunday rest. One rule for every
 * trigger; nothing here names a form, a channel or a client.
 *
 * WHAT changes for an answer: the row is stamped `metadata.answersPerson` (so
 * every later sweep also treats its first email as owed now, ahead of other
 * steps, on any hour and day), then `dispatchAnswerNow` sends it right away with
 * the sweep's own selection: caps, ramp, mailbox health, credential.
 *
 * Detached from `POST /orgs/send` (the caller already has its answer). A failure
 * here never loses the email: unstamped, it goes out with the next sweep at cold
 * pace, and the failure is logged LOUD.
 */

import { db } from "../../db";
import { sql } from "drizzle-orm";
import { getCampaignKind } from "../campaign-client";
import { dispatchAnswerNow, type DispatchSummary } from "./dispatch-worker";
import { isSelfSendDispatchEnabled } from "../../routes/self-send";

export interface AnswerNowOutcome {
  answersPerson: boolean;
  summary: DispatchSummary | null;
}

export async function sendAnswerNowIfOwed(params: {
  orgId: string;
  campaignId: string | null;
  instantlyCampaignId: string;
}): Promise<AnswerNowOutcome> {
  if (!params.campaignId) return { answersPerson: false, summary: null };

  const kind = await getCampaignKind(params.orgId, params.campaignId);
  if (kind !== "reactive") return { answersPerson: false, summary: null };

  await db.execute(sql`
    UPDATE instantly_campaigns
    SET metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object('answersPerson', true),
        updated_at = now()
    WHERE instantly_campaign_id = ${params.instantlyCampaignId}
  `);

  // The kill switch stops this path exactly as it stops the sweep.
  if (!isSelfSendDispatchEnabled()) return { answersPerson: true, summary: null };

  const summary = await dispatchAnswerNow(params.instantlyCampaignId);
  return { answersPerson: true, summary };
}
