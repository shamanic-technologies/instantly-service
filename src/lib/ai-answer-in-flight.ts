/**
 * ONE PATH PER REPLY (ESCALATION-AND-AI-REPLY-1126, owner 2026-11-26).
 *
 * A prospect's reply is either answered by our AI or handed to the brand's
 * team, never both. Living Vital 2026-11-09: Markus asked whether the psyllium
 * grows in Switzerland; the escalation claimed the thread at 15:30:12.5 and
 * told the client "Our AI paused so you can answer it yourself", and the AI's
 * own answer left 1.4 s later. The reply gate was a plain SELECT run before the
 * claim existed, so both went through. The founder then answered by hand too.
 *
 * The two writers now meet on ONE row (`instantly_campaigns`), each in a single
 * UPDATE, so Postgres's row lock orders them:
 *   - an automated answer MARKS the thread (`ai_answer_started_at`) only while
 *     no escalation is claimed; no row = the thread is handed over, nothing is
 *     sent (409 `handed_over`). The mark is cleared once the answer went out or
 *     failed (`finishAiAnswer`).
 *   - the escalation claim (lib/escalate-reply) returns the mark it found. An
 *     answer in flight is WAITED ON (`waitForAiAnswer`), then the escalation
 *     reads whether the AI answered since the prospect wrote
 *     (`findAiAnswerSince`). If it did, the in-thread hand-over is not sent and
 *     the brand's team is told the AI answered, its words quoted; if not, the
 *     usual "our AI paused" email is true.
 *
 * Never a content rule, never a per-brand case: the AI still decides whether to
 * answer; this only makes what the client is told match what was sent.
 */

import { sql } from "drizzle-orm";
import { db } from "../db";
import { htmlToText } from "./forward-positive-reply";
import { stripAccountSignature } from "./send-lead";

function rowsOf(result: unknown): Record<string, unknown>[] {
  return ((result as { rows?: Record<string, unknown>[] } | undefined)?.rows ?? []);
}

/**
 * Mark an automated answer in flight on this thread, only while no escalation
 * is claimed on it. Returns the mark (to clear exactly it later), or null when
 * the thread is handed to a person.
 */
export async function startAiAnswer(instantlyCampaignId: string): Promise<string | null> {
  const result = await db.execute(sql`
    UPDATE instantly_campaigns
    SET ai_answer_started_at = clock_timestamp() AT TIME ZONE 'UTC'
    WHERE instantly_campaign_id = ${instantlyCampaignId}
      AND escalated_at IS NULL
    RETURNING ai_answer_started_at::text AS "mark"
  `);
  const row = rowsOf(result)[0];
  return row ? String(row.mark) : null;
}

/**
 * Clear THIS answer's mark (a second answer that marked the thread after it
 * keeps its own). Never throws: the answer is already out or already failed.
 */
export async function finishAiAnswer(instantlyCampaignId: string, mark: string): Promise<void> {
  try {
    await db.execute(sql`
      UPDATE instantly_campaigns
      SET ai_answer_started_at = NULL
      WHERE instantly_campaign_id = ${instantlyCampaignId}
        AND ai_answer_started_at::text = ${mark}
    `);
  } catch (error) {
    console.error(
      `[instantly-service] ai-answer: could not clear the in-flight mark on ${instantlyCampaignId}: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }
}

/** A mark older than this is a process that died mid-answer, not an answer in flight. */
export const AI_ANSWER_STALE_MS = 3 * 60_000;
/** How often the escalation re-reads the mark while an answer is in flight. */
export const AI_ANSWER_POLL_MS = 1_000;

const defaultSleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/**
 * Wait until no automated answer is in flight on this thread (the mark is
 * cleared, or stale). Returns at once when none is.
 */
export async function waitForAiAnswer(
  instantlyCampaignId: string,
  options: { sleep?: (ms: number) => Promise<void> } = {},
): Promise<void> {
  const sleep = options.sleep ?? defaultSleep;
  for (;;) {
    // Age computed by Postgres on the same UTC clock the mark was written with.
    const result = await db.execute(sql`
      SELECT (ai_answer_started_at IS NOT NULL
              AND ai_answer_started_at > (clock_timestamp() AT TIME ZONE 'UTC')
                  - make_interval(secs => ${AI_ANSWER_STALE_MS / 1000})) AS "inFlight"
      FROM instantly_campaigns
      WHERE instantly_campaign_id = ${instantlyCampaignId} LIMIT 1
    `);
    if (rowsOf(result)[0]?.inFlight !== true) return;
    await sleep(AI_ANSWER_POLL_MS);
  }
}

export interface AiAnswer {
  sentAt: string;
  /** The answer's own words: signature and quoted conversation cut. */
  text: string;
}

/**
 * Pure: the words of an answer as it was built (HTML body, signature and the
 * quoted conversation appended). Everything from the signature on is cut.
 */
export function answerWords(bodyHtml: string): string {
  return htmlToText(stripAccountSignature(bodyHtml)).trim();
}

/**
 * The latest automated answer sent on this thread at or after `since` (our own
 * sender's record of every answer that left), or null. A hand reply by a
 * person is not the AI's answer.
 */
export async function findAiAnswerSince(instantlyCampaignId: string, since: Date): Promise<AiAnswer | null> {
  const result = await db.execute(sql`
    SELECT to_char(created_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "sentAt", message_inputs->>'bodyHtml' AS "bodyHtml"
    FROM smtp_sent_messages
    WHERE instantly_campaign_id = ${instantlyCampaignId}
      AND kind = 'manual_reply'
      AND decision->>'sentBy' = 'automation'
      AND created_at >= (${since.toISOString()}::timestamptz AT TIME ZONE 'UTC')
    ORDER BY created_at DESC
    LIMIT 1
  `);
  const row = rowsOf(result)[0];
  if (!row) return null;
  return {
    sentAt: String(row.sentAt),
    text: answerWords(String(row.bodyHtml ?? "")),
  };
}
