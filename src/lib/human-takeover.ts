/**
 * A human took over the conversation, so the automated responder stops.
 *
 * The AI follow-up loop runs across three services: lead-service holds the
 * queue, workflow-service's `ai-meeting-booking` DAG claims a lead and drafts an
 * answer, and this service sends it through `POST /orgs/replies`. Nowhere in
 * that loop does anything ask whether a PERSON has already answered.
 *
 * lead-service's own queue documents the stop that should cover this — "they
 * answered again, the observer of that reply says so" — and no service in the
 * fleet has ever posted it. So a thread somebody answered by hand stays
 * claimable, and the responder drafts on top of a live human conversation.
 * Measured in production before this shipped: 26 replies sent by a person from
 * Instantly's Unibox across 11 threads, every one of them still claimable.
 *
 * ⚠️ THE GATE BELONGS HERE BECAUSE THIS SERVICE PERFORMED THE SEND. Nothing
 * else in the fleet knows what actually went out on a thread — the same reason
 * the per-brand re-contact window lives in this repo. Do NOT resolve this by
 * asking another service; the send evidence is ours.
 *
 * ⚠️ IT READS THE SOURCES, NEVER THE `messages` PROJECTION. That table unifies
 * exactly the two sources below and would be the obvious thing to query, but it
 * is refreshed by a 10-minute worker — and the window that matters here is the
 * minutes between a person answering and the responder's next claim. A gate
 * reading a lagging projection would wave through precisely the case it exists
 * to catch.
 */

import { sql } from "drizzle-orm";

import { db } from "../db";
import { MANUAL_REPLY_STEP } from "./manual-reply-step";

/**
 * Who asked for this reply to be sent.
 *
 * `POST /orgs/replies` had no actor at all: the DAG's draft and a person's own
 * words arrived through the same door, were recorded identically at step 0, and
 * were indistinguishable afterwards. The field is what makes the gate below
 * possible, and it is frozen onto the bronze row at write — the caller already
 * knows, and nothing downstream can re-derive it.
 */
export type ReplySender = "human" | "automation";

/**
 * What an absent `sent_by` means.
 *
 * ⚠️ `automation`, deliberately, and the choice is measured rather than
 * cautious. The DAG is the ONLY caller of `POST /orgs/replies` in the fleet
 * (checked across every clone), and api-service proxies no route to it, so no
 * human path can reach it today without declaring itself. Defaulting to `human`
 * would be the safer-looking pick and would leave the gate inert until
 * workflow-service ships its half, which is to say it would not prevent the
 * thing it exists to prevent. When a human surface IS built, it declares
 * `human` explicitly.
 */
export const DEFAULT_REPLY_SENDER: ReplySender = "automation";

/** Resolve the wire value, absent or not, to the vocabulary above. */
export function resolveReplySender(
  value: ReplySender | null | undefined,
): ReplySender {
  return value ?? DEFAULT_REPLY_SENDER;
}

/** An answer a person put on the thread, and where we learned about it. */
export interface HumanAnswer {
  /** When they sent it, ISO 8601 UTC. */
  at: string;
  /**
   * `dispatched` — through this service, declared `human`.
   * `instantly_unibox` — sent by hand from Instantly's own inbox.
   */
  source: "dispatched" | "instantly_unibox";
}

function rowsOf(result: unknown): Record<string, unknown>[] {
  const r = result as { rows?: unknown };
  if (Array.isArray(r?.rows)) return r.rows as Record<string, unknown>[];
  return Array.isArray(result) ? (result as Record<string, unknown>[]) : [];
}

/**
 * The latest answer a PERSON put on this thread after the prospect last wrote,
 * or null if the only answers since are ours.
 *
 * Two sources, and both are required — measured, not assumed:
 *
 *   1. `smtp_dispatch_raw` at step 0 carrying `sentBy: 'human'`. This is what a
 *      human surface writes once it exists, on either transport, and it is the
 *      only place a reply WE dispatched is guaranteed to appear: of the three
 *      replies this service had sent in production, TWO were absent from
 *      Instantly's own mirror.
 *   2. `instantly_emails_raw` outbound rows that are not a sequence step
 *      (`ue_type` 3 or 4) and that our dispatcher did not produce. Those are
 *      manual sends from Instantly's Unibox, which is how every human takeover
 *      in production so far actually happened.
 *
 * ⚠️ SOURCE 1 IS SELF-BACKFILLING AND NEEDS NO DATA MIGRATION. It requires
 * `sentBy = 'human'` to be present, so every step-0 row written before this
 * shipped — all of which were the DAG's, since it was the only caller — is
 * correctly not a human answer. Source 2 is exact for the same reason in
 * reverse: a manual Instantly send we cannot tie to one of our own dispatches
 * was made by a person, by construction.
 *
 * ⚠️ TIMES ARE COMPARED IN UTC EXPLICITLY. `dispatched_at` and `polled_at` are
 * naive `timestamp` columns while Instantly's `timestamp_email` carries a zone,
 * so an implicit coercion would silently resolve the naive ones against
 * whatever the session's TimeZone happens to be. The comparison decides whether
 * a reply goes out; it must not depend on a server setting.
 *
 * Known residual, stated: a person answering from their OWN mail client rather
 * than through a surface we own is invisible here unless Instantly mirrored it.
 * The IMAP poller reads a mailbox's inbox, not its sent folder, so there is no
 * third source to add — closing that would mean reading Sent, which is a
 * different piece of work.
 *
 * Fails loud. A gate that cannot read its own history must not wave a send
 * through.
 */
export async function findHumanTakeover(
  instantlyCampaignId: string,
): Promise<HumanAnswer | null> {
  const result = await db.execute(sql`
    WITH latest_inbound AS (
      SELECT max(at) AS at
      FROM (
        SELECT (e.payload->>'timestamp_email')::timestamptz AS at
        FROM instantly_emails_raw e
        WHERE e.instantly_campaign_id = ${instantlyCampaignId}
          AND e.payload->>'ue_type' = '2'

        UNION ALL

        SELECT COALESCE(m.received_at, m.polled_at) AT TIME ZONE 'UTC' AS at
        FROM imap_messages_raw m
        WHERE m.instantly_campaign_id = ${instantlyCampaignId}
          AND m.kind IN ('reply', 'auto_reply')
      ) t
    ),
    human_answers AS (
      SELECT d.dispatched_at AT TIME ZONE 'UTC' AS at,
             'dispatched' AS source
      FROM smtp_dispatch_raw d
      WHERE d.instantly_campaign_id = ${instantlyCampaignId}
        AND d.step = ${MANUAL_REPLY_STEP}
        AND d.outcome = 'sent'
        AND d.payload->>'sentBy' = 'human'

      UNION ALL

      SELECT (e.payload->>'timestamp_email')::timestamptz AS at,
             'instantly_unibox' AS source
      FROM instantly_emails_raw e
      WHERE e.instantly_campaign_id = ${instantlyCampaignId}
        AND e.payload->>'ue_type' IN ('3', '4')
        AND NOT EXISTS (
          SELECT 1
          FROM smtp_dispatch_raw d
          WHERE d.instantly_campaign_id = ${instantlyCampaignId}
            AND d.step = ${MANUAL_REPLY_STEP}
            AND d.payload->>'instantlyEmailId' = e.instantly_email_id
        )
    )
    SELECT h.at AS "at", h.source AS "source"
    FROM human_answers h, latest_inbound li
    WHERE h.at IS NOT NULL
      AND (li.at IS NULL OR h.at > li.at)
    ORDER BY h.at DESC
    LIMIT 1
  `);

  const row = rowsOf(result)[0];
  if (!row) return null;

  const at = row.at;
  return {
    at: at instanceof Date ? at.toISOString() : new Date(String(at)).toISOString(),
    source: String(row.source) === "dispatched" ? "dispatched" : "instantly_unibox",
  };
}
