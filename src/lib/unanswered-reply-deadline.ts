/**
 * A prospect's reply a responder took but never answered: after 24 h, the
 * client is asked to answer it themselves (lib/ask-client-to-answer, staff in Cc).
 *
 * When a positive reply lands, campaign-service is asked to run the responder
 * and its answer decides (lib/trigger-sales-interest-campaign): nobody takes it
 * = the client is asked right away. But "taken" is not "answered": a stuck run,
 * or a lead the run never reached, sends nothing. Measured 2026-10-11 over 45
 * days of positive replies: median answer 5 h, p90 7 days, 12 of 28 never.
 *
 * So this sweep reads the OUTCOME: every positive reply (interested, question,
 * meeting request) we learned of more than `UNANSWERED_REPLY_DEADLINE_MS` ago,
 * however long ago, whose thread is still that kind, with no answer sent into it since THEY LAST wrote (our dispatcher's
 * step 0, an Instantly Unibox message, or a staff answer our IMAP poller filed;
 * dated against the prospect's NEWEST message from the Instantly mirror or our
 * IMAP copy, not against when we labeled it), not escalated to staff, and not
 * asked yet, gets
 * the answer request. Same once-per-thread claim as the live path. The ask
 * itself re-reads the whole thread: any message of ours after their question
 * sends nothing, and a question older than a few days goes to staff as a
 * check-in (lib/ask-client-to-answer).
 *
 * ⚠️ NO LOOKBACK (UNANSWERED-POSITIVE-1013). A 14-day window left every
 * positive reply older than two weeks with nobody asked, forever: Robert at
 * acupunctureconnections.com wrote "I'll take a look at it. Thanks" on Sep 18
 * and never heard from us (48 such threads on 2026-10-13, Feb to Sep). The
 * once-per-thread claim makes the backlog a one-time pass; an old thread goes
 * to staff as a check-in, never to the client.
 *
 * ⚠️ Threads staff were alerted about on 2026-10-11 (the retired staff-only
 * alert, `metadata.infoRequestUnansweredAlert`) are left with staff: one of the
 * three was a reply from August, relabeled later.
 *
 * Fails loud on the read (the worker logs it, the next tick retries); each send
 * is fail-soft on its own.
 */

import { sql } from "drizzle-orm";

import { db } from "../db";
import { maybeAskClientToAnswer } from "./ask-client-to-answer";

/** A working day: past the responder's normal answer time (median 5 h). */
export const UNANSWERED_REPLY_DEADLINE_MS = 24 * 60 * 60 * 1000;
/** The reply kinds a responder is asked to answer and the client may be asked about. */
export const ANSWERABLE_REPLY_KINDS = ["lead_interested", "lead_info_requested", "lead_meeting_requested"] as const;

export interface UnansweredReply {
  instantlyCampaignId: string;
  leadEmail: string;
  campaignId: string | null;
  orgId: string;
  userId: string;
  runId: string;
  brandIds: string[] | null;
  /** The thread's reply kind now (one of ANSWERABLE_REPLY_KINDS). */
  replyKind: string;
  /** When THEY last wrote (their newest message, else when we learned of the reply). */
  repliedAt: Date;
}

function rowsOf(result: unknown): Record<string, unknown>[] {
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

const kindsSql = sql.join(
  ANSWERABLE_REPLY_KINDS.map((k) => sql`${k}`),
  sql`, `,
);

/** Positive replies nobody answered within the deadline and the client was not asked about. */
export async function findUnansweredReplies(asOf: Date = new Date()): Promise<UnansweredReply[]> {
  const cutoff = new Date(asOf.getTime() - UNANSWERED_REPLY_DEADLINE_MS).toISOString();
  const result = await db.execute(sql`
    WITH q AS (
      SELECT e.campaign_id AS instantly_campaign_id, max(e.created_at) AS known_at
      FROM instantly_events e
      WHERE e.event_type IN (${kindsSql})
        AND e.inferred = false
        AND e.withdrawn_at IS NULL
      GROUP BY e.campaign_id
    )
    SELECT c.instantly_campaign_id, c.lead_email, c.campaign_id, c.org_id, c.user_id, c.run_id, c.brand_ids,
           g.reply_kind, a.asked_at
    FROM q
    JOIN instantly_campaigns c ON c.instantly_campaign_id = q.instantly_campaign_id
    JOIN instantly_lead_status_current g ON g.instantly_campaign_id = c.instantly_campaign_id
    -- When THEY LAST wrote, not when we learned: a reply relabeled weeks later
    -- (Dr. Gallego: labeled Oct 8) is dated by her newest message, WHEREVER it
    -- was ingested. Her Sep 14 objection exists only in our IMAP copy; reading
    -- the Instantly mirror alone dated the thread Aug 2 and our Sep 4 bump read
    -- as the answer (2026-10-12). GREATEST skips a source with nothing.
    CROSS JOIN LATERAL (
      SELECT COALESCE(
        GREATEST(
          (
            SELECT max((m.payload->>'timestamp_email')::timestamptz) AT TIME ZONE 'UTC'
            FROM instantly_emails_raw m
            WHERE m.instantly_campaign_id = c.instantly_campaign_id
              AND m.payload->>'ue_type' = '2'
              AND lower(m.payload->>'from_address_email') = lower(c.lead_email)
          ),
          (
            SELECT max(COALESCE(i.received_at, i.polled_at))
            FROM imap_messages_raw i
            WHERE i.instantly_campaign_id = c.instantly_campaign_id
              AND i.kind = 'reply'
          )
        ),
        q.known_at
      ) AS asked_at
    ) a
    WHERE q.known_at <= ${cutoff}::timestamp
      AND g.reply_kind IN (${kindsSql})
      AND c.org_id IS NOT NULL AND c.user_id IS NOT NULL AND c.run_id IS NOT NULL
      AND c.client_answer_requested_at IS NULL
      AND c.escalated_at IS NULL
      AND (c.metadata->'infoRequestUnansweredAlert') IS NULL
      AND NOT EXISTS (
        SELECT 1 FROM smtp_dispatch_raw d
        WHERE d.instantly_campaign_id = c.instantly_campaign_id
          AND d.step = 0
          AND d.dispatched_at >= a.asked_at
      )
      AND NOT EXISTS (
        SELECT 1 FROM instantly_emails_raw m
        WHERE m.instantly_campaign_id = c.instantly_campaign_id
          AND m.payload->>'ue_type' = '3'
          AND (m.payload->>'timestamp_email')::timestamptz >= a.asked_at AT TIME ZONE 'UTC'
      )
      AND NOT EXISTS (
        SELECT 1 FROM imap_messages_raw i
        WHERE i.instantly_campaign_id = c.instantly_campaign_id
          AND i.kind = 'staff_reply'
          AND COALESCE(i.received_at, i.polled_at) >= a.asked_at
      )
    ORDER BY q.known_at
  `);
  return rowsOf(result).map((r) => ({
    instantlyCampaignId: String(r.instantly_campaign_id),
    leadEmail: String(r.lead_email),
    campaignId: (r.campaign_id as string | null) ?? null,
    orgId: String(r.org_id),
    userId: String(r.user_id),
    runId: String(r.run_id),
    brandIds: Array.isArray(r.brand_ids) ? (r.brand_ids as string[]) : null,
    replyKind: String(r.reply_kind),
    repliedAt: asUtcDate(r.asked_at),
  }));
}

/** `timestamp without time zone` columns hold UTC; node-postgres would read them as local time. */
function asUtcDate(value: unknown): Date {
  if (value instanceof Date) return value;
  const text = String(value);
  return new Date(/[zZ]|[+-]\d{2}(:?\d{2})?$/.test(text) ? text : `${text.replace(" ", "T")}Z`);
}

let inFlight: Promise<{ unanswered: number; asked: number }> | null = null;

/** One sweep. Concurrent calls join the one in flight. Throws when the read fails. */
export function askClientsAboutUnansweredReplies(
  asOf: Date = new Date(),
): Promise<{ unanswered: number; asked: number }> {
  if (inFlight) return inFlight;
  inFlight = (async () => {
    const unanswered = await findUnansweredReplies(asOf);
    let asked = 0;
    for (const u of unanswered) {
      const outcome = await maybeAskClientToAnswer(
        {
          instantlyCampaignId: u.instantlyCampaignId,
          campaignId: u.campaignId,
          orgId: u.orgId,
          userId: u.userId,
          runId: u.runId,
          brandIds: u.brandIds,
        },
        u.leadEmail,
        { because: "overdue", replyKind: u.replyKind, repliedAt: u.repliedAt },
      );
      if (outcome.sent) asked += 1;
    }
    if (unanswered.length > 0) {
      console.log(`[instantly-service] unanswered-reply-deadline: ${unanswered.length} unanswered, ${asked} clients asked`);
    }
    return { unanswered: unanswered.length, asked };
  })().finally(() => {
    inFlight = null;
  });
  return inFlight;
}
