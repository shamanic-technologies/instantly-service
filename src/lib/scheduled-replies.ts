/**
 * What is left of the waiting room for answers that were not due yet.
 *
 * ⚠️ NOTHING ENTERS IT ANY MORE (2026-09-25). A one-to-one answer to a prospect
 * who wrote back used to wait here for their Mon-Fri 08:00-17:00 window; the
 * owner reversed that, and `replyToLead` now sends at once, at any hour. This
 * module only drains the rows written before the change, and the table stays
 * as the record of what was held.
 *
 * ⚠️ A DRAFT IS NOT SENT LATE. An answer's words were written for the moment
 * they were drafted: "tomorrow, September 25th", drafted on the 24th and held
 * until the 25th, would have told the prospect the wrong day. So a waiting row
 * is split by AGE, not by any window (`planScheduledReplies`):
 *
 *   - drafted within `SCHEDULED_REPLY_MAX_DRAFT_AGE_MS` — its words are still
 *     true, it goes out now;
 *   - older, from the automated responder — it is SUPERSEDED, never sent: the
 *     lead is put back in lead-service's follow-up queue due now and the
 *     responder's campaign is asked to run, so a fresh answer is drafted for
 *     the moment it will actually be read;
 *   - older, from a person — sent as written. Those are a human's own words and
 *     nobody can redraft them; holding them longer only makes them staler.
 *
 * A reply is still not a sequence step: it takes no `sequence_steps` row and no
 * `sequence_costs` hold, and it is recorded in bronze at `MANUAL_REPLY_STEP` (0)
 * when it goes out.
 */

import { sql } from "drizzle-orm";

import { db } from "../db";
import { resolveReplySender, type ReplySender } from "./human-takeover";

/** One answer that was held for its prospect's morning. */
export interface ScheduledReply {
  id: string;
  orgId: string;
  userId: string;
  campaignId: string;
  instantlyCampaignId: string;
  leadEmail: string;
  bodyHtml: string;
  /** Who asked for it — replayed by the drain so the takeover gate reads true. */
  sentBy: ReplySender;
  /** The prospect's IANA timezone, or null when we hold none. */
  timezone: string | null;
  scheduledFor: Date;
  /** When the words were drafted — what decides whether they are still true. */
  createdAt: Date;
  attempts: number;
}

/**
 * How many times a waiting reply is retried before it is given up on.
 *
 * A reply that keeps failing is failing for a reason a retry cannot fix (a dead
 * mailbox, a revoked credential), and an unbounded retry would re-send the same
 * refusal every hour forever. The row is kept, with its last error, so the
 * failure is readable rather than silent.
 */
export const MAX_SCHEDULED_REPLY_ATTEMPTS = 5;

/**
 * How old a drafted answer may be and still go out as written.
 *
 * Long enough to cover the dispatch tick that drains the row plus a retry after
 * a transient refusal; short enough that no relative day in the words ("today",
 * "tomorrow", a weekday) can have rolled over in the prospect's timezone in any
 * way that matters. Anything older is redrafted rather than sent.
 */
export const SCHEDULED_REPLY_MAX_DRAFT_AGE_MS = 15 * 60_000;

export interface ScheduledReplyPlan {
  /** Rows whose words are still true — sent now. */
  send: ScheduledReply[];
  /** Automated drafts too old to send — superseded and redrafted. */
  redraft: ScheduledReply[];
}

/**
 * What to do with each waiting reply at `asOf` — pure.
 *
 * ⚠️ NO WINDOW AND NO WEEKEND GATE. Both were the hold this change removes; a
 * waiting reply is owed NOW. The only question left is whether its words are
 * still true when sent — see the module comment. Oldest first, ties on id.
 */
export function planScheduledReplies(
  replies: readonly ScheduledReply[],
  asOf: Date,
): ScheduledReplyPlan {
  const ordered = replies
    .slice()
    .sort(
      (a, b) =>
        a.createdAt.getTime() - b.createdAt.getTime() || a.id.localeCompare(b.id),
    );

  const plan: ScheduledReplyPlan = { send: [], redraft: [] };
  for (const reply of ordered) {
    const age = asOf.getTime() - reply.createdAt.getTime();
    if (reply.sentBy === "human" || age <= SCHEDULED_REPLY_MAX_DRAFT_AGE_MS) {
      plan.send.push(reply);
    } else {
      plan.redraft.push(reply);
    }
  }
  return plan;
}

/**
 * `created_at` is a naive `timestamp` holding UTC; node-postgres hands it back as
 * a string without a zone (or a Date built in the process's zone). Read it as
 * UTC explicitly so the draft age does not depend on where the process runs.
 */
function asUtcDate(value: unknown): Date {
  if (value instanceof Date) return value;
  const text = String(value);
  return new Date(/[zZ]|[+-]\d\d:?\d\d$/.test(text) ? text : `${text.replace(" ", "T")}Z`);
}

/** Everything still waiting, whatever its due date — the drain decides purely. */
export async function loadPendingScheduledReplies(): Promise<ScheduledReply[]> {
  const result = await db.execute(sql`
    SELECT
      r.id                    AS "id",
      r.org_id                AS "orgId",
      r.user_id               AS "userId",
      r.campaign_id           AS "campaignId",
      r.instantly_campaign_id AS "instantlyCampaignId",
      r.lead_email            AS "leadEmail",
      r.body_html             AS "bodyHtml",
      r.sent_by               AS "sentBy",
      r.timezone              AS "timezone",
      r.scheduled_for         AS "scheduledFor",
      r.created_at            AS "createdAt",
      r.attempts              AS "attempts"
    FROM scheduled_replies r
    WHERE r.status = 'pending'
      AND r.attempts < ${MAX_SCHEDULED_REPLY_ATTEMPTS}
    ORDER BY r.scheduled_for ASC, r.id ASC
  `);

  return (result.rows as Record<string, unknown>[]).map((row) => ({
    id: String(row.id),
    orgId: String(row.orgId),
    userId: String(row.userId),
    campaignId: String(row.campaignId),
    instantlyCampaignId: String(row.instantlyCampaignId),
    leadEmail: String(row.leadEmail),
    bodyHtml: String(row.bodyHtml),
    // A row enqueued before the column existed carries null, and resolves to
    // the same default an undeclared caller gets.
    sentBy: resolveReplySender(
      row.sentBy === "human" || row.sentBy === "automation"
        ? (row.sentBy as ReplySender)
        : null,
    ),
    timezone:
      row.timezone === null || row.timezone === undefined
        ? null
        : String(row.timezone),
    scheduledFor: new Date(row.scheduledFor as string),
    createdAt: asUtcDate(row.createdAt),
    attempts: Number(row.attempts ?? 0),
  }));
}

/**
 * The drafted words were too old to send and a fresh answer has been asked for.
 * Terminal: the row is kept, with why, and is never selected again.
 */
export async function markScheduledReplySuperseded(id: string): Promise<void> {
  await db.execute(sql`
    UPDATE scheduled_replies
    SET status = 'superseded',
        last_error = 'draft too old to send as written; lead re-queued for a fresh answer',
        updated_at = now()
    WHERE id = ${id}
  `);
}

/** The answer went out. */
export async function markScheduledReplySent(id: string): Promise<void> {
  await db.execute(sql`
    UPDATE scheduled_replies
    SET status = 'sent', sent_at = now(), updated_at = now(), last_error = NULL
    WHERE id = ${id}
  `);
}

/**
 * The answer did not go out.
 *
 * The row stays `pending` and is retried next run until the attempt budget is
 * spent, at which point it becomes `failed` and stops being selected — with its
 * last error kept, so nobody has to guess why a buyer was never answered.
 */
export async function markScheduledReplyFailed(
  id: string,
  attempts: number,
  error: unknown,
): Promise<void> {
  const next = attempts + 1;
  const message = error instanceof Error ? error.message : String(error);
  const status = next >= MAX_SCHEDULED_REPLY_ATTEMPTS ? "failed" : "pending";

  await db.execute(sql`
    UPDATE scheduled_replies
    SET attempts = ${next},
        status = ${status},
        last_error = ${message},
        updated_at = now()
    WHERE id = ${id}
  `);
}
