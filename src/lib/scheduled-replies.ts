/**
 * The waiting room for an answer that is not due yet.
 *
 * Every message this service sends already waits for the RECIPIENT's Mon-Fri
 * 08:00-17:00 window — a sequence step through `isWithinLocalSendWindow`, an
 * Instantly-transport send through the campaign schedule it dispatches against.
 * The one-to-one answer to a prospect who wrote back did not: it went out at
 * whatever moment the caller ran, so a prospect who replied at 23:00 their time
 * could receive our answer at 23:05. That reads as a machine, and it is the one
 * message in the whole system where reading as a machine costs the most.
 *
 * ⚠️ THIS IS NOT A SECOND SCHEDULER. It owns no hours, no days, no timezone
 * rules and no defaults of its own — every one of those comes from
 * `sending-window.ts` / `sending-calendar.ts`, which is also where the sequence
 * dispatch worker gets them. This module only remembers WHAT is waiting; the
 * existing hourly worker decides when the waiting is over.
 *
 * ⚠️ A REPLY IS STILL NOT A SEQUENCE STEP. It takes no `sequence_steps` row and
 * no `sequence_costs` hold, and when it finally goes out it is recorded in
 * bronze at `MANUAL_REPLY_STEP` (0) exactly as an immediate reply was. That is
 * load-bearing: the IMAP poller correlates the prospect's NEXT answer through
 * that row, and the forwarded thread stays complete because of it. Only the
 * MOMENT of dispatch changed.
 */

import { sql } from "drizzle-orm";

import { db } from "../db";
import { scheduledReplies } from "../db/schema";
import { isSendingDay } from "./sending-calendar";
import { isWithinLocalSendWindow } from "./sending-window";

/** One answer waiting for its prospect's morning. */
export interface ScheduledReply {
  id: string;
  orgId: string;
  userId: string;
  campaignId: string;
  instantlyCampaignId: string;
  leadEmail: string;
  bodyHtml: string;
  /** The prospect's IANA timezone, or null when we hold none. */
  timezone: string | null;
  scheduledFor: Date;
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
 * Which waiting replies may go out at `asOf` — pure, so the rules are testable
 * without a database or a mail server.
 *
 * The two gates are the SAME ones `selectDueSteps` applies to sequence steps,
 * from the same modules, in the same order:
 *
 *   - `isSendingDay` — nothing goes out on a weekend, on either transport,
 *     because both run on the same mailboxes and the weekly placement test
 *     claims the Saturday slot precisely because mailboxes are empty then.
 *   - `isWithinLocalSendWindow` — the prospect's own business hours, in their
 *     own timezone, with the fleet default when we hold none.
 *
 * Oldest-due first, so a reply held over a weekend goes out before one that only
 * just came due. Ties break on id purely for determinism.
 *
 * ⚠️ Do NOT add a reply-only rule here. If replies ever need to behave
 * differently from the rest of our sending, that is a change to the window, not
 * a second window.
 */
export function selectDueScheduledReplies(
  replies: readonly ScheduledReply[],
  asOf: Date,
): ScheduledReply[] {
  if (!isSendingDay(asOf)) return [];

  return replies
    .filter((reply) => reply.scheduledFor.getTime() <= asOf.getTime())
    .filter((reply) => isWithinLocalSendWindow(asOf, reply.timezone))
    .slice()
    .sort(
      (a, b) =>
        a.scheduledFor.getTime() - b.scheduledFor.getTime() ||
        a.id.localeCompare(b.id),
    );
}

export interface EnqueueScheduledReplyInput {
  orgId: string;
  userId: string;
  campaignId: string;
  instantlyCampaignId: string;
  leadEmail: string;
  bodyHtml: string;
  timezone: string | null;
  /** The first instant the prospect's window opens. A LOWER BOUND. */
  scheduledFor: Date;
}

/**
 * Record an answer as waiting.
 *
 * Fail-loud: a reply we cannot enqueue is a reply nobody will ever send, and
 * reporting success for it would leave a buyer unanswered with no trace.
 */
export async function enqueueScheduledReply(
  input: EnqueueScheduledReplyInput,
): Promise<{ id: string; scheduledFor: Date }> {
  const [row] = await db
    .insert(scheduledReplies)
    .values({
      orgId: input.orgId,
      userId: input.userId,
      campaignId: input.campaignId,
      instantlyCampaignId: input.instantlyCampaignId,
      leadEmail: input.leadEmail,
      bodyHtml: input.bodyHtml,
      timezone: input.timezone,
      scheduledFor: input.scheduledFor,
    })
    .returning({
      id: scheduledReplies.id,
      scheduledFor: scheduledReplies.scheduledFor,
    });

  return {
    id: row.id,
    scheduledFor:
      row.scheduledFor instanceof Date
        ? row.scheduledFor
        : new Date(String(row.scheduledFor)),
  };
}

/** Everything still waiting, whatever its due date — the drain filters purely. */
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
      r.timezone              AS "timezone",
      r.scheduled_for         AS "scheduledFor",
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
    timezone:
      row.timezone === null || row.timezone === undefined
        ? null
        : String(row.timezone),
    scheduledFor: new Date(row.scheduledFor as string),
    attempts: Number(row.attempts ?? 0),
  }));
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
