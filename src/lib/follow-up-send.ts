// FOLLOW-UP sends (NEVER-NO-FOLLOWUP-1014, owner 2026-10-14: « on ne se retrouve
// jamais dans une situation de non-followup, sauf unsubscribe, unqualified,
// opt-out, ou réponse positive »).
//
// A caller states on `POST /orgs/send` that the email CONTINUES the brand's own
// earlier outreach to this person (`followUp: true`). Every send gate applies to
// it: opt-out (org-wide), not-a-prospect, the verifier, credits. (There is no
// re-contact window at send time any more: RECONTACT-OFF-1015, it is the serve
// tactic "Fresh contacts only".)
//
// Two facts this module reads, both from OUR send evidence:
// - is a step of this brand's sequence to the person still QUEUED? Then a
//   follow-up would race it (two of our emails in a few days): refused with its
//   own code, `brand_sequence_queued`. The SAME send retried (same run) is not a
//   race and is left to the reservation, which answers it `200 duplicate`.
// - which thread does it continue? The thread holding the person's LATEST
//   message to the brand (their last real reply), from the mailbox they wrote
//   to (RIGHT-MOMENT-OR-LATER-1014, owner 2026-10-14: the follow-up is a reply
//   in the thread they wrote in last). Only when they never wrote back: the
//   brand's latest sequence with a REAL `email_sent` to them. The follow-up
//   lands in THAT thread (same mailbox, `Re:` its subject, In-Reply-To/
//   References its messages) when that mailbox can still send; otherwise it
//   starts a new thread from the mailbox the fill order picks. Generic: no
//   branch on a tag, a workflow or a brand.

import { sql } from "drizzle-orm";
import { db } from "../db";
import { normalizeLeadEmail } from "./lead-email";
import { subjectForStep } from "./self-send/message";

export const FOLLOW_UP_QUEUED_REFUSAL_CODE = "brand_sequence_queued";

export interface QueuedBrandSequence {
  instantlyCampaignId: string;
  brandId: string;
}

export interface FollowUpThread {
  /** The sequence this follow-up continues. */
  instantlyCampaignId: string;
  /** The mailbox that wrote to the person in it. */
  accountEmail: string | null;
  /** Its first email's subject (null = we never stored one: no thread). */
  subject: string | null;
  /**
   * Why this thread: `their_last_reply` = the person wrote in it last;
   * `our_last_email` = they never wrote back, it is where we wrote last.
   */
  reason: FollowUpThreadReason;
}

export type FollowUpThreadReason = "their_last_reply" | "our_last_email";

/** One sequence of the brand to the person with a real email sent. */
export interface FollowUpThreadCandidate {
  instantlyCampaignId: string;
  /** The mailbox that sent the sequence. */
  accountEmail: string | null;
  subject: string | null;
  lastSentAt: Date;
  /** The person's latest real reply in it (null = they never wrote in it). */
  lastReplyAt: Date | null;
  /** The mailbox that reply reached (null = unknown: the sequence's own). */
  replyAccountEmail: string | null;
}

function rowsOf<T>(result: unknown): T[] {
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as T[]) : [];
}

/**
 * A sequence of this org + brand to the person that still has a step waiting
 * to leave (`sequence_costs` provisioned on an active row: the send queue's own
 * definition). `runId` = this send's run, excluded: a retry of the same send is
 * a duplicate, not a race. Fail loud.
 */
export async function findQueuedBrandSequence(
  orgId: string,
  leadEmail: string,
  brandIds: string[],
  runId: string | null,
): Promise<QueuedBrandSequence | null> {
  const email = normalizeLeadEmail(leadEmail);
  if (!email || brandIds.length === 0) return null;
  const result = await db.execute(sql`
    SELECT c.instantly_campaign_id AS "instantlyCampaignId",
           (SELECT b FROM unnest(c.brand_ids) b WHERE b = ANY(${sql.param(brandIds)}::text[]) LIMIT 1) AS "brandId"
    FROM instantly_campaigns c
    WHERE c.org_id = ${orgId}
      AND lower(c.lead_email) = ${email}
      AND c.brand_ids && ${sql.param(brandIds)}::text[]
      AND c.status = 'active'
      AND c.instantly_campaign_id NOT LIKE 'reserving:%'
      AND c.run_id IS DISTINCT FROM ${runId}
      AND EXISTS (
        SELECT 1 FROM sequence_costs sc
        WHERE sc.instantly_campaign_id = c.instantly_campaign_id
          AND sc.status = 'provisioned'
      )
    ORDER BY c.created_at DESC
    LIMIT 1
  `);
  const [row] = rowsOf<{ instantlyCampaignId: string; brandId: string }>(result);
  return row ?? null;
}

/**
 * Pure: the thread a follow-up continues. The one holding the person's latest
 * reply wins, from the mailbox that reply reached; only when they never wrote
 * back, the sequence where we wrote last. Null = no candidate.
 */
export function pickFollowUpThread(
  candidates: FollowUpThreadCandidate[],
): FollowUpThread | null {
  const time = (d: Date | null) => (d ? d.getTime() : Number.NEGATIVE_INFINITY);
  const replied = candidates.filter((c) => c.lastReplyAt !== null);
  const pick =
    replied.length > 0
      ? replied.reduce((a, b) => (time(b.lastReplyAt) > time(a.lastReplyAt) ? b : a))
      : candidates.length > 0
        ? candidates.reduce((a, b) => (time(b.lastSentAt) > time(a.lastSentAt) ? b : a))
        : null;
  if (!pick) return null;
  const subject =
    typeof pick.subject === "string" && pick.subject.trim() !== "" ? pick.subject : null;
  return replied.length > 0
    ? {
        instantlyCampaignId: pick.instantlyCampaignId,
        accountEmail: pick.replyAccountEmail ?? pick.accountEmail ?? null,
        subject,
        reason: "their_last_reply",
      }
    : {
        instantlyCampaignId: pick.instantlyCampaignId,
        accountEmail: pick.accountEmail ?? null,
        subject,
        reason: "our_last_email",
      };
}

function toDate(v: unknown): Date | null {
  if (v === null || v === undefined) return null;
  const d = v instanceof Date ? v : new Date(String(v));
  return Number.isNaN(d.getTime()) ? null : d;
}

/**
 * Every sequence of this org + brand to the person with a REAL email sent
 * (inferred rows assert a send nobody witnessed), with the person's latest
 * real reply in it: a `reply_received` we witnessed (not inferred, not a
 * staff statement `source='manual'`, whose time is when staff spoke). An
 * autoreply is not the person writing. Then `pickFollowUpThread`. Null = we
 * never wrote to them for this brand in this org: the follow-up simply starts
 * a thread. Fail loud.
 */
export async function findFollowUpThread(
  orgId: string,
  leadEmail: string,
  brandIds: string[],
): Promise<FollowUpThread | null> {
  const email = normalizeLeadEmail(leadEmail);
  if (!email || brandIds.length === 0) return null;
  const result = await db.execute(sql`
    SELECT c.instantly_campaign_id AS "instantlyCampaignId",
           c.account_email AS "accountEmail",
           (SELECT s.subject FROM sequence_steps s
             WHERE s.instantly_campaign_id = c.instantly_campaign_id AND s.step = 1) AS "subject",
           sent.last_sent AS "lastSentAt",
           reply.last_reply AS "lastReplyAt",
           reply.account_email AS "replyAccountEmail"
    FROM instantly_campaigns c
    JOIN LATERAL (
      SELECT MAX(e.timestamp) AS last_sent
      FROM instantly_events e
      WHERE e.campaign_id = c.instantly_campaign_id
        AND e.event_type = 'email_sent'
        AND e.inferred = false
    ) sent ON sent.last_sent IS NOT NULL
    LEFT JOIN LATERAL (
      SELECT e.timestamp AS last_reply, e.account_email
      FROM instantly_events e
      WHERE e.campaign_id = c.instantly_campaign_id
        AND e.event_type = 'reply_received'
        AND e.inferred = false
        AND e.source <> 'manual'
      ORDER BY e.timestamp DESC
      LIMIT 1
    ) reply ON true
    WHERE c.org_id = ${orgId}
      AND lower(c.lead_email) = ${email}
      AND c.brand_ids && ${sql.param(brandIds)}::text[]
  `);
  const candidates = rowsOf<Record<string, unknown>>(result).flatMap((row) => {
    const lastSentAt = toDate(row.lastSentAt);
    if (!lastSentAt) return [];
    return [
      {
        instantlyCampaignId: String(row.instantlyCampaignId),
        accountEmail: typeof row.accountEmail === "string" ? row.accountEmail : null,
        subject: typeof row.subject === "string" ? row.subject : null,
        lastSentAt,
        lastReplyAt: toDate(row.lastReplyAt),
        replyAccountEmail:
          typeof row.replyAccountEmail === "string" && row.replyAccountEmail.trim() !== ""
            ? row.replyAccountEmail
            : null,
      },
    ];
  });
  return pickFollowUpThread(candidates);
}

/**
 * Pure: does the follow-up land in `thread`? Only from the SAME mailbox (a reply
 * in a thread from another sender reads as a stranger) and only when we know
 * the thread's subject. Returns the subject every step of the follow-up carries
 * (`Re: <thread subject>`), or null = new thread with the caller's own subject.
 */
export function threadSubjectFor(
  thread: FollowUpThread | null,
  chosenAccountEmail: string,
): string | null {
  if (!thread || !thread.subject || !thread.accountEmail) return null;
  if (thread.accountEmail.trim().toLowerCase() !== chosenAccountEmail.trim().toLowerCase()) {
    return null;
  }
  return subjectForStep(thread.subject, 2);
}

/** The 409 body: explicit, distinct from every other refusal of the route. */
export function followUpQueuedRefusal(leadEmail: string, queued: QueuedBrandSequence) {
  return {
    error: "A sequence of this brand is still queued for this person",
    code: FOLLOW_UP_QUEUED_REFUSAL_CODE,
    details:
      `${leadEmail} still has an email of brand ${queued.brandId} waiting to leave; ` +
      `a follow-up now would race it. No email was sent and nothing was billed.`,
    brandId: queued.brandId,
  };
}
