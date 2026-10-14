// FOLLOW-UP sends (NEVER-NO-FOLLOWUP-1014, owner 2026-10-14: « on ne se retrouve
// jamais dans une situation de non-followup, sauf unsubscribe, unqualified,
// opt-out, ou réponse positive »).
//
// A caller states on `POST /orgs/send` that the email CONTINUES the brand's own
// earlier outreach to this person (`followUp: true`). The per-brand re-contact
// window (recontact-window.ts) exists so two COLD campaigns of one brand never
// both pitch the same person; a follow-up of our own thread is not a second
// pitch, so the window does not apply to it. Every other gate still does:
// opt-out (org-wide), not-a-prospect, the verifier, credits.
//
// Two facts this module reads, both from OUR send evidence:
// - is a step of this brand's sequence to the person still QUEUED? Then a
//   follow-up would race it (two of our emails in a few days): refused with its
//   own code, `brand_sequence_queued`. The SAME send retried (same run) is not a
//   race and is left to the reservation, which answers it `200 duplicate`.
// - which thread does it continue? The brand's latest sequence with a REAL
//   `email_sent` to the person. The follow-up lands in THAT thread (same
//   mailbox, `Re:` its subject, In-Reply-To/References its messages) when that
//   mailbox can still send; otherwise it starts a new thread from the mailbox
//   the fill order picks. Generic: no branch on a tag, a workflow or a brand.

import { sql } from "drizzle-orm";
import { db } from "../db";
import { normalizeLeadEmail } from "./recontact-window";
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
 * The brand's latest sequence to this person with a REAL email sent (inferred
 * rows assert a send nobody witnessed). Null = we never wrote to them for this
 * brand in this org: the follow-up simply starts a thread. Fail loud.
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
             WHERE s.instantly_campaign_id = c.instantly_campaign_id AND s.step = 1) AS "subject"
    FROM instantly_campaigns c
    JOIN LATERAL (
      SELECT MAX(e.timestamp) AS last_sent
      FROM instantly_events e
      WHERE e.campaign_id = c.instantly_campaign_id
        AND e.event_type = 'email_sent'
        AND e.inferred = false
    ) sent ON sent.last_sent IS NOT NULL
    WHERE c.org_id = ${orgId}
      AND lower(c.lead_email) = ${email}
      AND c.brand_ids && ${sql.param(brandIds)}::text[]
    ORDER BY sent.last_sent DESC
    LIMIT 1
  `);
  const [row] = rowsOf<FollowUpThread>(result);
  if (!row) return null;
  return {
    instantlyCampaignId: row.instantlyCampaignId,
    accountEmail: row.accountEmail ?? null,
    subject: typeof row.subject === "string" && row.subject.trim() !== "" ? row.subject : null,
  };
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

/** The 409 body: explicit, distinct from `recent_brand_contact`. */
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
