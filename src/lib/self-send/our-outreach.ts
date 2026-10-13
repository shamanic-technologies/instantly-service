/**
 * What WE sent a prospect, as text, for the reply classifier to read beside
 * their reply.
 *
 * Why it exists (prod 2026-10-13, DefiniteJobs): our email "Hiring at Carter &
 * Co this month? ... DefiniteJobs puts UK vacancies in front of local job
 * seekers. Would a free first listing help?" got "yes we are hiring two
 * warehouse operatives, a free first listing sounds good, can we have a quick
 * call?". The classifier saw only the subject and the reply, read "hiring" as a
 * job enquiry, and called a buyer `lead_off_topic`. "Off topic" means "not about
 * buying what we sell", and what we sell is only knowable from our email.
 *
 * Read from `sequence_steps`, where EVERY email we send ourselves lives,
 * whoever wrote it: an LLM-generated sequence and a body the client pushed
 * through the API are stored the same way. Only the steps already sent are
 * read (up to the latest real `email_sent`, at least step 1): an unsent
 * follow-up is not something they are answering. A thread with no stored step
 * (an Instantly-held sequence) returns null and the classifier judges as
 * before. A DB error throws: a classification without context it should have
 * had is the bug this module exists to fix.
 */

import { sql } from "drizzle-orm";

import { db } from "../../db";
import { htmlToText } from "../forward-positive-reply";

/** A reply's verdict needs our offer, not our whole sequence verbatim. */
export const OUR_OUTREACH_MAX_CHARS = 3000;

function rowsOf(result: unknown): Record<string, unknown>[] {
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

/** Pure: the steps, oldest first, as one text block, capped. Null when empty. */
export function renderOurOutreach(
  steps: { subject: string | null; bodyHtml: string }[],
): string | null {
  const parts = steps
    .map((s) => htmlToText(s.bodyHtml).trim())
    .filter((text) => text.length > 0);
  if (parts.length === 0) return null;
  return parts.join("\n\n---\n\n").slice(0, OUR_OUTREACH_MAX_CHARS);
}

export async function loadOurOutreach(instantlyCampaignId: string): Promise<string | null> {
  const result = await db.execute(sql`
    SELECT s.subject, s.body_html
      FROM sequence_steps s
     WHERE s.instantly_campaign_id = ${instantlyCampaignId}
       AND s.step <= GREATEST(1, COALESCE((
             SELECT MAX(e.step) FROM instantly_events e
              WHERE e.campaign_id = ${instantlyCampaignId}
                AND e.event_type = 'email_sent'
                AND e.inferred = false
           ), 1))
     ORDER BY s.step
  `);
  return renderOurOutreach(
    rowsOf(result).map((r) => ({
      subject: (r.subject as string | null) ?? null,
      bodyHtml: String(r.body_html ?? ""),
    })),
  );
}
