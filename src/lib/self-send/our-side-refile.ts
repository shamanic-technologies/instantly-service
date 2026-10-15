/**
 * Take back the "replies" our side wrote (SENDER-NOT-PROSPECT-1015).
 *
 * Before `our-side-sender.ts`, an inbound message on a prospect's thread was
 * the prospect's reply whoever wrote it: a reply-all by the client's rep (Dev,
 * Doc Dinners, 2026-10-14) was filed as the prospect's meeting request, counted
 * in the brand's stats and celebrated to the rep himself. This re-applies the
 * SAME rule to stored `reply` rows and refiles a match as our own answer
 * (`staff_reply`), exactly like `forwarded-replies.ts` reclassifies a row:
 *
 *  - every silver event the row promoted is archived in
 *    `instantly_events_retracted` (reason `our-side-refiled:reply->staff_reply`)
 *    and deleted; `delivery_status` goes back to `sent` only when no other reply
 *    stands (`retractRowEvents`);
 *  - the bronze row and its `messages` projection become `staff_reply`: the
 *    message STAYS in the conversation, as our side's;
 *  - gold is rebuilt (`refreshLeadStatusCurrent`), so the stats read the
 *    prospect's own latest kind again;
 *  - a celebration claim the message took is given back when nothing of its
 *    kind is left, so a real meeting request from the prospect later is still
 *    announced. The email already sent cannot be unsent.
 *
 * Nothing is sent. The responder is not asked anything: a `staff_reply` after
 * the prospect's last message is a person's answer, so the takeover gate
 * refuses the responder on that thread (`human-takeover.ts`, `staff_cc`).
 * Dry run by default; names every row.
 */

import { sql } from "drizzle-orm";

import { db } from "../../db";
import { POSITIVE_QUALIFICATION_EVENT_TYPES } from "../forward-positive-reply";
import { refreshLeadStatusCurrent } from "../status-gold";
import { reprojectImapMessage } from "../messages-sync";
import { STAFF_REPLY_KIND } from "./inbound";
import { retractRowEvents } from "./forwarded-replies";
import { isOurSideSender, loadOurSide, quickSenderSide, type OurSide } from "./our-side-sender";

export interface OurSideRefileCandidate {
  rowId: string;
  instantlyCampaignId: string;
  leadEmail: string;
  fromAddress: string | null;
  receivedAt: string | null;
  /** Silver events this row promoted (what the refile takes back). */
  events: string[];
}

export interface OurSideRefileSummary {
  dryRun: boolean;
  rowsRead: number;
  refiled: OurSideRefileCandidate[];
}

function rowsOf(result: unknown): Record<string, unknown>[] {
  return (result as { rows?: Record<string, unknown>[] }).rows ?? [];
}

const MAX_SINCE_DAYS = 365;

export async function refileOurSideReplies(
  options: { dryRun?: boolean; sinceDays?: number } = {},
): Promise<OurSideRefileSummary> {
  const dryRun = options.dryRun !== false;
  const sinceDays = Math.min(Math.max(Math.floor(options.sinceDays ?? 120), 1), MAX_SINCE_DAYS);

  const rows = rowsOf(
    await db.execute(sql`
      SELECT m.id, m.instantly_campaign_id, c.lead_email, m.from_address,
             COALESCE(m.received_at, m.polled_at) AS at,
             COALESCE((SELECT array_agg(e.event_type ORDER BY e.event_type) FROM instantly_events e
                       WHERE e.source = 'self_send' AND e.source_row_id = m.id), '{}') AS events
      FROM imap_messages_raw m
      JOIN instantly_campaigns c ON c.instantly_campaign_id = m.instantly_campaign_id
      WHERE m.kind = 'reply'
        AND COALESCE(m.received_at, m.polled_at) > now() - make_interval(days => ${sinceDays})
    `),
  );

  const summary: OurSideRefileSummary = { dryRun, rowsRead: rows.length, refiled: [] };
  const ourSides = new Map<string, OurSide>();

  for (const row of rows) {
    const instantlyCampaignId = String(row.instantly_campaign_id);
    const leadEmail = String(row.lead_email);
    const fromAddress = (row.from_address as string | null) ?? null;
    if (quickSenderSide(fromAddress, leadEmail) === "prospect") continue;
    let ourSide = ourSides.get(instantlyCampaignId);
    if (!ourSide) {
      ourSide = await loadOurSide(instantlyCampaignId);
      ourSides.set(instantlyCampaignId, ourSide);
    }
    if (!isOurSideSender(fromAddress, leadEmail, ourSide)) continue;

    const at = row.at == null ? null : new Date(row.at as string | Date);
    const candidate: OurSideRefileCandidate = {
      rowId: String(row.id),
      instantlyCampaignId,
      leadEmail,
      fromAddress,
      receivedAt: at ? at.toISOString() : null,
      events: Array.isArray(row.events) ? (row.events as string[]) : [],
    };
    summary.refiled.push(candidate);
    if (dryRun) continue;

    await retractRowEvents(candidate.rowId, `our-side-refiled:reply->${STAFF_REPLY_KIND}`);
    await db.execute(sql`
      UPDATE imap_messages_raw
      SET kind = ${STAFF_REPLY_KIND},
          payload = payload || jsonb_build_object('reclassifiedFrom', 'reply'::text, 'reclassifiedAt', now(),
                                                  'reclassifiedReason', 'our_side_sender'::text)
      WHERE id = ${candidate.rowId} AND kind = 'reply'
    `);
    await reprojectImapMessage(candidate.rowId);
    if (at) await releaseClaimsTakenBy(instantlyCampaignId, at);
    await refreshLeadStatusCurrent(instantlyCampaignId, leadEmail);
    console.log(
      `[instantly-service] our-side-refile: row=${candidate.rowId} campaign=${instantlyCampaignId} lead=${leadEmail} from=${fromAddress ?? "?"} events=[${candidate.events.join(",")}] refiled ${STAFF_REPLY_KIND}`,
    );
  }
  return summary;
}

/**
 * Give back a celebration claim taken at or after the refiled message when no
 * event of its kind is left on the thread.
 */
async function releaseClaimsTakenBy(instantlyCampaignId: string, at: Date): Promise<void> {
  const positive = [...POSITIVE_QUALIFICATION_EVENT_TYPES];
  await db.execute(sql`
    UPDATE instantly_campaigns
    SET meeting_request_celebrated_at = NULL, updated_at = now()
    WHERE instantly_campaign_id = ${instantlyCampaignId}
      AND meeting_request_celebrated_at >= ${at.toISOString()}::timestamp
      AND NOT EXISTS (SELECT 1 FROM instantly_events e
                      WHERE e.campaign_id = ${instantlyCampaignId}
                        AND e.event_type = 'lead_meeting_requested' AND e.withdrawn_at IS NULL)
  `);
  await db.execute(sql`
    UPDATE instantly_campaigns
    SET positive_reply_forwarded_at = NULL, updated_at = now()
    WHERE instantly_campaign_id = ${instantlyCampaignId}
      AND positive_reply_forwarded_at >= ${at.toISOString()}::timestamp
      AND NOT EXISTS (SELECT 1 FROM instantly_events e
                      WHERE e.campaign_id = ${instantlyCampaignId}
                        AND e.event_type = ANY(${sql.param(positive)}::text[]) AND e.withdrawn_at IS NULL)
  `);
}
