/**
 * Which campaign a reply goes into when the caller names only the prospect.
 *
 * A sales rep (a person, or the client's own agent through the MCP) holds "reply
 * to jane@acme.com: <words>", not a campaign id. The thread is ours, so finding
 * it is ours too: the gateway stays a passthrough.
 *
 * A THREAD is a campaign of this org in which the prospect WROTE BACK (a real
 * `reply_received` / `auto_reply_received` event). A campaign they never answered
 * holds nothing to reply into, and counting it would make a person who was cold
 * emailed twice but answered once read as ambiguous.
 *
 *  - exactly one thread: reply in it;
 *  - several: refuse (`ambiguous_thread`) and list them, newest message first,
 *    so the caller can name one;
 *  - none, but we did email them: `no_reply_to_thread`, the same refusal the
 *    campaign-named call gives;
 *  - never emailed by this org: `campaign_not_found`.
 *
 * Naming the campaign skips all of this and behaves exactly as before.
 */
import { sql } from "drizzle-orm";

import { db } from "../db";
import { ReplyToLeadError } from "./reply-to-lead";

export interface ReplyThreadCandidate {
  campaignId: string;
  /** Newest email in that thread, either direction. Null if none recorded. */
  lastMessageAt: string | null;
  /** True when the prospect wrote back in this campaign. */
  replied: boolean;
}

/** Refused because the address alone does not pick ONE thread. */
export class AmbiguousReplyThreadError extends ReplyToLeadError {
  constructor(
    email: string,
    public readonly candidates: ReplyThreadCandidate[],
  ) {
    super(
      "ambiguous_thread",
      409,
      `${email} has ${candidates.length} threads in this org; name one with campaign_id`,
    );
  }
}

/** Pure: pick the one thread, or refuse with a reason the caller can act on. */
export function pickReplyThread(
  email: string,
  candidates: ReplyThreadCandidate[],
): string {
  if (candidates.length === 0) {
    throw new ReplyToLeadError(
      "campaign_not_found",
      404,
      `No campaign in this org for ${email}`,
    );
  }
  const threads = candidates.filter((c) => c.replied);
  if (threads.length === 0) {
    throw new ReplyToLeadError(
      "no_reply_to_thread",
      409,
      `${email} never wrote back, so there is no thread to reply into`,
    );
  }
  if (threads.length > 1) throw new AmbiguousReplyThreadError(email, threads);
  return threads[0].campaignId;
}

/**
 * Every campaign of this org that holds the address, newest message first.
 *
 * Same address normalization as every other lead lookup here (`lower(...)`).
 * Platform sends (no campaign id) and reservation sentinels are not addressable
 * by the reply route, so they are not candidates.
 */
export async function loadReplyThreadCandidates(
  orgId: string,
  email: string,
): Promise<ReplyThreadCandidate[]> {
  const result = await db.execute(sql`
    SELECT
      c.campaign_id AS "campaignId",
      MAX(e."timestamp") FILTER (
        WHERE e.event_type IN ('email_sent', 'reply_received', 'auto_reply_received')
      ) AS "lastMessageAt",
      BOOL_OR(e.event_type IN ('reply_received', 'auto_reply_received')) AS "replied"
    FROM instantly_campaigns c
    LEFT JOIN instantly_events e
      ON e.campaign_id = c.instantly_campaign_id
     AND lower(e.lead_email) = lower(c.lead_email)
     AND e.inferred = false
    WHERE c.org_id = ${orgId}
      AND lower(c.lead_email) = lower(${email})
      AND c.campaign_id IS NOT NULL
      AND c.instantly_campaign_id NOT LIKE 'reserving:%'
    GROUP BY c.campaign_id
    ORDER BY "lastMessageAt" DESC NULLS LAST, c.campaign_id ASC
  `);
  return (result.rows as Record<string, unknown>[]).map((row) => ({
    campaignId: String(row.campaignId),
    lastMessageAt:
      row.lastMessageAt === null || row.lastMessageAt === undefined
        ? null
        : row.lastMessageAt instanceof Date
          ? row.lastMessageAt.toISOString()
          : new Date(String(row.lastMessageAt)).toISOString(),
    replied: row.replied === true,
  }));
}

/** The campaign to reply into: the named one, else the prospect's one thread. */
export async function resolveReplyCampaignId(
  orgId: string,
  email: string,
  campaignId: string | undefined,
): Promise<string> {
  if (campaignId) return campaignId;
  return pickReplyThread(email, await loadReplyThreadCandidates(orgId, email));
}
