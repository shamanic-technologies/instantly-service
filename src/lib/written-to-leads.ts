/**
 * Every lead we have actually written to — `GET /orgs/written-to-leads`.
 *
 * `GET /orgs/engaged-leads` lists only the leads who replied or clicked, by
 * design. A Unibox wants the whole outbox: a person we wrote to three times and
 * who never answered is still a conversation (ours), and the owner expects to
 * see it (2026-10-09). This read is the superset — one row per (sequence, lead)
 * with at least one REAL `email_sent` — carrying the same engagement signals,
 * so a written-to-only person and a person who replied are both listable from
 * one call, and `engaged` says which of them `engaged-leads` would also list.
 *
 * ⚠️ READ FROM GOLD (`instantly_lead_status_current`), like engaged-leads.
 * `sent` there is `BOOL_OR(email_sent)` over real silver events, and
 * `last_delivered_at` is `MAX(email_sent.timestamp)` — despite its name it is
 * the last SEND, bounced or not (see `status-gold.ts`). One projection, one
 * answer: this count is the brand's `sent` count on `POST /orgs/status`.
 *
 * ⚠️ PAGED, unlike engaged-leads. The largest brand holds ~20k written-to rows;
 * the population is not small by construction, so a caller walks it with a
 * keyset cursor on the gold primary key `(instantly_campaign_id, lead_email)`.
 * Keyset, not OFFSET: a row promoted mid-walk cannot shift a page boundary and
 * duplicate or skip a lead.
 *
 * Declares no cost and sends nothing.
 */

import { sql } from "drizzle-orm";

import { db } from "../db";
import {
  ENGAGEMENT_PREDICATE_SQL,
  disqualifiedByKind,
  isoOrNull,
  rowsOf,
} from "./engaged-leads";

export const WRITTEN_TO_DEFAULT_LIMIT = 1000;
export const WRITTEN_TO_MAX_LIMIT = 5000;

export interface WrittenToLead {
  campaignId: string | null;
  instantlyCampaignId: string;
  leadEmail: string;
  brandIds: string[];
  /** Our first real send in this sequence. */
  firstSentAt: string;
  /** Our latest real send in this sequence (bounced or not). */
  lastSentAt: string;
  /** True iff `GET /orgs/engaged-leads` lists this row too. */
  engaged: boolean;
  replied: boolean;
  clicked: boolean;
  unsubscribed: boolean;
  bounced: boolean;
  firstRepliedAt: string | null;
  firstClickedAt: string | null;
  replyClassification: string | null;
  replyKind: string | null;
  disqualified: boolean;
}

export interface WrittenToLeadsFilters {
  orgId: string;
  brandId?: string;
  campaignId?: string;
  limit?: number;
  cursor?: string;
}

export interface WrittenToLeadsPage {
  leads: WrittenToLead[];
  /** Pass back as `cursor` for the next page. Null on the last page. */
  nextCursor: string | null;
}

interface CursorKey {
  instantlyCampaignId: string;
  leadEmail: string;
}

export function encodeCursor(key: CursorKey): string {
  return Buffer.from(
    JSON.stringify([key.instantlyCampaignId, key.leadEmail]),
    "utf8",
  ).toString("base64url");
}

/** Throws on a cursor this service did not mint — the route answers 400. */
export function decodeCursor(cursor: string): CursorKey {
  let parsed: unknown;
  try {
    parsed = JSON.parse(Buffer.from(cursor, "base64url").toString("utf8"));
  } catch {
    throw new InvalidCursorError(cursor);
  }
  if (
    !Array.isArray(parsed) ||
    parsed.length !== 2 ||
    typeof parsed[0] !== "string" ||
    typeof parsed[1] !== "string"
  ) {
    throw new InvalidCursorError(cursor);
  }
  return { instantlyCampaignId: parsed[0], leadEmail: parsed[1] };
}

export class InvalidCursorError extends Error {
  constructor(cursor: string) {
    super(`invalid cursor: ${cursor}`);
    this.name = "InvalidCursorError";
  }
}

interface GoldRow {
  campaignId: string | null;
  instantlyCampaignId: string;
  leadEmail: string;
  brandIds: unknown;
  firstSentAt: unknown;
  lastSentAt: unknown;
  engaged: boolean;
  replied: boolean;
  clicked: boolean;
  unsubscribed: boolean;
  bounced: boolean;
  firstRepliedAt: unknown;
  firstClickedAt: unknown;
  replyClassification: string | null;
  replyKind: string | null;
}

export function toWrittenToLead(row: GoldRow): WrittenToLead {
  const firstSentAt = isoOrNull(row.firstSentAt);
  const lastSentAt = isoOrNull(row.lastSentAt);
  if (firstSentAt === null || lastSentAt === null) {
    // Unreachable through the query (`sent` implies both). Fail loud rather
    // than list a "written-to" lead with no send instant.
    throw new Error(
      `[instantly-service] written-to lead ${row.leadEmail} on ${row.instantlyCampaignId} has no send timestamp`,
    );
  }
  return {
    campaignId: row.campaignId,
    instantlyCampaignId: row.instantlyCampaignId,
    leadEmail: row.leadEmail,
    brandIds: Array.isArray(row.brandIds) ? row.brandIds.map(String) : [],
    firstSentAt,
    lastSentAt,
    engaged: row.engaged === true,
    replied: row.replied === true,
    clicked: row.clicked === true,
    unsubscribed: row.unsubscribed === true,
    bounced: row.bounced === true,
    firstRepliedAt: isoOrNull(row.firstRepliedAt),
    firstClickedAt: isoOrNull(row.firstClickedAt),
    replyClassification: row.replyClassification,
    replyKind: row.replyKind,
    disqualified: disqualifiedByKind(row.replyKind),
  };
}

export async function fetchWrittenToLeads(
  filters: WrittenToLeadsFilters,
): Promise<WrittenToLeadsPage> {
  const limit = filters.limit ?? WRITTEN_TO_DEFAULT_LIMIT;
  const conditions = [sql`org_id = ${filters.orgId}`, sql`sent`];

  if (filters.brandId !== undefined) {
    conditions.push(sql`${filters.brandId} = ANY(brand_ids)`);
  }
  if (filters.campaignId !== undefined) {
    conditions.push(sql`campaign_id = ${filters.campaignId}`);
  }
  if (filters.cursor !== undefined) {
    const after = decodeCursor(filters.cursor);
    conditions.push(
      sql`(instantly_campaign_id, lead_email) > (${after.instantlyCampaignId}, ${after.leadEmail})`,
    );
  }

  const where = sql.join(conditions, sql` AND `);

  // One extra row tells whether another page exists without a COUNT.
  const result = await db.execute(sql`
    SELECT campaign_id            AS "campaignId",
           instantly_campaign_id  AS "instantlyCampaignId",
           lead_email             AS "leadEmail",
           brand_ids              AS "brandIds",
           first_sent_at          AS "firstSentAt",
           last_delivered_at      AS "lastSentAt",
           ${sql.raw(ENGAGEMENT_PREDICATE_SQL)} AS engaged,
           replied,
           clicked,
           unsubscribed,
           bounced,
           first_replied_at       AS "firstRepliedAt",
           first_clicked_at       AS "firstClickedAt",
           reply_classification   AS "replyClassification",
           reply_kind             AS "replyKind"
    FROM instantly_lead_status_current
    WHERE ${where}
    ORDER BY instantly_campaign_id ASC, lead_email ASC
    LIMIT ${limit + 1}
  `);

  const rows = rowsOf(result) as unknown as GoldRow[];
  const page = rows.slice(0, limit).map(toWrittenToLead);
  const last = page[page.length - 1];
  return {
    leads: page,
    nextCursor:
      rows.length > limit && last !== undefined
        ? encodeCursor({
            instantlyCampaignId: last.instantlyCampaignId,
            leadEmail: last.leadEmail,
          })
        : null,
  };
}
