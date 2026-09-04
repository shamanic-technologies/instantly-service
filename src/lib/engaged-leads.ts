/**
 * The leads worth opening a conversation panel on.
 *
 * `GET /orgs/conversations` can already return what a prospect wrote, but it
 * answers about ONE (campaign, lead) pair the caller must already know. A
 * dashboard holds a lead, not a campaign id, and has no way to tell which of an
 * org's tens of thousands of contacted leads ever showed interest — so the
 * conversation read was reachable only by someone who already knew the answer.
 * This is the discovery half: the list, from which each row's thread is one
 * further call on the identity this returns.
 *
 * ⚠️ THE GATE LIVES HERE, NOT IN THE CONVERSATION READ. It is tempting to make
 * `/orgs/conversations` refuse a lead that has not engaged, so a single call
 * enforces the display policy. That would make an endpoint lie about data it
 * holds: the thread with a silent prospect exists and is perfectly readable,
 * and a worker drafting a follow-up legitimately wants it. A list is the right
 * home for "which of these deserve a human's attention"; the read stays pure.
 *
 * ⚠️ EVERY SIGNAL IS READ FROM GOLD (`instantly_lead_status_current`), NOT
 * RE-DERIVED FROM THE EVENT LOG. That row is already the projection this
 * service maintains — it carries the manual-statement precedence, the withdrawn
 * -statement skip and the reply-kind resolution, all of which a fresh aggregate
 * over `instantly_events` would have to re-implement and would eventually
 * re-implement differently. One projection, one answer.
 *
 * Declares no cost and sends nothing.
 */

import { sql } from "drizzle-orm";

import { db } from "../db";
import { isDisqualifyingReplyKind, isReplyKind } from "./reply-kind";

/**
 * What counts as interest.
 *
 * `replied AND NOT unsubscribed` — any human reply, whatever its sentiment. A
 * negative reply is still a conversation someone should be able to read, and a
 * "not right now" is recyclable pipeline. An unsubscribe request is the one
 * answer that is explicitly a request to stop, so it is excluded rather than
 * surfaced as an opportunity.
 *
 * Autoresponders need no exclusion clause: `auto_reply_received` and
 * `lead_out_of_office` are distinct event types that never set `replied`, so an
 * out-of-office cannot reach this list. That is a property of the silver
 * vocabulary, not something this gate enforces — see `reply-kind.ts`.
 *
 * `clicked` — the prospect followed a link we sent, which on both transports
 * means they landed on the page. ⚠️ This is the ONLY visit signal this service
 * has: a click on our own tracked redirect. It is NOT a general website visit,
 * and an anonymous visit that never went through one of our links is invisible
 * here by construction. Do not relabel this field as "visited the site".
 */
export const ENGAGEMENT_PREDICATE_SQL =
  "((replied AND NOT unsubscribed) OR clicked)";

/** One engaged lead, with the identity `GET /orgs/conversations` takes. */
export interface EngagedLead {
  /** The caller's own campaign id. Null on a platform send, which has none. */
  campaignId: string | null;
  /** This service's per-lead sequence id — stable even when campaignId is null. */
  instantlyCampaignId: string;
  leadEmail: string;
  brandIds: string[];
  /**
   * When this lead FIRST showed interest — the earlier of their first reply and
   * their first click.
   *
   * ⚠️ Deliberately NOT paired with a single `engagementKind` scalar. A lead who
   * clicked on Monday and replied on Friday has one engagement start and two
   * signals; collapsing that into one enum forces a choice between a timestamp
   * that disagrees with its own label and a label that disagrees with the
   * timestamp. The two booleans below say which signals exist, and the two
   * timestamps say when each happened. Nothing can contradict anything.
   */
  engagedAt: string;
  replied: boolean;
  clicked: boolean;
  firstRepliedAt: string | null;
  firstClickedAt: string | null;
  /** Coarse sentiment: positive | negative | neutral. Null when unqualified. */
  replyClassification: string | null;
  /** The finer reading of the same statement. Null when no reply kind is on record. */
  replyKind: string | null;
  /** True only for a kind that is permanent about the PERSON (wrong person, changed job). */
  disqualified: boolean;
}

export interface EngagedLeadsFilters {
  orgId: string;
  brandId?: string;
  campaignId?: string;
  /** Only leads whose engagement STARTED at or after this instant. */
  since?: string;
  limit?: number;
}

/** A row as gold returns it, before it is a domain object. */
interface GoldRow {
  campaignId: string | null;
  instantlyCampaignId: string;
  leadEmail: string;
  brandIds: unknown;
  engagedAt: unknown;
  replied: boolean;
  clicked: boolean;
  firstRepliedAt: unknown;
  firstClickedAt: unknown;
  replyClassification: string | null;
  replyKind: string | null;
}

/** node-postgres resolves `db.execute` to a QueryResult OBJECT, never an array. */
function rowsOf(result: unknown): Record<string, unknown>[] {
  if (Array.isArray(result)) return result as Record<string, unknown>[];
  const rows = (result as { rows?: unknown })?.rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

/**
 * Normalise a timestamp to ISO 8601 UTC, matching what `POST /orgs/status`
 * emits for its own `first*At` fields — a consumer reading both surfaces must
 * not get two formats for the same instant.
 *
 * ⚠️ node-postgres hands a `timestamp` (no time zone) column back as a NAIVE
 * string like `2026-09-03 13:21:37.397`, and `new Date()` reads that as LOCAL
 * time. The columns store UTC, so a consumer in any non-UTC zone would shift
 * the instant by its own offset. The `Z` is appended explicitly rather than
 * left to the runtime's zone: this service's container happens to run UTC, but
 * that is incidental and not something the output format should depend on.
 */
function isoOrNull(value: unknown): string | null {
  if (value == null) return null;
  if (value instanceof Date) return value.toISOString();

  const raw = String(value);
  const naive = /^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?$/.test(raw);
  const parsed = new Date(naive ? `${raw.replace(" ", "T")}Z` : raw);
  if (Number.isNaN(parsed.getTime())) {
    throw new Error(
      `[instantly-service] engaged-leads: unparseable timestamp ${raw}`,
    );
  }
  return parsed.toISOString();
}

/**
 * Pure mapper — gold row to domain object.
 *
 * `disqualified` is derived STRICTLY from `replyKind` via the shared
 * `isDisqualifyingReplyKind`, never computed independently here. A second
 * definition of "disqualified" is how two surfaces start disagreeing about the
 * same person, and `POST /orgs/status` already answers this question.
 */
export function toEngagedLead(row: GoldRow): EngagedLead {
  const replyKind = row.replyKind;
  const engagedAt = isoOrNull(row.engagedAt);
  if (engagedAt === null) {
    // Unreachable through the query, which requires at least one of the two
    // timestamps. Fail loud rather than emit a lead with no engagement instant.
    throw new Error(
      `[instantly-service] engaged lead ${row.leadEmail} on ${row.instantlyCampaignId} has no engagement timestamp`,
    );
  }

  return {
    campaignId: row.campaignId,
    instantlyCampaignId: row.instantlyCampaignId,
    leadEmail: row.leadEmail,
    brandIds: Array.isArray(row.brandIds) ? row.brandIds.map(String) : [],
    engagedAt,
    replied: row.replied === true,
    clicked: row.clicked === true,
    firstRepliedAt: isoOrNull(row.firstRepliedAt),
    firstClickedAt: isoOrNull(row.firstClickedAt),
    replyClassification: row.replyClassification,
    replyKind,
    disqualified:
      replyKind !== null && isReplyKind(replyKind)
        ? isDisqualifyingReplyKind(replyKind)
        : false,
  };
}

/**
 * The org's engaged leads, most recently engaged first.
 *
 * One flat read of gold — no aggregation over the event log, no per-lead
 * fan-out. The population is small by construction (a few hundred per org at
 * the largest, against tens of thousands of contacted leads), which is why
 * there is no pagination: a caller asking for an org's engaged leads wants all
 * of them, and a silent `limit` default would hide the tail. `limit` exists for
 * a caller that explicitly wants a preview.
 */
export async function fetchEngagedLeads(
  filters: EngagedLeadsFilters,
): Promise<EngagedLead[]> {
  const conditions = [sql`org_id = ${filters.orgId}`];

  if (filters.brandId !== undefined) {
    conditions.push(sql`${filters.brandId} = ANY(brand_ids)`);
  }
  if (filters.campaignId !== undefined) {
    conditions.push(sql`campaign_id = ${filters.campaignId}`);
  }
  if (filters.since !== undefined) {
    conditions.push(
      sql`LEAST(
            COALESCE(first_replied_at, first_clicked_at),
            COALESCE(first_clicked_at, first_replied_at)
          ) >= ${filters.since}::timestamp`,
    );
  }

  const where = sql.join(conditions, sql` AND `);
  const limit =
    filters.limit === undefined ? sql`` : sql` LIMIT ${filters.limit}`;

  const result = await db.execute(sql`
    SELECT campaign_id            AS "campaignId",
           instantly_campaign_id  AS "instantlyCampaignId",
           lead_email             AS "leadEmail",
           brand_ids              AS "brandIds",
           LEAST(
             COALESCE(first_replied_at, first_clicked_at),
             COALESCE(first_clicked_at, first_replied_at)
           )                      AS "engagedAt",
           replied,
           clicked,
           first_replied_at       AS "firstRepliedAt",
           first_clicked_at       AS "firstClickedAt",
           reply_classification   AS "replyClassification",
           reply_kind             AS "replyKind"
    FROM instantly_lead_status_current
    WHERE ${where}
      AND ((replied AND NOT unsubscribed) OR clicked)
    ORDER BY "engagedAt" DESC, lead_email ASC${limit}
  `);

  return rowsOf(result).map((r) => toEngagedLead(r as unknown as GoldRow));
}
