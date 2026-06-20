import { Router, Request, Response } from "express";
import { db } from "../db";
import { sql, type SQL } from "drizzle-orm";
import { StatsQuerySchema, GroupedStatsRequestSchema } from "../schemas";
import { statsCacheKey, getOrSetCachedStats } from "../lib/stats-cache";

const router = Router();

// Internal emails/domains excluded from all stats.
// Events where lead_email matches these (or equals the sender) are never counted.
const EXCLUDED_EMAILS = ["kevin.lourd@gmail.com", "kevin@distribute.you"];
const EXCLUDED_DOMAINS = [
  "distribute.you",
  "mcpfactory.org",
  "growthagency.dev",
  "growthservice.org",
  "pressbeat.io",
  "kevinlourd.com",
  "polaritycourse.com",
];

/** SQL fragment that filters out internal/sender events */
export function internalExclusionClause(): SQL {
  const domainConditions = EXCLUDED_DOMAINS.map(
    (d) => sql`e.lead_email LIKE ${"%" + d}`,
  );
  return sql`
    (e.account_email IS NULL OR e.lead_email != e.account_email)
    AND e.lead_email NOT IN (${sql.join(
      EXCLUDED_EMAILS.map((e) => sql`${e}`),
      sql`, `,
    )})
    AND NOT (${sql.join(domainConditions, sql` OR `)})
  `;
}

export const ZERO_REPLIES_DETAIL = {
  interested: 0,
  meetingBooked: 0,
  closed: 0,
  notInterested: 0,
  wrongPerson: 0,
  unsubscribe: 0,
  neutral: 0,
  autoReply: 0,
  outOfOffice: 0,
};

const ZERO_RECIPIENT_STATS = {
  contacted: 0,
  sent: 0,
  delivered: 0,
  opened: 0,
  bounced: 0,
  clicked: 0,
  unsubscribed: 0,
  notSending: 0,
  cancelled: 0,
  repliesPositive: 0,
  repliesNegative: 0,
  repliesNeutral: 0,
  repliesAutoReply: 0,
  repliesDetail: { ...ZERO_REPLIES_DETAIL },
};

const ZERO_EMAIL_STATS = {
  sent: 0,
  delivered: 0,
  opened: 0,
  clicked: 0,
  bounced: 0,
  unsubscribed: 0,
};

export interface EngagementLatencyMetric {
  averageMs: number | null;
  medianMs: number | null;
  sampleSize: number;
}

export interface EngagementLatencyGroup {
  key: string;
  workflowSlugs: string[];
  timeToFirstLinkClick: EngagementLatencyMetric;
  timeToFirstPositiveReply: EngagementLatencyMetric;
}

export interface EngagementLatencyGroupInput {
  key: string;
  workflowSlugs: string[];
}

const ZERO_ENGAGEMENT_LATENCY_METRIC: EngagementLatencyMetric = {
  averageMs: null,
  medianMs: null,
  sampleSize: 0,
};

/** Compute reply aggregates from detail counts */
export function buildRepliesFromDetail(detail: typeof ZERO_REPLIES_DETAIL) {
  return {
    repliesPositive: detail.interested + detail.meetingBooked + detail.closed,
    repliesNegative: detail.notInterested + detail.wrongPerson + detail.unsubscribe,
    repliesNeutral: detail.neutral,
    repliesAutoReply: detail.autoReply + detail.outOfOffice,
    repliesDetail: detail,
  };
}

function readFiniteNumber(value: unknown, field: string): number {
  const numeric = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(numeric)) {
    throw new Error(`Invalid engagement latency field ${field}`);
  }
  return numeric;
}

function readNullableFiniteNumber(value: unknown, field: string): number | null {
  if (value === null) return null;
  return readFiniteNumber(value, field);
}

function buildLatencyMetric(
  row: Record<string, unknown>,
  sampleSizeField: string,
  averageMsField: string,
  medianMsField: string,
): EngagementLatencyMetric {
  const sampleSize = readFiniteNumber(row[sampleSizeField], sampleSizeField);
  if (!Number.isInteger(sampleSize) || sampleSize < 0) {
    throw new Error(`Invalid engagement latency sample size ${sampleSizeField}`);
  }
  if (sampleSize === 0) return { ...ZERO_ENGAGEMENT_LATENCY_METRIC };

  const averageMs = readNullableFiniteNumber(row[averageMsField], averageMsField);
  const medianMs = readNullableFiniteNumber(row[medianMsField], medianMsField);
  if (averageMs === null || medianMs === null) {
    throw new Error(`Missing engagement latency metric for non-empty sample ${sampleSizeField}`);
  }
  return { averageMs, medianMs, sampleSize };
}

/**
 * Public-safe engagement latency aggregate.
 *
 * The caller supplies public grouping keys and their workflow slug sets. This
 * service owns the dated email events, so it computes first-send -> first
 * engagement distributions here and returns only aggregate metrics.
 */
export async function queryEngagementLatencyGroups(
  groups: EngagementLatencyGroupInput[],
): Promise<EngagementLatencyGroup[]> {
  if (groups.length === 0) return [];

  const values = groups.flatMap((group) =>
    group.workflowSlugs.map((workflowSlug) => ({ key: group.key, workflowSlug })),
  );
  if (values.length === 0) {
    throw new Error("Engagement latency groups require at least one workflow slug");
  }

  const groupWorkflowValues = sql.join(
    values.map((value) => sql`(${value.key}, ${value.workflowSlug})`),
    sql`, `,
  );

  const result = await db.execute(sql`
    WITH group_workflows(group_key, workflow_slug) AS (
      VALUES ${groupWorkflowValues}
    ),
    group_keys AS (
      SELECT group_key
      FROM group_workflows
      GROUP BY group_key
    ),
    events_in_group AS (
      SELECT
        gw.group_key,
        e.lead_email,
        e.event_type,
        e.timestamp,
        e.inferred
      FROM instantly_events e
      JOIN instantly_campaigns c ON c.instantly_campaign_id = e.campaign_id
      JOIN group_workflows gw ON gw.workflow_slug = c.workflow_slug
      WHERE ${internalExclusionClause()}
        AND e.lead_email IS NOT NULL
    ),
    first_sends AS (
      SELECT
        group_key,
        lead_email,
        MIN(timestamp) AS first_sent_at
      FROM events_in_group
      WHERE event_type = 'email_sent'
        AND inferred = false
      GROUP BY group_key, lead_email
    ),
    recipient_latencies AS (
      SELECT
        s.group_key,
        s.lead_email,
        MIN(EXTRACT(EPOCH FROM (e.timestamp - s.first_sent_at)) * 1000)
          FILTER (WHERE e.event_type = 'email_link_clicked') AS click_latency_ms,
        MIN(EXTRACT(EPOCH FROM (e.timestamp - s.first_sent_at)) * 1000)
          FILTER (
            WHERE e.event_type IN (
              'lead_interested',
              'lead_meeting_booked',
              'lead_closed'
            )
          ) AS positive_reply_latency_ms
      FROM first_sends s
      LEFT JOIN events_in_group e
        ON e.group_key = s.group_key
        AND e.lead_email = s.lead_email
        AND e.timestamp >= s.first_sent_at
      GROUP BY s.group_key, s.lead_email
    ),
    aggregates AS (
      SELECT
        group_key,
        COUNT(click_latency_ms)::int AS click_sample_size,
        AVG(click_latency_ms)::float8 AS click_average_ms,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY click_latency_ms)::float8 AS click_median_ms,
        COUNT(positive_reply_latency_ms)::int AS positive_reply_sample_size,
        AVG(positive_reply_latency_ms)::float8 AS positive_reply_average_ms,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY positive_reply_latency_ms)::float8 AS positive_reply_median_ms
      FROM recipient_latencies
      GROUP BY group_key
    )
    SELECT
      g.group_key AS "groupKey",
      COALESCE(a.click_sample_size, 0)::int AS "clickSampleSize",
      a.click_average_ms AS "clickAverageMs",
      a.click_median_ms AS "clickMedianMs",
      COALESCE(a.positive_reply_sample_size, 0)::int AS "positiveReplySampleSize",
      a.positive_reply_average_ms AS "positiveReplyAverageMs",
      a.positive_reply_median_ms AS "positiveReplyMedianMs"
    FROM group_keys g
    LEFT JOIN aggregates a ON a.group_key = g.group_key
    ORDER BY g.group_key
  `);

  const rows = Array.isArray(result) ? result : (result as any).rows ?? [];
  const rowByKey = new Map<string, Record<string, unknown>>(
    rows.map((row: Record<string, unknown>) => [String(row.groupKey), row]),
  );

  return groups.map((group) => {
    const row = rowByKey.get(group.key);
    if (!row) {
      return {
        key: group.key,
        workflowSlugs: group.workflowSlugs,
        timeToFirstLinkClick: { ...ZERO_ENGAGEMENT_LATENCY_METRIC },
        timeToFirstPositiveReply: { ...ZERO_ENGAGEMENT_LATENCY_METRIC },
      };
    }

    return {
      key: group.key,
      workflowSlugs: group.workflowSlugs,
      timeToFirstLinkClick: buildLatencyMetric(
        row,
        "clickSampleSize",
        "clickAverageMs",
        "clickMedianMs",
      ),
      timeToFirstPositiveReply: buildLatencyMetric(
        row,
        "positiveReplySampleSize",
        "positiveReplyAverageMs",
        "positiveReplyMedianMs",
      ),
    };
  });
}

/** SQL fragment that filters out internal emails on the campaigns table (uses c.lead_email) */
export function campaignExclusionClause(): SQL {
  const domainConditions = EXCLUDED_DOMAINS.map(
    (d) => sql`c.lead_email LIKE ${"%" + d}`,
  );
  return sql`
    c.lead_email NOT IN (${sql.join(
      EXCLUDED_EMAILS.map((e) => sql`${e}`),
      sql`, `,
    )})
    AND NOT (${sql.join(domainConditions, sql` OR `)})
  `;
}

export interface CampaignAggregates {
  contacted: number;
  notSending: number;
  cancelled: number;
}

const ZERO_CAMPAIGN_AGGREGATES: CampaignAggregates = { contacted: 0, notSending: 0, cancelled: 0 };

/**
 * Aggregate per-lead counts derived from the campaigns table (NOT the events
 * table). One DB roundtrip:
 *   - `contacted` = row count (lead pushed to Instantly).
 *   - `notSending` = distinct lead_email where Instantly's diagnostic
 *     `not_sending_status` is currently flagged (lead live-stuck).
 *   - `cancelled` = row count where the retry-stuck job has terminally
 *     killed the campaign (delivery_status='cancelled').
 */
export async function queryCampaignAggregates(whereClause: SQL): Promise<CampaignAggregates> {
  const result = await db.execute(sql`
    SELECT
      COUNT(*)::int AS "emailsContacted",
      COUNT(DISTINCT c.lead_email) FILTER (WHERE c.not_sending_status IS NOT NULL)::int AS "notSending",
      COUNT(*) FILTER (WHERE c.delivery_status = 'cancelled')::int AS "cancelled"
    FROM instantly_campaigns c
    WHERE ${whereClause}
      AND ${campaignExclusionClause()}
  `);
  const rows = Array.isArray(result) ? result : (result as any).rows ?? [];
  const row = rows[0] as any;
  return {
    contacted: row?.emailsContacted ?? 0,
    notSending: row?.notSending ?? 0,
    cancelled: row?.cancelled ?? 0,
  };
}

/** Same aggregates as queryCampaignAggregates, grouped by dimension. */
export async function queryGroupedCampaignAggregates(
  whereClause: SQL,
  groupBy: string,
  timezone = "UTC",
): Promise<Map<string, CampaignAggregates>> {
  const groupCol = campaignGroupColumn(groupBy, timezone);
  if (!groupCol) return new Map();

  const lateralJoin = groupBy === "brandId" ? BRAND_LATERAL_JOIN : sql``;
  // For groupBy=day, groupCol is the parameterized localDayKey fragment (it
  // carries the timezone bind). Emitting it more than once in the same statement
  // re-emits the bind and drifts param positions — that fragility broke this
  // query twice (42803 when groupCol sat in both SELECT and GROUP BY; 08P01 when
  // the ordinal-fix's own comment still interpolated ${groupCol}). Mirror
  // queryGroupedSentiment / queryGroupedStats: compute the group key ONCE inside
  // a CTE, then reference the `group_key` alias in the outer WHERE/GROUP BY. For
  // non-day groupings groupCol is a bare column (sql.raw, no params) so the
  // inline form stays safe.
  const query = groupBy === "day" ? sql`
    WITH grouped_campaigns AS (
      SELECT
        ${groupCol} AS group_key,
        c.lead_email,
        c.not_sending_status,
        c.delivery_status
      FROM instantly_campaigns c
      WHERE ${whereClause}
        AND ${campaignExclusionClause()}
    )
    SELECT
      c.group_key AS "groupKey",
      COUNT(*)::int AS "emailsContacted",
      COUNT(DISTINCT c.lead_email) FILTER (WHERE c.not_sending_status IS NOT NULL)::int AS "notSending",
      COUNT(*) FILTER (WHERE c.delivery_status = 'cancelled')::int AS "cancelled"
    FROM grouped_campaigns c
    WHERE c.group_key IS NOT NULL
    GROUP BY c.group_key
  ` : sql`
    SELECT
      ${groupCol} AS "groupKey",
      COUNT(*)::int AS "emailsContacted",
      COUNT(DISTINCT c.lead_email) FILTER (WHERE c.not_sending_status IS NOT NULL)::int AS "notSending",
      COUNT(*) FILTER (WHERE c.delivery_status = 'cancelled')::int AS "cancelled"
    FROM instantly_campaigns c
    ${lateralJoin}
    WHERE ${whereClause}
      AND ${campaignExclusionClause()}
      AND ${groupCol} IS NOT NULL
    GROUP BY ${groupCol}
  `;
  const result = await db.execute(query);
  const rows = Array.isArray(result) ? result : (result as any).rows ?? [];
  return new Map(
    rows.map((r: any) => [
      r.groupKey,
      {
        contacted: r.emailsContacted ?? 0,
        notSending: r.notSending ?? 0,
        cancelled: r.cancelled ?? 0,
      } as CampaignAggregates,
    ]),
  );
}

const GROUP_BY_COLUMNS: Record<string, string> = {
  brandId: "brand_id",
  campaignId: "c.campaign_id",
  workflowSlug: "c.workflow_slug",
  featureSlug: "c.feature_slug",
  leadEmail: "e.lead_email",
  audienceId: "c.metadata->>'audienceId'",
};

function localDayKey(timestampExpr: SQL, timezone: string): SQL {
  return sql`TO_CHAR(((${timestampExpr}) AT TIME ZONE 'UTC') AT TIME ZONE ${timezone}, 'YYYY-MM-DD')`;
}

function eventGroupColumn(groupBy: string, timezone: string): SQL | null {
  if (groupBy === "day") return localDayKey(sql`e.timestamp`, timezone);
  const col = GROUP_BY_COLUMNS[groupBy];
  return col ? sql.raw(col) : null;
}

/**
 * Group column for the campaign-table aggregates (contacted / notSending /
 * cancelled). For `day` we bucket each campaign row by its `created_at` local
 * day — `created_at` IS the contacted timestamp (one campaign row = one lead
 * pushed; CLAUDE.md: firstContactedAt = MIN(c.created_at)). Bucketing the SAME
 * rows that feed the cumulative COUNT(*) guarantees day-sum == cumulative
 * contacted by construction.
 */
function campaignGroupColumn(groupBy: string, timezone: string): SQL | null {
  if (groupBy === "day") return localDayKey(sql`c.created_at`, timezone);
  const col = GROUP_BY_COLUMNS[groupBy];
  return col ? sql.raw(col) : null;
}

function sentimentGroupColumn(groupBy: string, timezone: string): SQL | null {
  if (groupBy === "day") return localDayKey(sql`ls.timestamp`, timezone);
  const col = SENTIMENT_GROUP_BY_COLUMNS[groupBy];
  return col ? sql.raw(col) : null;
}

/** SQL fragment for LATERAL unnest of brand_ids, used when groupBy=brandId */
const BRAND_LATERAL_JOIN = sql`CROSS JOIN LATERAL unnest(c.brand_ids) AS brand_id`;

/**
 * The 8 mutually-exclusive reply-sentiment event types (mirrors
 * REPLY_CLASSIFICATION_MAP in silver-promote.ts). A lead's CURRENT sentiment is
 * the LATEST of these per (campaign_id, lead_email) — NOT every sentiment event
 * ever recorded.
 *
 * Why this matters: a reply can be re-qualified. A webhook auto-classifies a
 * reply `lead_interested`, then an operator manually re-qualifies it
 * `lead_not_interested` via POST /orgs/manual-qualifications. The silver event
 * log faithfully keeps BOTH rows (append-only audit trail). Counting raw events
 * (`COUNT(*) FILTER (WHERE event_type = 'lead_interested')`) therefore never
 * drops the stale positive when a reply is later re-qualified negative — the
 * positive-reply totals at model/brand level stay frozen. Gold must instead
 * count each lead's CURRENT sentiment (latest event, manual winning ties).
 */
export const SENTIMENT_EVENT_TYPES = [
  "lead_interested",
  "lead_meeting_booked",
  "lead_closed",
  "lead_not_interested",
  "lead_wrong_person",
  "lead_neutral",
  "lead_out_of_office",
  "auto_reply_received",
] as const;

export interface SentimentDetail {
  interested: number;
  meetingBooked: number;
  closed: number;
  notInterested: number;
  wrongPerson: number;
  neutral: number;
  autoReply: number;
  outOfOffice: number;
}

const ZERO_SENTIMENT_DETAIL: SentimentDetail = {
  interested: 0,
  meetingBooked: 0,
  closed: 0,
  notInterested: 0,
  wrongPerson: 0,
  neutral: 0,
  autoReply: 0,
  outOfOffice: 0,
};

/**
 * Derived table (CTE body): one row per (campaign_id, lead_email) carrying ONLY
 * that lead's CURRENT sentiment — the latest sentiment event, manual winning
 * ties. This is the gold-layer projection of "current qualification"; bronze +
 * silver are left untouched (the full reclassification history stays in the
 * event log). The `e.` alias matches `internalExclusionClause()`.
 */
function latestSentimentCteBody(): SQL {
  const typeList = sql.join(
    SENTIMENT_EVENT_TYPES.map((t) => sql`${t}`),
    sql`, `,
  );
  return sql`
    SELECT DISTINCT ON (e.campaign_id, e.lead_email)
      e.campaign_id AS campaign_id,
      e.lead_email AS lead_email,
      e.timestamp AS timestamp,
      e.event_type AS sentiment
    FROM instantly_events e
    WHERE e.event_type IN (${typeList})
      AND ${internalExclusionClause()}
    ORDER BY e.campaign_id, e.lead_email,
      e.timestamp DESC, (e.source = 'manual') DESC, e.created_at DESC, e.id DESC
  `;
}

/** Per-sentiment COUNT FILTERs over the latest-sentiment derived table. */
const SENTIMENT_COUNT_COLUMNS = sql`
  COUNT(*) FILTER (WHERE ls.sentiment = 'lead_interested')::int AS "rdInterested",
  COUNT(*) FILTER (WHERE ls.sentiment = 'lead_meeting_booked')::int AS "rdMeetingBooked",
  COUNT(*) FILTER (WHERE ls.sentiment = 'lead_closed')::int AS "rdClosed",
  COUNT(*) FILTER (WHERE ls.sentiment = 'lead_not_interested')::int AS "rdNotInterested",
  COUNT(*) FILTER (WHERE ls.sentiment = 'lead_wrong_person')::int AS "rdWrongPerson",
  COUNT(*) FILTER (WHERE ls.sentiment = 'lead_neutral')::int AS "rdNeutral",
  COUNT(*) FILTER (WHERE ls.sentiment = 'auto_reply_received')::int AS "rdAutoReply",
  COUNT(*) FILTER (WHERE ls.sentiment = 'lead_out_of_office')::int AS "rdOutOfOffice"
`;

function rowToSentimentDetail(row: Record<string, number> | undefined): SentimentDetail {
  if (!row) return { ...ZERO_SENTIMENT_DETAIL };
  return {
    interested: row.rdInterested ?? 0,
    meetingBooked: row.rdMeetingBooked ?? 0,
    closed: row.rdClosed ?? 0,
    notInterested: row.rdNotInterested ?? 0,
    wrongPerson: row.rdWrongPerson ?? 0,
    neutral: row.rdNeutral ?? 0,
    autoReply: row.rdAutoReply ?? 0,
    outOfOffice: row.rdOutOfOffice ?? 0,
  };
}

/** Group column map for the sentiment query — outer scope exposes `ls` + `c`
 *  (NOT `e`), so leadEmail resolves to `ls.lead_email`. */
const SENTIMENT_GROUP_BY_COLUMNS: Record<string, string> = {
  brandId: "brand_id",
  campaignId: "c.campaign_id",
  workflowSlug: "c.workflow_slug",
  featureSlug: "c.feature_slug",
  leadEmail: "ls.lead_email",
  audienceId: "c.metadata->>'audienceId'",
};

/**
 * Current-sentiment counts grouped by dimension, derived from the latest
 * sentiment event per lead. One DB roundtrip; merged into repliesDetail by the
 * caller. Mirrors the queryGroupedCampaignAggregates pattern.
 */
export async function queryGroupedSentiment(
  whereClause: SQL,
  groupBy: string,
  timezone = "UTC",
): Promise<Map<string, SentimentDetail>> {
  const groupCol = sentimentGroupColumn(groupBy, timezone);
  if (!groupCol) return new Map();

  const lateralJoin = groupBy === "brandId" ? BRAND_LATERAL_JOIN : sql``;
  if (groupBy === "day") {
    const result = await db.execute(sql`
      WITH latest_sentiment AS (${latestSentimentCteBody()}),
      grouped_sentiment AS (
        SELECT
          ${groupCol} AS group_key,
          ls.sentiment
        FROM latest_sentiment ls
        JOIN instantly_campaigns c ON c.instantly_campaign_id = ls.campaign_id
        WHERE ${whereClause}
      )
      SELECT
        ls.group_key AS "groupKey",
        ${SENTIMENT_COUNT_COLUMNS}
      FROM grouped_sentiment ls
      WHERE ls.group_key IS NOT NULL
      GROUP BY ls.group_key
    `);
    const rows = Array.isArray(result) ? result : (result as any).rows ?? [];
    return new Map(
      rows.map((r: any) => [String(r.groupKey), rowToSentimentDetail(r)]),
    );
  }

  const result = await db.execute(sql`
    WITH latest_sentiment AS (${latestSentimentCteBody()})
    SELECT
      ${groupCol} AS "groupKey",
      ${SENTIMENT_COUNT_COLUMNS}
    FROM latest_sentiment ls
    JOIN instantly_campaigns c ON c.instantly_campaign_id = ls.campaign_id
    ${lateralJoin}
    WHERE ${whereClause}
      AND ${groupCol} IS NOT NULL
    GROUP BY ${groupCol}
  `);
  const rows = Array.isArray(result) ? result : (result as any).rows ?? [];
  return new Map(
    rows.map((r: any) => [String(r.groupKey), rowToSentimentDetail(r)]),
  );
}

/** Aggregate current-sentiment counts (no grouping). */
export async function querySentiment(whereClause: SQL): Promise<SentimentDetail> {
  const result = await db.execute(sql`
    WITH latest_sentiment AS (${latestSentimentCteBody()})
    SELECT
      ${SENTIMENT_COUNT_COLUMNS}
    FROM latest_sentiment ls
    JOIN instantly_campaigns c ON c.instantly_campaign_id = ls.campaign_id
    WHERE ${whereClause}
  `);
  const rows = Array.isArray(result) ? result : (result as any).rows ?? [];
  return rowToSentimentDetail(rows[0] as Record<string, number> | undefined);
}

/**
 * Per-step current-sentiment counts, keyed by step.
 *
 * A reply is ONE outcome per lead for the whole sequence — not a per-step event.
 * So each lead contributes its CURRENT sentiment (latest event, manual winning
 * ties) EXACTLY ONCE, attributed to its **last reached step** = `MAX(step)` of
 * the lead's `email_sent` events. The sequence stops on reply, so the last sent
 * step ≈ the step the prospect replied to (auto); for a manual negative
 * (Instantly never saw the reply) it is simply the last step reached.
 *
 * Consequence: a reply re-qualified negative shows up ONLY as negative on the
 * lead's last step — never as a stale positive on the step where the original
 * auto `lead_interested` event happened. Step-level email metrics
 * (sent/opened/clicked/bounced) stay genuinely per-step in the caller; only the
 * reply *sentiment* is collapsed to the last step here.
 */
export async function queryStepSentiment(whereClause: SQL): Promise<Map<number, SentimentDetail>> {
  const result = await db.execute(sql`
    WITH latest_sentiment AS (${latestSentimentCteBody()}),
    ls AS (
      SELECT
        lc.campaign_id,
        lc.sentiment,
        (
          SELECT MAX(es.step) FROM instantly_events es
          WHERE es.campaign_id = lc.campaign_id
            AND es.lead_email = lc.lead_email
            AND es.event_type = 'email_sent'
        ) AS step
      FROM latest_sentiment lc
    )
    SELECT
      ls.step AS "step",
      ${SENTIMENT_COUNT_COLUMNS}
    FROM ls
    JOIN instantly_campaigns c ON c.instantly_campaign_id = ls.campaign_id
    WHERE ${whereClause}
      AND ls.step IS NOT NULL
    GROUP BY ls.step
  `);
  const rows = Array.isArray(result) ? result : (result as any).rows ?? [];
  const map = new Map<number, SentimentDetail>();
  for (const r of rows as Array<Record<string, number>>) {
    map.set(Number(r.step), rowToSentimentDetail(r));
  }
  return map;
}

/** Execute grouped stats query and return array of { key, recipientStats, emailStats } */
export async function queryGroupedStats(
  whereClause: SQL,
  groupBy: string,
  timezone = "UTC",
): Promise<Array<{ key: string; recipientStats: typeof ZERO_RECIPIENT_STATS; emailStats: typeof ZERO_EMAIL_STATS }>> {
  const groupCol = eventGroupColumn(groupBy, timezone);
  if (!groupCol) return [];

  const lateralJoin = groupBy === "brandId" ? BRAND_LATERAL_JOIN : sql``;
  const eventsQuery = groupBy === "day" ? sql`
      WITH grouped_events AS (
        SELECT
          ${groupCol} AS group_key,
          e.event_type,
          e.lead_email,
          e.campaign_id,
          e.step
        FROM instantly_events e
        JOIN instantly_campaigns c ON c.instantly_campaign_id = e.campaign_id
        WHERE ${whereClause}
          AND ${internalExclusionClause()}
      )
      SELECT
        e.group_key AS "groupKey",
        COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'email_sent'), 0)::int AS "esSent",
        COALESCE(COUNT(DISTINCT CONCAT(e.lead_email, '::', e.campaign_id, '::', e.step)) FILTER (WHERE e.event_type = 'email_opened'), 0)::int AS "esOpened",
        COALESCE(COUNT(DISTINCT CONCAT(e.lead_email, '::', e.campaign_id, '::', e.step)) FILTER (WHERE e.event_type = 'email_link_clicked'), 0)::int AS "esClicked",
        COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'email_bounced'), 0)::int AS "esBounced",
        COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_unsubscribed'), 0)::int AS "esUnsubscribed",
        COALESCE(COUNT(DISTINCT e.lead_email) FILTER (WHERE e.event_type = 'email_sent'), 0)::int AS "rsSent",
        COALESCE(COUNT(DISTINCT e.lead_email) FILTER (WHERE e.event_type = 'email_opened'), 0)::int AS "rsOpened",
        COALESCE(COUNT(DISTINCT e.lead_email) FILTER (WHERE e.event_type = 'email_link_clicked'), 0)::int AS "rsClicked",
        COALESCE(COUNT(DISTINCT e.lead_email) FILTER (WHERE e.event_type = 'email_bounced'), 0)::int AS "rsBounced",
        COALESCE(COUNT(DISTINCT e.lead_email) FILTER (WHERE e.event_type = 'lead_unsubscribed'), 0)::int AS "rsUnsubscribed",
        COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_unsubscribed'), 0)::int AS "rdUnsubscribe"
      FROM grouped_events e
      WHERE e.group_key IS NOT NULL
      GROUP BY e.group_key
      ORDER BY e.group_key
    ` : sql`
      SELECT
        ${groupCol} AS "groupKey",
        COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'email_sent'), 0)::int AS "esSent",
        COALESCE(COUNT(DISTINCT CONCAT(e.lead_email, '::', e.campaign_id, '::', e.step)) FILTER (WHERE e.event_type = 'email_opened'), 0)::int AS "esOpened",
        COALESCE(COUNT(DISTINCT CONCAT(e.lead_email, '::', e.campaign_id, '::', e.step)) FILTER (WHERE e.event_type = 'email_link_clicked'), 0)::int AS "esClicked",
        COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'email_bounced'), 0)::int AS "esBounced",
        COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_unsubscribed'), 0)::int AS "esUnsubscribed",
        COALESCE(COUNT(DISTINCT e.lead_email) FILTER (WHERE e.event_type = 'email_sent'), 0)::int AS "rsSent",
        COALESCE(COUNT(DISTINCT e.lead_email) FILTER (WHERE e.event_type = 'email_opened'), 0)::int AS "rsOpened",
        COALESCE(COUNT(DISTINCT e.lead_email) FILTER (WHERE e.event_type = 'email_link_clicked'), 0)::int AS "rsClicked",
        COALESCE(COUNT(DISTINCT e.lead_email) FILTER (WHERE e.event_type = 'email_bounced'), 0)::int AS "rsBounced",
        COALESCE(COUNT(DISTINCT e.lead_email) FILTER (WHERE e.event_type = 'lead_unsubscribed'), 0)::int AS "rsUnsubscribed",
        COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_unsubscribed'), 0)::int AS "rdUnsubscribe"
      FROM instantly_events e
      JOIN instantly_campaigns c ON c.instantly_campaign_id = e.campaign_id
      ${lateralJoin}
      WHERE ${whereClause}
        AND ${internalExclusionClause()}
        AND ${groupCol} IS NOT NULL
      GROUP BY ${groupCol}
      ORDER BY ${groupCol}
    `;
  // These three reads are independent — run them concurrently. aggregatesMap is
  // campaign-level (contacted + notSending + cancelled), derived independently
  // of the events table. sentimentMap is each lead's CURRENT sentiment (latest
  // sentiment event, manual winning ties) — NOT every sentiment event ever
  // recorded, which never drops a re-qualified reply (see SENTIMENT_EVENT_TYPES;
  // `unsubscribe` stays an event count, a separate signal).
  const [result, aggregatesMap, sentimentMap] = await Promise.all([
    db.execute(eventsQuery),
    queryGroupedCampaignAggregates(whereClause, groupBy, timezone),
    queryGroupedSentiment(whereClause, groupBy, timezone),
  ]);
  const rows = Array.isArray(result) ? result : (result as any).rows ?? [];

  const buildGroup = (key: string, row: any | undefined) => {
    const sentiment = sentimentMap.get(String(key)) ?? ZERO_SENTIMENT_DETAIL;
    const detail = {
      interested: sentiment.interested,
      meetingBooked: sentiment.meetingBooked,
      closed: sentiment.closed,
      notInterested: sentiment.notInterested,
      wrongPerson: sentiment.wrongPerson,
      unsubscribe: row?.rdUnsubscribe ?? 0,
      neutral: sentiment.neutral,
      autoReply: sentiment.autoReply,
      outOfOffice: sentiment.outOfOffice,
    };
    const rsSent = row?.rsSent ?? 0;
    const rsBounced = row?.rsBounced ?? 0;
    const esSent = row?.esSent ?? 0;
    const esBounced = row?.esBounced ?? 0;
    const aggregates = aggregatesMap.get(key) ?? ZERO_CAMPAIGN_AGGREGATES;
    return {
      key,
      recipientStats: {
        contacted: aggregates.contacted,
        sent: rsSent,
        delivered: rsSent - rsBounced,
        opened: row?.rsOpened ?? 0,
        bounced: rsBounced,
        clicked: row?.rsClicked ?? 0,
        unsubscribed: row?.rsUnsubscribed ?? 0,
        notSending: aggregates.notSending,
        cancelled: aggregates.cancelled,
        ...buildRepliesFromDetail(detail),
      },
      emailStats: {
        sent: esSent,
        delivered: esSent - esBounced,
        opened: row?.esOpened ?? 0,
        clicked: row?.esClicked ?? 0,
        bounced: esBounced,
        unsubscribed: row?.esUnsubscribed ?? 0,
      },
    };
  };

  const eventGroups = rows.map((row: any) => buildGroup(row.groupKey as string, row));
  if (groupBy !== "day") return eventGroups;

  // Day mode only: `contacted` is a campaign-table fact (lead pushed), not an
  // event. A day with contacted leads but zero email events would be dropped if
  // we built groups from the events query alone — so UNION the aggregate-only
  // day keys (contacted/notSending/cancelled present, every event metric 0).
  const eventKeys = new Set(rows.map((r: any) => String(r.groupKey)));
  const extraGroups = [...aggregatesMap.keys()]
    .filter((key) => !eventKeys.has(String(key)))
    .map((key) => buildGroup(key, undefined));

  return [...eventGroups, ...extraGroups].sort((a, b) =>
    a.key < b.key ? -1 : a.key > b.key ? 1 : 0,
  );
}

/** Execute the aggregate stats query and return { recipientStats, emailStats } */
export async function queryStats(whereClause: SQL): Promise<{ recipientStats: typeof ZERO_RECIPIENT_STATS; emailStats: typeof ZERO_EMAIL_STATS }> {
  // These three reads are independent — run them concurrently to cut wall-clock
  // latency (and the time each holds a pool connection) under load. Reply
  // sentiment comes from each lead's CURRENT sentiment (latest sentiment event,
  // manual winning ties) — see queryGroupedStats / SENTIMENT_EVENT_TYPES.
  const [result, aggregates, sentiment] = await Promise.all([
    db.execute(sql`
      SELECT
        COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'email_sent'), 0)::int AS "esSent",
        COALESCE(COUNT(DISTINCT CONCAT(e.lead_email, '::', e.campaign_id, '::', e.step)) FILTER (WHERE e.event_type = 'email_opened'), 0)::int AS "esOpened",
        COALESCE(COUNT(DISTINCT CONCAT(e.lead_email, '::', e.campaign_id, '::', e.step)) FILTER (WHERE e.event_type = 'email_link_clicked'), 0)::int AS "esClicked",
        COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'email_bounced'), 0)::int AS "esBounced",
        COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_unsubscribed'), 0)::int AS "esUnsubscribed",
        COALESCE(COUNT(DISTINCT e.lead_email) FILTER (WHERE e.event_type = 'email_sent'), 0)::int AS "rsSent",
        COALESCE(COUNT(DISTINCT e.lead_email) FILTER (WHERE e.event_type = 'email_opened'), 0)::int AS "rsOpened",
        COALESCE(COUNT(DISTINCT e.lead_email) FILTER (WHERE e.event_type = 'email_link_clicked'), 0)::int AS "rsClicked",
        COALESCE(COUNT(DISTINCT e.lead_email) FILTER (WHERE e.event_type = 'email_bounced'), 0)::int AS "rsBounced",
        COALESCE(COUNT(DISTINCT e.lead_email) FILTER (WHERE e.event_type = 'lead_unsubscribed'), 0)::int AS "rsUnsubscribed",
        COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_unsubscribed'), 0)::int AS "rdUnsubscribe"
      FROM instantly_events e
      JOIN instantly_campaigns c ON c.instantly_campaign_id = e.campaign_id
      WHERE ${whereClause}
        AND ${internalExclusionClause()}
    `),
    queryCampaignAggregates(whereClause),
    querySentiment(whereClause),
  ]);
  const rows = Array.isArray(result) ? result : (result as any).rows ?? [];

  if (!rows.length) {
    return {
      recipientStats: {
        ...ZERO_RECIPIENT_STATS,
        contacted: aggregates.contacted,
        notSending: aggregates.notSending,
        cancelled: aggregates.cancelled,
        repliesDetail: { ...ZERO_REPLIES_DETAIL },
      },
      emailStats: { ...ZERO_EMAIL_STATS },
    };
  }

  const row = rows[0] as Record<string, number>;
  const detail = {
    interested: sentiment.interested,
    meetingBooked: sentiment.meetingBooked,
    closed: sentiment.closed,
    notInterested: sentiment.notInterested,
    wrongPerson: sentiment.wrongPerson,
    unsubscribe: row.rdUnsubscribe ?? 0,
    neutral: sentiment.neutral,
    autoReply: sentiment.autoReply,
    outOfOffice: sentiment.outOfOffice,
  };
  const rsSent = row.rsSent ?? 0;
  const rsBounced = row.rsBounced ?? 0;
  const esSent = row.esSent ?? 0;
  const esBounced = row.esBounced ?? 0;
  return {
    recipientStats: {
      contacted: aggregates.contacted,
      sent: rsSent,
      delivered: rsSent - rsBounced,
      opened: row.rsOpened ?? 0,
      bounced: rsBounced,
      clicked: row.rsClicked ?? 0,
      unsubscribed: row.rsUnsubscribed ?? 0,
      notSending: aggregates.notSending,
      cancelled: aggregates.cancelled,
      ...buildRepliesFromDetail(detail),
    },
    emailStats: {
      sent: esSent,
      delivered: esSent - esBounced,
      opened: row.esOpened ?? 0,
      clicked: row.esClicked ?? 0,
      bounced: esBounced,
      unsubscribed: row.esUnsubscribed ?? 0,
    },
  };
}

/** Add optional slug filter conditions. */
export function addSlugConditions(
  conditions: SQL[],
  data: {
    workflowSlugs?: string;
    featureSlugs?: string;
  },
): void {
  if (data.workflowSlugs) {
    const slugs = data.workflowSlugs.split(",").filter(Boolean);
    if (slugs.length > 0) {
      conditions.push(sql`c.workflow_slug IN (${sql.join(slugs.map((s) => sql`${s}`), sql`, `)})`);
    }
  }

  if (data.featureSlugs) {
    const slugs = data.featureSlugs.split(",").filter(Boolean);
    if (slugs.length > 0) {
      conditions.push(sql`c.feature_slug IN (${sql.join(slugs.map((s) => sql`${s}`), sql`, `)})`);
    }
  }
}

export interface StepStat {
  step: number;
  sent: number;
  delivered: number;
  opened: number;
  bounced: number;
  clicked: number;
  unsubscribed: number;
  repliesPositive: number;
  repliesNegative: number;
  repliesNeutral: number;
  repliesAutoReply: number;
  repliesDetail: typeof ZERO_REPLIES_DETAIL;
}

/**
 * Per-step breakdown. Email metrics (sent/opened/clicked/bounced/unsubscribe)
 * are genuinely per-step. Reply SENTIMENT, however, is ONE outcome per lead for
 * the whole sequence — counted via queryStepSentiment, which attributes each
 * lead's CURRENT sentiment to its last reached step (so a re-qualified negative
 * never shows a stale positive on an earlier step; see queryStepSentiment).
 *
 * The step events query and the step-sentiment query are independent — run
 * concurrently.
 */
export async function computeStepStats(whereClause: SQL): Promise<StepStat[]> {
  const [stepResult, stepSentimentMap] = await Promise.all([
    db.execute(sql`
      SELECT
        e.step,
        COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'email_sent'), 0)::int AS "sent",
        COALESCE(COUNT(DISTINCT e.lead_email) FILTER (WHERE e.event_type = 'email_opened'), 0)::int AS "opened",
        COALESCE(COUNT(DISTINCT e.lead_email) FILTER (WHERE e.event_type = 'email_link_clicked'), 0)::int AS "clicked",
        COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'email_bounced'), 0)::int AS "bounced",
        COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_unsubscribed'), 0)::int AS "unsubscribed",
        COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_unsubscribed'), 0)::int AS "rdUnsubscribe"
      FROM instantly_events e
      JOIN instantly_campaigns c ON c.instantly_campaign_id = e.campaign_id
      WHERE ${whereClause}
        AND ${internalExclusionClause()}
        AND e.step IS NOT NULL
      GROUP BY e.step
      ORDER BY e.step
    `),
    queryStepSentiment(whereClause),
  ]);
  const stepRows = Array.isArray(stepResult) ? stepResult : (stepResult as any).rows ?? [];
  return stepRows.map((sr: any) => {
    const sentiment = stepSentimentMap.get(Number(sr.step));
    const detail = {
      interested: sentiment?.interested ?? 0,
      meetingBooked: sentiment?.meetingBooked ?? 0,
      closed: sentiment?.closed ?? 0,
      notInterested: sentiment?.notInterested ?? 0,
      wrongPerson: sentiment?.wrongPerson ?? 0,
      unsubscribe: sr.rdUnsubscribe ?? 0,
      neutral: sentiment?.neutral ?? 0,
      autoReply: sentiment?.autoReply ?? 0,
      outOfOffice: sentiment?.outOfOffice ?? 0,
    };
    const sent = sr.sent ?? 0;
    const bounced = sr.bounced ?? 0;
    return {
      step: sr.step,
      sent,
      delivered: sent - bounced,
      opened: sr.opened ?? 0,
      bounced,
      clicked: sr.clicked ?? 0,
      unsubscribed: sr.unsubscribed ?? 0,
      ...buildRepliesFromDetail(detail),
    };
  });
}

/**
 * Full overall stats payload (recipientStats + emailStats, with per-step
 * breakdown folded into emailStats.stepStats). The overall aggregate and the
 * per-step breakdown are independent — run concurrently. The step breakdown is
 * non-fatal: on failure the overall stats still return (stepStats omitted),
 * mirroring the prior inline behavior.
 */
export async function computeStatsPayload(
  whereClause: SQL,
): Promise<{ recipientStats: typeof ZERO_RECIPIENT_STATS; emailStats: Record<string, unknown> }> {
  const [overall, stepStats] = await Promise.all([
    queryStats(whereClause),
    computeStepStats(whereClause).catch((stepError: any) => {
      console.error(
        `[instantly-service] Step query failed (overall stats still returned): ${stepError.cause?.message ?? stepError.message}`,
      );
      return [] as StepStat[];
    }),
  ]);
  return {
    recipientStats: overall.recipientStats,
    emailStats: {
      ...overall.emailStats,
      ...(stepStats.length > 0 && { stepStats }),
    },
  };
}

/**
 * GET /stats
 * Aggregated stats from webhook events. Filters via query params; runIds comma-separated.
 */
router.get("/stats", async (req: Request, res: Response) => {
  const parsed = StatsQuerySchema.safeParse(req.query);
  if (!parsed.success) {
    return res.status(400).json({
      error: "Invalid request",
      details: parsed.error.flatten(),
    });
  }
  const {
    runIds: runIdsRaw,
    brandId,
    campaignId,
    goal,
    brandProfileId,
    audienceId,
    workflowSlugs,
    featureSlugs,
    groupBy,
  } = parsed.data;
  const timezone = parsed.data.timezone ?? "UTC";
  const runIds = runIdsRaw ? runIdsRaw.split(",").filter(Boolean) : undefined;
  const orgId = res.locals.orgId as string;

  // Build WHERE clauses — always scope by org from header
  const conditions: SQL[] = [sql`c.org_id = ${orgId}`];
  if (runIds?.length) conditions.push(sql`c.run_id IN (${sql.join(runIds.map((id) => sql`${id}`), sql`, `)})`);
  if (brandId) conditions.push(sql`${brandId} = ANY(c.brand_ids)`);
  if (campaignId) conditions.push(sql`(c.id = ${campaignId} OR c.campaign_id = ${campaignId})`);
  if (goal) conditions.push(sql`c.metadata->>'goal' = ${goal}`);
  if (brandProfileId) conditions.push(sql`c.metadata->>'brandProfileId' = ${brandProfileId}`);
  if (audienceId) conditions.push(sql`c.metadata->>'audienceId' = ${audienceId}`);

  addSlugConditions(conditions, { workflowSlugs, featureSlugs });

  const whereClause = sql.join(conditions, sql` AND `);

  // Short-TTL cache: bursts of identical /orgs/stats calls re-aggregate over the
  // silver log against a tiny Neon compute and saturate it. Serve repeats within
  // the window from memory. Key is org-scoped so no cross-org leakage.
  const cacheKey = statsCacheKey(`stats:${orgId}`, {
    runIds: runIdsRaw,
    brandId,
    campaignId,
    goal,
    brandProfileId,
    audienceId,
    workflowSlugs,
    featureSlugs,
    groupBy,
    timezone,
  });
  try {
    const payload = await getOrSetCachedStats(cacheKey, async () => {
      if (groupBy) {
        const groups = await queryGroupedStats(whereClause, groupBy, timezone);
        return { groups };
      }
      return computeStatsPayload(whereClause);
    });
    return res.json(payload);
  } catch (error: any) {
    const msg = error.cause?.message ?? error.message ?? String(error);
    console.error(`[instantly-service] Failed to aggregate stats: ${msg}`, error);
    return res.status(500).json({ error: "Failed to aggregate stats" });
  }
});

/**
 * POST /stats/grouped
 * Batch stats by groups of run IDs — one aggregation per group in a single HTTP call.
 */
router.post("/stats/grouped", async (req: Request, res: Response) => {
  const parsed = GroupedStatsRequestSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({
      error: "Invalid request",
      details: parsed.error.flatten(),
    });
  }

  const entries = Object.entries(parsed.data.groups);
  if (entries.length === 0) {
    return res.json({ groups: [] });
  }

  try {
    const results = await Promise.all(
      entries.map(async ([key, { runIds }]) => {
        const orgId = res.locals.orgId as string;
        const whereClause = sql`c.org_id = ${orgId} AND c.run_id IN (${sql.join(runIds.map((id) => sql`${id}`), sql`, `)})`;
        const { recipientStats, emailStats } = await queryStats(whereClause);
        return { key, recipientStats, emailStats };
      }),
    );

    res.json({ groups: results });
  } catch (error: any) {
    const msg = error.cause?.message ?? error.message ?? String(error);
    console.error(`[stats/grouped] Failed to aggregate grouped stats: ${msg}`, error);
    res.status(500).json({ error: "Failed to aggregate grouped stats" });
  }
});

export default router;
