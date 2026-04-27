import { Router, Request, Response } from "express";
import { db } from "../db";
import { sql, type SQL } from "drizzle-orm";
import { StatsQuerySchema, GroupedStatsRequestSchema } from "../schemas";

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

/** Count contacted leads from instantly_campaigns (row exists = contacted) */
export async function queryContactedCount(whereClause: SQL): Promise<number> {
  const result = await db.execute(sql`
    SELECT COUNT(*)::int AS "emailsContacted"
    FROM instantly_campaigns c
    WHERE ${whereClause}
      AND ${campaignExclusionClause()}
  `);
  const rows = Array.isArray(result) ? result : (result as any).rows ?? [];
  return (rows[0] as any)?.emailsContacted ?? 0;
}

/** Count contacted leads grouped by dimension */
export async function queryGroupedContactedCount(
  whereClause: SQL,
  groupBy: string,
): Promise<Map<string, number>> {
  const col = GROUP_BY_COLUMNS[groupBy];
  if (!col) return new Map();

  const groupCol = sql.raw(col);
  const lateralJoin = groupBy === "brandId" ? BRAND_LATERAL_JOIN : sql``;
  const result = await db.execute(sql`
    SELECT
      ${groupCol} AS "groupKey",
      COUNT(*)::int AS "emailsContacted"
    FROM instantly_campaigns c
    ${lateralJoin}
    WHERE ${whereClause}
      AND ${campaignExclusionClause()}
      AND ${groupCol} IS NOT NULL
    GROUP BY ${groupCol}
  `);
  const rows = Array.isArray(result) ? result : (result as any).rows ?? [];
  return new Map(rows.map((r: any) => [r.groupKey, r.emailsContacted ?? 0]));
}

const GROUP_BY_COLUMNS: Record<string, string> = {
  brandId: "brand_id",
  campaignId: "c.campaign_id",
  workflowSlug: "c.workflow_slug",
  featureSlug: "c.feature_slug",
  leadEmail: "e.lead_email",
};

/** SQL fragment for LATERAL unnest of brand_ids, used when groupBy=brandId */
const BRAND_LATERAL_JOIN = sql`CROSS JOIN LATERAL unnest(c.brand_ids) AS brand_id`;

/** Execute grouped stats query and return array of { key, recipientStats, emailStats } */
export async function queryGroupedStats(
  whereClause: SQL,
  groupBy: string,
): Promise<Array<{ key: string; recipientStats: typeof ZERO_RECIPIENT_STATS; emailStats: typeof ZERO_EMAIL_STATS }>> {
  const col = GROUP_BY_COLUMNS[groupBy];
  if (!col) return [];

  const groupCol = sql.raw(col);
  const lateralJoin = groupBy === "brandId" ? BRAND_LATERAL_JOIN : sql``;
  const result = await db.execute(sql`
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
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_interested'), 0)::int AS "rdInterested",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_meeting_booked'), 0)::int AS "rdMeetingBooked",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_closed'), 0)::int AS "rdClosed",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_not_interested'), 0)::int AS "rdNotInterested",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_wrong_person'), 0)::int AS "rdWrongPerson",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_unsubscribed'), 0)::int AS "rdUnsubscribe",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_neutral'), 0)::int AS "rdNeutral",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'auto_reply_received'), 0)::int AS "rdAutoReply",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_out_of_office'), 0)::int AS "rdOutOfOffice"
    FROM instantly_events e
    JOIN instantly_campaigns c ON c.instantly_campaign_id = e.campaign_id
    ${lateralJoin}
    WHERE ${whereClause}
      AND ${internalExclusionClause()}
      AND ${groupCol} IS NOT NULL
    GROUP BY ${groupCol}
  `);
  const rows = Array.isArray(result) ? result : (result as any).rows ?? [];

  // Fetch contacted counts in parallel (from campaigns table, not events)
  const contactedMap = await queryGroupedContactedCount(whereClause, groupBy);

  const rawGroups = rows.map((row: any) => {
    const detail = {
      interested: row.rdInterested ?? 0,
      meetingBooked: row.rdMeetingBooked ?? 0,
      closed: row.rdClosed ?? 0,
      notInterested: row.rdNotInterested ?? 0,
      wrongPerson: row.rdWrongPerson ?? 0,
      unsubscribe: row.rdUnsubscribe ?? 0,
      neutral: row.rdNeutral ?? 0,
      autoReply: row.rdAutoReply ?? 0,
      outOfOffice: row.rdOutOfOffice ?? 0,
    };
    const rsSent = row.rsSent ?? 0;
    const rsBounced = row.rsBounced ?? 0;
    const esSent = row.esSent ?? 0;
    const esBounced = row.esBounced ?? 0;
    return {
      key: row.groupKey as string,
      recipientStats: {
        contacted: contactedMap.get(row.groupKey) ?? 0,
        sent: rsSent,
        delivered: rsSent - rsBounced,
        opened: row.rsOpened ?? 0,
        bounced: rsBounced,
        clicked: row.rsClicked ?? 0,
        unsubscribed: row.rsUnsubscribed ?? 0,
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
  });

  return rawGroups;
}

/** Execute the aggregate stats query and return { recipientStats, emailStats } */
export async function queryStats(whereClause: SQL): Promise<{ recipientStats: typeof ZERO_RECIPIENT_STATS; emailStats: typeof ZERO_EMAIL_STATS }> {
  const result = await db.execute(sql`
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
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_interested'), 0)::int AS "rdInterested",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_meeting_booked'), 0)::int AS "rdMeetingBooked",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_closed'), 0)::int AS "rdClosed",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_not_interested'), 0)::int AS "rdNotInterested",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_wrong_person'), 0)::int AS "rdWrongPerson",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_unsubscribed'), 0)::int AS "rdUnsubscribe",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_neutral'), 0)::int AS "rdNeutral",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'auto_reply_received'), 0)::int AS "rdAutoReply",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_out_of_office'), 0)::int AS "rdOutOfOffice"
    FROM instantly_events e
    JOIN instantly_campaigns c ON c.instantly_campaign_id = e.campaign_id
    WHERE ${whereClause}
      AND ${internalExclusionClause()}
  `);
  const rows = Array.isArray(result) ? result : (result as any).rows ?? [];

  const contacted = await queryContactedCount(whereClause);

  if (!rows.length) {
    return {
      recipientStats: { ...ZERO_RECIPIENT_STATS, contacted, repliesDetail: { ...ZERO_REPLIES_DETAIL } },
      emailStats: { ...ZERO_EMAIL_STATS },
    };
  }

  const row = rows[0] as Record<string, number>;
  const detail = {
    interested: row.rdInterested ?? 0,
    meetingBooked: row.rdMeetingBooked ?? 0,
    closed: row.rdClosed ?? 0,
    notInterested: row.rdNotInterested ?? 0,
    wrongPerson: row.rdWrongPerson ?? 0,
    unsubscribe: row.rdUnsubscribe ?? 0,
    neutral: row.rdNeutral ?? 0,
    autoReply: row.rdAutoReply ?? 0,
    outOfOffice: row.rdOutOfOffice ?? 0,
  };
  const rsSent = row.rsSent ?? 0;
  const rsBounced = row.rsBounced ?? 0;
  const esSent = row.esSent ?? 0;
  const esBounced = row.esBounced ?? 0;
  return {
    recipientStats: {
      contacted,
      sent: rsSent,
      delivered: rsSent - rsBounced,
      opened: row.rsOpened ?? 0,
      bounced: rsBounced,
      clicked: row.rsClicked ?? 0,
      unsubscribed: row.rsUnsubscribed ?? 0,
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
  const { runIds: runIdsRaw, brandId, campaignId, workflowSlugs, featureSlugs, groupBy } = parsed.data;
  const runIds = runIdsRaw ? runIdsRaw.split(",").filter(Boolean) : undefined;
  const orgId = res.locals.orgId as string;

  // Build WHERE clauses — always scope by org from header
  const conditions: SQL[] = [sql`c.org_id = ${orgId}`];
  if (runIds?.length) conditions.push(sql`c.run_id IN (${sql.join(runIds.map((id) => sql`${id}`), sql`, `)})`);
  if (brandId) conditions.push(sql`${brandId} = ANY(c.brand_ids)`);
  if (campaignId) conditions.push(sql`(c.id = ${campaignId} OR c.campaign_id = ${campaignId})`);

  addSlugConditions(conditions, { workflowSlugs, featureSlugs });

  const whereClause = sql.join(conditions, sql` AND `);

  // Handle groupBy requests
  if (groupBy) {
    try {
      const groups = await queryGroupedStats(whereClause, groupBy);
      return res.json({ groups });
    } catch (error: any) {
      const msg = error.cause?.message ?? error.message ?? String(error);
      console.error(`[instantly-service] Failed to aggregate grouped stats: ${msg}`, error);
      return res.status(500).json({ error: "Failed to aggregate stats" });
    }
  }

  try {
    const { recipientStats, emailStats } = await queryStats(whereClause);

    // Per-step breakdown (secondary stats) — non-fatal; overall stats still return on failure
    let stepStats: Array<{
      step: number; sent: number; delivered: number; opened: number; bounced: number; clicked: number; unsubscribed: number;
      repliesPositive: number; repliesNegative: number; repliesNeutral: number; repliesAutoReply: number;
      repliesDetail: typeof ZERO_REPLIES_DETAIL;
    }> = [];
    try {
      const stepResult = await db.execute(sql`
        SELECT
          e.step,
          COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'email_sent'), 0)::int AS "sent",
          COALESCE(COUNT(DISTINCT e.lead_email) FILTER (WHERE e.event_type = 'email_opened'), 0)::int AS "opened",
          COALESCE(COUNT(DISTINCT e.lead_email) FILTER (WHERE e.event_type = 'email_link_clicked'), 0)::int AS "clicked",
          COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'email_bounced'), 0)::int AS "bounced",
          COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_unsubscribed'), 0)::int AS "unsubscribed",
          COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_interested'), 0)::int AS "rdInterested",
          COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_meeting_booked'), 0)::int AS "rdMeetingBooked",
          COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_closed'), 0)::int AS "rdClosed",
          COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_not_interested'), 0)::int AS "rdNotInterested",
          COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_wrong_person'), 0)::int AS "rdWrongPerson",
          COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_unsubscribed'), 0)::int AS "rdUnsubscribe",
          COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_neutral'), 0)::int AS "rdNeutral",
          COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'auto_reply_received'), 0)::int AS "rdAutoReply",
          COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_out_of_office'), 0)::int AS "rdOutOfOffice"
        FROM instantly_events e
        JOIN instantly_campaigns c ON c.instantly_campaign_id = e.campaign_id
        WHERE ${whereClause}
          AND ${internalExclusionClause()}
          AND e.step IS NOT NULL
        GROUP BY e.step
        ORDER BY e.step
      `);
      const stepRows = Array.isArray(stepResult) ? stepResult : (stepResult as any).rows ?? [];
      stepStats = stepRows.map((sr: any) => {
        const detail = {
          interested: sr.rdInterested ?? 0,
          meetingBooked: sr.rdMeetingBooked ?? 0,
          closed: sr.rdClosed ?? 0,
          notInterested: sr.rdNotInterested ?? 0,
          wrongPerson: sr.rdWrongPerson ?? 0,
          unsubscribe: sr.rdUnsubscribe ?? 0,
          neutral: sr.rdNeutral ?? 0,
          autoReply: sr.rdAutoReply ?? 0,
          outOfOffice: sr.rdOutOfOffice ?? 0,
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
    } catch (stepError: any) {
      console.error(`[instantly-service] Step query failed (overall stats still returned): ${stepError.cause?.message ?? stepError.message}`);
    }

    res.json({
      recipientStats,
      emailStats: {
        ...emailStats,
        ...(stepStats.length > 0 && { stepStats }),
      },
    });
  } catch (error: any) {
    const msg = error.cause?.message ?? error.message ?? String(error);
    console.error(`[instantly-service] Failed to aggregate stats: ${msg}`, error);
    res.status(500).json({ error: "Failed to aggregate stats" });
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
