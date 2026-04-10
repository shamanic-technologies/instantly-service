import { Router, Request, Response } from "express";
import { db } from "../db";
import { sql, type SQL } from "drizzle-orm";
import { StatsQuerySchema, GroupedStatsRequestSchema } from "../schemas";
import {
  resolveWorkflowDynastySlugs,
  resolveFeatureDynastySlugs,
  fetchWorkflowDynasties,
  fetchFeatureDynasties,
  buildSlugToDynastyMap,
} from "../lib/dynasty-client";

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

const ZERO_STATS = {
  emailsContacted: 0,
  emailsSent: 0,
  emailsDelivered: 0,
  emailsOpened: 0,
  emailsClicked: 0,
  emailsReplied: 0,
  emailsBounced: 0,
  repliesInterested: 0,
  repliesMeetingBooked: 0,
  repliesClosed: 0,
  repliesNotInterested: 0,
  repliesNeutral: 0,
  repliesAutoReply: 0,
  repliesOutOfOffice: 0,
  repliesUnsubscribe: 0,
};

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
  // Dynasty groupBy uses the underlying slug column; re-keying happens post-query
  workflowDynastySlug: "c.workflow_slug",
  featureDynastySlug: "c.feature_slug",
};

/** SQL fragment for LATERAL unnest of brand_ids, used when groupBy=brandId */
const BRAND_LATERAL_JOIN = sql`CROSS JOIN LATERAL unnest(c.brand_ids) AS brand_id`;

/** Execute grouped stats query and return array of { key, stats, recipients } */
export async function queryGroupedStats(
  whereClause: SQL,
  groupBy: string,
  dynastyMap?: Map<string, string>,
): Promise<Array<{ key: string; stats: typeof ZERO_STATS; recipients: number }>> {
  const col = GROUP_BY_COLUMNS[groupBy];
  if (!col) return [];

  const groupCol = sql.raw(col);
  const lateralJoin = groupBy === "brandId" ? BRAND_LATERAL_JOIN : sql``;
  const result = await db.execute(sql`
    SELECT
      ${groupCol} AS "groupKey",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'email_sent'), 0)::int AS "emailsSent",
      COALESCE(
        COUNT(*) FILTER (WHERE e.event_type = 'email_sent')
        - COUNT(*) FILTER (WHERE e.event_type = 'email_bounced'),
      0)::int AS "emailsDelivered",
      COALESCE(COUNT(DISTINCT e.lead_email) FILTER (WHERE e.event_type = 'email_opened'), 0)::int AS "emailsOpened",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'email_link_clicked'), 0)::int AS "emailsClicked",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'reply_received'), 0)::int AS "emailsReplied",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'email_bounced'), 0)::int AS "emailsBounced",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_interested'), 0)::int AS "repliesInterested",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_meeting_booked'), 0)::int AS "repliesMeetingBooked",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_closed'), 0)::int AS "repliesClosed",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_not_interested'), 0)::int AS "repliesNotInterested",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_neutral'), 0)::int AS "repliesNeutral",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'auto_reply_received'), 0)::int AS "repliesAutoReply",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_out_of_office'), 0)::int AS "repliesOutOfOffice",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_unsubscribed'), 0)::int AS "repliesUnsubscribe",
      COALESCE(COUNT(DISTINCT e.lead_email) FILTER (WHERE e.event_type = 'email_sent'), 0)::int AS "recipients"
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
  // For dynasty groupBy, use the underlying slug column for contacted counts too
  const underlyingGroupBy = groupBy === "workflowDynastySlug" ? "workflowSlug"
    : groupBy === "featureDynastySlug" ? "featureSlug"
    : groupBy;
  const contactedMap = await queryGroupedContactedCount(whereClause, underlyingGroupBy);

  const rawGroups = rows.map((row: any) => ({
    key: row.groupKey as string,
    stats: {
      emailsContacted: contactedMap.get(row.groupKey) ?? 0,
      emailsSent: row.emailsSent ?? 0,
      emailsDelivered: row.emailsDelivered ?? 0,
      emailsOpened: row.emailsOpened ?? 0,
      emailsClicked: row.emailsClicked ?? 0,
      emailsReplied: row.emailsReplied ?? 0,
      emailsBounced: row.emailsBounced ?? 0,
      repliesInterested: row.repliesInterested ?? 0,
      repliesMeetingBooked: row.repliesMeetingBooked ?? 0,
      repliesClosed: row.repliesClosed ?? 0,
      repliesNotInterested: row.repliesNotInterested ?? 0,
      repliesNeutral: row.repliesNeutral ?? 0,
      repliesAutoReply: row.repliesAutoReply ?? 0,
      repliesOutOfOffice: row.repliesOutOfOffice ?? 0,
      repliesUnsubscribe: row.repliesUnsubscribe ?? 0,
    },
    recipients: row.recipients ?? 0,
  }));

  // If dynasty groupBy, re-key and merge by dynasty slug
  if (dynastyMap && (groupBy === "workflowDynastySlug" || groupBy === "featureDynastySlug")) {
    return mergeDynastyGroups(rawGroups, dynastyMap);
  }

  return rawGroups;
}

/** Merge per-slug groups into dynasty groups using the reverse map */
function mergeDynastyGroups(
  groups: Array<{ key: string; stats: typeof ZERO_STATS; recipients: number }>,
  dynastyMap: Map<string, string>,
): Array<{ key: string; stats: typeof ZERO_STATS; recipients: number }> {
  const merged = new Map<string, { stats: typeof ZERO_STATS; recipients: number }>();

  for (const group of groups) {
    // Fall back to the raw slug if not found in the dynasty map
    const dynastyKey = dynastyMap.get(group.key) ?? group.key;
    const existing = merged.get(dynastyKey);
    if (existing) {
      existing.stats.emailsContacted += group.stats.emailsContacted;
      existing.stats.emailsSent += group.stats.emailsSent;
      existing.stats.emailsDelivered += group.stats.emailsDelivered;
      existing.stats.emailsOpened += group.stats.emailsOpened;
      existing.stats.emailsClicked += group.stats.emailsClicked;
      existing.stats.emailsReplied += group.stats.emailsReplied;
      existing.stats.emailsBounced += group.stats.emailsBounced;
      existing.stats.repliesInterested += group.stats.repliesInterested;
      existing.stats.repliesMeetingBooked += group.stats.repliesMeetingBooked;
      existing.stats.repliesClosed += group.stats.repliesClosed;
      existing.stats.repliesNotInterested += group.stats.repliesNotInterested;
      existing.stats.repliesNeutral += group.stats.repliesNeutral;
      existing.stats.repliesAutoReply += group.stats.repliesAutoReply;
      existing.stats.repliesOutOfOffice += group.stats.repliesOutOfOffice;
      existing.stats.repliesUnsubscribe += group.stats.repliesUnsubscribe;
      existing.recipients += group.recipients;
    } else {
      merged.set(dynastyKey, { stats: { ...group.stats }, recipients: group.recipients });
    }
  }

  return Array.from(merged.entries()).map(([key, value]) => ({
    key,
    ...value,
  }));
}

/** Execute the aggregate stats query and return { stats, recipients } */
export async function queryStats(whereClause: SQL): Promise<{ stats: typeof ZERO_STATS; recipients: number }> {
  const result = await db.execute(sql`
    SELECT
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'email_sent'), 0)::int AS "emailsSent",
      COALESCE(
        COUNT(*) FILTER (WHERE e.event_type = 'email_sent')
        - COUNT(*) FILTER (WHERE e.event_type = 'email_bounced'),
      0)::int AS "emailsDelivered",
      COALESCE(COUNT(DISTINCT e.lead_email) FILTER (WHERE e.event_type = 'email_opened'), 0)::int AS "emailsOpened",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'email_link_clicked'), 0)::int AS "emailsClicked",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'reply_received'), 0)::int AS "emailsReplied",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'email_bounced'), 0)::int AS "emailsBounced",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_interested'), 0)::int AS "repliesInterested",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_meeting_booked'), 0)::int AS "repliesMeetingBooked",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_closed'), 0)::int AS "repliesClosed",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_not_interested'), 0)::int AS "repliesNotInterested",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_neutral'), 0)::int AS "repliesNeutral",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'auto_reply_received'), 0)::int AS "repliesAutoReply",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_out_of_office'), 0)::int AS "repliesOutOfOffice",
      COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_unsubscribed'), 0)::int AS "repliesUnsubscribe",
      COALESCE(COUNT(DISTINCT e.lead_email) FILTER (WHERE e.event_type = 'email_sent'), 0)::int AS "recipients"
    FROM instantly_events e
    JOIN instantly_campaigns c ON c.instantly_campaign_id = e.campaign_id
    WHERE ${whereClause}
      AND ${internalExclusionClause()}
  `);
  const rows = Array.isArray(result) ? result : (result as any).rows ?? [];

  // Fetch contacted count in parallel (from campaigns table, not events)
  const emailsContacted = await queryContactedCount(whereClause);

  if (!rows.length) {
    return { stats: { ...ZERO_STATS, emailsContacted }, recipients: 0 };
  }

  const row = rows[0] as Record<string, number>;
  return {
    stats: {
      emailsContacted,
      emailsSent: row.emailsSent ?? 0,
      emailsDelivered: row.emailsDelivered ?? 0,
      emailsOpened: row.emailsOpened ?? 0,
      emailsClicked: row.emailsClicked ?? 0,
      emailsReplied: row.emailsReplied ?? 0,
      emailsBounced: row.emailsBounced ?? 0,
      repliesInterested: row.repliesInterested ?? 0,
      repliesMeetingBooked: row.repliesMeetingBooked ?? 0,
      repliesClosed: row.repliesClosed ?? 0,
      repliesNotInterested: row.repliesNotInterested ?? 0,
      repliesNeutral: row.repliesNeutral ?? 0,
      repliesAutoReply: row.repliesAutoReply ?? 0,
      repliesOutOfOffice: row.repliesOutOfOffice ?? 0,
      repliesUnsubscribe: row.repliesUnsubscribe ?? 0,
    },
    recipients: row.recipients ?? 0,
  };
}

/** Build optional headers for inter-service calls from request context */
function buildInterServiceHeaders(req: Request, res: Response): Record<string, string> {
  const headers: Record<string, string> = {};
  const orgId = res.locals.orgId as string | undefined;
  const userId = res.locals.userId as string | undefined;
  const runId = res.locals.runId as string | undefined;
  if (orgId) headers["x-org-id"] = orgId;
  if (userId) headers["x-user-id"] = userId;
  if (runId) headers["x-run-id"] = runId;
  return headers;
}

/**
 * Resolve dynasty slug filters into slug arrays and add conditions.
 * Dynasty filters take priority over exact slug filters for the same dimension.
 * Returns true if a dynasty resolved to empty (= zero results), meaning we should short-circuit.
 */
export async function addDynastyConditions(
  conditions: SQL[],
  data: {
    workflowSlugs?: string;
    featureSlugs?: string;
    workflowDynastySlug?: string;
    featureDynastySlug?: string;
  },
  headers?: Record<string, string>,
): Promise<boolean> {
  // Workflow dimension: dynasty takes priority over plural slugs
  if (data.workflowDynastySlug) {
    const slugs = await resolveWorkflowDynastySlugs(data.workflowDynastySlug, headers);
    if (slugs.length === 0) return true; // empty dynasty → zero stats
    conditions.push(sql`c.workflow_slug IN (${sql.join(slugs.map((s) => sql`${s}`), sql`, `)})`);
  } else if (data.workflowSlugs) {
    const slugs = data.workflowSlugs.split(",").filter(Boolean);
    if (slugs.length > 0) {
      conditions.push(sql`c.workflow_slug IN (${sql.join(slugs.map((s) => sql`${s}`), sql`, `)})`);
    }
  }

  // Feature dimension: dynasty takes priority over plural slugs
  if (data.featureDynastySlug) {
    const slugs = await resolveFeatureDynastySlugs(data.featureDynastySlug, headers);
    if (slugs.length === 0) return true; // empty dynasty → zero stats
    conditions.push(sql`c.feature_slug IN (${sql.join(slugs.map((s) => sql`${s}`), sql`, `)})`);
  } else if (data.featureSlugs) {
    const slugs = data.featureSlugs.split(",").filter(Boolean);
    if (slugs.length > 0) {
      conditions.push(sql`c.feature_slug IN (${sql.join(slugs.map((s) => sql`${s}`), sql`, `)})`);
    }
  }

  return false;
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
  const { runIds: runIdsRaw, brandId, campaignId, workflowSlugs, featureSlugs, workflowDynastySlug, featureDynastySlug, groupBy } = parsed.data;
  const runIds = runIdsRaw ? runIdsRaw.split(",").filter(Boolean) : undefined;
  const orgId = res.locals.orgId as string;

  // Build WHERE clauses — always scope by org from header
  const conditions: SQL[] = [sql`c.org_id = ${orgId}`];
  if (runIds?.length) conditions.push(sql`c.run_id IN (${sql.join(runIds.map((id) => sql`${id}`), sql`, `)})`);
  if (brandId) conditions.push(sql`${brandId} = ANY(c.brand_ids)`);
  if (campaignId) conditions.push(sql`(c.id = ${campaignId} OR c.campaign_id = ${campaignId})`);

  const interServiceHeaders = buildInterServiceHeaders(req, res);
  const emptyDynasty = await addDynastyConditions(
    conditions,
    { workflowSlugs, featureSlugs, workflowDynastySlug, featureDynastySlug },
    interServiceHeaders,
  );

  if (emptyDynasty) {
    if (groupBy) return res.json({ groups: [] });
    return res.json({ stats: { ...ZERO_STATS }, recipients: 0 });
  }

  const whereClause = sql.join(conditions, sql` AND `);

  // Handle groupBy requests
  if (groupBy) {
    try {
      let dynastyMap: Map<string, string> | undefined;
      if (groupBy === "workflowDynastySlug") {
        const dynasties = await fetchWorkflowDynasties(interServiceHeaders);
        dynastyMap = buildSlugToDynastyMap(dynasties);
      } else if (groupBy === "featureDynastySlug") {
        const dynasties = await fetchFeatureDynasties(interServiceHeaders);
        dynastyMap = buildSlugToDynastyMap(dynasties);
      }
      const groups = await queryGroupedStats(whereClause, groupBy, dynastyMap);
      return res.json({ groups });
    } catch (error: any) {
      const msg = error.cause?.message ?? error.message ?? String(error);
      console.error(`[instantly-service] Failed to aggregate grouped stats: ${msg}`, error);
      return res.status(500).json({ error: "Failed to aggregate stats" });
    }
  }

  try {
    const { stats, recipients } = await queryStats(whereClause);

    // Per-step breakdown (secondary stats) — non-fatal; overall stats still return on failure
    let stepStats: { step: number; emailsSent: number; emailsOpened: number; emailsReplied: number; repliesInterested: number; repliesNeutral: number; repliesNotInterested: number; emailsBounced: number }[] = [];
    try {
      const stepResult = await db.execute(sql`
        SELECT
          e.step,
          COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'email_sent'), 0)::int AS "emailsSent",
          COALESCE(COUNT(DISTINCT e.lead_email) FILTER (WHERE e.event_type = 'email_opened'), 0)::int AS "emailsOpened",
          COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'reply_received'), 0)::int AS "emailsReplied",
          COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_interested'), 0)::int AS "repliesInterested",
          COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_neutral'), 0)::int AS "repliesNeutral",
          COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_not_interested'), 0)::int AS "repliesNotInterested",
          COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'email_bounced'), 0)::int AS "emailsBounced"
        FROM instantly_events e
        JOIN instantly_campaigns c ON c.instantly_campaign_id = e.campaign_id
        WHERE ${whereClause}
          AND ${internalExclusionClause()}
          AND e.step IS NOT NULL
        GROUP BY e.step
        ORDER BY e.step
      `);
      const stepRows = Array.isArray(stepResult) ? stepResult : (stepResult as any).rows ?? [];
      stepStats = stepRows.map((sr: any) => ({
        step: sr.step,
        emailsSent: sr.emailsSent ?? 0,
        emailsOpened: sr.emailsOpened ?? 0,
        emailsReplied: sr.emailsReplied ?? 0,
        repliesInterested: sr.repliesInterested ?? 0,
        repliesNeutral: sr.repliesNeutral ?? 0,
        repliesNotInterested: sr.repliesNotInterested ?? 0,
        emailsBounced: sr.emailsBounced ?? 0,
      }));
    } catch (stepError: any) {
      console.error(`[instantly-service] Step query failed (overall stats still returned): ${stepError.cause?.message ?? stepError.message}`);
    }

    res.json({
      stats,
      recipients,
      ...(stepStats.length > 0 && { stepStats }),
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
        const { stats, recipients } = await queryStats(whereClause);
        return { key, stats, recipients };
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
