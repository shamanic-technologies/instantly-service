import { Router, Request, Response } from "express";
import { sql, type SQL } from "drizzle-orm";
import { EngagementLatencyGroupedRequestSchema, StatsQuerySchema } from "../schemas";
import {
  computeStatsPayload,
  queryGroupedStats,
  queryEngagementLatencyGroups,
  addSlugConditions,
} from "./analytics";
import { statsCacheKey, getCachedStats, setCachedStats } from "../lib/stats-cache";

const router = Router();

function parseWorkflowSlugs(raw: unknown): string[] | null {
  if (typeof raw !== "string") return null;
  const slugs = Array.from(
    new Set(raw.split(",").map((slug) => slug.trim()).filter(Boolean)),
  );
  return slugs.length > 0 ? slugs : null;
}

/**
 * GET /public/stats/engagement-latency
 * Public-safe latency aggregate for one workflow slug set.
 */
router.get("/stats/engagement-latency", async (req: Request, res: Response) => {
  if (req.query.groupBy !== undefined) {
    return res.status(400).json({
      error: "Query parameter 'groupBy' is not supported; pass workflowSlugs instead",
    });
  }

  const workflowSlugs = parseWorkflowSlugs(req.query.workflowSlugs);
  if (!workflowSlugs) {
    return res.status(400).json({ error: "Query parameter 'workflowSlugs' is required" });
  }

  try {
    const groups = await queryEngagementLatencyGroups([
      { key: "__total__", workflowSlugs },
    ]);
    const group = groups[0];
    return res.json({
      workflowSlugs: group.workflowSlugs,
      timeToFirstLinkClick: group.timeToFirstLinkClick,
      timeToFirstPositiveReply: group.timeToFirstPositiveReply,
    });
  } catch (error: any) {
    const msg = error.cause?.message ?? error.message ?? String(error);
    console.error(`[instantly-service] Failed to aggregate public engagement latency: ${msg}`, error);
    return res.status(500).json({ error: "Failed to aggregate engagement latency" });
  }
});

/**
 * POST /public/stats/engagement-latency/grouped
 * Public-safe latency aggregates for caller-owned workflow slug groups.
 */
router.post("/stats/engagement-latency/grouped", async (req: Request, res: Response) => {
  const parsed = EngagementLatencyGroupedRequestSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({
      error: "Invalid request",
      details: parsed.error.flatten(),
    });
  }

  const groups = Object.entries(parsed.data.groups).map(([key, group]) => ({
    key,
    workflowSlugs: Array.from(new Set(group.workflowSlugs)),
  }));

  try {
    const latencyGroups = await queryEngagementLatencyGroups(groups);
    return res.json({ groups: latencyGroups });
  } catch (error: any) {
    const msg = error.cause?.message ?? error.message ?? String(error);
    console.error(`[instantly-service] Failed to aggregate grouped public engagement latency: ${msg}`, error);
    return res.status(500).json({ error: "Failed to aggregate engagement latency" });
  }
});

/**
 * GET /public/stats
 * Same as GET /stats but without identity headers (no org scoping).
 * Used by leaderboard / landing pages with no user context.
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
  const timezone = parsed.data.timezone ?? "UTC";
  const runIds = runIdsRaw ? runIdsRaw.split(",").filter(Boolean) : undefined;

  const conditions: SQL[] = [];
  if (runIds?.length) conditions.push(sql`c.run_id IN (${sql.join(runIds.map((id) => sql`${id}`), sql`, `)})`);
  if (brandId) conditions.push(sql`${brandId} = ANY(c.brand_ids)`);
  if (campaignId) conditions.push(sql`(c.id = ${campaignId} OR c.campaign_id = ${campaignId})`);

  addSlugConditions(conditions, { workflowSlugs, featureSlugs });

  const whereClause = conditions.length > 0
    ? sql.join(conditions, sql` AND `)
    : sql`TRUE`;

  // Short-TTL cache: the no-filter public total is byte-identical for every
  // caller (leaderboard / landing). Without it, a burst of identical calls
  // re-aggregates the whole silver log against a tiny Neon compute and saturates
  // it. No org scope here (public, cross-org) so the key has no org prefix.
  const cacheKey = statsCacheKey("public-stats", {
    runIds: runIdsRaw,
    brandId,
    campaignId,
    workflowSlugs,
    featureSlugs,
    groupBy,
    timezone,
  });
  const cached = getCachedStats(cacheKey);
  if (cached) return res.json(cached);

  // Handle groupBy requests
  if (groupBy) {
    try {
      const groups = await queryGroupedStats(whereClause, groupBy, timezone);
      const payload = { groups };
      setCachedStats(cacheKey, payload);
      return res.json(payload);
    } catch (error: any) {
      const msg = error.cause?.message ?? error.message ?? String(error);
      console.error(`[instantly-service] Failed to aggregate grouped stats: ${msg}`, error);
      return res.status(500).json({ error: "Failed to aggregate stats" });
    }
  }

  try {
    const payload = await computeStatsPayload(whereClause);
    setCachedStats(cacheKey, payload);
    res.json(payload);
  } catch (error: any) {
    const msg = error.cause?.message ?? error.message ?? String(error);
    console.error(`[instantly-service] Failed to aggregate stats: ${msg}`, error);
    res.status(500).json({ error: "Failed to aggregate stats" });
  }
});

export default router;
