import { errorString } from "../lib/db-error.js";
import { Router, Request, Response } from "express";
import { sql, type SQL } from "drizzle-orm";
import { EngagementLatencyGroupedRequestSchema, StatsQuerySchema } from "../schemas";
import {
  computeStatsPayload,
  queryGroupedStats,
  campaignIdNamesARow,
  readFamilySlice,
  statsFamilyPlan,
  featureFamilyPlan,
  queryEngagementLatencyGroups,
  addSlugConditions,
  timezoneCacheKeyPart,
} from "./analytics";
import { statsCacheKey, getOrSetCachedStats, STATS_CACHE_TTL_MS, STATS_REFRESH_AFTER_MS } from "../lib/stats-cache";
import { canonicalIanaTimezone, unrecognizedTimezoneFromError } from "../lib/timezone";

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
    const msg = error.cause?.message ?? error.message ?? errorString(error);
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
    workflowSlugs: Array.from(new Set(group.workflowSlugs ?? [])),
    ...(group.campaignIds ? { campaignIds: Array.from(new Set(group.campaignIds)) } : {}),
  }));

  try {
    const latencyGroups = await queryEngagementLatencyGroups(groups);
    return res.json({ groups: latencyGroups });
  } catch (error: any) {
    const msg = error.cause?.message ?? error.message ?? errorString(error);
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
  // Canonicalize before the cache key + SQL: a legacy alias the DB host's
  // tzdata lacks throws per row. See src/lib/timezone.ts.
  const timezone = canonicalIanaTimezone(parsed.data.timezone ?? "UTC");
  const runIds = runIdsRaw ? runIdsRaw.split(",").filter(Boolean) : undefined;

  const conditions: SQL[] = [];
  // The scope minus the campaign and the audience: what a family read splits by them.
  const familyConditions: SQL[] = [];
  // The scope minus the feature: what the feature family splits by it.
  const featureFamilyConditions: SQL[] = [];
  if (runIds?.length) {
    const runFilter = sql`c.run_id IN (${sql.join(runIds.map((id) => sql`${id}`), sql`, `)})`;
    conditions.push(runFilter);
    familyConditions.push(runFilter);
    featureFamilyConditions.push(runFilter);
  }
  if (brandId) {
    conditions.push(sql`${brandId} = ANY(c.brand_ids)`);
    familyConditions.push(sql`${brandId} = ANY(c.brand_ids)`);
    featureFamilyConditions.push(sql`${brandId} = ANY(c.brand_ids)`);
  }
  if (campaignId) conditions.push(sql`(c.id = ${campaignId} OR c.campaign_id = ${campaignId})`);
  if (goal) {
    conditions.push(sql`c.metadata->>'goal' = ${goal}`);
    familyConditions.push(sql`c.metadata->>'goal' = ${goal}`);
    featureFamilyConditions.push(sql`c.metadata->>'goal' = ${goal}`);
  }
  if (brandProfileId) {
    conditions.push(sql`c.metadata->>'brandProfileId' = ${brandProfileId}`);
    familyConditions.push(sql`c.metadata->>'brandProfileId' = ${brandProfileId}`);
    featureFamilyConditions.push(sql`c.metadata->>'brandProfileId' = ${brandProfileId}`);
  }
  if (audienceId) conditions.push(sql`c.metadata->>'audienceId' = ${audienceId}`);

  addSlugConditions(conditions, { workflowSlugs, featureSlugs });
  addSlugConditions(familyConditions, { workflowSlugs, featureSlugs });
  addSlugConditions(featureFamilyConditions, { workflowSlugs });

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
    goal,
    brandProfileId,
    audienceId,
    workflowSlugs,
    featureSlugs,
    groupBy,
    timezone: timezoneCacheKeyPart(groupBy, timezone),
  });
  try {
    const plan = statsFamilyPlan(groupBy, campaignId, audienceId);
    if (plan && !(campaignId && (await campaignIdNamesARow(campaignId)))) {
      const familyKey = statsCacheKey("public-stats-family", {
        runIds: runIdsRaw,
        brandId,
        goal,
        brandProfileId,
        workflowSlugs,
        featureSlugs,
        groupBy,
        partition: plan.partition.join(","),
      });
      const familyWhere = familyConditions.length > 0 ? sql.join(familyConditions, sql` AND `) : sql`TRUE`;
      const groups = await readFamilySlice(familyKey, familyWhere, "fleet", groupBy!, plan);
      return res.json({ groups });
    }
    const featurePlan = featureFamilyPlan(groupBy, featureSlugs, campaignId, audienceId);
    if (featurePlan) {
      const familyKey = statsCacheKey("public-stats-family", {
        runIds: runIdsRaw,
        brandId,
        goal,
        brandProfileId,
        workflowSlugs,
        groupBy,
        partition: featurePlan.partition.join(","),
      });
      const familyWhere = featureFamilyConditions.length > 0 ? sql.join(featureFamilyConditions, sql` AND `) : sql`TRUE`;
      const groups = await readFamilySlice(familyKey, familyWhere, "fleet", groupBy!, featurePlan);
      return res.json({ groups });
    }
    const payload = await getOrSetCachedStats(cacheKey, async () => {
      if (groupBy) {
        const groups = await queryGroupedStats(whereClause, "fleet", groupBy, timezone);
        return { groups };
      }
      return computeStatsPayload(whereClause, "fleet");
    }, STATS_CACHE_TTL_MS, { refreshAfterMs: STATS_REFRESH_AFTER_MS });
    return res.json(payload);
  } catch (error: any) {
    const msg = error.cause?.message ?? error.message ?? errorString(error);
    const badZone = unrecognizedTimezoneFromError(error);
    if (badZone) {
      console.error(
        `[instantly-service] public stats: database does not recognize timezone "${badZone}" (requested "${parsed.data.timezone}")`,
      );
      return res.status(400).json({
        error: "Invalid request",
        details: {
          fieldErrors: {
            timezone: [`Timezone not supported by the database: ${badZone}`],
          },
        },
      });
    }
    console.error(`[instantly-service] Failed to aggregate stats: ${msg}`, error);
    return res.status(500).json({ error: "Failed to aggregate stats", detail: msg });
  }
});

export default router;
