import { Router, Request, Response } from "express";
import { db } from "../db";
import { sql, type SQL } from "drizzle-orm";
import { StatsQuerySchema } from "../schemas";
import { queryStats, queryGroupedStats, internalExclusionClause, addDynastyConditions } from "./analytics";
import {
  fetchWorkflowDynasties,
  fetchFeatureDynasties,
  buildSlugToDynastyMap,
} from "../lib/dynasty-client";

const router = Router();

const ZERO_STATS = {
  emailsContacted: 0,
  emailsSent: 0,
  emailsDelivered: 0,
  emailsOpened: 0,
  emailsClicked: 0,
  emailsReplied: 0,
  emailsBounced: 0,
  repliesAutoReply: 0,
  repliesNotInterested: 0,
  repliesOutOfOffice: 0,
  repliesUnsubscribe: 0,
};

/**
 * GET /stats/public
 * Same as GET /stats but without identity headers (no org scoping).
 * Used by leaderboard / landing pages with no user context.
 */
router.get("/stats/public", async (req: Request, res: Response) => {
  const parsed = StatsQuerySchema.safeParse(req.query);
  if (!parsed.success) {
    return res.status(400).json({
      error: "Invalid request",
      details: parsed.error.flatten(),
    });
  }
  const { runIds: runIdsRaw, brandId, campaignId, workflowSlugs, featureSlugs, workflowDynastySlug, featureDynastySlug, groupBy } = parsed.data;
  const runIds = runIdsRaw ? runIdsRaw.split(",").filter(Boolean) : undefined;

  const conditions: SQL[] = [];
  if (runIds?.length) conditions.push(sql`c.run_id IN (${sql.join(runIds.map((id) => sql`${id}`), sql`, `)})`);
  if (brandId) conditions.push(sql`${brandId} = ANY(c.brand_ids)`);
  if (campaignId) conditions.push(sql`(c.id = ${campaignId} OR c.campaign_id = ${campaignId})`);

  // Public endpoint — pass available headers but don't require them
  const headers: Record<string, string> = {};
  const orgId = req.headers["x-org-id"] as string | undefined;
  const userId = req.headers["x-user-id"] as string | undefined;
  const runId = req.headers["x-run-id"] as string | undefined;
  if (orgId) headers["x-org-id"] = orgId;
  if (userId) headers["x-user-id"] = userId;
  if (runId) headers["x-run-id"] = runId;

  const emptyDynasty = await addDynastyConditions(
    conditions,
    { workflowSlugs, featureSlugs, workflowDynastySlug, featureDynastySlug },
    headers,
  );

  if (emptyDynasty) {
    if (groupBy) return res.json({ groups: [] });
    return res.json({ stats: { ...ZERO_STATS }, recipients: 0 });
  }

  const whereClause = conditions.length > 0
    ? sql.join(conditions, sql` AND `)
    : sql`TRUE`;

  // Handle groupBy requests
  if (groupBy) {
    try {
      let dynastyMap: Map<string, string> | undefined;
      if (groupBy === "workflowDynastySlug") {
        const dynasties = await fetchWorkflowDynasties(headers);
        dynastyMap = buildSlugToDynastyMap(dynasties);
      } else if (groupBy === "featureDynastySlug") {
        const dynasties = await fetchFeatureDynasties(headers);
        dynastyMap = buildSlugToDynastyMap(dynasties);
      }
      const groups = await queryGroupedStats(whereClause, groupBy, dynastyMap);
      return res.json({ groups });
    } catch (error: any) {
      const msg = error.cause?.message ?? error.message ?? String(error);
      console.error(`[stats/public] Failed to aggregate grouped stats: ${msg}`, error);
      return res.status(500).json({ error: "Failed to aggregate stats" });
    }
  }

  try {
    const { stats, recipients } = await queryStats(whereClause);

    let stepStats: { step: number; emailsSent: number; emailsOpened: number; emailsReplied: number; emailsBounced: number }[] = [];
    try {
      const stepResult = await db.execute(sql`
        SELECT
          e.step,
          COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'email_sent'), 0)::int AS "emailsSent",
          COALESCE(COUNT(DISTINCT e.lead_email) FILTER (WHERE e.event_type = 'email_opened'), 0)::int AS "emailsOpened",
          COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_interested'), 0)::int AS "emailsReplied",
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
        emailsBounced: sr.emailsBounced ?? 0,
      }));
    } catch (stepError: any) {
      console.error(`[stats/public] Step query failed (overall stats still returned): ${stepError.cause?.message ?? stepError.message}`);
    }

    res.json({
      stats,
      recipients,
      ...(stepStats.length > 0 && { stepStats }),
    });
  } catch (error: any) {
    const msg = error.cause?.message ?? error.message ?? String(error);
    console.error(`[stats/public] Failed to aggregate stats: ${msg}`, error);
    res.status(500).json({ error: "Failed to aggregate stats" });
  }
});

export default router;
