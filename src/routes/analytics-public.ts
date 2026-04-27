import { Router, Request, Response } from "express";
import { db } from "../db";
import { sql, type SQL } from "drizzle-orm";
import { StatsQuerySchema } from "../schemas";
import { queryStats, queryGroupedStats, internalExclusionClause, addDynastyConditions, buildRepliesFromDetail, ZERO_REPLIES_DETAIL } from "./analytics";
import {
  fetchWorkflowDynasties,
  fetchFeatureDynasties,
  buildSlugToDynastyMap,
} from "../lib/dynasty-client";

const router = Router();

const ZERO_RECIPIENT_STATS = {
  contacted: 0,
  sent: 0,
  delivered: 0,
  opened: 0,
  bounced: 0,
  clicked: 0,
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
};

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
    return res.json({ recipientStats: { ...ZERO_RECIPIENT_STATS }, emailStats: { ...ZERO_EMAIL_STATS } });
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
      console.error(`[instantly-service] Failed to aggregate grouped stats: ${msg}`, error);
      return res.status(500).json({ error: "Failed to aggregate stats" });
    }
  }

  try {
    const { recipientStats, emailStats } = await queryStats(whereClause);

    let stepStats: Array<{
      step: number; sent: number; delivered: number; opened: number; bounced: number; clicked: number;
      repliesPositive: number; repliesNegative: number; repliesNeutral: number; repliesAutoReply: number;
      repliesDetail: typeof ZERO_REPLIES_DETAIL;
    }> = [];
    try {
      const stepResult = await db.execute(sql`
        SELECT
          e.step,
          COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'email_sent'), 0)::int AS "sent",
          COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'email_opened'), 0)::int AS "opened",
          COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'email_link_clicked'), 0)::int AS "clicked",
          COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'email_bounced'), 0)::int AS "bounced",
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

export default router;
