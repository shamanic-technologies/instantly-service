import { Router, Request, Response } from "express";
import { db } from "../db";
import { instantlyAnalyticsSnapshots, instantlyCampaigns } from "../db/schema";
import { eq, or, sql, type SQL } from "drizzle-orm";
import { getCampaignAnalytics } from "../lib/instantly-client";
import { StatsRequestSchema } from "../schemas";

const router = Router();

/**
 * GET /campaigns/:campaignId/analytics
 * Fetch from Instantly API and save snapshot
 */
router.get("/:campaignId/analytics", async (req: Request, res: Response) => {
  const { campaignId } = req.params;

  try {
    // Look up by id (direct-created campaigns) or campaignId (send-created)
    const campaigns = await db
      .select()
      .from(instantlyCampaigns)
      .where(
        or(
          eq(instantlyCampaigns.id, campaignId),
          eq(instantlyCampaigns.campaignId, campaignId),
        ),
      );

    if (campaigns.length === 0) {
      return res.status(404).json({ error: "Campaign not found" });
    }

    // Aggregate analytics across all sub-campaigns
    let aggregated = {
      total_leads: 0, contacted: 0, opened: 0, replied: 0,
      bounced: 0, unsubscribed: 0,
    };
    let found = false;

    for (const campaign of campaigns) {
      const analytics = await getCampaignAnalytics(campaign.instantlyCampaignId);
      if (!analytics) continue;
      found = true;

      // Save snapshot per sub-campaign
      await db.insert(instantlyAnalyticsSnapshots).values({
        campaignId: campaign.instantlyCampaignId,
        totalLeads: analytics.leads_count,
        contacted: analytics.contacted_count,
        opened: analytics.open_count_unique,
        replied: analytics.reply_count,
        bounced: analytics.bounced_count,
        unsubscribed: analytics.unsubscribed_count,
        snapshotAt: new Date(),
        rawData: analytics,
      });

      // Use open_count_unique (unique recipients who opened) instead of
      // open_count (total open events including repeat opens)
      aggregated.total_leads += analytics.leads_count;
      aggregated.contacted += analytics.contacted_count;
      aggregated.opened += analytics.open_count_unique;
      aggregated.replied += analytics.reply_count;
      aggregated.bounced += analytics.bounced_count;
      aggregated.unsubscribed += analytics.unsubscribed_count;
    }

    res.json({ analytics: found ? aggregated : null });
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

/**
 * POST /stats
 * Aggregated stats from webhook events (mirrors postmark /stats pattern)
 */
router.post("/stats", async (req: Request, res: Response) => {
  const parsed = StatsRequestSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({
      error: "Invalid request",
      details: parsed.error.flatten(),
    });
  }
  const { runIds, clerkOrgId, brandId, appId, campaignId } = parsed.data;

  // Build WHERE clauses for campaign filters
  const conditions: SQL[] = [];
  if (runIds?.length) conditions.push(sql`c.run_id = ANY(${runIds})`);
  if (clerkOrgId) conditions.push(sql`c.clerk_org_id = ${clerkOrgId}`);
  if (brandId) conditions.push(sql`c.brand_id = ${brandId}`);
  if (appId) conditions.push(sql`c.app_id = ${appId}`);
  if (campaignId) conditions.push(sql`(c.id = ${campaignId} OR c.campaign_id = ${campaignId})`);

  if (conditions.length === 0) {
    return res.status(400).json({ error: "At least one filter required: runIds, clerkOrgId, brandId, appId, campaignId" });
  }

  const whereClause = sql.join(conditions, sql` AND `);

  const zeroStats = {
    stats: {
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
    },
    recipients: 0,
  };

  try {
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
        COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'auto_reply_received'), 0)::int AS "repliesAutoReply",
        COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_not_interested'), 0)::int AS "repliesNotInterested",
        COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_out_of_office'), 0)::int AS "repliesOutOfOffice",
        COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_unsubscribed'), 0)::int AS "repliesUnsubscribe",
        COALESCE(COUNT(DISTINCT e.lead_email) FILTER (WHERE e.event_type = 'email_sent'), 0)::int AS "recipients"
      FROM instantly_events e
      JOIN instantly_campaigns c ON c.instantly_campaign_id = e.campaign_id
      WHERE ${whereClause}
    `);
    const rows = Array.isArray(result) ? result : (result as any).rows ?? [];

    if (!rows.length) {
      return res.json(zeroStats);
    }

    const row = rows[0] as Record<string, number>;

    res.json({
      stats: {
        emailsSent: row.emailsSent ?? 0,
        emailsDelivered: row.emailsDelivered ?? 0,
        emailsOpened: row.emailsOpened ?? 0,
        emailsClicked: row.emailsClicked ?? 0,
        emailsReplied: row.emailsReplied ?? 0,
        emailsBounced: row.emailsBounced ?? 0,
        repliesAutoReply: row.repliesAutoReply ?? 0,
        repliesNotInterested: row.repliesNotInterested ?? 0,
        repliesOutOfOffice: row.repliesOutOfOffice ?? 0,
        repliesUnsubscribe: row.repliesUnsubscribe ?? 0,
      },
      recipients: row.recipients ?? 0,
    });
  } catch (error: any) {
    console.error(`[stats] Failed to aggregate stats: ${error.message}`);
    res.status(500).json({ error: error.message });
  }
});

export default router;
