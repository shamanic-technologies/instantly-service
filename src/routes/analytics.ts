import { Router, Request, Response } from "express";
import { db } from "../db";
import { instantlyAnalyticsSnapshots, instantlyCampaigns } from "../db/schema";
import { eq, inArray, and, type SQL } from "drizzle-orm";
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
    const [campaign] = await db
      .select()
      .from(instantlyCampaigns)
      .where(eq(instantlyCampaigns.id, campaignId));

    if (!campaign) {
      return res.status(404).json({ error: "Campaign not found" });
    }

    const analytics = await getCampaignAnalytics(campaign.instantlyCampaignId);

    if (!analytics) {
      return res.json({ analytics: null });
    }

    // Save snapshot
    await db.insert(instantlyAnalyticsSnapshots).values({
      campaignId: campaign.instantlyCampaignId,
      totalLeads: analytics.leads_count,
      contacted: analytics.contacted_count,
      opened: analytics.open_count,
      replied: analytics.reply_count,
      bounced: analytics.bounced_count,
      unsubscribed: analytics.unsubscribed_count,
      snapshotAt: new Date(),
      rawData: analytics,
    });

    res.json({ analytics });
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

/**
 * POST /stats
 * Aggregated stats by runIds (mirrors postmark /stats pattern)
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

  // Build filters
  const conditions: SQL[] = [];
  if (runIds?.length) conditions.push(inArray(instantlyCampaigns.runId, runIds));
  if (clerkOrgId) conditions.push(eq(instantlyCampaigns.clerkOrgId, clerkOrgId));
  if (brandId) conditions.push(eq(instantlyCampaigns.brandId, brandId));
  if (appId) conditions.push(eq(instantlyCampaigns.appId, appId));
  if (campaignId) conditions.push(eq(instantlyCampaigns.id, campaignId));

  if (conditions.length === 0) {
    return res.status(400).json({ error: "At least one filter required: runIds, clerkOrgId, brandId, appId, campaignId" });
  }

  try {
    const campaigns = await db
      .select()
      .from(instantlyCampaigns)
      .where(and(...conditions));

    if (campaigns.length === 0) {
      return res.json({
        totalCampaigns: 0,
        totalLeads: 0,
        contacted: 0,
        opened: 0,
        replied: 0,
        bounced: 0,
        unsubscribed: 0,
      });
    }

    // Fetch live analytics from Instantly for each campaign and save snapshots
    const analyticsResults = await Promise.all(
      campaigns.map(async (c) => {
        try {
          const analytics = await getCampaignAnalytics(c.instantlyCampaignId);
          if (!analytics) return null;

          // Save snapshot
          await db.insert(instantlyAnalyticsSnapshots).values({
            campaignId: c.instantlyCampaignId,
            totalLeads: analytics.leads_count,
            contacted: analytics.contacted_count,
            opened: analytics.open_count,
            replied: analytics.reply_count,
            bounced: analytics.bounced_count,
            unsubscribed: analytics.unsubscribed_count,
            snapshotAt: new Date(),
            rawData: analytics,
          });

          return analytics;
        } catch {
          return null;
        }
      })
    );

    // Aggregate
    const stats = {
      totalCampaigns: campaigns.length,
      totalLeads: 0,
      contacted: 0,
      opened: 0,
      replied: 0,
      bounced: 0,
      unsubscribed: 0,
    };

    for (const analytics of analyticsResults) {
      if (!analytics) continue;
      stats.totalLeads += analytics.leads_count;
      stats.contacted += analytics.contacted_count;
      stats.opened += analytics.open_count;
      stats.replied += analytics.reply_count;
      stats.bounced += analytics.bounced_count;
      stats.unsubscribed += analytics.unsubscribed_count;
    }

    res.json(stats);
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

export default router;
