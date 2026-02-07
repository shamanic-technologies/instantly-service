import { Router, Request, Response } from "express";
import { db } from "../db";
import { instantlyAnalyticsSnapshots, instantlyCampaigns } from "../db/schema";
import { eq, inArray } from "drizzle-orm";
import { getCampaignAnalytics } from "../lib/instantly-client";

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

    // Save snapshot
    await db.insert(instantlyAnalyticsSnapshots).values({
      campaignId: campaign.instantlyCampaignId,
      totalLeads: analytics.total_leads,
      contacted: analytics.contacted,
      opened: analytics.opened,
      replied: analytics.replied,
      bounced: analytics.bounced,
      unsubscribed: analytics.unsubscribed,
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
  const { runIds } = req.body as { runIds: string[] };

  if (!runIds || !Array.isArray(runIds)) {
    return res.status(400).json({ error: "runIds array required" });
  }

  try {
    const campaigns = await db
      .select()
      .from(instantlyCampaigns)
      .where(inArray(instantlyCampaigns.runId, runIds));

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

          // Save snapshot
          await db.insert(instantlyAnalyticsSnapshots).values({
            campaignId: c.instantlyCampaignId,
            totalLeads: analytics.total_leads,
            contacted: analytics.contacted,
            opened: analytics.opened,
            replied: analytics.replied,
            bounced: analytics.bounced,
            unsubscribed: analytics.unsubscribed,
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
      stats.totalLeads += analytics.total_leads;
      stats.contacted += analytics.contacted;
      stats.opened += analytics.opened;
      stats.replied += analytics.replied;
      stats.bounced += analytics.bounced;
      stats.unsubscribed += analytics.unsubscribed;
    }

    res.json(stats);
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

export default router;
