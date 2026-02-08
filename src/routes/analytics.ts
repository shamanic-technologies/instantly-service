import { Router, Request, Response } from "express";
import { db } from "../db";
import { instantlyAnalyticsSnapshots, instantlyCampaigns } from "../db/schema";
import { eq, inArray, and, type SQL } from "drizzle-orm";
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
  // #swagger.summary = 'Get aggregated stats by filters'
  // #swagger.description = 'Aggregates Instantly campaign analytics across campaigns matching the provided filters. At least one filter is required.'
  /* #swagger.requestBody = {
    required: true,
    content: {
      "application/json": {
        schema: {
          type: "object",
          properties: {
            runIds: { type: "array", items: { type: "string" }, description: "Filter by run IDs" },
            clerkOrgId: { type: "string", description: "Filter by Clerk organization ID" },
            brandId: { type: "string", description: "Filter by brand ID" },
            appId: { type: "string", description: "Filter by app ID" },
            campaignId: { type: "string", description: "Filter by campaign ID" }
          }
        }
      }
    }
  } */
  /* #swagger.responses[200] = {
    description: "Aggregated campaign stats",
    content: {
      "application/json": {
        schema: {
          type: "object",
          properties: {
            totalCampaigns: { type: "integer", description: "Number of campaigns matched" },
            totalLeads: { type: "integer", description: "Total leads across campaigns" },
            contacted: { type: "integer", description: "Total contacted leads" },
            opened: { type: "integer", description: "Total opens" },
            replied: { type: "integer", description: "Total replies" },
            bounced: { type: "integer", description: "Total bounces" },
            unsubscribed: { type: "integer", description: "Total unsubscribes" }
          }
        }
      }
    }
  } */
  /* #swagger.responses[400] = {
    description: "No filter provided",
    content: {
      "application/json": {
        schema: {
          type: "object",
          properties: {
            error: { type: "string", example: "At least one filter required: runIds, clerkOrgId, brandId, appId, campaignId" }
          }
        }
      }
    }
  } */
  const { runIds, clerkOrgId, brandId, appId, campaignId } = req.body as {
    runIds?: string[];
    clerkOrgId?: string;
    brandId?: string;
    appId?: string;
    campaignId?: string;
  };

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
