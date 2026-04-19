import { Router, Request, Response } from "express";
import { db } from "../db";
import { instantlyLeads, instantlyCampaigns } from "../db/schema";
import { eq, or, inArray, count } from "drizzle-orm";

const router = Router();

/**
 * GET /campaigns/:campaignId/leads
 * Returns leads from all sub-campaigns matching the given campaignId.
 */
router.get("/:campaignId/leads", async (req: Request, res: Response) => {
  const { campaignId } = req.params;
  const limit = req.query.limit ? parseInt(req.query.limit as string) : undefined;
  const skip = parseInt(req.query.skip as string) || 0;

  try {
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

    const instantlyIds = campaigns.map((c) => c.instantlyCampaignId);
    const whereClause = inArray(instantlyLeads.instantlyCampaignId, instantlyIds);

    const [{ totalCount }] = await db
      .select({ totalCount: count() })
      .from(instantlyLeads)
      .where(whereClause);

    let query = db
      .select()
      .from(instantlyLeads)
      .where(whereClause)
      .offset(skip);

    const leads = limit !== undefined
      ? await query.limit(limit)
      : await query;

    res.json({ leads, count: leads.length, totalCount });
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

export default router;
