import { Router, Request, Response } from "express";
import { db } from "../db";
import { instantlyCampaigns } from "../db/schema";
import { eq } from "drizzle-orm";
import {
  createCampaign as createInstantlyCampaign,
  getCampaign as getInstantlyCampaign,
  updateCampaignStatus as updateInstantlyStatus,
} from "../lib/instantly-client";
import {
  ensureOrganization,
  createRun,
  updateRun,
  addCosts,
} from "../lib/runs-client";

const router = Router();

interface CreateCampaignRequest {
  orgId: string;
  clerkOrgId: string;
  brandId: string;
  appId: string;
  runId?: string;
  name: string;
  accountIds?: string[];
  metadata?: Record<string, unknown>;
}

/**
 * POST /campaigns
 * Create a campaign in Instantly + DB, log run via runs-service (BLOCKING)
 */
router.post("/", async (req: Request, res: Response) => {
  const body = req.body as CreateCampaignRequest;

  if (!body.orgId || !body.name || !body.clerkOrgId || !body.brandId || !body.appId) {
    return res.status(400).json({
      error: "Missing required fields",
      required: ["orgId", "name", "clerkOrgId", "brandId", "appId"],
    });
  }

  try {
    // 1. Create run in runs-service FIRST (BLOCKING)
    const runsOrgId = await ensureOrganization(body.orgId);
    const run = await createRun({
      organizationId: runsOrgId,
      serviceName: "instantly-service",
      taskName: "campaign-create",
      parentRunId: body.runId,
    });

    try {
      // 2. Create campaign in Instantly
      const instantlyCampaign = await createInstantlyCampaign({
        name: body.name,
        account_ids: body.accountIds,
      });

      // 3. Record in database
      const [campaign] = await db
        .insert(instantlyCampaigns)
        .values({
          instantlyCampaignId: instantlyCampaign.id,
          name: body.name,
          status: instantlyCampaign.status,
          orgId: body.orgId,
          clerkOrgId: body.clerkOrgId,
          brandId: body.brandId,
          appId: body.appId,
          runId: run.id,
          metadata: body.metadata,
        })
        .returning();

      // 4. Log costs and complete run
      await addCosts(run.id, [
        { costName: "instantly-campaign-create", quantity: 1 },
      ]);
      await updateRun(run.id, "completed");

      res.status(201).json({
        success: true,
        campaign: {
          id: campaign.id,
          instantlyCampaignId: campaign.instantlyCampaignId,
          name: campaign.name,
          status: campaign.status,
        },
      });
    } catch (error: any) {
      await updateRun(run.id, "failed", error.message);
      throw error;
    }
  } catch (error: any) {
    console.error(`[campaigns] Failed to create campaign: ${error.message}`);
    res.status(500).json({
      error: "Failed to create campaign",
      details: error.message,
    });
  }
});

/**
 * GET /campaigns/:campaignId
 */
router.get("/:campaignId", async (req: Request, res: Response) => {
  const { campaignId } = req.params;

  try {
    const [campaign] = await db
      .select()
      .from(instantlyCampaigns)
      .where(eq(instantlyCampaigns.id, campaignId));

    if (!campaign) {
      return res.status(404).json({ error: "Campaign not found" });
    }

    res.json({ campaign });
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

/**
 * GET /campaigns/by-org/:orgId
 */
router.get("/by-org/:orgId", async (req: Request, res: Response) => {
  const { orgId } = req.params;

  try {
    const campaigns = await db
      .select()
      .from(instantlyCampaigns)
      .where(eq(instantlyCampaigns.orgId, orgId));

    res.json({ campaigns });
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

/**
 * PATCH /campaigns/:campaignId/status
 */
router.patch("/:campaignId/status", async (req: Request, res: Response) => {
  const { campaignId } = req.params;
  const { status } = req.body as { status: "active" | "paused" | "completed" };

  if (!status || !["active", "paused", "completed"].includes(status)) {
    return res.status(400).json({
      error: "Invalid status",
      allowed: ["active", "paused", "completed"],
    });
  }

  try {
    const [campaign] = await db
      .select()
      .from(instantlyCampaigns)
      .where(eq(instantlyCampaigns.id, campaignId));

    if (!campaign) {
      return res.status(404).json({ error: "Campaign not found" });
    }

    // Update in Instantly
    await updateInstantlyStatus(campaign.instantlyCampaignId, status);

    // Update in DB
    const [updated] = await db
      .update(instantlyCampaigns)
      .set({ status, updatedAt: new Date() })
      .where(eq(instantlyCampaigns.id, campaignId))
      .returning();

    res.json({ campaign: updated });
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

export default router;
