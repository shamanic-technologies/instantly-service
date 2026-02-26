import { Router, Request, Response } from "express";
import { db } from "../db";
import { instantlyLeads, instantlyCampaigns } from "../db/schema";
import { eq, and, or, inArray } from "drizzle-orm";
import {
  addLeads as addInstantlyLeads,
  listLeads as listInstantlyLeads,
  deleteLeads as deleteInstantlyLeads,
  Lead,
} from "../lib/instantly-client";
import {
  createRun,
  updateRun,
  addCosts,
} from "../lib/runs-client";
import { resolveInstantlyApiKey, KeyServiceError } from "../lib/key-client";
import { AddLeadsRequestSchema, DeleteLeadsRequestSchema } from "../schemas";

const router = Router();

/**
 * POST /campaigns/:campaignId/leads
 * Add leads to a campaign (BLOCKING runs-service)
 */
router.post("/:campaignId/leads", async (req: Request, res: Response) => {
  const { campaignId } = req.params;

  const parsed = AddLeadsRequestSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({
      error: "Invalid request",
      details: parsed.error.flatten(),
    });
  }
  const body = parsed.data;

  try {
    // Get campaign first (need orgId for key resolution)
    const [campaign] = await db
      .select()
      .from(instantlyCampaigns)
      .where(
        or(
          eq(instantlyCampaigns.id, campaignId),
          eq(instantlyCampaigns.campaignId, campaignId),
        ),
      );

    if (!campaign) {
      return res.status(404).json({ error: "Campaign not found" });
    }

    // Resolve Instantly API key (BYOK per-org)
    const apiKey = await resolveInstantlyApiKey(campaign.orgId, {
      method: "POST",
      path: "/campaigns/:campaignId/leads",
    });

    // 1. Create run in runs-service FIRST (BLOCKING)
    const run = await createRun({
      orgId: body.orgId,
      appId: campaign.appId,
      serviceName: "instantly-service",
      taskName: "leads-add",
      brandId: campaign.brandId,
      parentRunId: body.runId,
    });

    try {
      // 2. Add leads to Instantly
      const instantlyLeadsList: Lead[] = body.leads.map((l) => ({
        email: l.email,
        first_name: l.firstName,
        last_name: l.lastName,
        company_name: l.companyName,
        variables: l.customVariables,
      }));

      const result = await addInstantlyLeads(apiKey, {
        campaign_id: campaign.instantlyCampaignId,
        leads: instantlyLeadsList,
      });

      // 3. Record in database
      const leadsToInsert = body.leads.map((l) => ({
        instantlyCampaignId: campaign.instantlyCampaignId,
        email: l.email,
        firstName: l.firstName,
        lastName: l.lastName,
        companyName: l.companyName,
        customVariables: l.customVariables,
        orgId: body.orgId,
        runId: run.id,
      }));

      await db
        .insert(instantlyLeads)
        .values(leadsToInsert)
        .onConflictDoNothing();

      // 4. Log costs and complete run
      await addCosts(run.id, [
        { costName: "instantly-lead-add", quantity: body.leads.length },
      ]);
      await updateRun(run.id, "completed");

      res.status(201).json({
        success: true,
        added: result.added,
        total: body.leads.length,
      });
    } catch (error: any) {
      await updateRun(run.id, "failed", error.message);
      throw error;
    }
  } catch (error: any) {
    if (error instanceof KeyServiceError && error.statusCode === 404) {
      return res.status(422).json({
        error: "BYOK key not configured for this organization",
        details: "Please configure your Instantly API key before adding leads.",
      });
    }
    console.error(`[leads] Failed to add leads: ${error.message}`);
    res.status(500).json({
      error: "Failed to add leads",
      details: error.message,
    });
  }
});

/**
 * GET /campaigns/:campaignId/leads
 * Returns leads from all sub-campaigns matching the given campaignId.
 */
router.get("/:campaignId/leads", async (req: Request, res: Response) => {
  const { campaignId } = req.params;
  const limit = parseInt(req.query.limit as string) || 100;
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
    const leads = await db
      .select()
      .from(instantlyLeads)
      .where(inArray(instantlyLeads.instantlyCampaignId, instantlyIds))
      .limit(limit)
      .offset(skip);

    res.json({ leads, count: leads.length });
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

/**
 * DELETE /campaigns/:campaignId/leads
 */
router.delete("/:campaignId/leads", async (req: Request, res: Response) => {
  const { campaignId } = req.params;

  const parsed = DeleteLeadsRequestSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: "emails array required" });
  }
  const { emails } = parsed.data;

  try {
    // Get campaign first (need orgId for key resolution)
    const [campaign] = await db
      .select()
      .from(instantlyCampaigns)
      .where(
        or(
          eq(instantlyCampaigns.id, campaignId),
          eq(instantlyCampaigns.campaignId, campaignId),
        ),
      );

    if (!campaign) {
      return res.status(404).json({ error: "Campaign not found" });
    }

    // Resolve Instantly API key (BYOK per-org)
    const apiKey = await resolveInstantlyApiKey(campaign.orgId, {
      method: "DELETE",
      path: "/campaigns/:campaignId/leads",
    });

    // Delete from Instantly
    const result = await deleteInstantlyLeads(apiKey, campaign.instantlyCampaignId, emails);

    // Delete from DB
    for (const email of emails) {
      await db
        .delete(instantlyLeads)
        .where(
          and(
            eq(instantlyLeads.instantlyCampaignId, campaign.instantlyCampaignId),
            eq(instantlyLeads.email, email)
          )
        );
    }

    res.json({ success: true, deleted: result.deleted });
  } catch (error: any) {
    if (error instanceof KeyServiceError && error.statusCode === 404) {
      return res.status(422).json({
        error: "BYOK key not configured for this organization",
        details: "Please configure your Instantly API key before deleting leads.",
      });
    }
    res.status(500).json({ error: error.message });
  }
});

export default router;
