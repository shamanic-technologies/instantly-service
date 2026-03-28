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
  type TrackingHeaders,
} from "../lib/runs-client";
import { resolveInstantlyApiKey, KeyServiceError } from "../lib/key-client";
import { authorizeCreditSpend } from "../lib/billing-client";
import { AddLeadsRequestSchema, DeleteLeadsRequestSchema } from "../schemas";

/** Extract tracking headers from res.locals (set by identityHeaders middleware) */
function getTracking(res: Response): TrackingHeaders {
  const t: TrackingHeaders = {};
  if (res.locals.headerCampaignId) t.campaignId = res.locals.headerCampaignId;
  if (res.locals.headerBrandId) t.brandId = res.locals.headerBrandId;
  if (res.locals.headerWorkflowSlug) t.workflowSlug = res.locals.headerWorkflowSlug;
  if (res.locals.headerFeatureSlug) t.featureSlug = res.locals.headerFeatureSlug;
  return t;
}

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

    // Resolve Instantly API key using header identity
    const orgId = res.locals.orgId as string;
    const userId = res.locals.userId as string;
    const { key: apiKey, keySource } = await resolveInstantlyApiKey(orgId, userId, {
      method: "POST",
      path: "/campaigns/:campaignId/leads",
    });

    // Credit authorization (platform keys only)
    if (keySource === "platform") {
      const tracking = getTracking(res);
      const auth = await authorizeCreditSpend(
        [{ costName: "instantly-lead-add", quantity: body.leads.length }],
        "instantly-lead-add",
        {
          orgId,
          userId,
          runId: res.locals.runId as string,
          campaignId: tracking.campaignId,
          brandId: campaign.brandId ?? undefined,
          workflowSlug: tracking.workflowSlug,
          featureSlug: tracking.featureSlug,
        },
      );
      if (!auth.sufficient) {
        return res.status(402).json({
          error: "Insufficient credits",
          balance_cents: auth.balance_cents,
          required_cents: auth.required_cents,
        });
      }
    }

    // 1. Create run in runs-service FIRST (BLOCKING)
    const tracking = getTracking(res);
    const identity = { orgId, userId, runId: res.locals.runId as string, tracking };
    const run = await createRun({
      serviceName: "instantly-service",
      taskName: "leads-add",
      brandId: campaign.brandId,
    }, identity);

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
        orgId,
        runId: run.id,
      }));

      await db
        .insert(instantlyLeads)
        .values(leadsToInsert)
        .onConflictDoNothing();

      // 4. Log costs and complete run
      const runIdentity = { orgId, userId, runId: run.id, tracking };
      await addCosts(run.id, [
        { costName: "instantly-lead-add", quantity: body.leads.length, costSource: keySource },
      ], runIdentity);
      await updateRun(run.id, "completed", runIdentity);

      res.status(201).json({
        success: true,
        added: result.added,
        total: body.leads.length,
      });
    } catch (error: any) {
      await updateRun(run.id, "failed", { orgId, userId, runId: run.id }, error.message);
      throw error;
    }
  } catch (error: any) {
    if (error instanceof KeyServiceError && error.statusCode === 404) {
      return res.status(422).json({
        error: "API key not configured for this organization",
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

    // Resolve Instantly API key using header identity
    const orgId = res.locals.orgId as string;
    const userId = res.locals.userId as string;
    const { key: apiKey } = await resolveInstantlyApiKey(orgId, userId, {
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
        error: "API key not configured for this organization",
        details: "Please configure your Instantly API key before deleting leads.",
      });
    }
    res.status(500).json({ error: error.message });
  }
});

export default router;
