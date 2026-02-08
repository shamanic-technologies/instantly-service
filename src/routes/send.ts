import { Router, Request, Response } from "express";
import { db } from "../db";
import { organizations, instantlyCampaigns, instantlyLeads } from "../db/schema";
import { eq } from "drizzle-orm";
import {
  createCampaign as createInstantlyCampaign,
  addLeads as addInstantlyLeads,
  updateCampaignStatus,
  Lead,
} from "../lib/instantly-client";
import {
  ensureOrganization,
  createRun,
  updateRun,
  addCosts,
} from "../lib/runs-client";
import { SendRequestSchema } from "../schemas";

const router = Router();

async function getOrCreateOrganization(clerkOrgId: string): Promise<string> {
  const [existing] = await db
    .select()
    .from(organizations)
    .where(eq(organizations.clerkOrgId, clerkOrgId));

  if (existing) {
    return existing.id;
  }

  const [created] = await db
    .insert(organizations)
    .values({ clerkOrgId })
    .returning();

  return created.id;
}

async function getOrCreateCampaign(
  campaignId: string,
  organizationId: string | null,
  email: { subject: string; body: string },
  runId: string,
  clerkOrgId: string | undefined,
  brandId: string,
  appId: string
): Promise<{ id: string; instantlyCampaignId: string; isNew: boolean }> {
  const [existing] = await db
    .select()
    .from(instantlyCampaigns)
    .where(eq(instantlyCampaigns.id, campaignId));

  if (existing) {
    return {
      id: existing.id,
      instantlyCampaignId: existing.instantlyCampaignId,
      isNew: false,
    };
  }

  const instantlyCampaign = await createInstantlyCampaign({
    name: `Campaign ${campaignId}`,
    email,
  });

  const [created] = await db
    .insert(instantlyCampaigns)
    .values({
      id: campaignId,
      instantlyCampaignId: instantlyCampaign.id,
      name: `Campaign ${campaignId}`,
      status: instantlyCampaign.status,
      orgId: organizationId,
      clerkOrgId,
      brandId,
      appId,
      runId,
    })
    .returning();

  return {
    id: created.id,
    instantlyCampaignId: created.instantlyCampaignId,
    isNew: true,
  };
}

/**
 * POST /send
 * Add a lead to a campaign and send email via Instantly
 */
router.post("/", async (req: Request, res: Response) => {
  const parsed = SendRequestSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({
      error: "Invalid request",
      details: parsed.error.flatten(),
    });
  }
  const body = parsed.data;

  try {
    // 1. Get or create organization (only if orgId provided)
    let organizationId: string | null = null;
    if (body.orgId) {
      organizationId = await getOrCreateOrganization(body.orgId);
    }

    // 2. Create run in runs-service (only if orgId provided)
    let sendRun: { id: string } | null = null;
    if (body.orgId) {
      const runsOrgId = await ensureOrganization(body.orgId);
      sendRun = await createRun({
        organizationId: runsOrgId,
        serviceName: "instantly-service",
        taskName: "email-send",
        parentRunId: body.runId,
      });
    }

    try {
      // 3. Get or create campaign
      const campaign = await getOrCreateCampaign(
        body.campaignId,
        organizationId,
        body.email,
        body.runId,
        body.orgId,
        body.brandId,
        body.appId
      );

      // 4. Add lead to campaign
      const lead: Lead = {
        email: body.to,
        first_name: body.firstName,
        last_name: body.lastName,
        company_name: body.company,
        variables: body.variables,
      };

      const result = await addInstantlyLeads({
        campaign_id: campaign.instantlyCampaignId,
        leads: [lead],
      });

      // 5. Save lead to database
      const [savedLead] = await db
        .insert(instantlyLeads)
        .values({
          instantlyCampaignId: campaign.instantlyCampaignId,
          email: body.to,
          firstName: body.firstName,
          lastName: body.lastName,
          companyName: body.company,
          customVariables: body.variables,
          orgId: organizationId,
          runId: sendRun?.id,
        })
        .onConflictDoNothing()
        .returning();

      // 6. Activate campaign if new
      if (campaign.isNew) {
        await updateCampaignStatus(campaign.instantlyCampaignId, "active");
        await db
          .update(instantlyCampaigns)
          .set({ status: "active", updatedAt: new Date() })
          .where(eq(instantlyCampaigns.id, campaign.id));
      }

      // 7. Log costs and complete run (only if tracking)
      if (sendRun) {
        await addCosts(sendRun.id, [
          { costName: "instantly-lead-add", quantity: 1 },
        ]);
        if (campaign.isNew) {
          await addCosts(sendRun.id, [
            { costName: "instantly-campaign-create", quantity: 1 },
          ]);
        }
        await updateRun(sendRun.id, "completed");
      }

      res.status(200).json({
        success: true,
        campaignId: campaign.id,
        leadId: savedLead?.id,
        added: result.added,
      });
    } catch (error: any) {
      if (sendRun) {
        await updateRun(sendRun.id, "failed", error.message);
      }
      throw error;
    }
  } catch (error: any) {
    console.error(`[send] Failed to send — to=${body.to} error="${error.message}"`);
    res.status(500).json({
      error: "Failed to send email",
      details: error.message,
    });
  }
});

export default router;
