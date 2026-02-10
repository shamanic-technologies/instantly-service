import { Router, Request, Response } from "express";
import { db } from "../db";
import { organizations, instantlyCampaigns, instantlyLeads } from "../db/schema";
import { eq, and } from "drizzle-orm";
import {
  createCampaign as createInstantlyCampaign,
  updateCampaign as updateInstantlyCampaign,
  getCampaign as getInstantlyCampaign,
  addLeads as addInstantlyLeads,
  updateCampaignStatus,
  listAccounts,
  Lead,
} from "../lib/instantly-client";
import {
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
    console.log(`[send] Reusing existing campaign ${campaignId} → instantly=${existing.instantlyCampaignId}`);
    return {
      id: existing.id,
      instantlyCampaignId: existing.instantlyCampaignId,
      isNew: false,
    };
  }

  // Fetch available email accounts so Instantly can actually send
  const accounts = await listAccounts();
  const accountIds = accounts.map((a) => a.email);
  console.log(`[send] Found ${accounts.length} accounts: ${JSON.stringify(accountIds)}`);

  console.log(`[send] Creating new campaign ${campaignId} with subject="${email.subject}"`);
  const instantlyCampaign = await createInstantlyCampaign({
    name: `Campaign ${campaignId}`,
    email,
  });
  console.log(`[send] Created instantly campaign id=${instantlyCampaign.id} status=${instantlyCampaign.status}`);

  // Assign sending accounts via PATCH (V2 ignores account_ids in create body)
  if (accountIds.length > 0) {
    console.log(`[send] Assigning ${accountIds.length} accounts to campaign ${instantlyCampaign.id}`);
    await updateInstantlyCampaign(instantlyCampaign.id, {
      email_list: accountIds,
      bcc_list: ["kevin@mcpfactory.org"],
      open_tracking: true,
      link_tracking: true,
    });

    // Verify accounts were actually assigned
    const verified = await getInstantlyCampaign(instantlyCampaign.id) as unknown as Record<string, unknown>;
    console.log(`[send] Verify after PATCH — email_list=${JSON.stringify(verified.email_list)} bcc_list=${JSON.stringify(verified.bcc_list)} not_sending_status=${JSON.stringify(verified.not_sending_status)}`);
  }

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
  console.log(`[send] POST /send to=${body.to} campaignId=${body.campaignId} subject="${body.email.subject}"`);

  try {
    // 1. Get or create organization (only if orgId provided)
    let organizationId: string | null = null;
    if (body.orgId) {
      organizationId = await getOrCreateOrganization(body.orgId);
    }

    // 2. Create run in runs-service (only if orgId provided)
    let sendRun: { id: string } | null = null;
    if (body.orgId) {
      sendRun = await createRun({
        clerkOrgId: body.orgId,
        appId: body.appId,
        serviceName: "instantly-service",
        taskName: "email-send",
        brandId: body.brandId,
        campaignId: body.campaignId,
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

      // 4. Check if lead already exists in this campaign (avoid wasting uploaded contact slots)
      const [existingLead] = await db
        .select()
        .from(instantlyLeads)
        .where(
          and(
            eq(instantlyLeads.instantlyCampaignId, campaign.instantlyCampaignId),
            eq(instantlyLeads.email, body.to)
          )
        );

      let savedLead = existingLead;
      let added = 0;

      if (existingLead) {
        console.log(`[send] Lead ${body.to} already exists in campaign ${campaign.instantlyCampaignId}, skipping addLeads`);
      }

      if (!existingLead) {
        // 5. Add lead to campaign in Instantly
        const lead: Lead = {
          email: body.to,
          first_name: body.firstName,
          last_name: body.lastName,
          company_name: body.company,
          variables: body.variables,
        };

        console.log(`[send] Adding lead ${body.to} to instantly campaign ${campaign.instantlyCampaignId}`);
        const result = await addInstantlyLeads({
          campaign_id: campaign.instantlyCampaignId,
          leads: [lead],
        });
        added = result.added;
        console.log(`[send] addLeads result: added=${added}`);

        // 6. Save lead to database
        const [created] = await db
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

        if (created) savedLead = created;
      }

      // 7. Activate campaign if new
      if (campaign.isNew) {
        console.log(`[send] Activating new campaign ${campaign.instantlyCampaignId}`);
        await updateCampaignStatus(campaign.instantlyCampaignId, "active");

        // Verify campaign state after activation
        const postActivate = await getInstantlyCampaign(campaign.instantlyCampaignId) as unknown as Record<string, unknown>;
        console.log(`[send] Post-activate — status=${postActivate.status} email_list=${JSON.stringify(postActivate.email_list)} not_sending_status=${JSON.stringify(postActivate.not_sending_status)}`);

        await db
          .update(instantlyCampaigns)
          .set({ status: "active", updatedAt: new Date() })
          .where(eq(instantlyCampaigns.id, campaign.id));
      }

      // 8. Log costs and complete run (only if tracking)
      if (sendRun) {
        await addCosts(sendRun.id, [
          { costName: "instantly-email-send", quantity: 1 },
        ]);
        await updateRun(sendRun.id, "completed");
      }

      console.log(`[send] Done — to=${body.to} campaign=${campaign.id} isNew=${campaign.isNew} added=${added}`);
      res.status(200).json({
        success: true,
        campaignId: campaign.id,
        leadId: savedLead?.id,
        added,
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
