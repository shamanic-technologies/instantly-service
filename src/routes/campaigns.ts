import { Router, Request, Response } from "express";
import { db } from "../db";
import { instantlyCampaigns } from "../db/schema";
import { eq, or } from "drizzle-orm";
import {
  createCampaign as createInstantlyCampaign,
  updateCampaign as updateInstantlyCampaign,
  getCampaign as getInstantlyCampaign,
  updateCampaignStatus as updateInstantlyStatus,
} from "../lib/instantly-client";
import {
  createRun,
  updateRun,
  addCosts,
} from "../lib/runs-client";
import { handleCampaignError } from "../lib/campaign-error-handler";
import { resolveInstantlyApiKey, KeyServiceError } from "../lib/key-client";
import {
  CreateCampaignRequestSchema,
  UpdateStatusRequestSchema,
} from "../schemas";

const router = Router();

/**
 * POST /campaigns
 * Create a campaign in Instantly + DB, log run via runs-service (BLOCKING)
 */
router.post("/", async (req: Request, res: Response) => {
  const parsed = CreateCampaignRequestSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({
      error: "Invalid request",
      details: parsed.error.flatten(),
    });
  }
  const body = parsed.data;

  try {
    // 0. Resolve Instantly API key (BYOK per-org)
    const apiKey = await resolveInstantlyApiKey(body.clerkOrgId, {
      method: "POST",
      path: "/campaigns",
    });

    // 1. Create run in runs-service FIRST (BLOCKING)
    const run = await createRun({
      clerkOrgId: body.clerkOrgId,
      appId: body.appId,
      serviceName: "instantly-service",
      taskName: "campaign-create",
      brandId: body.brandId,
      parentRunId: body.runId,
    });

    try {
      // 2. Create campaign in Instantly (no sequence steps — steps are added via POST /send)
      const instantlyCampaign = await createInstantlyCampaign(apiKey, {
        name: body.name,
        steps: [],
      });

      // Assign sending accounts via PATCH (V2 ignores account_ids in create body)
      if (body.accountIds && body.accountIds.length > 0) {
        await updateInstantlyCampaign(apiKey, instantlyCampaign.id, {
          email_list: body.accountIds,
        });
      }

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
    if (error instanceof KeyServiceError && error.statusCode === 404) {
      return res.status(422).json({
        error: "BYOK key not configured for this organization",
        details: "Please configure your Instantly API key before creating campaigns.",
      });
    }
    console.error(`[campaigns] Failed to create campaign: ${error.message}`);
    res.status(500).json({
      error: "Failed to create campaign",
      details: error.message,
    });
  }
});

/**
 * GET /campaigns/:campaignId
 * Looks up by id (direct-created) or campaignId column (send-created).
 * Returns an array when multiple sub-campaigns exist for a logical campaign.
 */
router.get("/:campaignId", async (req: Request, res: Response) => {
  const { campaignId } = req.params;

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

    // Backwards compat: return first as `campaign`, full list as `campaigns`
    res.json({ campaign: campaigns[0], campaigns });
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
 * Updates all sub-campaigns matching the given campaignId.
 */
router.patch("/:campaignId/status", async (req: Request, res: Response) => {
  const { campaignId } = req.params;

  const parsed = UpdateStatusRequestSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({
      error: "Invalid status",
      allowed: ["active", "paused", "completed"],
    });
  }
  const { status } = parsed.data;

  try {
    // Look up campaigns first (need clerkOrgId for key resolution)
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

    // Resolve Instantly API key (BYOK per-org)
    const apiKey = await resolveInstantlyApiKey(campaigns[0].clerkOrgId, {
      method: "PATCH",
      path: "/campaigns/:campaignId/status",
    });

    // Update all sub-campaigns in Instantly + DB
    const updated = [];
    for (const campaign of campaigns) {
      await updateInstantlyStatus(apiKey, campaign.instantlyCampaignId, status);
      const [row] = await db
        .update(instantlyCampaigns)
        .set({ status, updatedAt: new Date() })
        .where(eq(instantlyCampaigns.id, campaign.id))
        .returning();
      updated.push(row);
    }

    res.json({ campaign: updated[0], campaigns: updated });
  } catch (error: any) {
    res.status(500).json({ error: error.message });
  }
});

/**
 * POST /campaigns/check-status
 * Poll all active campaigns against the Instantly API to detect error states.
 * For each errored campaign: updates DB, cancels provisions, fails run, notifies admin.
 */
router.post("/check-status", async (_req: Request, res: Response) => {
  try {
    const activeCampaigns = await db
      .select()
      .from(instantlyCampaigns)
      .where(eq(instantlyCampaigns.status, "active"));

    console.log(`[campaigns] check-status: checking ${activeCampaigns.length} active campaigns`);

    // Group campaigns by clerkOrgId for per-org key resolution
    const campaignsByOrg = new Map<string | null, typeof activeCampaigns>();
    for (const c of activeCampaigns) {
      const key = c.clerkOrgId ?? null;
      if (!campaignsByOrg.has(key)) campaignsByOrg.set(key, []);
      campaignsByOrg.get(key)!.push(c);
    }

    const errors: {
      instantlyCampaignId: string;
      campaignId: string | null;
      leadEmail: string | null;
      reason: string;
    }[] = [];
    let checked = 0;

    for (const [orgId, orgCampaigns] of campaignsByOrg) {
      let apiKey: string;
      try {
        apiKey = await resolveInstantlyApiKey(orgId, {
          method: "POST",
          path: "/campaigns/check-status",
        });
      } catch (error: unknown) {
        const message = error instanceof Error ? error.message : String(error);
        console.warn(
          `[campaigns] check-status: skipping ${orgCampaigns.length} campaigns for org ${orgId} — key error: ${message}`,
        );
        continue;
      }

      for (const campaign of orgCampaigns) {
        checked++;
        try {
          const instantly = (await getInstantlyCampaign(
            apiKey,
            campaign.instantlyCampaignId,
          )) as unknown as Record<string, unknown>;

          if (instantly.not_sending_status) {
            const reason = `not_sending_status: ${JSON.stringify(instantly.not_sending_status)}`;
            console.error(
              `[campaigns] check-status: ${campaign.instantlyCampaignId} error — ${reason}`,
            );
            await handleCampaignError(campaign.instantlyCampaignId, reason);
            errors.push({
              instantlyCampaignId: campaign.instantlyCampaignId,
              campaignId: campaign.campaignId,
              leadEmail: campaign.leadEmail,
              reason,
            });
          }
        } catch (error: unknown) {
          const message = error instanceof Error ? error.message : String(error);
          console.error(
            `[campaigns] check-status: failed to check ${campaign.instantlyCampaignId}: ${message}`,
          );
        }

        // Small delay to avoid rate limiting
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    }

    console.log(
      `[campaigns] check-status: done — checked=${checked} errors=${errors.length}`,
    );
    res.json({ checked, errors });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[campaigns] check-status failed: ${message}`);
    res.status(500).json({ error: message });
  }
});

export default router;
