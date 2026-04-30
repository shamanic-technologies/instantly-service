import { Router, Request, Response } from "express";
import { db } from "../db";
import { instantlyCampaigns } from "../db/schema";
import { eq, or } from "drizzle-orm";
import {
  getCampaign as getInstantlyCampaign,
  updateCampaignStatus as updateInstantlyStatus,
} from "../lib/instantly-client";
import { handleCampaignError } from "../lib/campaign-error-handler";
import { resolveInstantlyApiKey } from "../lib/key-client";
import { UpdateStatusRequestSchema } from "../schemas";
import { traceEvent } from "../lib/trace-event";

const router = Router();

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
 * GET /orgs/campaigns
 * List campaigns for the org from x-org-id header.
 */
router.get("/", async (req: Request, res: Response) => {
  const orgId = res.locals.orgId as string;

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
    // Look up campaigns first (need orgId for key resolution)
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

    // Resolve Instantly API key using header identity
    const orgId = res.locals.orgId as string;
    const userId = res.locals.userId as string;
    const { key: apiKey } = await resolveInstantlyApiKey(orgId, userId, {
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

    traceEvent(res.locals.runId as string || "unknown", { service: "instantly-service", event: "campaign-status-update", detail: `campaignId=${campaignId}, status=${status}, count=${updated.length}` }, req.headers).catch(() => {});
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

    // Group campaigns by orgId for per-org key resolution
    const campaignsByOrg = new Map<string | null, typeof activeCampaigns>();
    for (const c of activeCampaigns) {
      const key = c.orgId ?? null;
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
        if (!orgId) throw new Error("Campaign missing orgId");
        const keyResult = await resolveInstantlyApiKey(orgId, "system", {
          method: "POST",
          path: "/campaigns/check-status",
        });
        apiKey = keyResult.key;
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
