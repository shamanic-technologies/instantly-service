import { Router, Request, Response } from "express";
import { randomUUID } from "crypto";
import { db } from "../db";
import { instantlyCampaigns } from "../db/schema";
import { eq, or } from "drizzle-orm";
import { updateCampaignStatus as updateInstantlyStatus } from "../lib/instantly-client";
import { resolveInstantlyApiKey } from "../lib/key-client";
import { UpdateStatusRequestSchema } from "../schemas";
import { traceEvent } from "../lib/trace-event";
import { reconcileAll } from "../lib/reconcile";
import { refundStrandedHolds } from "../lib/refund-stranded-holds";

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
 * POST /campaigns/reconcile
 * Daily catch-up: pulls Instantly's per-campaign state and promotes any events
 * missed by the webhook into the silver event log. See lib/reconcile.ts.
 *
 * Returns 202 Accepted immediately and runs the job in the background.
 * Synchronous execution exceeded the Cloudflare/Railway 15min proxy timeout
 * on the GH Actions runner, so the cron caller could not observe completion.
 * Caller verifies progress via Railway logs (`reconcile: done`) or by polling
 * the `instantly_*_raw` bronze tables.
 */
router.post("/reconcile", (_req: Request, res: Response) => {
  const runId = randomUUID();
  const startedAt = new Date().toISOString();
  console.log(`[instantly-service] reconcile: dispatched run=${runId}`);
  res.status(202).json({ runId, startedAt });
  reconcileAll().catch((error: unknown) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[instantly-service] reconcile run=${runId} failed: ${message}`);
  });
});

/**
 * POST /campaigns/refund-stranded-holds
 * One-time backlog refund for the provisioned-hold leak (issue #335): cancels
 * `provisioned` sequence_costs stranded on locally-terminal (paused/completed)
 * campaigns. Idempotent + resumable — safe to call repeatedly. Optional
 * `{ limit }` body bounds the batch (campaign count). MUST run in-cluster
 * (cancelling calls runs-service over `*.railway.internal`).
 *
 * Mirrors /reconcile: 202 + background execution (the sweep can exceed proxy
 * timeouts over ~12k rows). Watch Railway logs for `refund-stranded-holds: done`.
 */
router.post("/refund-stranded-holds", (req: Request, res: Response) => {
  const rawLimit = (req.body as { limit?: unknown })?.limit;
  const limit = typeof rawLimit === "number" && rawLimit > 0 ? Math.floor(rawLimit) : undefined;
  const runId = randomUUID();
  const startedAt = new Date().toISOString();
  console.log(`[instantly-service] refund-stranded-holds: dispatched run=${runId} limit=${limit ?? "all"}`);
  res.status(202).json({ runId, startedAt, limit: limit ?? null });
  refundStrandedHolds({ limit }).catch((error: unknown) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[instantly-service] refund-stranded-holds run=${runId} failed: ${message}`);
  });
});

export default router;
