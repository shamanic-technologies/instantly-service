import { Router, Request, Response } from "express";
import { db } from "../db";
import { instantlyEvents, instantlyCampaigns, sequenceCosts } from "../db/schema";
import { eq, and } from "drizzle-orm";
import { WebhookPayloadSchema } from "../schemas";
import { updateCostStatus, updateRun } from "../lib/runs-client";

const router = Router();

/** Events that indicate the sequence has stopped for this lead */
const SEQUENCE_STOP_EVENTS = new Set([
  "reply_received",
  "email_bounced",
  "lead_unsubscribed",
  "lead_not_interested",
]);

/**
 * When a follow-up email is sent, convert the matching provisioned cost to actual.
 */
async function handleFollowUpSent(
  instantlyCampaignId: string,
  leadEmail: string,
  step: number,
): Promise<void> {
  const [campaign] = await db
    .select()
    .from(instantlyCampaigns)
    .where(eq(instantlyCampaigns.instantlyCampaignId, instantlyCampaignId));

  if (!campaign?.campaignId) return;

  const [cost] = await db
    .select()
    .from(sequenceCosts)
    .where(
      and(
        eq(sequenceCosts.campaignId, campaign.campaignId),
        eq(sequenceCosts.leadEmail, leadEmail),
        eq(sequenceCosts.step, step),
        eq(sequenceCosts.status, "provisioned"),
      ),
    );

  if (!cost) return;

  try {
    await updateCostStatus(cost.runId, cost.costId, "actual");
    await db
      .update(sequenceCosts)
      .set({ status: "actual", updatedAt: new Date() })
      .where(eq(sequenceCosts.id, cost.id));
    await updateRun(cost.runId, "completed");
    console.log(`[webhooks] Converted provisioned cost ${cost.costId} to actual and completed run ${cost.runId} for step ${step}`);
  } catch (error: any) {
    console.error(`[webhooks] Failed to convert cost ${cost.costId}: ${error.message}`);
  }
}

/**
 * When the sequence stops (reply, bounce, unsub, not_interested),
 * cancel all remaining provisioned costs for this lead.
 */
async function cancelRemainingProvisions(
  instantlyCampaignId: string,
  leadEmail: string,
  eventType: string,
): Promise<void> {
  const [campaign] = await db
    .select()
    .from(instantlyCampaigns)
    .where(eq(instantlyCampaigns.instantlyCampaignId, instantlyCampaignId));

  if (!campaign?.campaignId) return;

  const remaining = await db
    .select()
    .from(sequenceCosts)
    .where(
      and(
        eq(sequenceCosts.campaignId, campaign.campaignId),
        eq(sequenceCosts.leadEmail, leadEmail),
        eq(sequenceCosts.status, "provisioned"),
      ),
    );

  for (const cost of remaining) {
    try {
      await updateCostStatus(cost.runId, cost.costId, "cancelled");
      await db
        .update(sequenceCosts)
        .set({ status: "cancelled", updatedAt: new Date() })
        .where(eq(sequenceCosts.id, cost.id));
      await updateRun(cost.runId, "failed", `Sequence stopped: ${eventType}`);
      console.log(`[webhooks] Cancelled provisioned cost ${cost.costId} and failed run ${cost.runId} for step ${cost.step}`);
    } catch (error: any) {
      console.error(`[webhooks] Failed to cancel cost ${cost.costId}: ${error.message}`);
    }
  }
}

/** Maps webhook event types to deliveryStatus values */
const DELIVERY_STATUS_MAP: Record<string, string> = {
  email_sent: "sent",
  campaign_completed: "delivered",
  reply_received: "replied",
  email_bounced: "bounced",
  lead_unsubscribed: "unsubscribed",
};

async function updateDeliveryStatus(
  instantlyCampaignId: string,
  eventType: string,
): Promise<void> {
  const newStatus = DELIVERY_STATUS_MAP[eventType];
  if (!newStatus) return;

  try {
    await db
      .update(instantlyCampaigns)
      .set({ deliveryStatus: newStatus, updatedAt: new Date() })
      .where(eq(instantlyCampaigns.instantlyCampaignId, instantlyCampaignId));
    console.log(`[webhooks] Updated deliveryStatus to '${newStatus}' for campaign ${instantlyCampaignId}`);
  } catch (error: any) {
    console.error(`[webhooks] Failed to update deliveryStatus for ${instantlyCampaignId}: ${error.message}`);
  }
}

/**
 * GET /webhooks/instantly/config
 * Returns the webhook URL that BYOK customers should paste into their Instantly dashboard.
 */
router.get("/instantly/config", (_req: Request, res: Response) => {
  const domain = process.env.RAILWAY_PUBLIC_DOMAIN;
  if (!domain) {
    return res.status(500).json({ error: "RAILWAY_PUBLIC_DOMAIN not available" });
  }
  res.json({ webhookUrl: `https://${domain}/webhooks/instantly` });
});

/**
 * POST /webhooks/instantly
 * Receives Instantly webhook events.
 * Verification: campaign_id must exist in our database (UUID is unguessable).
 *
 * In addition to recording the event, handles cost lifecycle:
 * - email_sent (step > 1): convert provisioned cost → actual
 * - reply/bounce/unsub/not_interested: cancel all remaining provisions
 */
router.post("/instantly", async (req: Request, res: Response) => {
  // 1. Parse payload first
  const parsed = WebhookPayloadSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: "Missing event_type" });
  }
  const payload = parsed.data;

  // 2. Verify via campaign_id DB lookup
  if (!payload.campaign_id) {
    return res.status(400).json({ error: "Missing campaign_id" });
  }

  const [campaign] = await db
    .select()
    .from(instantlyCampaigns)
    .where(eq(instantlyCampaigns.instantlyCampaignId, payload.campaign_id));

  if (!campaign) {
    return res.status(401).json({ error: "Unknown campaign_id" });
  }

  try {
    // 3. Record the event with step/variant
    await db.insert(instantlyEvents).values({
      eventType: payload.event_type,
      campaignId: payload.campaign_id,
      leadEmail: payload.lead_email,
      accountEmail: payload.email_account,
      step: payload.step,
      variant: payload.variant,
      timestamp: payload.timestamp ? new Date(payload.timestamp) : new Date(),
      rawPayload: req.body,
    });

    // 4. Update delivery status
    await updateDeliveryStatus(payload.campaign_id, payload.event_type);

    // 5. Handle cost lifecycle based on event type
    if (payload.lead_email) {
      if (payload.event_type === "email_sent" && payload.step && payload.step > 1) {
        await handleFollowUpSent(payload.campaign_id, payload.lead_email, payload.step);
      } else if (SEQUENCE_STOP_EVENTS.has(payload.event_type)) {
        await cancelRemainingProvisions(payload.campaign_id, payload.lead_email, payload.event_type);
      }
    }

    res.json({
      success: true,
      eventType: payload.event_type,
    });
  } catch (error: any) {
    console.error(`[webhooks] Failed to process webhook: ${error.message}`);
    res.status(500).json({ error: error.message });
  }
});

export default router;
