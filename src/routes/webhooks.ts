import { Router, Request, Response } from "express";
import { db } from "../db";
import { instantlyEvents, instantlyCampaigns, sequenceCosts } from "../db/schema";
import { eq, and } from "drizzle-orm";
import { WebhookPayloadSchema } from "../schemas";
import { updateCostStatus } from "../lib/runs-client";

const router = Router();

function getWebhookSecret(): string {
  const secret = process.env.INSTANTLY_WEBHOOK_SECRET;
  if (!secret) {
    throw new Error("INSTANTLY_WEBHOOK_SECRET is required");
  }
  return secret;
}

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
  // Look up our internal campaign to get the logical campaignId
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
    console.log(`[webhooks] Converted provisioned cost ${cost.costId} to actual for step ${step}`);
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
      console.log(`[webhooks] Cancelled provisioned cost ${cost.costId} for step ${cost.step}`);
    } catch (error: any) {
      console.error(`[webhooks] Failed to cancel cost ${cost.costId}: ${error.message}`);
    }
  }
}

/**
 * POST /webhooks/instantly
 * Receives Instantly webhook events (requires valid secret).
 *
 * In addition to recording the event, handles cost lifecycle:
 * - email_sent (step > 1): convert provisioned cost → actual
 * - reply/bounce/unsub/not_interested: cancel all remaining provisions
 */
router.post("/instantly", async (req: Request, res: Response) => {
  const WEBHOOK_SECRET = getWebhookSecret();
  // Check secret in query param, header, or authorization
  const secret = req.query.secret || req.headers["x-instantly-signature"] || req.headers["authorization"];
  if (secret !== WEBHOOK_SECRET && secret !== `Bearer ${WEBHOOK_SECRET}`) {
    return res.status(401).json({ error: "Invalid webhook secret" });
  }

  const parsed = WebhookPayloadSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: "Missing event_type" });
  }
  const payload = parsed.data;

  try {
    // 1. Record the event with step/variant
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

    // 2. Handle cost lifecycle based on event type
    if (payload.campaign_id && payload.lead_email) {
      if (payload.event_type === "email_sent" && payload.step && payload.step > 1) {
        // Follow-up sent → convert provisioned to actual
        await handleFollowUpSent(payload.campaign_id, payload.lead_email, payload.step);
      } else if (SEQUENCE_STOP_EVENTS.has(payload.event_type)) {
        // Sequence stopped → cancel remaining provisions
        await cancelRemainingProvisions(payload.campaign_id, payload.lead_email);
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
