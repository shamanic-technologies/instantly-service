import { Router, Request, Response } from "express";
import { db } from "../db";
import { instantlyEvents } from "../db/schema";
import { WebhookPayloadSchema } from "../schemas";

const router = Router();

function getWebhookSecret(): string {
  const secret = process.env.INSTANTLY_WEBHOOK_SECRET;
  if (!secret) {
    throw new Error("INSTANTLY_WEBHOOK_SECRET is required");
  }
  return secret;
}

/**
 * POST /webhooks/instantly
 * Receives Instantly webhook events (requires valid secret)
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
    await db.insert(instantlyEvents).values({
      eventType: payload.event_type,
      campaignId: payload.campaign_id,
      leadEmail: payload.lead_email,
      accountEmail: payload.email_account,
      timestamp: payload.timestamp ? new Date(payload.timestamp) : new Date(),
      rawPayload: req.body,
    });

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
