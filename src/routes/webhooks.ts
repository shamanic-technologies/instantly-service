import { Router, Request, Response } from "express";
import { db } from "../db";
import { instantlyEvents } from "../db/schema";

const router = Router();
const WEBHOOK_SECRET = process.env.INSTANTLY_WEBHOOK_SECRET;

if (!WEBHOOK_SECRET) {
  throw new Error("INSTANTLY_WEBHOOK_SECRET is required");
}

interface InstantlyWebhookPayload {
  event_type: string;
  campaign_id?: string;
  lead_email?: string;
  account_email?: string;
  timestamp?: string;
  [key: string]: unknown;
}

/**
 * POST /webhooks/instantly
 * Receives Instantly webhook events (requires valid secret)
 */
router.post("/instantly", async (req: Request, res: Response) => {
  // Check secret in query param, header, or authorization
  const secret = req.query.secret || req.headers["x-instantly-signature"] || req.headers["authorization"];
  if (secret !== WEBHOOK_SECRET && secret !== `Bearer ${WEBHOOK_SECRET}`) {
    return res.status(401).json({ error: "Invalid webhook secret" });
  }

  const payload = req.body as InstantlyWebhookPayload;

  if (!payload.event_type) {
    return res.status(400).json({ error: "Missing event_type" });
  }

  try {
    await db.insert(instantlyEvents).values({
      eventType: payload.event_type,
      campaignId: payload.campaign_id,
      leadEmail: payload.lead_email,
      accountEmail: payload.account_email,
      timestamp: payload.timestamp ? new Date(payload.timestamp) : new Date(),
      rawPayload: payload,
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
