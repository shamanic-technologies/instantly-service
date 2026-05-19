import { Router, Request, Response } from "express";
import { db } from "../db";
import { instantlyCampaigns } from "../db/schema";
import { eq } from "drizzle-orm";
import { WebhookPayloadSchema } from "../schemas";
import { traceEvent } from "../lib/trace-event";
import { insertWebhookPayload } from "../lib/bronze";
import { promoteFromWebhookPayload } from "../lib/silver-promote";

const router = Router();

/**
 * GET /webhooks/instantly/config
 * Returns the webhook URL that BYOK customers should paste into their Instantly dashboard.
 */
router.get("/instantly/config", (_req: Request, res: Response) => {
  const baseUrl = process.env.INSTANTLY_SERVICE_URL;
  if (!baseUrl) {
    return res.status(500).json({ error: "INSTANTLY_SERVICE_URL not configured" });
  }
  res.json({ webhookUrl: `${baseUrl}/webhooks/instantly` });
});

/**
 * POST /webhooks/instantly
 * Receives Instantly webhook events.
 *
 * Flow (bronze → silver):
 *   1. Parse + validate payload (Zod).
 *   2. Verify via campaign_id DB lookup (UUID is unguessable).
 *   3. Insert raw payload into bronze (instantly_webhook_payloads_raw).
 *   4. Promote to silver (instantly_events) — idempotent via dedupe index.
 *      Silver promotion fires side effects (delivery_status update, reply
 *      classification, sequence cost lifecycle) ONLY on first insert.
 */
router.post("/instantly", async (req: Request, res: Response) => {
  const parsed = WebhookPayloadSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: "Missing event_type" });
  }
  const payload = parsed.data;

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
    traceEvent(
      campaign.runId || "unknown",
      {
        service: "instantly-service",
        event: "webhook-received",
        detail: `type=${payload.event_type}, campaign=${payload.campaign_id}, lead=${payload.lead_email ?? "none"}, step=${payload.step ?? "none"}`,
      },
      req.headers,
    ).catch(() => {});

    const bronzeRef = await insertWebhookPayload(
      payload.campaign_id,
      campaign.orgId,
      req.body,
    );

    const result = await promoteFromWebhookPayload({
      bronzeRowId: bronzeRef.id,
      payload: {
        event_type: payload.event_type,
        campaign_id: payload.campaign_id,
        lead_email: payload.lead_email,
        email_account: payload.email_account,
        step: payload.step,
        variant: payload.variant,
        timestamp: payload.timestamp,
      },
      rawPayload: req.body,
    });

    if (result.promoted) {
      traceEvent(
        campaign.runId || "unknown",
        {
          service: "instantly-service",
          event: "webhook-promoted",
          detail: `type=${payload.event_type}, silverEventId=${result.silverEventId}`,
        },
        req.headers,
      ).catch(() => {});
    }

    res.json({
      success: true,
      eventType: payload.event_type,
      bronzeRowId: bronzeRef.id,
      promoted: result.promoted,
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    traceEvent(
      campaign.runId || "unknown",
      {
        service: "instantly-service",
        event: "webhook-error",
        detail: message,
        level: "error",
      },
      req.headers,
    ).catch(() => {});
    console.error(`[instantly-service] webhook failed: ${message}`);
    res.status(500).json({ error: message });
  }
});

export default router;
