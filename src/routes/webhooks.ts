import { Router, Request, Response } from "express";
import { db } from "../db";
import { instantlyCampaigns } from "../db/schema";
import { eq, or, sql } from "drizzle-orm";
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
 *   2. Verify via campaign_id DB lookup. Matches against current
 *      `instantly_campaign_id` OR any entry in
 *      `metadata.redispatchHistory[*].from` — retry-stuck overwrites the
 *      current id on redispatch, but delayed events (e.g. open pixels)
 *      from the OLD Instantly campaign still arrive with the stale id.
 *   3. Insert raw payload into bronze (instantly_webhook_payloads_raw).
 *   4. Promote to silver (instantly_events) — idempotent via dedupe index.
 *      Silver promotion fires side effects (delivery_status update, reply
 *      classification, sequence cost lifecycle) ONLY on first insert.
 *
 * Failure handling: EVERY path returns HTTP 200. There is no 4xx and no 5xx
 * branch — an unusable payload answers 200 with `degraded: true` and a
 * `degradedReason`, and is logged loudly. Rationale: Instantly disables a
 * webhook after repeated delivery failures, and it counts a 4xx as a failure
 * exactly like a 5xx. It subscribes us to `all_events`, which includes
 * account-level events carrying no `campaign_id` at all, so rejecting those
 * accumulated failures until the whole webhook was disabled (observed
 * 2026-08-25: disabled at 07:47 UTC; before that, 2026-05-20, a promoteEvent
 * bug caused 6 days of silence). Losing one unattributable event is far
 * cheaper than losing every event, and reconcile cron backfills at 03:00 UTC.
 *
 * Degraded reasons: `invalid_payload` (no event_type), `missing_campaign_id`
 * (an event we cannot attribute — bronze requires a non-null campaign id, so
 * it is dropped), `unknown_campaign_id` (matches no local row nor alias),
 * `campaign lookup failed: ...`, `bronze failed: ...`, `silver failed: ...`.
 */
router.post("/instantly", async (req: Request, res: Response) => {
  const parsed = WebhookPayloadSchema.safeParse(req.body);
  if (!parsed.success) {
    console.warn(
      `[instantly-service] webhook invalid_payload (returning 200 to avoid Instantly auto-disable): body=${JSON.stringify(req.body).slice(0, 500)}`,
    );
    return res.json({
      success: true,
      eventType: null,
      bronzeRowId: null,
      promoted: false,
      degraded: true,
      degradedReason: "invalid_payload",
    });
  }
  const payload = parsed.data;

  // Instantly subscribes us to `all_events`, which includes account-level events
  // carrying no campaign_id. Bronze keys on a non-null instantly_campaign_id, so
  // such an event cannot be stored — log it and acknowledge, never reject.
  if (!payload.campaign_id) {
    console.warn(
      `[instantly-service] webhook missing_campaign_id (returning 200 to avoid Instantly auto-disable): type=${payload.event_type} account=${payload.email_account ?? "none"} lead=${payload.lead_email ?? "none"}`,
    );
    return res.json({
      success: true,
      eventType: payload.event_type,
      bronzeRowId: null,
      promoted: false,
      degraded: true,
      degradedReason: "missing_campaign_id",
    });
  }

  // Match the current instantly_campaign_id OR any historical alias preserved
  // in `metadata.redispatchHistory[*].from`. The GIN index on `metadata`
  // (migration 0016) backs the JSONB containment check.
  let campaign: typeof instantlyCampaigns.$inferSelect | undefined;
  try {
    [campaign] = await db
      .select()
      .from(instantlyCampaigns)
      .where(
        or(
          eq(instantlyCampaigns.instantlyCampaignId, payload.campaign_id),
          sql`${instantlyCampaigns.metadata} @? ${sql.raw(
            `'$.redispatchHistory[*] ? (@.from == ${JSON.stringify(payload.campaign_id)})'`,
          )}`,
        ),
      )
      .limit(1);
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(
      `[instantly-service] webhook campaign lookup failed (returning 200 to avoid Instantly auto-disable): campaign=${payload.campaign_id} type=${payload.event_type} error=${message}`,
    );
    return res.json({
      success: true,
      eventType: payload.event_type,
      bronzeRowId: null,
      promoted: false,
      degraded: true,
      degradedReason: `campaign lookup failed: ${message}`,
    });
  }

  if (!campaign) {
    console.warn(
      `[instantly-service] webhook unknown_campaign_id (returning 200 + degraded to avoid Instantly retry): campaign=${payload.campaign_id} type=${payload.event_type} lead=${payload.lead_email ?? "none"}`,
    );
    return res.json({
      success: true,
      eventType: payload.event_type,
      bronzeRowId: null,
      promoted: false,
      degraded: true,
      degradedReason: "unknown_campaign_id",
    });
  }

  // Promote events to silver under the campaign's CURRENT instantly_campaign_id
  // so they dedup against existing rows. The webhook payload's stale id only
  // matters for lookup — once we have the row, silver writes use the canonical
  // current id.
  const canonicalCampaignId = campaign.instantlyCampaignId;

  const isAliasMatch = canonicalCampaignId !== payload.campaign_id;
  traceEvent(
    campaign.runId || "unknown",
    {
      service: "instantly-service",
      event: "webhook-received",
      detail: `type=${payload.event_type}, campaign=${payload.campaign_id}${isAliasMatch ? ` (alias→${canonicalCampaignId})` : ""}, lead=${payload.lead_email ?? "none"}, step=${payload.step ?? "none"}`,
    },
    req.headers,
  ).catch(() => {});

  let bronzeRowId: string | null = null;
  let bronzeError: string | null = null;
  try {
    const bronzeRef = await insertWebhookPayload(
      canonicalCampaignId,
      campaign.orgId,
      req.body,
    );
    bronzeRowId = bronzeRef.id;
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    bronzeError = message;
    console.error(
      `[instantly-service] webhook bronze write failed (returning 200 to avoid auto-pause): campaign=${canonicalCampaignId} type=${payload.event_type} error=${message}`,
    );
    traceEvent(
      campaign.runId || "unknown",
      {
        service: "instantly-service",
        event: "webhook-bronze-error",
        detail: message,
        level: "error",
      },
      req.headers,
    ).catch(() => {});
  }

  let silverResult: { promoted: boolean; silverEventId: string | null } | null = null;
  let silverError: string | null = null;
  if (bronzeRowId) {
    try {
      silverResult = await promoteFromWebhookPayload({
        bronzeRowId,
        payload: {
          event_type: payload.event_type,
          campaign_id: canonicalCampaignId,
          lead_email: payload.lead_email,
          email_account: payload.email_account,
          step: payload.step,
          variant: payload.variant,
          timestamp: payload.timestamp,
        },
        rawPayload: req.body,
      });

      if (silverResult.promoted) {
        traceEvent(
          campaign.runId || "unknown",
          {
            service: "instantly-service",
            event: "webhook-promoted",
            detail: `type=${payload.event_type}, silverEventId=${silverResult.silverEventId}`,
          },
          req.headers,
        ).catch(() => {});
      }
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : String(error);
      silverError = message;
      console.error(
        `[instantly-service] webhook silver promote failed (bronze row=${bronzeRowId} persisted, returning 200 to avoid auto-pause): campaign=${canonicalCampaignId} type=${payload.event_type} error=${message}`,
      );
      traceEvent(
        campaign.runId || "unknown",
        {
          service: "instantly-service",
          event: "webhook-silver-error",
          detail: message,
          level: "error",
        },
        req.headers,
      ).catch(() => {});
    }
  }

  const degraded = bronzeError !== null || silverError !== null;
  const degradedReason =
    bronzeError && silverError
      ? `bronze+silver failed: ${bronzeError}; ${silverError}`
      : bronzeError
        ? `bronze failed: ${bronzeError}`
        : silverError
          ? `silver failed: ${silverError}`
          : null;

  res.json({
    success: true,
    eventType: payload.event_type,
    bronzeRowId,
    promoted: silverResult?.promoted ?? false,
    degraded,
    degradedReason,
  });
});

export default router;
