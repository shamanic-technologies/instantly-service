/**
 * Shared campaign error handler.
 *
 * When an Instantly campaign enters an error state (detected via
 * `not_sending_status`), this module:
 *   1. Updates our DB status (default "error", or caller-supplied)
 *   2. Cancels all remaining actual/provisioned costs for every step
 *   3. Marks the associated run as "failed" (MUST succeed — throws on failure)
 *   4. Sends an admin notification email (non-fatal)
 *
 * Two terminal modes:
 *   - `failed` (default) — Instantly reported an error inline, no retry possible.
 *   - `cancelled` — discovered post-hoc by the retry-stuck cron (24h+ contacted
 *     rows with not_sending_status set). Same cost-cancel semantics, but signals
 *     "stuck and refunded" rather than "errored".
 */

import { db } from "../db";
import { instantlyCampaigns, sequenceCosts } from "../db/schema";
import { eq, and, or } from "drizzle-orm";
import { updateRun, updateCostStatus, type IdentityContext } from "./runs-client";
import { sendEmail } from "./email-client";

const ADMIN_EMAIL = process.env.ADMIN_NOTIFICATION_EMAIL || "kevin@distribute.you";

export type CampaignErrorTerminal = "failed" | "cancelled";

export interface HandleCampaignErrorOptions {
  /**
   * Final `delivery_status` value. Defaults to `failed`. Use `cancelled` for
   * the retry-stuck cron path so the lifecycle is distinguishable.
   */
  terminalStatus?: CampaignErrorTerminal;
  /**
   * Extra metadata keys to merge into the campaign row (e.g. `notSendingStatus`,
   * `retryCount`). Always merged on top of existing metadata + `errorReason`.
   */
  extraMetadata?: Record<string, unknown>;
}

/**
 * Handle a campaign that has entered an error state on Instantly's side.
 *
 * @param instantlyCampaignId - The Instantly campaign ID (not our internal ID)
 * @param reason - Human-readable reason (typically JSON of not_sending_status)
 * @param options - Terminal-status override + extra metadata to persist
 */
export async function handleCampaignError(
  instantlyCampaignId: string,
  reason: string,
  options: HandleCampaignErrorOptions = {},
): Promise<void> {
  const terminalStatus: CampaignErrorTerminal = options.terminalStatus ?? "failed";
  const extraMetadata = options.extraMetadata ?? {};

  // 1. Look up campaign in our DB
  const [campaign] = await db
    .select()
    .from(instantlyCampaigns)
    .where(eq(instantlyCampaigns.instantlyCampaignId, instantlyCampaignId));

  if (!campaign) {
    console.warn(
      `[campaign-error] No campaign found for instantlyCampaignId=${instantlyCampaignId}`,
    );
    return;
  }

  // Already handled — skip when the campaign reached a terminal status earlier
  // (avoids double-cancel / double-notify on cron retry or webhook redelivery).
  if (campaign.status === "error" || campaign.deliveryStatus === "cancelled") {
    return;
  }

  console.error(
    `[campaign-error] Campaign ${instantlyCampaignId} (${campaign.campaignId}/${campaign.leadEmail}) → ${terminalStatus}: ${reason}`,
  );

  // Build identity context from campaign record (automated process)
  const identity: IdentityContext = {
    orgId: campaign.orgId || "system",
    userId: campaign.userId || "00000000-0000-0000-0000-000000000000",
    runId: campaign.runId || undefined,
  };

  // 2. Update DB status + deliveryStatus with reason + caller metadata
  const existingMetadata =
    (campaign.metadata as Record<string, unknown>) || {};
  await db
    .update(instantlyCampaigns)
    .set({
      status: "error",
      deliveryStatus: terminalStatus,
      metadata: { ...existingMetadata, errorReason: reason, ...extraMetadata },
      updatedAt: new Date(),
    })
    .where(eq(instantlyCampaigns.id, campaign.id));

  // 3. Cancel all remaining provisioned costs
  if (campaign.campaignId && campaign.leadEmail) {
    const remaining = await db
      .select()
      .from(sequenceCosts)
      .where(
        and(
          eq(sequenceCosts.campaignId, campaign.campaignId),
          eq(sequenceCosts.leadEmail, campaign.leadEmail),
          or(
            eq(sequenceCosts.status, "provisioned"),
            eq(sequenceCosts.status, "actual"),
          ),
        ),
      );

    for (const cost of remaining) {
      const costIdentity: IdentityContext = { ...identity, runId: cost.runId };
      await updateCostStatus(cost.runId, cost.costId, "cancelled", costIdentity);
      await db
        .update(sequenceCosts)
        .set({ status: "cancelled", updatedAt: new Date() })
        .where(eq(sequenceCosts.id, cost.id));
      console.log(
        `[campaign-error] Cancelled ${cost.status} cost ${cost.costId} for step ${cost.step}`,
      );
      // Fail the step's run (may already be completed for step 1)
      try {
        await updateRun(cost.runId, "failed", costIdentity, reason);
        console.log(`[campaign-error] Failed run ${cost.runId} for step ${cost.step}`);
      } catch (runErr: unknown) {
        const msg = runErr instanceof Error ? runErr.message : String(runErr);
        console.warn(`[campaign-error] Could not fail run ${cost.runId}: ${msg}`);
      }
    }
  }

  // 4. Mark the run as failed (MUST succeed — let it throw)
  if (campaign.runId) {
    await updateRun(campaign.runId, "failed", identity, reason);
    console.log(
      `[campaign-error] Marked run ${campaign.runId} as failed`,
    );
  }

  // 5. Send admin notification (non-fatal)
  try {
    await sendEmail(
      {
        appId: "instantly-service",
        eventType: "campaign-error",
        recipientEmail: ADMIN_EMAIL,
        metadata: {
          campaignId: campaign.campaignId || "unknown",
          leadEmail: campaign.leadEmail || "unknown",
          instantlyCampaignId,
          errorReason: reason,
        },
      },
      identity,
    );
    console.log(`[campaign-error] Admin notification sent to ${ADMIN_EMAIL}`);
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.warn(
      `[campaign-error] Failed to send admin notification: ${message}`,
    );
  }
}
