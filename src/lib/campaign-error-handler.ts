/**
 * Shared campaign error handler.
 *
 * When an Instantly campaign enters an error state (detected via
 * `not_sending_status`), this module:
 *   1. Updates our DB status to "error"
 *   2. Cancels all remaining provisioned costs for follow-up steps
 *   3. Marks the associated run as "failed" (MUST succeed — throws on failure)
 *   4. Sends an admin notification email (non-fatal)
 */

import { db } from "../db";
import { instantlyCampaigns, sequenceCosts } from "../db/schema";
import { eq, and, or } from "drizzle-orm";
import { updateRun } from "./runs-client";
import { updateCostStatus } from "./runs-client";
import { sendEmail } from "./email-client";

const ADMIN_EMAIL = process.env.ADMIN_NOTIFICATION_EMAIL || "kevin@distribute.you";

/**
 * Handle a campaign that has entered an error state on Instantly's side.
 *
 * @param instantlyCampaignId - The Instantly campaign ID (not our internal ID)
 * @param reason - Human-readable reason (typically JSON of not_sending_status)
 */
export async function handleCampaignError(
  instantlyCampaignId: string,
  reason: string,
): Promise<void> {
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

  // Already handled
  if (campaign.status === "error") {
    return;
  }

  console.error(
    `[campaign-error] Campaign ${instantlyCampaignId} (${campaign.campaignId}/${campaign.leadEmail}) → error: ${reason}`,
  );

  // 2. Update DB status to "error" with reason in metadata
  const existingMetadata =
    (campaign.metadata as Record<string, unknown>) || {};
  await db
    .update(instantlyCampaigns)
    .set({
      status: "error",
      deliveryStatus: "failed",
      metadata: { ...existingMetadata, errorReason: reason },
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
      await updateCostStatus(cost.runId, cost.costId, "cancelled");
      await db
        .update(sequenceCosts)
        .set({ status: "cancelled", updatedAt: new Date() })
        .where(eq(sequenceCosts.id, cost.id));
      console.log(
        `[campaign-error] Cancelled ${cost.status} cost ${cost.costId} for step ${cost.step}`,
      );
      // Fail the step's run (may already be completed for step 1)
      try {
        await updateRun(cost.runId, "failed", reason);
        console.log(`[campaign-error] Failed run ${cost.runId} for step ${cost.step}`);
      } catch (runErr: unknown) {
        const msg = runErr instanceof Error ? runErr.message : String(runErr);
        console.warn(`[campaign-error] Could not fail run ${cost.runId}: ${msg}`);
      }
    }
  }

  // 4. Mark the run as failed (MUST succeed — let it throw)
  if (campaign.runId) {
    await updateRun(campaign.runId, "failed", reason);
    console.log(
      `[campaign-error] Marked run ${campaign.runId} as failed`,
    );
  }

  // 5. Send admin notification (non-fatal)
  try {
    await sendEmail({
      appId: "instantly-service",
      eventType: "campaign-error",
      recipientEmail: ADMIN_EMAIL,
      metadata: {
        campaignId: campaign.campaignId || "unknown",
        leadEmail: campaign.leadEmail || "unknown",
        instantlyCampaignId,
        errorReason: reason,
      },
    });
    console.log(`[campaign-error] Admin notification sent to ${ADMIN_EMAIL}`);
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.warn(
      `[campaign-error] Failed to send admin notification: ${message}`,
    );
  }
}
