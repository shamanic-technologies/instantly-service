/**
 * Stopping a sequence WE dispatch.
 *
 * On the Instantly transport, "stop this lead" means pausing the Instantly
 * campaign and letting the nightly reconcile discover that pause and do the rest
 * — cancel the remaining provisioned holds, delete the contact, mark the row
 * terminal. That is why the Instantly-side effects are deliberately minimal and
 * why CLAUDE.md forbids marking the local row terminal there: doing so makes
 * `reconcileAll` SKIP the row, so its finish closure never runs and the holds
 * leak forever.
 *
 * ⚠️ ON THIS TRANSPORT THE ASYMMETRY REVERSES, and it is not a style choice.
 * There is no Instantly campaign to pause, and `reconcileAll` skips a `self:` row
 * outright — so nothing downstream will ever discover the stop. Both halves have
 * to happen HERE: cancel the holds (the org's refund) and mark the row, which is
 * what removes the lead from the dispatch worker's due set.
 */

import { eq } from "drizzle-orm";

import { db } from "../../db";
import { instantlyCampaigns } from "../../db/schema";
import { cancelRemainingProvisions } from "../silver-promote";

/** The subset of a campaign row a local stop needs. */
export interface SelfSendStopTarget {
  instantlyCampaignId: string;
  campaignId: string | null;
  orgId: string | null;
  userId: string | null;
  runId: string | null;
}

/**
 * Stop a self-dispatched sequence: refund what will never be sent, then take the
 * lead out of the worker's reach.
 *
 * Order matters. The holds are cancelled FIRST, because the status write is what
 * makes the row invisible to the dispatch worker — and a crash between the two
 * should leave a row that still sends over one that has silently lost its refund.
 *
 * Idempotent: `cancelRemainingProvisions` only touches `provisioned` rows, and
 * the status write is a plain assignment.
 */
export async function stopSelfSendSequence(
  campaign: SelfSendStopTarget,
  leadEmail: string,
  reason: string,
): Promise<void> {
  await cancelRemainingProvisions(campaign, leadEmail);

  await db
    .update(instantlyCampaigns)
    .set({ status: "paused", updatedAt: new Date() })
    .where(eq(instantlyCampaigns.instantlyCampaignId, campaign.instantlyCampaignId));

  console.log(
    `[instantly-service] self-send: stopped campaign=${campaign.instantlyCampaignId} lead=${leadEmail} (${reason})`,
  );
}
