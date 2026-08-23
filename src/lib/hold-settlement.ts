/**
 * Settling a `sequence_costs` hold — the one place that knows a hold may carry
 * no cost.
 *
 * A hold is a row in a table that serves two masters. To billing it is a
 * reserved charge that must later actualize or cancel; to the send pipeline it
 * is a queued step, and `status='provisioned'` is what every ops surface reads
 * as "not sent yet". Since the Instantly subscriptions became a fixed cost we
 * absorb instead of rebilling, new holds are written with `cost_id = NULL`:
 * still a queue entry, no longer a charge.
 *
 * So settling splits in two. The LOCAL status flip always happens — that is
 * what removes the step from the due set and makes the dispatch worker
 * idempotent. The runs-service PATCH happens only for a hold that actually
 * carries a cost id, i.e. a historical row declared before the cutover.
 *
 * Errors are NOT swallowed. `updateCostStatus` throws on failure and the throw
 * propagates before the local flip, so each caller keeps its own semantics — a
 * terminal run-gone 404 flips the row to `cancelled`, a transient error leaves
 * it `provisioned` for the next sweep.
 */
import { eq } from "drizzle-orm";
import { db } from "../db";
import { sequenceCosts } from "../db/schema";
import { updateCostStatus, type IdentityContext } from "./runs-client";

/** The subset of a `sequence_costs` row settling needs. */
export interface SettleableHold {
  /** `sequence_costs.id` — the local row to flip. */
  id: string;
  /** Runs-service run that owns the cost, when there is one. */
  runId: string;
  /** Runs-service cost id, or NULL for a hold we never billed. */
  costId: string | null;
}

/** A hold resolves either into real spend or into a released reservation. */
export type HoldSettlement = "actual" | "cancelled";

/**
 * Flip a hold to its terminal state, declaring the change to runs-service only
 * when the hold was billed in the first place.
 *
 * Throws whatever `updateCostStatus` throws, before touching the local row.
 */
export async function settleHoldCost(
  hold: SettleableHold,
  target: HoldSettlement,
  identity: IdentityContext,
): Promise<void> {
  if (hold.costId !== null) {
    await updateCostStatus(hold.runId, hold.costId, target, identity);
  }

  await db
    .update(sequenceCosts)
    .set({ status: target, updatedAt: new Date() })
    .where(eq(sequenceCosts.id, hold.id));
}
