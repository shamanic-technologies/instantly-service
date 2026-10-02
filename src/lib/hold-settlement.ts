/**
 * Settling a `sequence_costs` hold — the one place that knows which runs-service
 * costs a hold carries.
 *
 * A hold is a row in a table that serves two masters. To billing it is a
 * reserved charge that must later actualize or cancel; to the send pipeline it
 * is a queued step, and `status='provisioned'` is what every ops surface reads
 * as "not sent yet".
 *
 * A billed hold (written from 2026-10-02) carries TWO cost ids, the step's email
 * split across `instantly-account-email-sent` (`costId`) and
 * `instantly-domain-email-sent` (`domainCostId`). A hold written while sending
 * was not billed (2026-08-24 → 2026-10-02) carries neither; a pre-0038 hold
 * carries one `costId` per row (two rows per step).
 *
 * So settling splits in two. Every cost id the hold carries is PATCHed to the
 * target first; then the LOCAL status flip happens — that is what removes the
 * step from the due set and makes the dispatch worker idempotent.
 *
 * Errors are NOT swallowed. `updateCostStatus` throws on failure and the throw
 * propagates before the local flip, so each caller keeps its own semantics — a
 * terminal run-gone 404 flips the row to `cancelled`, a transient error leaves
 * it `provisioned` for the next sweep (re-PATCHing an already-settled cost to
 * the same status is harmless).
 */
import { eq } from "drizzle-orm";
import { db } from "../db";
import { sequenceCosts } from "../db/schema";
import { updateCostStatus, type IdentityContext } from "./runs-client";

/** The subset of a `sequence_costs` row settling needs. */
export interface SettleableHold {
  /** `sequence_costs.id` — the local row to flip. */
  id: string;
  /** Runs-service run that owns the costs, when there are any. */
  runId: string;
  /** `instantly-account-email-sent` cost id, or NULL for an unbilled hold. */
  costId: string | null;
  /** `instantly-domain-email-sent` cost id, NULL on every pre-2026-10-02 hold. */
  domainCostId?: string | null;
}

/** A hold resolves either into real spend or into a released reservation. */
export type HoldSettlement = "actual" | "cancelled";

/** Every runs-service cost id a hold carries, in declaration order. */
export function holdCostIds(hold: SettleableHold): string[] {
  return [hold.costId, hold.domainCostId ?? null].filter(
    (id): id is string => id !== null,
  );
}

/**
 * Flip a hold to its terminal state, declaring the change to runs-service for
 * every cost the hold carries.
 *
 * Throws whatever `updateCostStatus` throws, before touching the local row.
 */
export async function settleHoldCost(
  hold: SettleableHold,
  target: HoldSettlement,
  identity: IdentityContext,
): Promise<void> {
  for (const costId of holdCostIds(hold)) {
    await updateCostStatus(hold.runId, costId, target, identity);
  }

  await db
    .update(sequenceCosts)
    .set({ status: target, updatedAt: new Date() })
    .where(eq(sequenceCosts.id, hold.id));
}
