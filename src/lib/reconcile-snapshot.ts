/**
 * Reconcile-snapshot IO glue (background refresh + fast read).
 *
 * The Instantly side of GET /internal/audit/reconcile requires a fleet-wide
 * THROTTLED API sweep (`listAllCampaignAnalytics` + `listAllCampaignSequenceLengths`)
 * that runs for minutes — far past the gateway/browser timeout. So instead of
 * doing it synchronously in the request, we pre-aggregate the five Instantly
 * counts into a single-row silver snapshot (`instantly_reconcile_snapshot`) via
 * a background refresh, and the GET reads that row in one fast query.
 *
 *   refreshInstantlySnapshot(apiKey) — run the sweep, upsert the snapshot (slow;
 *                                      only ever runs in the background).
 *   readInstantlySnapshot()          — fast single-row read for the request path.
 *   maybeTriggerRefresh(apiKey, why)  — fire-and-forget, guarded so concurrent
 *                                      stale/cold reads don't stack heavy sweeps.
 *
 * Fail loud throughout — a failed sweep propagates (never a fabricated zero).
 */

import { eq } from "drizzle-orm";
import { db } from "../db";
import { instantlyReconcileSnapshot } from "../db/schema";
import {
  listAllCampaignAnalytics,
  listAllCampaignSequenceLengths,
} from "./instantly-client";
import {
  summarizeInstantlyCounts,
  type InstantlyReconcileCounts,
} from "./reconcile-audit";

/** Fixed sentinel PK — the snapshot table holds exactly one row. */
export const RECONCILE_SNAPSHOT_ID = "singleton";

export interface SnapshotRead {
  counts: InstantlyReconcileCounts;
  refreshedAt: Date;
}

// Per-replica in-flight guard: an on-read stale/cold trigger must not stack a
// second fleet-wide sweep while one is already running. Explicit operator
// refreshes (POST /reconcile/refresh) bypass this and always run.
let refreshInFlight = false;

/**
 * Run the fleet-wide Instantly sweep, aggregate the five reconcilable counts,
 * and upsert them into the single snapshot row. Slow (minutes) by design —
 * ONLY call from a background context, never on the request path. Fail loud.
 */
export async function refreshInstantlySnapshot(
  apiKey: string,
): Promise<InstantlyReconcileCounts> {
  const [analytics, campaignSequences] = await Promise.all([
    listAllCampaignAnalytics(apiKey),
    listAllCampaignSequenceLengths(apiKey),
  ]);
  const counts = summarizeInstantlyCounts(analytics, campaignSequences);
  const refreshedAt = new Date();

  await db
    .insert(instantlyReconcileSnapshot)
    .values({ id: RECONCILE_SNAPSHOT_ID, ...counts, refreshedAt })
    .onConflictDoUpdate({
      target: instantlyReconcileSnapshot.id,
      set: {
        activeCampaigns: counts.activeCampaigns,
        emailsSent: counts.emailsSent,
        contactedDispatched: counts.contactedDispatched,
        contactsStored: counts.contactsStored,
        pendingSends: counts.pendingSends,
        refreshedAt,
      },
    });

  return counts;
}

/** Fast single-row read of the pre-aggregated Instantly counts, or null if never computed. */
export async function readInstantlySnapshot(): Promise<SnapshotRead | null> {
  const rows = await db
    .select()
    .from(instantlyReconcileSnapshot)
    .where(eq(instantlyReconcileSnapshot.id, RECONCILE_SNAPSHOT_ID))
    .limit(1);

  const row = rows[0];
  if (!row) return null;

  return {
    counts: {
      activeCampaigns: row.activeCampaigns,
      emailsSent: row.emailsSent,
      contactedDispatched: row.contactedDispatched,
      contactsStored: row.contactsStored,
      pendingSends: row.pendingSends,
    },
    refreshedAt: row.refreshedAt,
  };
}

/**
 * Fire-and-forget background refresh for the request path (cold read → seed;
 * stale read → revalidate). Guarded by an in-flight flag so multiple concurrent
 * stale reads don't launch parallel fleet-wide sweeps. Errors are logged, never
 * thrown into the request. Returns true if a refresh was started.
 */
export function maybeTriggerRefresh(apiKey: string, reason: string): boolean {
  if (refreshInFlight) return false;
  refreshInFlight = true;
  refreshInstantlySnapshot(apiKey)
    .then((counts) =>
      console.log(
        `[audit] reconcile-snapshot: refreshed (${reason}) ${JSON.stringify(counts)}`,
      ),
    )
    .catch((error: unknown) => {
      const message = error instanceof Error ? error.message : String(error);
      console.error(
        `[audit] reconcile-snapshot: refresh (${reason}) failed: ${message}`,
      );
    })
    .finally(() => {
      refreshInFlight = false;
    });
  return true;
}
