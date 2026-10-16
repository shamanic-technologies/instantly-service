/**
 * Own in-process drive for the lead-provenance copy (lib/lead-provenance-sync.ts,
 * SOURCE-YIELD-1016): first pass 60 s after boot (deploys recreate the container
 * more often than the interval fires), then every 30 minutes. Joins a pass
 * already running. A failed pass is logged loud; the next one retries.
 */
import { syncLeadProvenance } from "./lead-provenance-sync";
import { deleteCachedStatsWhere } from "./stats-cache";

export const LEAD_PROVENANCE_WORKER_INTERVAL_MS = 30 * 60 * 1000;
export const LEAD_PROVENANCE_WORKER_FIRST_TICK_MS = 60 * 1000;

let timer: NodeJS.Timeout | null = null;

export async function leadProvenanceTick(): Promise<void> {
  try {
    const summary = await syncLeadProvenance();
    if (summary.inserted > 0) deleteCachedStatsWhere((key) => key.startsWith("lead-source-yield"));
    if (summary.batches > 0) {
      console.log(`[instantly-service] lead-provenance-sync: pass done ${JSON.stringify(summary)}`);
    }
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[instantly-service] lead-provenance-sync: pass FAILED: ${message}`);
  }
}

export function startLeadProvenanceWorker(): void {
  if (timer) return;
  setTimeout(() => void leadProvenanceTick(), LEAD_PROVENANCE_WORKER_FIRST_TICK_MS).unref?.();
  timer = setInterval(() => void leadProvenanceTick(), LEAD_PROVENANCE_WORKER_INTERVAL_MS);
  timer.unref?.();
  console.log(
    `[instantly-service] lead-provenance worker armed (first pass in ${LEAD_PROVENANCE_WORKER_FIRST_TICK_MS}ms, then every ${LEAD_PROVENANCE_WORKER_INTERVAL_MS}ms)`,
  );
}
