/**
 * The retry behind the outcome triggers this service fires (lib/website-visit-trigger.ts).
 *
 * A fire is delivered by the click that wrote it, detached; when that delivery cannot reach campaign-service's
 * door (or read the catalogue, or the campaign) the row stays `pending` with a backoff, and THIS drain delivers
 * it when due. An in-process interval, never a cron (a GitHub cron declares a cadence, it does not deliver one):
 * a first pass 60 s after boot, then every minute; a slow pass never stacks onto the next. State lives in the
 * table, so a deploy mid-pass loses nothing (a taken row's lease runs out and the next pass takes it again).
 */
import { drainOutcomeTriggerFires } from "./website-visit-trigger";

export const OUTCOME_TRIGGER_DRAIN_INTERVAL_MS = 60_000;
export const OUTCOME_TRIGGER_DRAIN_FIRST_TICK_MS = 60_000;

let timer: NodeJS.Timeout | null = null;
let inFlight = false;

/** One pass. Exported for tests. */
export async function runOutcomeTriggerDrain(): Promise<void> {
  if (inFlight) return;
  inFlight = true;
  try {
    const counts = await drainOutcomeTriggerFires();
    if (counts.delivered + counts.skipped + counts.refused + counts.retry > 0) {
      console.log(`[instantly-service] outcome-trigger drain: ${JSON.stringify(counts)}`);
    }
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[instantly-service] outcome-trigger drain failed (retried next pass): ${message}`);
  } finally {
    inFlight = false;
  }
}

/** Idempotent: a second call while armed is a no-op. */
export function startOutcomeTriggerWorker(): void {
  if (timer) return;
  setTimeout(() => void runOutcomeTriggerDrain(), OUTCOME_TRIGGER_DRAIN_FIRST_TICK_MS).unref?.();
  timer = setInterval(() => void runOutcomeTriggerDrain(), OUTCOME_TRIGGER_DRAIN_INTERVAL_MS);
  timer.unref?.();
  console.log(
    `[instantly-service] outcome-trigger worker armed (first pass in ${OUTCOME_TRIGGER_DRAIN_FIRST_TICK_MS}ms, then every ${OUTCOME_TRIGGER_DRAIN_INTERVAL_MS}ms)`,
  );
}
