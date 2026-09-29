/**
 * In-process drive for the per-reply verdict projection (lib/reply-verdicts).
 * Armed after `listen`, mutexed, state entirely in the DB — a deploy mid-tick
 * loses nothing. Two minutes, well inside the qualification fallback's
 * 15-minute grace, so the fallback always sees a fresh "no verdict" set.
 */
import { syncReplyVerdicts } from "./reply-verdicts";

export const REPLY_VERDICTS_INTERVAL_MS = 2 * 60 * 1000;
/** Kind events created in the last two days; the overlap is free (unique event id). */
const TICK_WINDOW_DAYS = 2;

let timer: NodeJS.Timeout | null = null;
let inFlight = false;

export function startReplyVerdictsWorker(): void {
  if (timer) return;
  timer = setInterval(() => {
    if (inFlight) return;
    inFlight = true;
    syncReplyVerdicts({ sinceDays: TICK_WINDOW_DAYS })
      .catch((error: unknown) => {
        const message = error instanceof Error ? error.message : String(error);
        console.error(`[instantly-service] reply-verdicts tick failed: ${message}`);
      })
      .finally(() => {
        inFlight = false;
      });
  }, REPLY_VERDICTS_INTERVAL_MS);
  timer.unref?.();
  console.log(`[instantly-service] reply-verdicts worker armed, every ${REPLY_VERDICTS_INTERVAL_MS}ms`);
}
