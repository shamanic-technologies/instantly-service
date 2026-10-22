/**
 * Own in-process drive for lib/queued-sequence-timezone. The dispatch tick also runs it right
 * before selecting, but its interval restarts on every deploy: a first pass 60 s after boot,
 * then every 5 minutes. Joins a pass already running. A failure is logged and retried next tick.
 */
import { errorMessage } from "./db-error.js";
import { healQueuedSequenceTimezones } from "./queued-sequence-timezone";

export const TIMEZONE_HEAL_INTERVAL_MS = 5 * 60 * 1000;
export const TIMEZONE_HEAL_FIRST_TICK_MS = 60 * 1000;

let timer: NodeJS.Timeout | null = null;

function tick(): void {
  healQueuedSequenceTimezones().catch((error: unknown) => {
    const message = errorMessage(error);
    console.error(`[instantly-service] queued-sequence-timezone tick failed: ${message}`);
  });
}

export function startTimezoneHealWorker(): void {
  if (timer) return;
  setTimeout(tick, TIMEZONE_HEAL_FIRST_TICK_MS).unref?.();
  timer = setInterval(tick, TIMEZONE_HEAL_INTERVAL_MS);
  timer.unref?.();
  console.log(
    `[instantly-service] queued-sequence-timezone worker armed (first pass in ${TIMEZONE_HEAL_FIRST_TICK_MS}ms, then every ${TIMEZONE_HEAL_INTERVAL_MS}ms)`,
  );
}
