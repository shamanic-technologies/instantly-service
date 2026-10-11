/**
 * Own in-process drive for the unanswered-question deadline (lib/info-request-deadline).
 *
 * Deploys restart the container more often than a long interval fires, so: a
 * first pass 60 s after boot, then every 15 minutes. Joins a sweep already
 * running. A failure is logged loud and the next tick retries.
 */
import { alertOverdueInfoRequests } from "./info-request-deadline";

export const INFO_REQUEST_DEADLINE_INTERVAL_MS = 15 * 60 * 1000;
export const INFO_REQUEST_DEADLINE_FIRST_TICK_MS = 60 * 1000;

let timer: NodeJS.Timeout | null = null;

function tick(): void {
  alertOverdueInfoRequests().catch((error: unknown) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[instantly-service] info-request-deadline tick FAILED: ${message}; unanswered questions are not being chased`);
  });
}

export function startInfoRequestDeadlineWorker(): void {
  if (timer) return;
  setTimeout(tick, INFO_REQUEST_DEADLINE_FIRST_TICK_MS).unref?.();
  timer = setInterval(tick, INFO_REQUEST_DEADLINE_INTERVAL_MS);
  timer.unref?.();
  console.log(
    `[instantly-service] info-request-deadline worker armed (first pass in ${INFO_REQUEST_DEADLINE_FIRST_TICK_MS}ms, then every ${INFO_REQUEST_DEADLINE_INTERVAL_MS}ms)`,
  );
}
