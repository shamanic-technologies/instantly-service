/**
 * In-process drive for the interrupted-send sweeps (lib/stale-reservations): a first
 * pass 60 s after boot (a crash or deploy is exactly what leaves the rows behind, and
 * deploys restart longer timers), then every 10 minutes. A failure is logged loud and
 * the next tick retries.
 */
import { reclaimInterruptedSends } from "./stale-reservations";

export const STALE_RESERVATION_INTERVAL_MS = 10 * 60 * 1000;
export const STALE_RESERVATION_FIRST_TICK_MS = 60 * 1000;

let timer: NodeJS.Timeout | null = null;

function tick(): void {
  reclaimInterruptedSends().catch((error: unknown) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[instantly-service] stale-reservation tick failed: ${message}`);
  });
}

export function startStaleReservationWorker(): void {
  if (timer) return;
  setTimeout(tick, STALE_RESERVATION_FIRST_TICK_MS).unref?.();
  timer = setInterval(tick, STALE_RESERVATION_INTERVAL_MS);
  timer.unref?.();
  console.log(
    `[instantly-service] stale-reservation worker armed (first pass in ${STALE_RESERVATION_FIRST_TICK_MS}ms, then every ${STALE_RESERVATION_INTERVAL_MS}ms)`,
  );
}
