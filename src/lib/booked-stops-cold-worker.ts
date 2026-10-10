/**
 * Own in-process drive for the booked-meeting stop (lib/booked-stops-cold).
 *
 * The dispatch tick also runs the sweep, but its 10-minute interval restarts on every deploy, and
 * the fleet deploys this service more often than that (2026-10-10: three hotfixes in 15 minutes),
 * so a timer-only sweep can go a whole morning without running while Instantly keeps sending. So:
 * a first pass 60 s after boot, then every 2 minutes. Joins a sweep already running (shared
 * in-flight promise in the sweep itself). A failure is logged loud and the next tick retries.
 */
import { stopQueuedSequencesOfBookedPeople } from "./booked-stops-cold";

export const BOOKED_STOPS_INTERVAL_MS = 2 * 60 * 1000;
export const BOOKED_STOPS_FIRST_TICK_MS = 60 * 1000;

const CALLER = { method: "POST", path: "/internal/booked-stops-cold" };

let timer: NodeJS.Timeout | null = null;

function tick(): void {
  stopQueuedSequencesOfBookedPeople(CALLER).catch((error: unknown) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[instantly-service] booked-stops-cold tick failed: ${message}`);
  });
}

export function startBookedStopsWorker(): void {
  if (timer) return;
  setTimeout(tick, BOOKED_STOPS_FIRST_TICK_MS).unref?.();
  timer = setInterval(tick, BOOKED_STOPS_INTERVAL_MS);
  timer.unref?.();
  console.log(
    `[instantly-service] booked-stops-cold worker armed (first pass in ${BOOKED_STOPS_FIRST_TICK_MS}ms, then every ${BOOKED_STOPS_INTERVAL_MS}ms)`,
  );
}
