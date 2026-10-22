/**
 * In-process drive of the inbox-placement episodes (lib/placement-alert.ts,
 * MAILBOX-MONITORING-1019): first pass 60 s after boot (deploys recreate the
 * container more often than the interval fires), then hourly. Records episodes
 * for the Monitoring page; sends no email (PLACEMENT-ALERT-NO-EMAIL-1022).
 * Joins a pass already running. Failures are logged loud; the next tick retries.
 */
import { runPlacementAlert } from "./placement-alert";

export const PLACEMENT_ALERT_INTERVAL_MS = 60 * 60 * 1000;
export const PLACEMENT_ALERT_FIRST_TICK_MS = 60 * 1000;

let timer: NodeJS.Timeout | null = null;
let inFlight: Promise<void> | null = null;

export function placementAlertTick(): Promise<void> {
  if (inFlight) return inFlight;
  inFlight = (async () => {
    try {
      const tick = await runPlacementAlert({ dryRun: false });
      console.log(
        `[instantly-service] placement-alert: done ${JSON.stringify({
          evaluated: tick.evaluations.length,
          episodesWritten: tick.written.length,
        })}`,
      );
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : String(error);
      console.error(`[instantly-service] placement-alert: pass FAILED: ${message}`);
    }
  })().finally(() => {
    inFlight = null;
  });
  return inFlight;
}

export function startPlacementAlertWorker(): void {
  if (timer) return;
  setTimeout(() => void placementAlertTick(), PLACEMENT_ALERT_FIRST_TICK_MS).unref?.();
  timer = setInterval(() => void placementAlertTick(), PLACEMENT_ALERT_INTERVAL_MS);
  timer.unref?.();
}
