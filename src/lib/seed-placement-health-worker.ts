/**
 * In-process drive of the seed placement health check (GANDI-OPEN-1030): first
 * pass 60 s after boot, then hourly. Reports to the Monitoring watchdog registry
 * (red on the hub, never an email). Failures are logged loud; the registry turns
 * the check red by itself once the reports stop.
 */
import { errorMessage } from "./db-error.js";
import { reportSeedPlacementHealth } from "./seed-placement/health";

export const SEED_PLACEMENT_HEALTH_INTERVAL_MS = 60 * 60 * 1000;
export const SEED_PLACEMENT_HEALTH_FIRST_TICK_MS = 60 * 1000;

let timer: NodeJS.Timeout | null = null;
let inFlight: Promise<void> | null = null;

export function seedPlacementHealthTick(): Promise<void> {
  if (inFlight) return inFlight;
  inFlight = (async () => {
    try {
      const health = await reportSeedPlacementHealth();
      console.log(`[instantly-service] seed-placement-health: reported ${JSON.stringify(health)}`);
    } catch (error: unknown) {
      console.error(`[instantly-service] seed-placement-health: report FAILED: ${errorMessage(error)}`);
    }
  })().finally(() => {
    inFlight = null;
  });
  return inFlight;
}

export function startSeedPlacementHealthWorker(): void {
  if (timer) return;
  setTimeout(() => void seedPlacementHealthTick(), SEED_PLACEMENT_HEALTH_FIRST_TICK_MS).unref?.();
  timer = setInterval(() => void seedPlacementHealthTick(), SEED_PLACEMENT_HEALTH_INTERVAL_MS);
  timer.unref?.();
}
