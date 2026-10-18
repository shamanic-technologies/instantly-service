/**
 * Own in-process drive for the cloud-network list (`datacenter-networks.ts`).
 * Deploys recreate the container more often than a daily interval fires, so the
 * cadence lives in the DATABASE: every tick (first one minute after boot, then
 * hourly) refreshes when the list is older than `DATACENTER_REFRESH_EVERY_MS` or
 * was never loaded. Joins a pass already running. Failures are logged loud; the
 * next tick retries.
 */
import {
  DATACENTER_REFRESH_EVERY_MS,
  datacenterNetworksFetchedAt,
  syncDatacenterNetworks,
} from "./datacenter-networks";

export const DATACENTER_WORKER_FIRST_TICK_MS = 60 * 1000;
export const DATACENTER_WORKER_INTERVAL_MS = 60 * 60 * 1000;

let timer: NodeJS.Timeout | null = null;
let inFlight: Promise<void> | null = null;

export function datacenterNetworksTick(now: Date = new Date()): Promise<void> {
  if (inFlight) return inFlight;
  inFlight = (async () => {
    try {
      const last = await datacenterNetworksFetchedAt();
      if (last !== null && now.getTime() - last.getTime() < DATACENTER_REFRESH_EVERY_MS) return;
      const summary = await syncDatacenterNetworks();
      console.log(`[instantly-service] datacenter-networks: refreshed ${JSON.stringify(summary)}`);
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : String(error);
      console.error(`[instantly-service] datacenter-networks: refresh FAILED: ${message}`);
    }
  })().finally(() => {
    inFlight = null;
  });
  return inFlight;
}

export function startDatacenterNetworksWorker(): void {
  if (timer) return;
  setTimeout(() => void datacenterNetworksTick(), DATACENTER_WORKER_FIRST_TICK_MS).unref?.();
  timer = setInterval(() => void datacenterNetworksTick(), DATACENTER_WORKER_INTERVAL_MS);
  timer.unref?.();
  console.log(
    `[instantly-service] datacenter-networks worker armed (first pass in ${DATACENTER_WORKER_FIRST_TICK_MS}ms, then every ${DATACENTER_WORKER_INTERVAL_MS}ms)`,
  );
}
