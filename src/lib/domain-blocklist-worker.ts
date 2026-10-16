/**
 * Own in-process drive for the sending-domain blocklist check
 * (lib/domain-blocklist-sync.ts, DOMAIN-BLOCKLIST-1016). Deploys recreate the
 * container more often than a daily interval fires, so the cadence lives in the
 * DATABASE, not the timer: every tick (first 2 min after boot, then hourly) runs
 * the check when the last one is older than `BLOCKLIST_CHECK_EVERY_MS`, then
 * delivers any staff alert still pending (a failed send is retried next tick).
 * Joins a pass already running. Failures are logged loud; the next tick retries.
 */
import { lastCheckAt, runBlocklistCheck } from "./domain-blocklist-sync";
import { deliverPendingBlocklistAlerts } from "./domain-blocklist-alert";
import { fileDueRemovals } from "./blocklist-removal";
import { loadOwnerContact } from "./owner-identity-client";

/** Pause between two removal requests to the same list: a person's pace. */
export const REMOVAL_SPACING_MS = 60 * 1000;

export const BLOCKLIST_CHECK_EVERY_MS = 23 * 60 * 60 * 1000;
export const BLOCKLIST_WORKER_INTERVAL_MS = 60 * 60 * 1000;
export const BLOCKLIST_WORKER_FIRST_TICK_MS = 2 * 60 * 1000;

let timer: NodeJS.Timeout | null = null;
let inFlight: Promise<void> | null = null;

export function blocklistTick(now: Date = new Date()): Promise<void> {
  if (inFlight) return inFlight;
  inFlight = (async () => {
    let checkedNow = false;
    try {
      const last = await lastCheckAt();
      if (last === null || now.getTime() - last.getTime() >= BLOCKLIST_CHECK_EVERY_MS) {
        const summary = await runBlocklistCheck({ asOf: now });
        console.log(`[instantly-service] domain-blocklist: check done ${JSON.stringify(summary)}`);
        checkedNow = true;
      }
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : String(error);
      console.error(`[instantly-service] domain-blocklist: check FAILED: ${message}`);
    }
    try {
      const alerts = await deliverPendingBlocklistAlerts();
      if (alerts.attempted > 0) {
        console.log(`[instantly-service] domain-blocklist: alerts ${JSON.stringify(alerts)}`);
      }
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : String(error);
      console.error(`[instantly-service] domain-blocklist: alert delivery FAILED: ${message}`);
    }
    // Removal requests ride the DAILY check (after its alert went out), so a
    // failure before the form is retried once a day, and a request that reached
    // the form waits a week (REMOVAL_COOLDOWN_DAYS).
    if (checkedNow) {
      try {
        const removals = await fileDueRemovals({ now, loadContact: loadOwnerContact, spacingMs: REMOVAL_SPACING_MS });
        console.log(`[instantly-service] domain-blocklist: removal requests ${JSON.stringify(removals)}`);
      } catch (error: unknown) {
        const message = error instanceof Error ? error.message : String(error);
        console.error(`[instantly-service] domain-blocklist: removal requests FAILED: ${message}`);
      }
    }
  })().finally(() => {
    inFlight = null;
  });
  return inFlight;
}

export function startDomainBlocklistWorker(): void {
  if (timer) return;
  setTimeout(() => void blocklistTick(), BLOCKLIST_WORKER_FIRST_TICK_MS).unref?.();
  timer = setInterval(() => void blocklistTick(), BLOCKLIST_WORKER_INTERVAL_MS);
  timer.unref?.();
  console.log(
    `[instantly-service] domain-blocklist worker armed (first pass in ${BLOCKLIST_WORKER_FIRST_TICK_MS}ms, then every ${BLOCKLIST_WORKER_INTERVAL_MS}ms, check every ${BLOCKLIST_CHECK_EVERY_MS}ms)`,
  );
}
