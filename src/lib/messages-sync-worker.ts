/**
 * In-process drive for the messages projection.
 *
 * Same shape as the click-promotion worker: an interval armed after `listen`,
 * a mutex so a slow tick never stacks on the next, and state that lives
 * entirely in the DB (the unique source index) so a deploy mid-tick loses
 * nothing. Not a cron — GitHub Actions schedules slip by hours under load and
 * a projection read by a dashboard should not.
 */

import { syncMessages } from "./messages-sync";

export const MESSAGES_SYNC_INTERVAL_MS = (() => {
  const raw = process.env.MESSAGES_SYNC_INTERVAL_MS;
  const parsed = raw ? Number(raw) : NaN;
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 10 * 60 * 1000;
})();

/** One day per tick: a few thousand rows, and the overlap with the last tick is free. */
export const MESSAGES_SYNC_TICK_WINDOW_DAYS = 1;

let timer: NodeJS.Timeout | null = null;
let inFlight = false;

export function startMessagesSyncWorker(): void {
  if (timer) return;
  timer = setInterval(() => {
    if (inFlight) return;
    inFlight = true;
    syncMessages({ sinceDays: MESSAGES_SYNC_TICK_WINDOW_DAYS })
      .catch((error: unknown) => {
        const message = error instanceof Error ? error.message : String(error);
        console.error(`[instantly-service] messages-sync tick failed: ${message}`);
      })
      .finally(() => {
        inFlight = false;
      });
  }, MESSAGES_SYNC_INTERVAL_MS);
  timer.unref?.();
  console.log(
    `[instantly-service] messages-sync worker armed, every ${MESSAGES_SYNC_INTERVAL_MS}ms`,
  );
}

export function stopMessagesSyncWorker(): void {
  if (timer) clearInterval(timer);
  timer = null;
}
