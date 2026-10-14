/**
 * Own in-process drive for the verification-history mirror
 * (lib/verification-history-mirror). Deploys recreate the container more often
 * than a long interval fires, so: a first pass 60 s after boot, then every 5
 * minutes. Joins a pass already running. A failure is logged loud and the next
 * tick retries from the saved cursor.
 */
import { syncVerificationHistory } from "./verification-history-mirror";

export const VERIFICATION_MIRROR_INTERVAL_MS = 5 * 60 * 1000;
export const VERIFICATION_MIRROR_FIRST_TICK_MS = 60 * 1000;

let timer: NodeJS.Timeout | null = null;

function tick(): void {
  syncVerificationHistory()
    .then((r) => {
      if (r.rows > 0 || !r.caughtUp) {
        console.log(
          `[instantly-service] verification-history-mirror: ${r.rows} row(s) in ${r.pages} page(s), caughtUp=${r.caughtUp}`,
        );
      }
    })
    .catch((error: unknown) => {
      const message = error instanceof Error ? error.message : String(error);
      console.error(`[instantly-service] verification-history-mirror tick failed: ${message}`);
    });
}

export function startVerificationHistoryMirrorWorker(): void {
  if (timer) return;
  setTimeout(tick, VERIFICATION_MIRROR_FIRST_TICK_MS).unref?.();
  timer = setInterval(tick, VERIFICATION_MIRROR_INTERVAL_MS);
  timer.unref?.();
  console.log(
    `[instantly-service] verification-history-mirror worker armed (first pass in ${VERIFICATION_MIRROR_FIRST_TICK_MS}ms, then every ${VERIFICATION_MIRROR_INTERVAL_MS}ms)`,
  );
}
