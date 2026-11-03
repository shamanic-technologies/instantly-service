/**
 * In-process retry of the follow-ups put back by `restore-cut-followups.ts` whose
 * address got NO verdict (the verifier failed: vendor down, or the org could not
 * pay the 2¢ check). Owner 2026-11-03, option 1: "they go out once the client
 * tops up". A first pass 5 min after boot, then every 6 hours; a refused check
 * costs nothing and is asked again next tick. A failure is logged loud.
 */
import { errorMessage } from "./db-error.js";
import { restoreCutFollowups } from "./restore-cut-followups";

export const RESTORE_AWAITING_VERDICT_INTERVAL_MS = 6 * 60 * 60 * 1000;
export const RESTORE_AWAITING_VERDICT_FIRST_TICK_MS = 5 * 60 * 1000;

let timer: NodeJS.Timeout | null = null;

function tick(): void {
  restoreCutFollowups({
    dryRun: false,
    awaitingVerdict: true,
    caller: { method: "WORKER", path: "/restore-cut-followups/awaiting-verdict" },
  }).catch((error: unknown) => {
    console.error(`[instantly-service] restore-cut-followups awaiting-verdict tick failed: ${errorMessage(error)}`);
  });
}

export function startRestoreAwaitingVerdictWorker(): void {
  if (timer) return;
  setTimeout(tick, RESTORE_AWAITING_VERDICT_FIRST_TICK_MS).unref?.();
  timer = setInterval(tick, RESTORE_AWAITING_VERDICT_INTERVAL_MS);
  timer.unref?.();
  console.log(
    `[instantly-service] restore-cut-followups awaiting-verdict worker armed (first pass in ${RESTORE_AWAITING_VERDICT_FIRST_TICK_MS}ms, then every ${RESTORE_AWAITING_VERDICT_INTERVAL_MS}ms)`,
  );
}
