/**
 * In-process drive for the send outcome declarations (lib/send-outcomes): a first
 * pass 60 s after boot (deploys restart longer timers), then every minute. A pass
 * never stacks onto the next (joined in-flight). A failure is logged loud and the
 * next tick retries: the rows stay undeclared until runs-service has them.
 */
import { errorMessage } from "./db-error.js";
import { declareSendOutcomes } from "./send-outcomes";

export const SEND_OUTCOMES_INTERVAL_MS = 60 * 1000;
export const SEND_OUTCOMES_FIRST_TICK_MS = 60 * 1000;

let timer: NodeJS.Timeout | null = null;

function tick(): void {
  declareSendOutcomes().catch((error: unknown) => {
    console.error(`[instantly-service] send-outcomes tick failed: ${errorMessage(error)}`);
  });
}

export function startSendOutcomesWorker(): void {
  if (timer) return;
  setTimeout(tick, SEND_OUTCOMES_FIRST_TICK_MS).unref?.();
  timer = setInterval(tick, SEND_OUTCOMES_INTERVAL_MS);
  timer.unref?.();
  console.log(
    `[instantly-service] send-outcomes worker armed (first pass in ${SEND_OUTCOMES_FIRST_TICK_MS}ms, then every ${SEND_OUTCOMES_INTERVAL_MS}ms)`,
  );
}
