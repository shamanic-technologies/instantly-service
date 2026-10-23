/**
 * In-process drive for the ledger declarations (lib/send-outcomes, then
 * lib/positive-reply-outcomes): a first
 * pass 60 s after boot (deploys restart longer timers), then every minute. A pass
 * never stacks onto the next (joined in-flight). A failure is logged loud and the
 * next tick retries: the rows stay undeclared until runs-service has them.
 */
import { errorMessage } from "./db-error.js";
import { declarePositiveReplies } from "./positive-reply-outcomes";
import { declareSendOutcomes } from "./send-outcomes";

export const SEND_OUTCOMES_INTERVAL_MS = 60 * 1000;
export const SEND_OUTCOMES_FIRST_TICK_MS = 60 * 1000;

let timer: NodeJS.Timeout | null = null;

function tick(): void {
  // Sends first: a positive reply consumes the person's message_sent, so its
  // producer should already be in the ledger.
  declareSendOutcomes()
    .catch((error: unknown) => {
      console.error(`[instantly-service] send-outcomes tick failed: ${errorMessage(error)}`);
    })
    .then(() => declarePositiveReplies())
    .catch((error: unknown) => {
      console.error(`[instantly-service] positive-reply-outcomes tick failed: ${errorMessage(error)}`);
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
