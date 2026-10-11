/**
 * Own in-process drive for the unanswered-reply deadline (lib/unanswered-reply-deadline).
 *
 * Deploys restart the container more often than a long interval fires, so: a
 * first pass 60 s after boot, then every 15 minutes. Joins a sweep already
 * running. A failure is logged loud and the next tick retries.
 */
import { askClientsAboutUnansweredReplies } from "./unanswered-reply-deadline";

export const UNANSWERED_REPLY_DEADLINE_INTERVAL_MS = 15 * 60 * 1000;
export const UNANSWERED_REPLY_DEADLINE_FIRST_TICK_MS = 60 * 1000;

let timer: NodeJS.Timeout | null = null;

function tick(): void {
  askClientsAboutUnansweredReplies().catch((error: unknown) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[instantly-service] unanswered-reply-deadline tick FAILED: ${message}; unanswered replies are not being chased`);
  });
}

export function startUnansweredReplyDeadlineWorker(): void {
  if (timer) return;
  setTimeout(tick, UNANSWERED_REPLY_DEADLINE_FIRST_TICK_MS).unref?.();
  timer = setInterval(tick, UNANSWERED_REPLY_DEADLINE_INTERVAL_MS);
  timer.unref?.();
  console.log(
    `[instantly-service] unanswered-reply-deadline worker armed (first pass in ${UNANSWERED_REPLY_DEADLINE_FIRST_TICK_MS}ms, then every ${UNANSWERED_REPLY_DEADLINE_INTERVAL_MS}ms)`,
  );
}
