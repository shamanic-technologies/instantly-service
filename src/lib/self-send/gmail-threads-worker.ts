/**
 * Own in-process drive for the Gmail-thread fold-in (self-send/gmail-threads).
 *
 * The mirror refreshes about every 15 minutes, so does this: a first pass 60 s
 * after boot (deploys restart the container more often than the interval
 * fires), then every 15 minutes. Joins a pass already running. A failure is
 * logged loud and the next tick retries.
 */
import { runGmailThreadsPass } from "./gmail-threads";

export const GMAIL_THREADS_INTERVAL_MS = 15 * 60 * 1000;
export const GMAIL_THREADS_FIRST_TICK_MS = 60 * 1000;

let timer: NodeJS.Timeout | null = null;

function tick(): void {
  runGmailThreadsPass()
    .then((s) => {
      console.log(
        `[instantly-service] gmail-threads: done leads=${s.leadsRead} read=${s.messagesRead} filed=${s.filed} staffAnswers=${s.staffAnswers} already=${s.alreadyIngested} noSequence=${s.noSequence} ambiguous=${s.ambiguous} batchesFailed=${s.batchesFailed}`,
      );
    })
    .catch((error: unknown) => {
      const message = error instanceof Error ? error.message : String(error);
      console.error(`[instantly-service] gmail-threads tick FAILED: ${message}; conversations held from our own mailboxes are not being folded in`);
    });
}

export function startGmailThreadsWorker(): void {
  if (timer) return;
  setTimeout(tick, GMAIL_THREADS_FIRST_TICK_MS).unref?.();
  timer = setInterval(tick, GMAIL_THREADS_INTERVAL_MS);
  timer.unref?.();
  console.log(
    `[instantly-service] gmail-threads worker armed (first pass in ${GMAIL_THREADS_FIRST_TICK_MS}ms, then every ${GMAIL_THREADS_INTERVAL_MS}ms)`,
  );
}
