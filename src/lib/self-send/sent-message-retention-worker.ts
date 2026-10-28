/**
 * Prunes `smtp_sent_messages` past `SENT_MESSAGE_RETENTION_DAYS`
 * (FACTORY-RECORD-SEND-1028). First pass one minute after boot, then every six
 * hours; one indexed DELETE on `sent_at`. Failures are logged loud; the next
 * tick retries.
 */
import { errorMessage } from "../db-error.js";
import { pruneSentMessages, SENT_MESSAGE_RETENTION_DAYS } from "./send-record";

export const SENT_MESSAGE_RETENTION_FIRST_TICK_MS = 60 * 1000;
export const SENT_MESSAGE_RETENTION_INTERVAL_MS = 6 * 60 * 60 * 1000;

let timer: NodeJS.Timeout | null = null;
let inFlight: Promise<void> | null = null;

export function sentMessageRetentionTick(now: Date = new Date()): Promise<void> {
  if (inFlight) return inFlight;
  inFlight = (async () => {
    try {
      const deleted = await pruneSentMessages(now);
      if (deleted > 0) {
        console.log(
          `[instantly-service] sent-messages: pruned ${deleted} older than ${SENT_MESSAGE_RETENTION_DAYS} days`,
        );
      }
    } catch (error: unknown) {
      console.error(`[instantly-service] sent-messages: prune FAILED: ${errorMessage(error)}`);
    }
  })().finally(() => {
    inFlight = null;
  });
  return inFlight;
}

export function startSentMessageRetentionWorker(): void {
  if (timer) return;
  setTimeout(() => void sentMessageRetentionTick(), SENT_MESSAGE_RETENTION_FIRST_TICK_MS).unref?.();
  timer = setInterval(() => void sentMessageRetentionTick(), SENT_MESSAGE_RETENTION_INTERVAL_MS);
  timer.unref?.();
  console.log(
    `[instantly-service] sent-message retention worker armed (first pass in ${SENT_MESSAGE_RETENTION_FIRST_TICK_MS}ms, then every ${SENT_MESSAGE_RETENTION_INTERVAL_MS}ms)`,
  );
}
