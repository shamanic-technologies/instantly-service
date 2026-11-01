/**
 * Own in-process drive for the out-of-office reading (absence-judgment.ts,
 * OOO-RETURN-DATE-1101). The dispatch run also judges right after its poll; this
 * timer covers idle ticks (nights, Sundays), the autoresponders stored before
 * deploy, and the follow-up date statements. First pass 60 s after boot, then
 * every 5 minutes; joins a pass already running. A failure is logged loud and
 * the next tick retries.
 */
import { errorMessage } from "../db-error.js";
import { judgeStoredAbsences } from "./absence-judgment";

export const ABSENCE_INTERVAL_MS = 5 * 60 * 1000;
export const ABSENCE_FIRST_TICK_MS = 60 * 1000;

let timer: NodeJS.Timeout | null = null;

function tick(): void {
  judgeStoredAbsences().catch((error: unknown) => {
    console.error(`[instantly-service] absence tick failed: ${errorMessage(error)}`);
  });
}

export function startAbsenceJudgmentWorker(): void {
  if (timer) return;
  setTimeout(tick, ABSENCE_FIRST_TICK_MS).unref?.();
  timer = setInterval(tick, ABSENCE_INTERVAL_MS);
  timer.unref?.();
  console.log(
    `[instantly-service] absence worker armed (first pass in ${ABSENCE_FIRST_TICK_MS}ms, then every ${ABSENCE_INTERVAL_MS}ms)`,
  );
}
