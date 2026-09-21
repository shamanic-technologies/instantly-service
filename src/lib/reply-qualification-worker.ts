/**
 * The drain behind the reply-qualification fallback.
 *
 * A reply Instantly gave no verdict on is invisible to every gate in this
 * service, so something has to come back for it. That something is an
 * in-process interval rather than a cron, for the reason already written into
 * `click-promotion-worker.ts` and re-measured on this repo's own hourly job:
 * GitHub Actions delivered 6.2 runs a day against 24 declared, with gaps of
 * hours. A prospect who asks for pricing, or who writes "STOP!", cannot wait on
 * a schedule that slips by hours.
 *
 * ⚠️ NO KILL-SWITCH, deliberately, and for the same reason the click drain has
 * none: this is the ONLY path by which such a reply is ever qualified, so a
 * switch would not slow the fallback down, it would end it — silently, since an
 * unqualified reply logs nothing and looks exactly like a lead nobody answered.
 *
 * State lives entirely in the event log (a reply with no kind), so a deploy that
 * kills the process mid-tick loses nothing: the next container arms the interval
 * and picks up whatever is still waiting.
 */

import { runReplyQualificationFallback } from "./reply-qualification-fallback";

/**
 * How often the drain runs.
 *
 * Five minutes against a fifteen-minute grace period: a reply is picked up
 * within one tick of becoming eligible, so the worst case is 20 minutes from
 * the reply arriving. An idle tick is one indexed query.
 */
export const QUALIFICATION_FALLBACK_INTERVAL_MS = (() => {
  const DEFAULT_MS = 5 * 60_000;
  const raw = process.env.QUALIFICATION_FALLBACK_INTERVAL_MS;
  if (!raw) return DEFAULT_MS;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed <= 0) return DEFAULT_MS;
  return parsed;
})();

let timer: NodeJS.Timeout | null = null;
let inFlight = false;

/** Idempotent: a second call while armed is a no-op. */
export function startReplyQualificationWorker(): void {
  if (timer) return;

  timer = setInterval(() => {
    // A slow tick must not stack onto the next one — two concurrent sweeps would
    // select the same replies and each pay the model for the same answer.
    if (inFlight) return;
    inFlight = true;

    runReplyQualificationFallback()
      .then((summary) => {
        if (summary.candidates > 0) {
          console.log(
            `[instantly-service] qualification-fallback: ${JSON.stringify(summary)}`,
          );
        }
      })
      .catch((error: unknown) => {
        const message = error instanceof Error ? error.message : String(error);
        console.error(
          `[instantly-service] qualification-fallback tick failed: ${message}`,
        );
      })
      .finally(() => {
        inFlight = false;
      });
  }, QUALIFICATION_FALLBACK_INTERVAL_MS);

  // Never hold the event loop open on account of this timer.
  timer.unref?.();

  console.log(
    `[instantly-service] qualification-fallback worker armed (every ${QUALIFICATION_FALLBACK_INTERVAL_MS}ms)`,
  );
}

export function stopReplyQualificationWorker(): void {
  if (!timer) return;
  clearInterval(timer);
  timer = null;
}
