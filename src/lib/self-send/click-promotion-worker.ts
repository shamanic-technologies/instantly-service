/**
 * The drain behind the deferred click promotion.
 *
 * A self-send click is recorded undecided and becomes a silver event only once
 * its pairing window has closed (see `click-classification.ts`). Something has
 * to come back for it, and that something is an in-process interval rather than
 * a cron: the hold is ~2 minutes, GitHub Actions schedules routinely slip by
 * far more than that and are skipped outright under load, and every click in
 * the fleet would inherit that slip.
 *
 * ⚠️ NO KILL-SWITCH, deliberately. This is the ONLY path by which a self-send
 * click reaches silver, so a switch would not degrade click tracking, it would
 * end it — and silently, since an undecided hit logs nothing.
 *
 * State lives entirely in the DB (`tracking_hits_raw.classification IS NULL`),
 * so a deploy that kills the process mid-tick loses nothing: the next container
 * starts the interval again and drains whatever accumulated. `POST
 * /internal/self-send/promote-clicks` runs the same sweep by hand.
 */

import { promotePendingClicks } from "./click-promotion";

/** How often the drain runs. Tick cost is one indexed query when idle. */
export const CLICK_PROMOTION_INTERVAL_MS = (() => {
  const DEFAULT_MS = 60_000;
  const raw = process.env.CLICK_PROMOTION_INTERVAL_MS;
  if (!raw) return DEFAULT_MS;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed <= 0) return DEFAULT_MS;
  return parsed;
})();

let timer: NodeJS.Timeout | null = null;
let inFlight = false;

/** Idempotent: a second call while armed is a no-op. */
export function startClickPromotionWorker(): void {
  if (timer) return;

  timer = setInterval(() => {
    // A slow tick must not stack onto the next one — two concurrent sweeps would
    // select the same undecided hits and race each other's promotions.
    if (inFlight) return;
    inFlight = true;

    promotePendingClicks()
      .then((summary) => {
        if (summary.decided > 0 || summary.failed > 0) {
          console.log(
            `[instantly-service] click-promotion: ${JSON.stringify(summary)}`,
          );
        }
      })
      .catch((error: unknown) => {
        const message = error instanceof Error ? error.message : String(error);
        console.error(`[instantly-service] click-promotion tick failed: ${message}`);
      })
      .finally(() => {
        inFlight = false;
      });
  }, CLICK_PROMOTION_INTERVAL_MS);

  // Never hold the event loop open on account of this timer.
  timer.unref?.();

  console.log(
    `[instantly-service] click-promotion worker armed (every ${CLICK_PROMOTION_INTERVAL_MS}ms)`,
  );
}

export function stopClickPromotionWorker(): void {
  if (!timer) return;
  clearInterval(timer);
  timer = null;
}
