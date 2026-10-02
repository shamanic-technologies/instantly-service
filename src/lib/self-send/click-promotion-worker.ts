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
import { backfillScannerClicks } from "./click-scanner-backfill";
import { reactivateScannerPausedSequences } from "./reactivate-scanner-paused";

/** How often the drain runs. Tick cost is one indexed query when idle. */
export const CLICK_PROMOTION_INTERVAL_MS = (() => {
  const DEFAULT_MS = 60_000;
  const raw = process.env.CLICK_PROMOTION_INTERVAL_MS;
  if (!raw) return DEFAULT_MS;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed <= 0) return DEFAULT_MS;
  return parsed;
})();

/**
 * How often decided clicks are re-judged and wrongly stopped sequences resumed.
 *
 * ⚠️ WHY A SECOND LOOP. A click is decided ~2 minutes after it lands, but much of
 * the evidence against it arrives later: the same address clicking for another
 * company tomorrow, the scanner's re-scans from other networks over the next
 * hours, a Defender verdict on its /24. Until this ran by hand only, every such
 * click kept its "human" verdict and its `stop-on-click` pause forever. The
 * repair is the backfill (re-decides `human` hits under the current rule) then
 * the reactivation (resumes what a now-scanner click stopped), both idempotent.
 */
export const CLICK_REPAIR_INTERVAL_MS = (() => {
  const DEFAULT_MS = 6 * 60 * 60 * 1000;
  const raw = process.env.CLICK_REPAIR_INTERVAL_MS;
  if (!raw) return DEFAULT_MS;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed <= 0) return DEFAULT_MS;
  return parsed;
})();

let timer: NodeJS.Timeout | null = null;
let repairTimer: NodeJS.Timeout | null = null;
let inFlight = false;
let repairInFlight = false;

/** Re-judge decided clicks, then resume what a scanner stopped. Exported for tests. */
export async function runClickRepair(): Promise<void> {
  if (repairInFlight) return;
  repairInFlight = true;
  try {
    const backfill = await backfillScannerClicks({ dryRun: false });
    const reactivation = await reactivateScannerPausedSequences({ dryRun: false });
    console.log(
      `[instantly-service] click-repair: done backfill=${JSON.stringify({
        scannerHits: backfill.scannerHits,
        leadsDemoted: backfill.leadsDemoted,
        reasons: backfill.reasons,
      })} reactivation=${JSON.stringify(reactivation)}`,
    );
  } finally {
    repairInFlight = false;
  }
}

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

  repairTimer = setInterval(() => {
    runClickRepair().catch((error: unknown) => {
      const message = error instanceof Error ? error.message : String(error);
      console.error(`[instantly-service] click-repair failed: ${message}`);
    });
  }, CLICK_REPAIR_INTERVAL_MS);
  repairTimer.unref?.();

  console.log(
    `[instantly-service] click-promotion worker armed (every ${CLICK_PROMOTION_INTERVAL_MS}ms)`,
  );
}

export function stopClickPromotionWorker(): void {
  if (repairTimer) {
    clearInterval(repairTimer);
    repairTimer = null;
  }
  if (!timer) return;
  clearInterval(timer);
  timer = null;
}
