/**
 * Heartbeat worker that drives `runRetryStuck` on a fixed interval.
 *
 * Replaces the daily GitHub Actions cron (which often blocked for hours and
 * dropped > 1000 rows per sweep on transient `fetch failed` errors). With a
 * 10-minute heartbeat, each tick processes a bounded `MAX_ROWS_PER_TICK`
 * chunk, holds the advisory lock for ~seconds, and converges the backlog
 * smoothly instead of in one daily megastampede.
 *
 * Lifecycle:
 *   - `startRetryStuckWorker()` arms the setInterval (no immediate tick on
 *     boot — port binding stays unblocked).
 *   - `stopRetryStuckWorker()` clears the interval. SIGTERM/SIGINT handlers
 *     wired automatically so Railway's graceful-shutdown signal stops new
 *     ticks before the process exits.
 *   - Idempotent: calling start twice is a no-op.
 *
 * Tick-internal errors propagate through `runRetryStuck`'s own logging — the
 * worker swallows any thrown rejection so a single bad tick can't crash the
 * service.
 */

import { runRetryStuck } from "./retry-stuck";

/**
 * Tick interval in ms. Override via env for staging / debugging.
 *
 * One tick processes up to MAX_ROWS_PER_TICK (100) rows, each costing ~7-8s
 * of wall-clock (Instantly throttle 110ms × 8 calls + DB + runs-service).
 * Observed prod duration is ~12-13min per tick. Setting the interval below
 * the worst-case tick duration causes adjacent ticks to short-circuit on
 * the advisory lock — wasted CPU + misleading "silent" gaps in DB writes.
 * Default 15min keeps a ~2min buffer between tick completion and the next
 * fire.
 */
export const RETRY_STUCK_TICK_INTERVAL_MS = (() => {
  const DEFAULT_MS = 15 * 60 * 1000;
  const raw = process.env.RETRY_STUCK_TICK_INTERVAL_MS;
  if (!raw) return DEFAULT_MS;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed <= 0) return DEFAULT_MS;
  return parsed;
})();

let intervalHandle: ReturnType<typeof setInterval> | null = null;
let signalHandlersInstalled = false;

function installSignalHandlers(): void {
  if (signalHandlersInstalled) return;
  signalHandlersInstalled = true;
  const handler = () => {
    console.log(
      `[instantly-service] retry-stuck worker: stopping (received shutdown signal)`,
    );
    stopRetryStuckWorker();
  };
  process.on("SIGTERM", handler);
  process.on("SIGINT", handler);
}

async function tick(): Promise<void> {
  try {
    await runRetryStuck();
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(
      `[instantly-service] retry-stuck worker: tick threw — ${message}`,
    );
  }
}

/**
 * Arm the heartbeat. Safe to call multiple times — subsequent calls are
 * ignored while the worker is already running.
 */
export function startRetryStuckWorker(): void {
  if (intervalHandle !== null) return;
  installSignalHandlers();
  intervalHandle = setInterval(() => {
    void tick();
  }, RETRY_STUCK_TICK_INTERVAL_MS);
  // Allow the Node event loop to exit naturally during graceful shutdown.
  if (typeof intervalHandle.unref === "function") {
    intervalHandle.unref();
  }
  console.log(
    `[instantly-service] retry-stuck worker: started (interval=${RETRY_STUCK_TICK_INTERVAL_MS}ms)`,
  );
}

/** Clear the interval. Safe to call when the worker isn't running. */
export function stopRetryStuckWorker(): void {
  if (intervalHandle === null) return;
  clearInterval(intervalHandle);
  intervalHandle = null;
}
