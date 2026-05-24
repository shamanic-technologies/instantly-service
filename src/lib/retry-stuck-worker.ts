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

/** Tick interval in ms. Override via env for staging / debugging. */
export const RETRY_STUCK_TICK_INTERVAL_MS = (() => {
  const raw = process.env.RETRY_STUCK_TICK_INTERVAL_MS;
  if (!raw) return 10 * 60 * 1000;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed <= 0) return 10 * 60 * 1000;
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
