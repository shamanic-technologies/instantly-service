/**
 * Retry-stuck worker — continuous loop, one row at a time.
 *
 * Design: dead-simple infinite loop.
 *   while (running):
 *     row = selectOneStuckRow()
 *     if !row: sleep IDLE_SLEEP_MS, continue
 *     processRow(row)
 *     -- no extra delay — instantly-client throttle already paces calls --
 *
 * Why a loop and not setInterval:
 *   - Each row takes ~7-8s wall-clock (8 Instantly calls × 110ms throttle
 *     + network + DB writes). A fixed interval that's too short causes
 *     ticks to short-circuit on advisory locks; too long leaves the worker
 *     idle while there's still backlog. A loop is naturally rate-limited
 *     by the actual work and uses 100% of available capacity when the
 *     backlog is non-empty.
 *   - One row at a time means no advisory lock — by construction no two
 *     concurrent processings of the same row inside this process. Multi-
 *     replica safety would require `FOR UPDATE SKIP LOCKED` on the SELECT;
 *     currently the service runs single-replica.
 *   - Sleeps only when the backlog is drained — wakes up immediately when
 *     work appears.
 *
 * Lifecycle:
 *   - `startRetryStuckWorker()` arms the loop. The first row is processed
 *     immediately (no boot delay).
 *   - SIGTERM / SIGINT flip a `shouldStop` flag; the current row finishes
 *     processing, the next loop iteration checks the flag and exits.
 *   - Idempotent: calling start twice is a no-op.
 */

import { processRow, selectOneStuckRow } from "./retry-stuck";

/** How long to sleep when the SELECT returns no candidates. */
export const RETRY_STUCK_IDLE_SLEEP_MS = (() => {
  const DEFAULT_MS = 60 * 1000;
  const raw = process.env.RETRY_STUCK_IDLE_SLEEP_MS;
  if (!raw) return DEFAULT_MS;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed <= 0) return DEFAULT_MS;
  return parsed;
})();

let running = false;
let shouldStop = false;
let signalHandlersInstalled = false;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

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

async function loop(): Promise<void> {
  while (!shouldStop) {
    let row;
    try {
      row = await selectOneStuckRow();
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : String(error);
      console.error(
        `[instantly-service] retry-stuck worker: selectOneStuckRow failed — ${message}`,
      );
      await sleep(RETRY_STUCK_IDLE_SLEEP_MS);
      continue;
    }

    if (!row) {
      await sleep(RETRY_STUCK_IDLE_SLEEP_MS);
      continue;
    }

    // `processRow` returns a discriminated outcome instead of throwing —
    // every failure is captured and counted at log time.
    await processRow(row);
  }
  running = false;
  console.log(`[instantly-service] retry-stuck worker: stopped`);
}

/**
 * Arm the loop. Safe to call multiple times — subsequent calls are
 * ignored while the worker is already running.
 */
export function startRetryStuckWorker(): void {
  if (running) return;
  running = true;
  shouldStop = false;
  installSignalHandlers();
  console.log(`[instantly-service] retry-stuck worker: started`);
  // Fire-and-forget the loop. Caller (src/index.ts) wraps in .catch.
  void loop();
}

/** Signal the loop to exit after the current row finishes. */
export function stopRetryStuckWorker(): void {
  shouldStop = true;
}

/** Test helper: report whether the loop is currently running. */
export function isRetryStuckWorkerRunning(): boolean {
  return running;
}
