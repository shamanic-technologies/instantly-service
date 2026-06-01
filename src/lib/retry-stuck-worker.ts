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

/**
 * How long to sleep when the SELECT returns no candidates.
 *
 * Default 4h. Rationale: the selection filter has a 72h floor on
 * `created_at`, so once the backlog drains, the next candidate cannot
 * appear before the next row crosses the 72h threshold. New `contacted`
 * rows trickle in slowly (only those Instantly hasn't dispatched after 3
 * days). Polling more aggressively than ~4h wastes DB queries with zero
 * chance of finding work.
 */
export const RETRY_STUCK_IDLE_SLEEP_MS = (() => {
  const DEFAULT_MS = 4 * 60 * 60 * 1000;
  const raw = process.env.RETRY_STUCK_IDLE_SLEEP_MS;
  if (!raw) return DEFAULT_MS;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed <= 0) return DEFAULT_MS;
  return parsed;
})();

/**
 * Kill-switch (fail-safe OFF). The retry-stuck worker arms ONLY when
 * `RETRY_STUCK_WORKER_ENABLED === "true"`. Any other value — including unset —
 * leaves the worker dormant: no stuck row is ever redispatched.
 *
 * Rationale (2026-06-01 emergency): the redispatch path creates a fresh ACTIVE
 * Instantly campaign per retry WITHOUT pausing the predecessor and WITHOUT
 * person-level reply/unsubscribe suppression, so the same prospect accumulated
 * many simultaneously-active campaigns (audit: 41,661 redundant active
 * campaigns, worst offender 9,994) — including people who had already replied
 * "stop". Defaulting OFF halts the bleed the instant this deploys, with no
 * Railway var required. Re-arm by setting RETRY_STUCK_WORKER_ENABLED=true ONLY
 * after the DIS-148 fix (pause-predecessor + person-level suppression +
 * live-status preflight + bounded retries) has landed.
 *
 * Read inside `startRetryStuckWorker` (not at module load) so tests can toggle
 * the env per case.
 */
function isWorkerEnabled(): boolean {
  return process.env.RETRY_STUCK_WORKER_ENABLED === "true";
}

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
  if (!isWorkerEnabled()) {
    console.warn(
      `[instantly-service] retry-stuck worker: DISABLED — kill-switch active ` +
        `(RETRY_STUCK_WORKER_ENABLED not "true"). No stuck rows will be redispatched. ` +
        `Re-arm only after the DIS-148 redispatch fix lands.`,
    );
    return;
  }
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
