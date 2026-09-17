/**
 * What actually makes a due step go out — an in-process interval, not a cron.
 *
 * ⚠️ THE CRON WAS THE BUG. `self-send-cron.yml` declares `15 * * * *` and its
 * own comment promises the delay between "due" and "sent" stays under an hour.
 * GitHub Actions does not honour that: measured over 16.1 days it fired 100
 * times, **6.2 runs a day against 24 declared**, with observed gaps of 2.5h to
 * 5.7h. Scheduled workflows are delayed under load and skipped outright rather
 * than queued, so nothing catches up afterwards. Prod 2026-09-17: five runs in
 * the preceding 24h, the last at 01:36 UTC, no dispatch attempt fleet-wide in
 * the 6h since, and 10,775 provisioned holds waiting — including 58 leads pushed
 * that morning whose local windows had been open since ~07:15 UTC.
 *
 * The same reasoning is already written down one module over, in
 * `click-promotion-worker.ts`: a hold measured in minutes cannot be serviced by
 * a trigger whose slip is measured in hours. A sending window is nine hours
 * wide, which sounds like plenty against a 5.7h gap until you notice a mailbox
 * spends most of that window at its daily cap — so the minutes in which a step
 * is BOTH due and fundable are far narrower than the window, and a multi-hour
 * gap lands outside them routinely.
 *
 * ⚠️ IT OWNS NO SCHEDULING RULES. Which step, in whose business hours, on which
 * mailbox and how many all stay in `dispatch.ts` and `runDispatch`. This module
 * decides ONE thing: how often to ask. In particular the weekend gate, the local
 * 08:00-17:00 window, the per-mailbox cap and the volume ramp are untouched —
 * a tick outside them selects nothing and returns having sent nothing.
 *
 * State lives entirely in the DB, so a deploy that kills the process mid-tick
 * loses nothing: the hold is still `provisioned`, the next container arms the
 * interval again and re-selects it. `POST /internal/self-send/dispatch` runs the
 * same sweep by hand, and `self-send-cron.yml` stays armed as a backstop — all
 * three land in `runDispatch`, whose mutex makes the extra triggers free.
 */

import { runDispatch } from "./dispatch-worker";
import { isSelfSendDispatchEnabled } from "../../routes/self-send";

/**
 * How long a due step can wait before a sweep looks at it.
 *
 * THIS IS THE BOUND THE FEATURE PROMISES: a step that comes due is picked up
 * within `SELF_SEND_DISPATCH_INTERVAL_MS` of coming due, plus the duration of
 * the run that picks it up. Ten minutes rather than one because a sending tick
 * reads every self-send mailbox first (the read-before-send ordering), and that
 * read is an IMAP session per mailbox — polling the fleet continuously is how
 * you get throttled by the very providers whose quotas the ramp protects. A tick
 * with nothing sendable skips the read entirely (see the probe in
 * `runDispatch`), so idle minutes cost two local queries.
 *
 * Overridable per environment, because the right number depends on fleet size
 * and on how long a poll takes there.
 */
export const SELF_SEND_DISPATCH_INTERVAL_MS = (() => {
  const DEFAULT_MS = 10 * 60_000;
  const raw = process.env.SELF_SEND_DISPATCH_INTERVAL_MS;
  if (!raw) return DEFAULT_MS;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed <= 0) return DEFAULT_MS;
  return parsed;
})();

let timer: NodeJS.Timeout | null = null;

/** Idempotent: a second call while armed is a no-op. */
export function startSelfSendDispatchWorker(): void {
  if (timer) return;

  timer = setInterval(() => {
    // Read the switch AT EACH TICK, never captured at arm time, so the operator
    // kill-switch means the same thing here as it does on the route: one env
    // change stops the whole sweep, with no account row touched.
    if (!isSelfSendDispatchEnabled()) return;

    // No in-flight guard here on purpose — `runDispatch` holds the only one, so
    // that it also covers the cron and any hand-run. A tick that lands on a
    // running sweep returns `skippedConcurrent` and does nothing.
    runDispatch({ pollFirst: true }).catch((error: unknown) => {
      const message = error instanceof Error ? error.message : String(error);
      console.error(`[instantly-service] self-send-dispatch tick failed: ${message}`);
    });
  }, SELF_SEND_DISPATCH_INTERVAL_MS);

  // Never hold the event loop open on account of this timer.
  timer.unref?.();

  console.log(
    `[instantly-service] self-send dispatch worker armed (every ${SELF_SEND_DISPATCH_INTERVAL_MS}ms)`,
  );
}

export function stopSelfSendDispatchWorker(): void {
  if (!timer) return;
  clearInterval(timer);
  timer = null;
}
