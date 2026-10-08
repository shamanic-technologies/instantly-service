/**
 * Frequent catch-up of the Unibox mirror, so a sequence email we SENT reaches
 * bronze within minutes, not the next day.
 *
 * ⚠️ WHY THIS EXISTS. An Instantly-transport send reached `instantly_emails_raw`
 * only through the daily Unibox cron (fires ~10:00-11:00 UTC, GitHub slips it)
 * or when an inbound event re-mirrored its campaign. The conversation read
 * (lib/lead-conversation, behind crm-service's person thread) trusts a NON-empty
 * mirror as the whole thread, so a followup sent after the cron ran was
 * invisible until the next day: measured 2026-10-08, 528 step >= 2 sends known
 * from `email_sent` events absent from the mirror, every one sent after that
 * day's 11:08 run, none older than 30 h. The Unibox showed "followup" as a bare
 * label with no email.
 *
 * ⚠️ NOT ON THE WEBHOOK PATH. Mirroring per campaign on `email_sent` would cost
 * one `/emails` call per send against a 20 req/min workspace cap, and sends
 * burst when schedules open; awaited inside `promoteEvent` it would stall the
 * webhook Instantly disables on failure. One newest-first walk of the whole
 * Unibox covers every send since the last tick in a page or two.
 *
 * Same shape as the messages-sync worker: interval armed after `listen`, a
 * mutex so a slow tick never stacks, all state in the DB (the insert conflicts
 * on `instantly_email_id`). The daily cron stays as the floor: it walks 60
 * pages without stopping early.
 */

import { backfillEmails } from "./emails-backfill";
import { resolvePlatformInstantlyApiKey } from "./key-client";

export const UNIBOX_MIRROR_INTERVAL_MS = (() => {
  const raw = process.env.UNIBOX_MIRROR_INTERVAL_MS;
  const parsed = raw ? Number(raw) : NaN;
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 15 * 60 * 1000;
})();

/**
 * Upper bound per tick. ~580 Unibox emails a day is ~1 page per 15 minutes; a
 * burst at schedule open or a missed tick fits well inside 20 pages (70 s at
 * the mandated 3.5 s pacing on the `/emails` slot).
 */
export const UNIBOX_MIRROR_MAX_PAGES = 20;

let timer: NodeJS.Timeout | null = null;
let inFlight = false;

export async function runUniboxMirrorTick(): Promise<void> {
  if (inFlight) return;
  inFlight = true;
  try {
    const apiKey = await resolvePlatformInstantlyApiKey({
      method: "POST",
      path: "/internal/audit/emails-backfill",
    });
    const summary = await backfillEmails(apiKey, {
      maxPages: UNIBOX_MIRROR_MAX_PAGES,
      stopAtKnownPage: true,
    });
    console.log(`[instantly-service] unibox-mirror: ${JSON.stringify(summary)}`);
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[instantly-service] unibox-mirror tick failed: ${message}`);
  } finally {
    inFlight = false;
  }
}

export function startUniboxMirrorWorker(): void {
  if (timer) return;
  timer = setInterval(() => {
    void runUniboxMirrorTick();
  }, UNIBOX_MIRROR_INTERVAL_MS);
  timer.unref?.();
  console.log(
    `[instantly-service] unibox-mirror worker armed, every ${UNIBOX_MIRROR_INTERVAL_MS}ms`,
  );
}

export function stopUniboxMirrorWorker(): void {
  if (timer) clearInterval(timer);
  timer = null;
}
