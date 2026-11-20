/**
 * Routed inbound, in-process drive (PF-CLOUDFLARE-1120): first pass 60 s after
 * boot, then every 15 min (google-service's mirror refreshes about as often).
 * Each pass: (1) moves every scheduled domain one step on Cloudflare Email
 * Routing (`email-routing.ts`: catch-all, root MX when due, live read); (2) files
 * the replies the live domains' catch-all forwarded to the staff Gmail
 * (`forwarded-replies.ts`, last {@link ROUTED_REPLIES_WINDOW_MS}, every address
 * of ours on those domains). Joins a pass already running; a failure is logged
 * loud and the next tick retries.
 */
import { errorMessage } from "../db-error.js";
import { loadLiveRoutedDomains, loadRoutedAddresses, syncEmailRouting } from "./email-routing";
import { LATE_AFTER_MS, recoverForwardedReplies } from "./forwarded-replies";

export const EMAIL_ROUTING_INTERVAL_MS = 15 * 60 * 1000;
export const EMAIL_ROUTING_FIRST_TICK_MS = 60 * 1000;
/** Re-read window: covers a missed pass or a mirror delay; a re-read files nothing twice. */
export const ROUTED_REPLIES_WINDOW_MS = LATE_AFTER_MS;

let timer: NodeJS.Timeout | null = null;
let inFlight: Promise<void> | null = null;

export function emailRoutingTick(asOf: Date = new Date()): Promise<void> {
  if (inFlight) return inFlight;
  inFlight = (async () => {
    try {
      const routing = await syncEmailRouting({ dryRun: false, asOf });
      if (routing.domains > 0) console.log(`[instantly-service] email-routing: ${JSON.stringify(routing)}`);
    } catch (error: unknown) {
      console.error(`[instantly-service] email-routing: sync FAILED: ${errorMessage(error)}`);
    }
    try {
      const live = await loadLiveRoutedDomains();
      const mailboxes = await loadRoutedAddresses(live);
      if (mailboxes.length === 0) return;
      const summary = await recoverForwardedReplies({
        dryRun: false,
        mailboxes,
        asOf,
        since: new Date(asOf.getTime() - ROUTED_REPLIES_WINDOW_MS),
        includeSpam: true,
      });
      const { recovered, ...counts } = summary;
      console.log(
        `[instantly-service] email-routing: routed replies ${JSON.stringify({ ...counts, mailboxes: counts.mailboxes.length, recovered: recovered.length })}`,
      );
    } catch (error: unknown) {
      console.error(`[instantly-service] email-routing: routed replies FAILED: ${errorMessage(error)}; replies to routed domains are not being filed`);
    }
  })().finally(() => {
    inFlight = null;
  });
  return inFlight;
}

export function startEmailRoutingWorker(): void {
  if (timer) return;
  setTimeout(() => void emailRoutingTick(), EMAIL_ROUTING_FIRST_TICK_MS).unref?.();
  timer = setInterval(() => void emailRoutingTick(), EMAIL_ROUTING_INTERVAL_MS);
  timer.unref?.();
}
