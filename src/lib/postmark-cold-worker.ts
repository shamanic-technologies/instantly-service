/**
 * POSTMARK-COLD-1117 in-process drive: first pass 60 s after boot, then every
 * 15 min. Each pass moves every waiting Gandi domain one step closer to sending
 * through Postmark (DNS written, verification asked) and files Postmark's
 * bounce reports. A domain blocked by the account's domain cap is retried every
 * pass, so a plan upgrade finishes the migration by itself.
 */
import { errorMessage } from "./db-error.js";
import { onboardPostmarkDomains } from "./self-send/postmark-domains";
import { pollPostmarkBounces } from "./self-send/postmark-bounces";

export const POSTMARK_COLD_INTERVAL_MS = 15 * 60 * 1000;
export const POSTMARK_COLD_FIRST_TICK_MS = 60 * 1000;

let timer: NodeJS.Timeout | null = null;
let inFlight: Promise<void> | null = null;

export function postmarkColdTick(): Promise<void> {
  if (inFlight) return inFlight;
  inFlight = (async () => {
    try {
      const onboarding = await onboardPostmarkDomains({ dryRun: false });
      console.log(`[instantly-service] postmark-cold: onboarding ${JSON.stringify(onboarding)}`);
    } catch (error: unknown) {
      console.error(`[instantly-service] postmark-cold: onboarding FAILED: ${errorMessage(error)}`);
    }
    try {
      const bounces = await pollPostmarkBounces();
      console.log(`[instantly-service] postmark-cold: bounces ${JSON.stringify(bounces)}`);
    } catch (error: unknown) {
      console.error(`[instantly-service] postmark-cold: bounces FAILED: ${errorMessage(error)}`);
    }
  })().finally(() => {
    inFlight = null;
  });
  return inFlight;
}

export function startPostmarkColdWorker(): void {
  if (timer) return;
  setTimeout(() => void postmarkColdTick(), POSTMARK_COLD_FIRST_TICK_MS).unref?.();
  timer = setInterval(() => void postmarkColdTick(), POSTMARK_COLD_INTERVAL_MS);
  timer.unref?.();
}
