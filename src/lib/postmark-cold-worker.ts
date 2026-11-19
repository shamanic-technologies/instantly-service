/**
 * Cold-mail pipes, in-process drive (POSTMARK-COLD-1117, SES-COLD-1119): first
 * pass 60 s after boot, then every 15 min. Each pass moves every Gandi sending
 * domain one step closer to sending through Amazon SES (identity, DNS written,
 * verification read; `ses-domains.ts`) and files Postmark's bounce reports for
 * what Postmark already sent. Postmark onboarding is no longer driven (SES-COLD-1119:
 * no domain uses Postmark; `POST /internal/seed-placement/postmark-domains/onboard`
 * stays by hand). SES bounces arrive by SNS on `/webhooks/ses`, not here.
 */
import { errorMessage } from "./db-error.js";
import { onboardSesDomains } from "./self-send/ses-domains";
import { pollPostmarkBounces } from "./self-send/postmark-bounces";

export const POSTMARK_COLD_INTERVAL_MS = 15 * 60 * 1000;
export const POSTMARK_COLD_FIRST_TICK_MS = 60 * 1000;

let timer: NodeJS.Timeout | null = null;
let inFlight: Promise<void> | null = null;

export function postmarkColdTick(): Promise<void> {
  if (inFlight) return inFlight;
  inFlight = (async () => {
    try {
      const onboarding = await onboardSesDomains({ dryRun: false });
      console.log(`[instantly-service] ses-cold: onboarding ${JSON.stringify(onboarding)}`);
    } catch (error: unknown) {
      console.error(`[instantly-service] ses-cold: onboarding FAILED: ${errorMessage(error)}`);
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
