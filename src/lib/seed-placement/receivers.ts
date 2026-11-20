/**
 * The Google receivers of the weekly seed test, ONE rule for the run, the
 * comparison and the Monitoring health check (PF-CLOUDFLARE-1120).
 *
 * ⚠️ WHY. Every Google receiver used to be a Primeforge mailbox of the fleet
 * (10 of 10 on 2026-11-20). Primeforge deletes them on 2026-11-29. Two traps
 * follow, and this module closes both:
 *   - a mailbox whose domain's inbound has moved to Cloudflare Email Routing (or
 *     will have by the test) receives nothing new: a seed sent to it reads
 *     MISSING, and missing drags every sender's score (`classify.ts`) until the
 *     whole fleet fails the delivery bar. It is no receiver
 *     (`loadDomainsInboundRoutedAway`). A send-only address (no login) is none
 *     either: nothing can read it;
 *   - with the fleet's Google mailboxes gone, the lifecycle gate (Gmail only,
 *     Microsoft never gates) gets no fresh evidence and every mailbox goes
 *     `delivery_evidence_stale` 16 days after the last Google test. So a Google
 *     mailbox that only RECEIVES can be listed in the manual `mailbox-credentials`
 *     key with `seedReceiver: true` (address, appPassword, imapHost
 *     `imap.gmail.com`): it joins every test beside the fleet's set (no
 *     per-domain cap: several `gmail.com` inboxes are several receivers), read by
 *     the same IMAP sync.
 */
import type { CallerInfo } from "../key-client";
import { loadManualCredentials, type MailboxCredential } from "../self-send/mailbox-credentials";
import { loadDomainsInboundRoutedAway } from "../self-send/email-routing";
import { espForReceiverHost, MAX_SEED_RECEIVERS, selectSeedReceivers, type SeedReceiver } from "./seeds";

/** Pure: the fleet's gradeable receivers (credentialed, inbound not routed away) plus the receive-only ones. */
export function selectGoogleSeedReceivers(
  pool: readonly string[],
  resolve: (email: string) => MailboxCredential | null,
  routedAway: ReadonlySet<string>,
  receiveOnly: readonly { address: string; imapHost: string }[],
): SeedReceiver[] {
  const candidates = [];
  for (const email of pool) {
    const credential = resolve(email);
    if (!credential) continue;
    if (routedAway.has(email.trim().toLowerCase().split("@")[1] ?? "")) continue;
    candidates.push({ email, imapHost: credential.imapHost });
  }
  const fleet = selectSeedReceivers(candidates, MAX_SEED_RECEIVERS);
  const extra: SeedReceiver[] = [];
  for (const r of [...receiveOnly].sort((a, b) => (a.address < b.address ? -1 : 1))) {
    const email = r.address.trim().toLowerCase();
    const recipientEsp = espForReceiverHost(email, r.imapHost);
    if (recipientEsp === null || fleet.some((f) => f.email === email) || extra.some((x) => x.email === email)) continue;
    extra.push({ email, recipientEsp });
  }
  return [...fleet, ...extra];
}

/** Read the inputs and apply the rule. */
export async function loadGoogleSeedReceivers(
  pool: readonly string[],
  resolve: (email: string) => MailboxCredential | null,
  caller: CallerInfo,
  asOf: Date = new Date(),
): Promise<SeedReceiver[]> {
  const [routedAway, manual] = await Promise.all([loadDomainsInboundRoutedAway(asOf), loadManualCredentials(caller)]);
  return selectGoogleSeedReceivers(pool, resolve, routedAway, manual.filter((m) => m.seedReceiver === true));
}
