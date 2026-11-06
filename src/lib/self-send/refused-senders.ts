/**
 * Addresses whose real mailbox the relay is refusing right now (`sender-health.ts`),
 * with the reason, for the send POOL and the ops reads (SENDING-INFRA-AUDIT-1106).
 *
 * The dispatcher already skipped a silenced mailbox, but the pool a NEW sequence
 * is drawn from did not: a production mailbox whose password Google stopped
 * accepting (`535 5.7.8`) kept being handed leads it could not send, and no read
 * said why its follow-ups had stopped (kevin@pressbeat.ai, 13 refusals on
 * 2026-11-05, 22 follow-ups waiting). One verdict, three readers: the dispatcher
 * (via `selectSilencedSmtpSenders`), the pool (`dropRefusedSenders`), the ops
 * rows (`refusedByRelay*`, `loginFailed`).
 */
import type { CallerInfo } from "../key-client";
import { getOrSetCachedStats } from "../stats-cache";
import { loadMailboxLogins } from "./mailbox-credentials";
import { silencedSenderDetails } from "./sender-health";

export interface RefusedSender {
  mailbox: string;
  since: string;
  response: string;
  responseCode: number | null;
  loginFailed: boolean;
}

const CACHE_KEY = "refused-sender-addresses";

/** Every ADDRESS (lower-cased) whose mailbox the relay refuses now, cached 60 s. */
export async function loadRefusedSenderAddresses(caller: CallerInfo): Promise<Map<string, RefusedSender>> {
  const entries = await getOrSetCachedStats(CACHE_KEY, async () => {
    const mailboxLogins = await loadMailboxLogins(caller);
    // Lazy: dispatch-worker imports the send path, which imports this module.
    const { loadSmtpSenderHealth } = await import("./dispatch-worker");
    const details = silencedSenderDetails(await loadSmtpSenderHealth(mailboxLogins));
    const out: Array<[string, RefusedSender]> = [];
    const seen = new Set<string>();
    for (const [address, login] of mailboxLogins) {
      const d = details.get(login);
      if (!d) continue;
      seen.add(login);
      out.push([address, { mailbox: login, since: d.since.toISOString(), response: d.response, responseCode: d.responseCode, loginFailed: d.loginFailed }]);
    }
    for (const [login, d] of details) {
      if (seen.has(login)) continue;
      out.push([login, { mailbox: login, since: d.since.toISOString(), response: d.response, responseCode: d.responseCode, loginFailed: d.loginFailed }]);
    }
    return out;
  });
  return new Map(entries);
}

/** Drop the accounts whose mailbox the relay refuses (a login failure included). */
export async function dropRefusedSenders<T extends { email: string }>(accounts: readonly T[], caller: CallerInfo): Promise<T[]> {
  const refused = await loadRefusedSenderAddresses(caller);
  return accounts.filter((a) => !refused.has(a.email.trim().toLowerCase()));
}
