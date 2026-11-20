/**
 * Sending addresses we can send from WITHOUT a login (PF-CLOUDFLARE-1120).
 *
 * ⚠️ WHY. "A mailbox" used to mean "an address we hold a password for": the
 * password sent the mail AND read the replies. Since SES-COLD-1119 an address on
 * a domain live on SES leaves through SES, whatever the mailbox: the password is
 * no longer needed to SEND. The 15 Primeforge domains lose their Google
 * mailboxes on 2026-11-29 (owner stops paying Primeforge) and keep sending
 * through SES; their replies come back through Cloudflare Email Routing to the
 * staff Gmail mirror (`email-routing.ts`), not through a login either.
 *
 * So for these addresses the honest definition is: a SENDER, its own real
 * mailbox for capacity (one address = one quota, `login` = the address), read
 * through its domain's routed inbound, never logged into. Every reader that
 * BUDGETS or POOLS sending (capacity, send pool, lifecycle transport, forecast,
 * ops) sees them through `loadMailboxLogins`; every reader that LOGS IN (IMAP
 * poll, inbox watcher, warmup, seed receivers) never does (`source: "ses"`).
 *
 * Who: every present account (`absent_since IS NULL`) on a domain live on SES,
 * read from our own rows. The SES kill switch takes them all out (no pipe left).
 */
import { sql } from "drizzle-orm";

import { db } from "../../db";
import { sesColdSendingDisabled } from "./ses-kill-switch";

export async function loadSendOnlyCandidates(): Promise<string[]> {
  if (sesColdSendingDisabled()) return [];
  const result = await db.execute(sql`
    SELECT DISTINCT lower(a.email) AS email
    FROM instantly_accounts a
    JOIN ses_sending_domains s
      ON s.domain = lower(split_part(a.email, '@', 2))
     AND s.live_since IS NOT NULL
    WHERE a.absent_since IS NULL
  `);
  const rows = ((result as { rows?: unknown[] }).rows ?? []) as { email: string }[];
  return rows.map((r) => r.email);
}
