/**
 * Our own people — a message THEY wrote is never a prospect's reply.
 *
 * A person on our side answers a prospect from their own mail client and CCs
 * the campaign's sending mailbox, so the answer lands in the thread we watch.
 * It arrives INBOUND on that mailbox (Instantly files it `ue_type 2`, the IMAP
 * poller finds it in the inbox), and every reader here took "inbound on a
 * campaign thread" to mean "the prospect wrote". Measured 2026-09-29: two such
 * messages from kevin@distribute.you ("(Ignore that last email)", "Please ignore
 * Matthew's last email") were recorded as replies from the lead, anchored the
 * automated responder's next answer, and hid the human takeover the CC was meant
 * to announce.
 *
 * WHO COUNTS: any address on a staff domain. The domain is the agency inbox's
 * own (`agencyInbox()`, kevin@distribute.you unless `ADMIN_NOTIFICATION_EMAIL`
 * says otherwise) plus `STAFF_DOMAINS`. A domain, not a list of addresses,
 * because a teammate answering from their own distribute.you address is the same
 * fact, and no prospect is ever on our own domain. Our COLD sending domains are
 * deliberately NOT staff domains: mail from a sending mailbox is already
 * recorded as outbound where it is sent, and treating the cold estate as staff
 * would reclassify warmup traffic nobody asked about.
 *
 * The one exception is the lead themself: an address equal to the lead's is the
 * lead, whatever domain it sits on (an internal test lead on distribute.you).
 */

import { sql, type SQL } from "drizzle-orm";

import { agencyInbox } from "./agency-inbox";

/** Staff domains beyond the agency inbox's own. Lowercase, no `@`. */
export const STAFF_DOMAINS: readonly string[] = ["distribute.you"];

/** Every domain whose senders are our own people. */
export function staffDomains(): string[] {
  const inboxDomain = agencyInbox().split("@")[1]?.trim().toLowerCase();
  const all = new Set<string>(STAFF_DOMAINS);
  if (inboxDomain) all.add(inboxDomain);
  return [...all];
}

/**
 * The bare address out of a From value that may carry a display name
 * (`Kevin Lourd <kevin@distribute.you>`). Lowercased; null when there is none.
 */
export function bareAddress(value: string | null | undefined): string | null {
  if (!value) return null;
  const angled = value.match(/<([^<>\s]+@[^<>\s]+)>/);
  const raw = angled ? angled[1] : value.match(/[^\s<>"',;]+@[^\s<>"',;]+/)?.[0];
  return raw ? raw.trim().toLowerCase() : null;
}

/**
 * True iff this sender is one of our own people and not the lead.
 *
 * An unreadable sender is NOT staff: the reading that stands when we cannot tell
 * is the one every reader already had, so an address we cannot parse changes
 * nothing.
 */
export function isStaffSender(
  from: string | null | undefined,
  leadEmail?: string | null,
): boolean {
  const address = bareAddress(from);
  if (!address) return false;
  if (leadEmail && address === leadEmail.trim().toLowerCase()) return false;
  const domain = address.split("@")[1];
  return domain !== undefined && staffDomains().includes(domain);
}

/**
 * SQL predicate: the address held in `fromExpr` is on a staff domain.
 *
 * `fromExpr` is a bare address column/expression (Instantly's
 * `from_address_email`). Callers that must also exclude the lead compare it to
 * the lead column themselves; on the Instantly mirror an inbound row from the
 * lead's own address is never on our domain in practice.
 */
export function staffSenderSql(fromExpr: SQL): SQL {
  const domains = staffDomains();
  const list = sql.join(
    domains.map((d) => sql`${d}`),
    sql`, `,
  );
  return sql`(lower(split_part(coalesce(${fromExpr}, ''), '@', 2)) IN (${list}))`;
}
