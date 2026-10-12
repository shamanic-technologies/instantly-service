/**
 * A mailbox we SEND from but whose inbound we never see is BLIND, and a blind
 * mailbox sends no more cold email.
 *
 * ⚠️ WHY. Every reply surface (the Unibox, positive-reply alerts, the stop on
 * reply, the unanswered-question check-ins) is fed by the IMAP poll reading the
 * sending mailbox. When nothing ever lands there, every prospect's answer is
 * lost AND their follow-ups keep going out, to someone who already wrote back.
 * Prod 2026-10-13 (PRESSBEAT-INBOUND-1013): six Gandi domains (pressbeat.ai,
 * growthagency.ch/.life/.forum/.network/.store) carried a Gandi forward on the
 * SAME address as the mailbox (`kevin@ → kevin.lourd@gmail.com`). On Gandi such
 * a forward redirects EVERY message, aliases included, without storing a copy:
 * `kevin@pressbeat.ai` had received ONE message since it was created (UIDNEXT
 * 2, the Gandi welcome) while its aliases sent 6,481 emails. The poll logged
 * the mailbox as polled every run, cleanly, and read nothing. Laura
 * (fusionmedcare.com) and Dylan (peakfamilychiropractic.com) answered and only
 * the owner's Gmail ever saw it.
 *
 * The rule is positive evidence, fleet-wide, keyed on no vendor or domain: a
 * real mailbox that sent at least {@link INBOUND_BLIND_MIN_SENT} emails in the
 * last {@link INBOUND_BLIND_WINDOW_DAYS} days and had NOT ONE inbound message
 * ingested in that window is blind. Calibrated on prod (14 days to 2026-10-13):
 * the five blind domains sent 58-122 emails each with zero inbound; every other
 * sending domain had at least 20 inbound rows (bounces, autoreplies, warmup,
 * prospects). Whatever the cause (a forward, a POP fetch draining the box, a
 * revoked password, a mailbox the poll never lists), the symptom is the same and
 * so is the answer.
 *
 * What it does: the dispatcher stops sending from it (first emails are rehomed
 * like a silenced mailbox's, follow-ups wait), `POST /orgs/send` never assigns it
 * a new sequence, and the ops table shows `blockReason: inbound_unreadable`.
 * Self-healing with no state: the first message ingested clears it on the next
 * read.
 */

import { sql } from "drizzle-orm";
import { db } from "../../db";
import { getOrSetCachedStats } from "../stats-cache";
import { loadMailboxLogins } from "./mailbox-credentials";
import type { CallerInfo } from "../key-client";

export const INBOUND_BLIND_WINDOW_DAYS = 14;
export const INBOUND_BLIND_MIN_SENT = 20;

/** The `blockReason` the ops table shows for a blind mailbox. */
export const INBOUND_UNREADABLE_REASON = "inbound_unreadable";

/** One sending ADDRESS's window: emails out, and whether anything came in. */
export interface AddressInboundRow {
  accountEmail: string;
  sent: number;
  hasInbound: boolean;
}

/**
 * The real mailboxes that are blind. Grain = the mailbox (aliases share one
 * INBOX): sends are summed across aliases, and inbound on ANY alias proves the
 * mailbox is read.
 */
export function selectInboundBlindMailboxes(
  rows: readonly AddressInboundRow[],
  mailboxOf: (accountEmail: string) => string,
  minSent: number = INBOUND_BLIND_MIN_SENT,
): Set<string> {
  const byMailbox = new Map<string, { sent: number; hasInbound: boolean }>();
  for (const row of rows) {
    const mailbox = mailboxOf(row.accountEmail);
    const entry = byMailbox.get(mailbox) ?? { sent: 0, hasInbound: false };
    entry.sent += row.sent;
    entry.hasInbound = entry.hasInbound || row.hasInbound;
    byMailbox.set(mailbox, entry);
  }
  const blind = new Set<string>();
  for (const [mailbox, entry] of byMailbox) {
    if (entry.sent >= minSent && !entry.hasInbound) blind.add(mailbox);
  }
  return blind;
}

/**
 * Every sending address with its window. The inbound side is an EXISTS on the
 * `(account_email, message_id)` unique index, so it costs one index probe per
 * sending address, never a scan of the bronze table.
 */
export async function loadAddressInboundRows(): Promise<AddressInboundRow[]> {
  const result = await db.execute(sql`
    SELECT s.account_email AS "accountEmail",
           s.sent          AS "sent",
           EXISTS (
             SELECT 1 FROM imap_messages_raw i
             WHERE i.account_email = s.account_email
               AND i.polled_at > now() - make_interval(days => ${INBOUND_BLIND_WINDOW_DAYS})
           )               AS "hasInbound"
    FROM (
      SELECT account_email, COUNT(*)::int AS sent
      FROM smtp_dispatch_raw
      WHERE outcome = 'sent'
        AND dispatched_at > now() - make_interval(days => ${INBOUND_BLIND_WINDOW_DAYS})
      GROUP BY 1
    ) s
  `);
  return (result.rows as Record<string, unknown>[]).map((row) => ({
    accountEmail: String(row.accountEmail),
    sent: Number(row.sent),
    hasInbound: row.hasInbound === true,
  }));
}

/** Lowercased real mailbox for an address; an unknown address is its own mailbox. */
export function mailboxResolver(
  mailboxLogins: ReadonlyMap<string, string>,
): (accountEmail: string) => string {
  return (accountEmail) => {
    const key = accountEmail.trim().toLowerCase();
    return mailboxLogins.get(key) ?? key;
  };
}

/** Blind real mailboxes, for a caller that already holds the login map. */
export async function loadInboundBlindMailboxes(
  mailboxLogins: ReadonlyMap<string, string>,
): Promise<Set<string>> {
  return selectInboundBlindMailboxes(
    await loadAddressInboundRows(),
    mailboxResolver(mailboxLogins),
  );
}

const BLIND_ADDRESSES_CACHE_KEY = "inbound-blind-addresses";

/**
 * Every ADDRESS whose real mailbox is blind, cached 60s (the send path calls it
 * per send). An alias that has not sent yet is still returned: a new sequence
 * on it would land in the same unread INBOX.
 */
export async function loadInboundBlindAddresses(caller: CallerInfo): Promise<Set<string>> {
  const addresses = await getOrSetCachedStats(BLIND_ADDRESSES_CACHE_KEY, async () => {
    const mailboxLogins = await loadMailboxLogins(caller);
    const rows = await loadAddressInboundRows();
    const mailboxOf = mailboxResolver(mailboxLogins);
    const blind = selectInboundBlindMailboxes(rows, mailboxOf);
    const out: string[] = [];
    for (const row of rows) {
      if (blind.has(mailboxOf(row.accountEmail))) out.push(row.accountEmail.trim().toLowerCase());
    }
    for (const [address, login] of mailboxLogins) {
      if (blind.has(login)) out.push(address);
    }
    return [...new Set(out)];
  });
  return new Set(addresses);
}

/** Drop the accounts whose mailbox is blind. */
export async function dropInboundBlind<T extends { email: string }>(
  accounts: readonly T[],
  caller: CallerInfo,
): Promise<T[]> {
  const blind = await loadInboundBlindAddresses(caller);
  return accounts.filter((a) => !blind.has(a.email.trim().toLowerCase()));
}
