/**
 * Who wrote this message in a prospect's thread: the PROSPECT, or OUR SIDE?
 *
 * ⚠️ WHY (SENDER-NOT-PROSPECT-1015). Correlation (`correlateSend`) answers
 * "which thread is this in", never "who wrote it". A reply-all in a prospect
 * thread by someone on the client's side reads, to every reader keyed on the
 * thread, exactly like the prospect answering. Prod 2026-10-14, Doc Dinners:
 * our persona handed Jerry (alpinespinalrehab.com) to the brand's rep with
 * dev@docdinners.com in Cc; Dev wrote "When would be a good time to jump on a
 * quick call?" to Jerry in that thread. It was filed as JERRY's reply,
 * classified a meeting request, counted in the brand's stats, queued for the
 * responder, and the 🎉 "Dev-Raj Mandroop (Alpine Spinal Rehab) wants to book a
 * call" email went to Dev himself.
 *
 * WHO IS OUR SIDE, positively identified, never "anyone who is not the lead":
 *  - our own people (`isStaffSender`);
 *  - every domain we send from (`loadOwnMailDomains`: another persona's mailbox);
 *  - the brand itself: its website domain and its sales rep (brand-service);
 *  - the client's own people (the org's members, client-service);
 *  - anyone WE copied into this thread (Cc/Bcc of every answer we dispatched,
 *    both transports): the colleague we handed the prospect to.
 *
 * ⚠️ A PROSPECT WRITING FROM ANOTHER ADDRESS STAYS THE PROSPECT. Anyone not
 * positively ours is the prospect's side: a second address of their own, an
 * assistant or colleague at their company, a personal Gmail. Prod 2026-04-09:
 * jts@travelnewsgroup.com answered our email to jts@etn.travel and copied his
 * other address; a rule "writes TO the lead from another domain = not them"
 * (`isOurSideAnswer`, retired here) would have dropped that reply. The lead's
 * own domain is never ours, and a public mailbox domain (gmail.com…) is never
 * matched by DOMAIN, only by exact address: two strangers share gmail.com.
 *
 * Fail LOUD: an unreadable source (brand-service, client-service, the DB)
 * throws, and the caller leaves the message unfiled to be re-read; filing it
 * either way on a guess would be wrong in one direction or the other.
 */

import { sql } from "drizzle-orm";

import { db } from "../../db";
import { getBrandHandoffContext } from "../brand-client";
import { brandTeamMembers } from "../notify-brand-team";
import { bareAddress, isStaffSender } from "../staff-senders";
import { loadOwnMailDomains } from "./own-mail-domains";

/** Who our side is for ONE thread. Lowercased addresses and domains. */
export interface OurSide {
  addresses: ReadonlySet<string>;
  domains: ReadonlySet<string>;
}

/**
 * Domains anyone can hold an address on. Matched by exact address only: a
 * shared domain says nothing about who is on whose side.
 */
export const PUBLIC_MAIL_DOMAINS: ReadonlySet<string> = new Set([
  "gmail.com",
  "googlemail.com",
  "yahoo.com",
  "ymail.com",
  "hotmail.com",
  "outlook.com",
  "live.com",
  "msn.com",
  "aol.com",
  "icloud.com",
  "me.com",
  "mac.com",
  "proton.me",
  "protonmail.com",
  "gmx.com",
  "gmx.net",
  "mail.com",
  "zoho.com",
  "yandex.com",
  "comcast.net",
  "att.net",
  "verizon.net",
  "sbcglobal.net",
  "bellsouth.net",
]);

function domainOf(address: string): string {
  return address.split("@")[1] ?? "";
}

/**
 * Pure: is `from` on our side of the prospect's thread? Cheap facts first (the
 * lead, our staff), so a caller can skip loading `ourSide` when they decide.
 */
export function quickSenderSide(
  from: string | null | undefined,
  leadEmail: string,
): "prospect" | "ours" | "unknown" {
  const address = bareAddress(from);
  // An unreadable sender keeps the reading every reader already had.
  if (!address) return "prospect";
  if (address === leadEmail.trim().toLowerCase()) return "prospect";
  if (isStaffSender(address, leadEmail)) return "ours";
  return "unknown";
}

/** Pure: the full rule, given who our side is for this thread. */
export function isOurSideSender(
  from: string | null | undefined,
  leadEmail: string,
  ourSide: OurSide,
): boolean {
  const quick = quickSenderSide(from, leadEmail);
  if (quick !== "unknown") return quick === "ours";
  const address = bareAddress(from) as string;
  if (ourSide.addresses.has(address)) return true;
  const domain = domainOf(address);
  if (!domain || domain === domainOf(leadEmail.trim().toLowerCase())) return false;
  if (PUBLIC_MAIL_DOMAINS.has(domain)) return false;
  return ourSide.domains.has(domain);
}

function splitAddresses(value: unknown): string[] {
  if (typeof value !== "string" || !value) return [];
  return value
    .split(/[,;]/)
    .map((part) => bareAddress(part))
    .filter((a): a is string => Boolean(a));
}

function rowsOf(result: unknown): Record<string, unknown>[] {
  return (result as { rows?: Record<string, unknown>[] }).rows ?? [];
}

/** Load who our side is for the thread of one sequence. Throws on any unreadable source. */
export async function loadOurSide(instantlyCampaignId: string): Promise<OurSide> {
  const addresses = new Set<string>();
  const domains = new Set<string>(await loadOwnMailDomains());

  const [campaign] = rowsOf(
    await db.execute(sql`
      SELECT org_id, brand_ids FROM instantly_campaigns
      WHERE instantly_campaign_id = ${instantlyCampaignId}
      LIMIT 1
    `),
  );

  // Everyone we copied on an answer in this thread (the rep at a hand-over).
  const copied = rowsOf(
    await db.execute(sql`
      SELECT payload->>'cc' AS cc, payload->>'bcc' AS bcc
      FROM smtp_dispatch_raw
      WHERE instantly_campaign_id = ${instantlyCampaignId}
        AND (payload ? 'cc' OR payload ? 'bcc')
      UNION ALL
      SELECT payload->>'cc_address_email_list', payload->>'bcc_address_email_list'
      FROM instantly_emails_raw
      WHERE instantly_campaign_id = ${instantlyCampaignId}
        AND payload->>'ue_type' = '3'
    `),
  );
  for (const row of copied) {
    for (const a of [...splitAddresses(row.cc), ...splitAddresses(row.bcc)]) addresses.add(a);
  }

  const orgId = typeof campaign?.org_id === "string" ? campaign.org_id : null;
  const brandIds = Array.isArray(campaign?.brand_ids)
    ? (campaign.brand_ids as unknown[]).filter((b): b is string => typeof b === "string")
    : [];
  if (orgId) {
    for (const brandId of brandIds) {
      const brand = await getBrandHandoffContext(brandId, orgId);
      if (brand?.domain) domains.add(brand.domain);
      const rep = bareAddress(brand?.rep.email ?? null);
      if (rep) addresses.add(rep);
    }
    for (const member of await brandTeamMembers(orgId)) {
      const address = bareAddress(member.email);
      if (address) addresses.add(address);
    }
  }
  return { addresses, domains };
}

/**
 * Was this message, filed on `send`'s thread, written by our side? Loads
 * `ourSide` only when the cheap facts cannot decide.
 */
export async function senderIsOurSide(
  from: string | null | undefined,
  send: { instantlyCampaignId: string; leadEmail: string },
): Promise<boolean> {
  const quick = quickSenderSide(from, send.leadEmail);
  if (quick !== "unknown") return quick === "ours";
  return isOurSideSender(from, send.leadEmail, await loadOurSide(send.instantlyCampaignId));
}
