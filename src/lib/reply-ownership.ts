/**
 * WHO ACTS ON A PERSON'S EMAIL: the email path (this service) or nobody here
 * (EMAIL-REPLY-ORPHAN-1027). `POST /orgs/reply-ownership`.
 *
 * ⚠️ WHY. crm-service reads the brands' connected Gmail and reports a person's
 * answer to campaign-service's trigger door, EXCEPT a mail addressed to one of
 * our sending mailboxes, which it left to "the email path". It decided that on
 * the RECIPIENT alone (is it in our account list?), but this service acts on a
 * mail only when it holds a sequence with the person that the mail can be filed
 * on. 2026-10-27: pabitrahalder1979@gmail.com answered kevin@distribute.you in a
 * thread the owner opened from his Gmail (no sequence of ours to him): crm
 * skipped it as ours, we never filed it, nobody answered. This read states the
 * email path's OWN rule, so the two halves can never both decline (a drop) nor
 * both act (a double).
 *
 * THE RULE (mirrors the three readers that file a person's mail here):
 *  1. `our_own_mail`: the sender is us (a staff address, or a domain we send
 *     from or own: warmup mesh, seed tests). Nobody to answer; never a person.
 *  2. A STAFF recipient (`isStaffSender`): the staff Gmail mirror is folded for
 *     every lead we hold a sequence with, any org (`gmail-threads.ts`), with the
 *     same sequence choice (`chooseSequence`): matched = `lead_sequence`;
 *     sequences of several orgs and no reference = `ambiguous_orgs` (declined
 *     there, so NOT ours).
 *  3. A sending mailbox we READ (smtp transport, present): the IMAP poll files a
 *     reply that threads on a send of ours, and the orphan judgment one from a
 *     lead this mailbox wrote to. Ours = a sequence of the person sent from that
 *     same real mailbox (`sending_mailbox_sequence`).
 *  4. Any other recipient (the org's own connected Gmail): folded for that ORG's
 *     leads only (`org_mailbox_sequence`).
 *  Else `no_sequence`: not ours, the caller acts.
 *
 * A sequence counts only once it really emailed the person at or before the
 * mail (`loadCandidates`): a mail before our first email is not a reply to us.
 * Fails LOUD: an unreadable answer must never read as "not ours" (a double) nor
 * "ours" (a drop).
 */

import { sql } from "drizzle-orm";

import { db } from "../db";
import { bareAddress, isStaffSender } from "./staff-senders";
import { loadOwnMailDomains } from "./self-send/own-mail-domains";
import {
  chooseSequence,
  loadCandidates,
  referencedCampaigns,
  type SequenceCandidate,
} from "./self-send/gmail-threads";

export type ReplyOwner = "email_path" | "none";

export type ReplyOwnershipReason =
  | "our_own_mail"
  | "lead_sequence"
  | "sending_mailbox_sequence"
  | "org_mailbox_sequence"
  | "ambiguous_orgs"
  | "no_sequence";

export interface ReplyOwnershipInput {
  from: string;
  to: string[];
  at: Date;
  references?: string[];
}

export interface ReplyOwnershipAnswer {
  from: string;
  owner: ReplyOwner;
  reason: ReplyOwnershipReason;
}

export interface ReplyOwnershipFacts {
  /** Domains we send from or own (staff included). */
  ownDomains: ReadonlySet<string>;
  /** Sending addresses we read (smtp, present), lowercased -> their real mailbox login. */
  readMailboxLogin: ReadonlyMap<string, string>;
  /** Every sending address we know, lowercased -> its real mailbox login. */
  mailboxLogin: ReadonlyMap<string, string>;
  /** The person's sequences that had really emailed them by the mail's date. */
  candidates: readonly SequenceCandidate[];
  /** Sequences the mail's references name. */
  referenced: ReadonlySet<string>;
}

function domainOf(address: string): string {
  return address.split("@")[1] ?? "";
}

/** Pure: the email path's answer for one mail, from its facts. */
export function decideReplyOwnership(
  orgId: string,
  input: { from: string; to: readonly string[] },
  facts: ReplyOwnershipFacts,
): { owner: ReplyOwner; reason: ReplyOwnershipReason } {
  const from = bareAddress(input.from) ?? input.from.trim().toLowerCase();
  if (isStaffSender(from) || facts.ownDomains.has(domainOf(from))) {
    return { owner: "email_path", reason: "our_own_mail" };
  }
  const recipients = [...new Set(input.to.map((t) => bareAddress(t)).filter((t): t is string => !!t && t !== from))];

  if (recipients.some((r) => isStaffSender(r))) {
    const choice = chooseSequence(facts.candidates, facts.referenced);
    if (choice.outcome === "matched") return { owner: "email_path", reason: "lead_sequence" };
    if (choice.outcome === "ambiguous") return { owner: "none", reason: "ambiguous_orgs" };
  }

  const readLogins = new Set(recipients.map((r) => facts.readMailboxLogin.get(r)).filter((l): l is string => !!l));
  if (readLogins.size > 0) {
    const sentFromThere = facts.candidates.some((c) => {
      const account = c.accountEmail?.trim().toLowerCase();
      if (!account) return false;
      return readLogins.has(facts.mailboxLogin.get(account) ?? account);
    });
    if (sentFromThere) return { owner: "email_path", reason: "sending_mailbox_sequence" };
  }

  const otherRecipient = recipients.some((r) => !isStaffSender(r) && !facts.readMailboxLogin.has(r));
  if (otherRecipient) {
    const orgChoice = chooseSequence(
      facts.candidates.filter((c) => c.orgId === orgId),
      facts.referenced,
    );
    if (orgChoice.outcome === "matched") return { owner: "email_path", reason: "org_mailbox_sequence" };
  }

  return { owner: "none", reason: "no_sequence" };
}

function rowsOf(result: unknown): Record<string, unknown>[] {
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

/** Every sending address -> its real mailbox login, and which of them we read. */
async function loadMailboxes(): Promise<{
  mailboxLogin: Map<string, string>;
  readMailboxLogin: Map<string, string>;
}> {
  const result = await db.execute(sql`
    SELECT lower(email) AS "email",
           lower(COALESCE(mailbox_login, email)) AS "login",
           (send_transport = 'smtp' AND absent_since IS NULL) AS "read"
    FROM instantly_accounts
  `);
  const mailboxLogin = new Map<string, string>();
  const readMailboxLogin = new Map<string, string>();
  for (const row of rowsOf(result)) {
    const email = String(row.email);
    const login = String(row.login);
    mailboxLogin.set(email, login);
    if (row.read === true) readMailboxLogin.set(email, login);
  }
  return { mailboxLogin, readMailboxLogin };
}

/** IO: the email path's answer for each mail, in order. */
export async function readReplyOwnership(
  orgId: string,
  messages: readonly ReplyOwnershipInput[],
): Promise<ReplyOwnershipAnswer[]> {
  const [ownDomains, mailboxes] = await Promise.all([loadOwnMailDomains(), loadMailboxes()]);
  const answers: ReplyOwnershipAnswer[] = [];
  for (const message of messages) {
    const from = bareAddress(message.from) ?? message.from.trim().toLowerCase();
    const candidates = await loadCandidates(from, message.at);
    const referenced =
      message.references && message.references.length > 0
        ? await referencedCampaigns(message.references)
        : new Set<string>();
    const decision = decideReplyOwnership(orgId, message, {
      ownDomains,
      ...mailboxes,
      candidates,
      referenced,
    });
    answers.push({ from, ...decision });
  }
  return answers;
}
