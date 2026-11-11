/**
 * Answering a prospect who replied, in the thread they replied on.
 *
 * A cold sequence exists to produce one thing: someone writes back. Until now
 * nothing in the fleet could write back to them. The only other send path routes
 * through the transactional provider, which means a different sending domain and
 * a brand-new thread — the prospect would see a message from a stranger, on a
 * domain that never contacted them, and the conversation they started would be
 * orphaned.
 *
 * So the reply goes out over the SAME pipe that carried the outreach, from the
 * SAME mailbox, threaded onto the message the prospect actually sent.
 *
 * ⚠️ THE SENDING IDENTITY IS RESOLVED HERE, NEVER ACCEPTED FROM THE CALLER.
 * Which mailbox answers is a fact about what already happened — it is
 * `instantly_campaigns.account_email`, persisted at send time (migration 0025),
 * with the mailbox Instantly recorded on the inbound message as the fallback.
 * A caller-supplied from-address would let a reply arrive from a mailbox this
 * prospect has never heard from, which is exactly the failure this path exists
 * to prevent. The persona (display name + signature) follows the same account,
 * through the existing `buildReplyBodyWithSignature`.
 *
 * ⚠️ NO SILENT FALLBACK. A reply that cannot be threaded is a DIFFERENT email
 * than the caller asked for. Every refusal is an explicit status + a named
 * `code` the caller can branch on; nothing degrades into a fresh cold email.
 *
 * Transport-aware, because the fleet runs two pipes. On the Instantly transport
 * the thread lives in Instantly's Unibox and `POST /emails/reply` threads onto
 * it; on ours, both halves are in bronze and we send the SMTP reply ourselves
 * with `In-Reply-To` / `References`. Either way the prospect sees one thread.
 *
 * ⚠️ THE AGENCY INBOX IS ON EVERY REPLY, IN CC — VISIBLE, NEVER BCC. A human
 * has to be able to read the exchange and to be pulled INTO it, and only a CC
 * survives a reply-all: on a BCC the prospect's answer never reaches them, so
 * the thread silently goes back to being invisible the moment the conversation
 * continues. The address has ONE home (`agency-inbox.ts`), shared with the
 * positive-reply forward and the campaign-error notification.
 *
 * ⚠️ SEQUENCE SENDS CARRY NO CC. A visible agency address on cold outreach
 * reads as a mail-merge to a prospect who has never spoken to us — this applies
 * ONLY to the one-to-one answer, which is a conversation they started.
 *
 * Declares NO cost — same reasoning as the sequence sends themselves: the
 * mailbox estate is a fixed cost we absorb rather than rebill, so a zero-priced
 * row would assert something false (see CLAUDE.md, "Sending declares NO cost").
 */

import { randomUUID } from "node:crypto";
import { errorMessage } from "./db-error.js";
import { sql } from "drizzle-orm";

import { db } from "../db";
import { smtpDispatchRaw } from "../db/schema";
import { agencyInbox } from "./agency-inbox";
import { ReplyToLeadError } from "./reply-to-lead-error";
import { findStatedNotToBrand } from "./stated-not-to-brand";
import { findStandingOptOuts } from "./lead-optouts";
import {
  describeAttachment,
  resolveReplyAttachments,
  type ResolvedAttachment,
  type SentAttachment,
} from "./offer-documents";
import { salesRepCopyList } from "./sales-rep-copy";
import { isStaffSender } from "./staff-senders";
import {
  getAccount,
  listEmails,
  replyToEmail,
  type Account,
  type EmailRecord,
} from "./instantly-client";
import { resolveInstantlyApiKey, type CallerInfo } from "./key-client";
import { buildReplyBodyWithSignature } from "./send-lead";
import { fillSenderTokensInSubject } from "./sender-placeholders";
import {
  resolveMailboxCredential,
  type MailboxCredential,
} from "./self-send/mailbox-credentials";
import { buildFromHeader, subjectForStep } from "./self-send/message";
import { dispatchMessage } from "./self-send/smtp";
import { recordSentMessage } from "./self-send/send-record";
import {
  resolveTransportForSend,
  SEND_TRANSPORT_SMTP,
  type SendTransport,
} from "./self-send/transport";
import { MANUAL_REPLY_STEP } from "./manual-reply-step";
import { findProspectWroteElsewhere, type ElsewhereDestination } from "./reply-destination";
import type { ReplySender } from "./reply-sender";

const CALLER: CallerInfo = { method: "POST", path: "/orgs/replies" };

/**
 * The visible copy on a one-to-one reply: the agency inbox, plus the brand's
 * sales rep when it stated one.
 *
 * The rep JOINS the agency inbox and never replaces it — a human on our side
 * still has to be able to read the exchange and be pulled into it. Both
 * transports take the list as one comma-separated string, which is what
 * Instantly's reply API expects and what nodemailer accepts, so the two halves
 * cannot drift into two spellings of one list.
 *
 * Deduplicated case-insensitively: on a brand whose rep IS the agency address,
 * naming it twice would put the same mailbox on the header twice.
 */
export function replyCcList(agency: string, salesRepCopy: string[]): string {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const address of [agency, ...salesRepCopy]) {
    const trimmed = address.trim();
    if (!trimmed) continue;
    const key = trimmed.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(trimmed);
  }
  return out.join(",");
}

/** The (cc, bcc) pair a reply carries: the hand-over's own, else the default. */
function replyCopy(
  input: ReplyToLeadInput,
  salesRepCopy: string[],
): { cc: string; bcc: string } {
  if (input.copy) {
    return {
      cc: replyCcList("", input.copy.cc),
      bcc: replyCcList("", input.copy.bcc),
    };
  }
  return { cc: replyCcList(agencyInbox(), salesRepCopy), bcc: "" };
}

/** The signed body, with the quoted conversation (if any) after the signature. */
function signedBody(
  input: ReplyToLeadInput,
  account: Account,
  campaign: CampaignRow,
): string {
  // The thread's pinned signature (the client's own, or null = ours): a reply
  // never changes the signature the prospect has been reading.
  const signed = buildReplyBodyWithSignature(input.bodyHtml, account, campaign.signatureHtml);
  return input.quotedHtml ? `${signed}${input.quotedHtml}` : signed;
}

/**
 * The `step` a manual reply is recorded under — defined in its own module so
 * readers can take it without importing this one. Re-exported here because
 * every existing importer takes it from this module.
 */
export { MANUAL_REPLY_STEP };

export { ReplyToLeadError };

export interface ReplyToLeadInput {
  orgId: string;
  userId: string;
  /** Logical campaign id — the same key manual qualifications and opt-outs use. */
  campaignId: string;
  leadEmail: string;
  /** The answer itself, HTML. Signed here; never signed by the caller. */
  bodyHtml: string;
  /**
   * Who asked for this to be sent — a person, or the automated responder.
   *
   * It decides which gates apply: only an `automation` reply goes through the
   * hand-over and stated-not-to-brand gates; a `human` one never does. A
   * person's earlier hand reply never refuses either (lib/reply-sender).
   * Frozen onto the bronze row so a reader can tell who wrote what. Absent on
   * the wire resolves to `automation`, see `DEFAULT_REPLY_SENDER`.
   */
  sentBy: ReplySender;
  /**
   * HAND-OVER ONLY (lib/escalate-reply). Replaces the default visible copy
   * (agency inbox + rep in Cc) with exactly these lists: the client's rep in
   * Cc — a party to the conversation from now on — and the agency inbox in Bcc,
   * observing. Never set by the reply route: the wire cannot reach it.
   */
  copy?: { cc: string[]; bcc: string[] };
  /**
   * Pre-rendered quote of the conversation, appended AFTER the signature.
   * Absent = the lead's whole conversation is read and quoted here
   * (lib/quoted-conversation, QUOTE-THREAD-1103); "" = no quote.
   */
  quotedHtml?: string;
  /**
   * The escalation's own hand-over. It is the one automated write allowed on a
   * thread whose escalation is claimed; every other automated reply there is
   * refused (`handed_over`).
   */
  handoff?: boolean;
  /**
   * Offer documents (brand-service ids) to attach as real files. Resolved here,
   * bytes included, before anything leaves; absent or empty = a reply exactly
   * as before. See lib/offer-documents.
   */
  attachmentDocumentIds?: string[];
  /**
   * WHO the answer goes to, as the caller (the AI reading the Unibox) chose
   * (EVERY-PERSON-ON-THE-THREAD-1109). Each list given becomes that header
   * EXACTLY; a list left out keeps today's default (To = the thread's lead,
   * Cc = agency inbox + rep, no Bcc). Absent = byte-identical to before. Our
   * own sender only (refused 422 on an Instantly-pipe thread).
   */
  recipients?: { to?: string[]; cc?: string[]; bcc?: string[] };
  /**
   * The RFC Message-ID of the message this answer continues: any message of a
   * thread this org holds (ours or theirs, whoever wrote it). The answer leaves
   * from the mailbox that holds that message, In-Reply-To = it, References =
   * the thread up to it. The thread is resolved from the message, so `campaignId`
   * is not needed and `leadEmail` may be anyone on it (it is the default To).
   */
  threadMessageId?: string;
  /** A NEW thread (no In-Reply-To) with this subject, from the thread's mailbox. */
  newThreadSubject?: string;
}

export interface ReplyToLeadResult {
  transport: SendTransport;
  instantlyCampaignId: string;
  leadEmail: string;
  /** The mailbox that answered — resolved here, not supplied. */
  accountEmail: string;
  /** The From header as the prospect sees it, persona included. */
  from: string;
  subject: string;
  /** The agency inbox this reply was CC'd to, as the prospect sees it. */
  cc: string;
  /** Blind copy — empty except on a hand-over or when the caller chose one. */
  bcc: string;
  /** Who it went to. Present only when the caller chose the recipients. */
  to?: string;
  /** Identifier of the message we sent (Instantly's email id, or a Message-Id). */
  messageId: string;
  /** The message this one threads onto. Null only on a NEW thread the caller asked for. */
  inReplyTo: string | null;
  /** The files this reply carried. Absent when it carried none. */
  attachments?: SentAttachment[];
}

/**
 * A reply that has been fully resolved but not yet sent.
 *
 * Resolving everything first keeps every named refusal ahead of the send: the
 * thread, the mailbox and the credential are checked before anything leaves.
 */
interface PreparedInstantlyReply {
  transport: "instantly";
  sentBy: ReplySender;
  key: string;
  target: { emailId: string; subject: string; eaccount: string | null };
  accountEmail: string;
  account: Account;
  subject: string;
  bodyHtml: string;
  cc: string;
  bcc: string;
}

interface PreparedSmtpReply {
  transport: typeof SEND_TRANSPORT_SMTP;
  sentBy: ReplySender;
  anchor: SelfSendThreadAnchor;
  accountEmail: string;
  credential: MailboxCredential;
  account: Account;
  subject: string;
  bodyHtml: string;
  from: string;
  cc: string;
  bcc: string;
  /** The To header. Absent = the thread's lead (as before). */
  to?: string;
}

/**
 * What happened to the answer. There is one success now: it went out.
 *
 * ⚠️ AN ANSWER TO A PROSPECT WHO JUST WROTE TO US IS NEVER HELD (2026-09-25).
 * It used to wait for their Mon-Fri 08:00-17:00 window and came back as a
 * `scheduled` outcome; the owner reversed that. A prospect who replies is at
 * their inbox RIGHT NOW, and an answer that lands the next morning loses them —
 * measured on a positive reply read at 17:43 New York time and queued for 16
 * hours. It also froze the drafted words: "tomorrow, September 25th" would have
 * gone out ON September 25th. Cold sequence steps keep their window; only the
 * one-to-one answer changed. Do NOT reintroduce a hold here.
 */
export type ReplyToLeadOutcome = { status: "sent"; reply: ReplyToLeadResult };

export interface CampaignRow {
  /** The stored campaign row this sequence belongs to. */
  campaignId: string;
  instantlyCampaignId: string;
  leadEmail: string;
  accountEmail: string | null;
  sendTransport: SendTransport;
  /** When the row was created — the order the sequences happened in. */
  createdAt: string | null;
  /**
   * The prospect's IANA timezone, raw as the caller supplied it at send time
   * (migration 0046). Null on a row written before it; the fleet default then
   * applies, the same zone the Instantly schedule degrades to.
   */
  timezone: string | null;
  /**
   * The brand this sequence was sent for — what the CC list is resolved from.
   *
   * Read off the row rather than off the inbound `x-brand-id` header for the
   * same reason the sending identity is: which brand this conversation belongs
   * to is a fact about what already happened, not a caller's argument. Null on
   * a platform send and on a row written before brand tagging, and both mean
   * the reply goes out exactly as it did before the rep existed.
   */
  brandId: string | null;
  /**
   * EVERY brand this sequence was sent for (`brandId` is the first of them).
   * What the "stated not to the brand" gate asks crm-service about.
   * Optional: rows built elsewhere (tests, other readers) may not carry it, and
   * the gate then falls back to `brandId`.
   */
  brandIds?: string[];
  /**
   * The signature block this thread was sent with (migration 0073): the
   * client's own, pinned at send. Null = our default, built from the account.
   */
  signatureHtml: string | null;
}

/**
 * Every sequence this org holds for one lead across the given campaign rows,
 * OLDEST FIRST.
 *
 * A campaign as the customer knows it is many stored rows (campaign-service
 * keeps an ancestor per workflow change), and one prospect can sit in several of
 * them — so reading a whole campaign's exchange means reading every row of it
 * that holds this lead. Callers that want a single row pass a single id.
 *
 * Matched on `lower(lead_email)` — the same normalization every per-person
 * lookup and the serve-side suppression use. `Joe@X.com` and `joe@x.com` are one inbox,
 * and a case-sensitive lookup would refuse to answer a prospect we did email.
 *
 * The ids are bound one by one rather than through `= ANY(<js array>)`: drizzle
 * expands that into a ROW expression which trips Postgres' 1664-entry limit.
 */
export async function loadCampaignSequences(
  orgId: string,
  campaignIds: string[],
  leadEmail: string,
): Promise<CampaignRow[]> {
  if (campaignIds.length === 0) return [];

  const idList = sql.join(
    campaignIds.map((id) => sql`${id}`),
    sql`, `,
  );

  const result = await db.execute(sql`
    SELECT ${CAMPAIGN_ROW_COLUMNS}
    FROM instantly_campaigns c
    WHERE c.org_id = ${orgId}
      AND c.campaign_id IN (${idList})
      AND lower(c.lead_email) = lower(${leadEmail})
    ORDER BY c.created_at ASC, c.instantly_campaign_id ASC
  `);

  return (result.rows as Record<string, unknown>[]).map(toCampaignRow);
}

/** The columns every CampaignRow is read with (one spelling for every reader). */
const CAMPAIGN_ROW_COLUMNS = sql`
      c.campaign_id           AS "campaignId",
      c.instantly_campaign_id AS "instantlyCampaignId",
      c.lead_email            AS "leadEmail",
      c.account_email         AS "accountEmail",
      c.send_transport        AS "sendTransport",
      c.timezone              AS "timezone",
      c.brand_ids[1]          AS "brandId",
      to_jsonb(c.brand_ids)   AS "brandIds",
      c.signature_html        AS "signatureHtml",
      c.created_at            AS "createdAt"`;

function toCampaignRow(row: Record<string, unknown>): CampaignRow {
  return {
    campaignId: String(row.campaignId),
    instantlyCampaignId: String(row.instantlyCampaignId),
    leadEmail: String(row.leadEmail),
    accountEmail:
      row.accountEmail === null || row.accountEmail === undefined
        ? null
        : String(row.accountEmail),
    sendTransport: resolveTransportForSend(
      row.sendTransport === null || row.sendTransport === undefined
        ? null
        : String(row.sendTransport),
    ),
    createdAt:
      row.createdAt === null || row.createdAt === undefined
        ? null
        : String(row.createdAt),
    timezone:
      row.timezone === null || row.timezone === undefined
        ? null
        : String(row.timezone),
    brandId:
      row.brandId === null || row.brandId === undefined
        ? null
        : String(row.brandId),
    brandIds: Array.isArray(row.brandIds) ? (row.brandIds as unknown[]).map(String) : [],
    signatureHtml:
      row.signatureHtml === null || row.signatureHtml === undefined
        ? null
        : String(row.signatureHtml),
  };
}

/** A message of one of this org's threads, as `threadMessageId` names it. */
export interface ChosenThreadMessage {
  campaign: CampaignRow;
  /** The mailbox that holds the message (received it, or sent it). */
  mailbox: string;
  messageId: string;
  subject: string;
  at: string;
}

/**
 * The thread a caller chose by one of its messages (EVERY-PERSON-ON-THE-THREAD-1109):
 * a message we received on a thread (any sender) or one we sent, of THIS org.
 * Null when the org holds no such message. The newest copy wins when several
 * aliases of a mailbox hold it.
 */
export async function findThreadMessage(
  orgId: string,
  messageId: string,
): Promise<ChosenThreadMessage | null> {
  const id = messageId.trim();
  if (!id) return null;
  const result = await db.execute(sql`
    SELECT ${CAMPAIGN_ROW_COLUMNS}, x.mailbox AS "mailbox", x.subject AS "msgSubject", x.at AS "msgAt"
    FROM (
      SELECT m.instantly_campaign_id, m.account_email AS mailbox, m.subject,
             COALESCE(m.received_at, m.polled_at) AS at
      FROM imap_messages_raw m
      WHERE m.message_id = ${id}
        AND m.instantly_campaign_id IS NOT NULL
        AND m.kind IN ('reply', 'auto_reply', 'staff_reply')
      UNION ALL
      SELECT d.instantly_campaign_id, d.account_email, COALESCE(d.payload->>'subject', ''),
             d.dispatched_at
      FROM smtp_dispatch_raw d
      WHERE d.message_id = ${id} AND d.outcome = 'sent'
    ) x
    JOIN instantly_campaigns c ON c.instantly_campaign_id = x.instantly_campaign_id
    WHERE c.org_id = ${orgId}
    ORDER BY x.at DESC
    LIMIT 1
  `);
  const row = (result.rows as Record<string, unknown>[])[0];
  if (!row) return null;
  const campaign = toCampaignRow(row);
  let subject = typeof row.msgSubject === "string" ? row.msgSubject : "";
  if (!subject) {
    const step = await db.execute(sql`
      SELECT s.subject FROM sequence_steps s
      WHERE s.instantly_campaign_id = ${campaign.instantlyCampaignId} AND s.step = 1
      LIMIT 1
    `);
    const first = (step.rows as Record<string, unknown>[])[0];
    subject = typeof first?.subject === "string" ? first.subject : "";
  }
  return {
    campaign,
    mailbox: String(row.mailbox),
    messageId: id,
    subject,
    at: row.msgAt instanceof Date ? row.msgAt.toISOString() : String(row.msgAt),
  };
}

/**
 * The anchor of a chosen message: In-Reply-To = it, References = every message
 * of its thread up to and including it, in the order they happened.
 */
async function loadChosenAnchor(chosen: ChosenThreadMessage): Promise<SelfSendThreadAnchor> {
  const chain = await db.execute(sql`
    SELECT "messageId" FROM (
      SELECT d.message_id AS "messageId", d.dispatched_at AS "at"
      FROM smtp_dispatch_raw d
      WHERE d.instantly_campaign_id = ${chosen.campaign.instantlyCampaignId}
        AND d.outcome = 'sent'
        AND d.message_id IS NOT NULL
      UNION
      SELECT m.message_id, COALESCE(m.received_at, m.polled_at)
      FROM imap_messages_raw m
      WHERE m.instantly_campaign_id = ${chosen.campaign.instantlyCampaignId}
        AND m.kind IN ('reply', 'auto_reply', 'staff_reply')
    ) t
    WHERE t."at" <= ${chosen.at}::timestamp
    ORDER BY t."at"
  `);
  const references: string[] = [];
  for (const r of chain.rows as Record<string, unknown>[]) {
    const id = String(r.messageId);
    if (id !== chosen.messageId && !references.includes(id)) references.push(id);
  }
  references.push(chosen.messageId);
  return { inReplyTo: chosen.messageId, subject: chosen.subject, references };
}

/** A caller-chosen address list as one header value (deduplicated, original casing). */
function headerList(addresses: string[]): string {
  return replyCcList("", addresses);
}

/** Every address the caller chose, lowercased. */
function chosenAddresses(r: NonNullable<ReplyToLeadInput["recipients"]>): string[] {
  return [...(r.to ?? []), ...(r.cc ?? []), ...(r.bcc ?? [])]
    .map((a) => a.trim().toLowerCase())
    .filter((a) => a.length > 0);
}

/**
 * The campaign this lead sits in, for this org — the most recent row of the one
 * campaign id asked for.
 *
 * Expressed through {@link loadCampaignSequences} on purpose: the reply path and
 * the conversation read must never disagree about which sequences exist, and two
 * spellings of that query is how they would.
 */
export async function loadCampaign(
  orgId: string,
  campaignId: string,
  leadEmail: string,
): Promise<CampaignRow | null> {
  const rows = await loadCampaignSequences(orgId, [campaignId], leadEmail);
  return rows.length > 0 ? rows[rows.length - 1] : null;
}

/**
 * The escalation claimed on this thread, or null. `handedTo` is null while the
 * hand-over is still in flight (claimed, not yet recorded).
 */
export async function findEscalationHandoff(
  instantlyCampaignId: string,
): Promise<{ at: string; handedTo: string | null } | null> {
  const result = await db.execute(sql`
    SELECT c.escalated_at AS "at", c.escalation_handed_to AS "handedTo"
    FROM instantly_campaigns c
    WHERE c.instantly_campaign_id = ${instantlyCampaignId}
      AND c.escalated_at IS NOT NULL
    LIMIT 1
  `);
  const row = ((result as { rows?: Record<string, unknown>[] } | undefined)?.rows ?? [])[0];
  if (!row) return null;
  return {
    at: row.at instanceof Date ? row.at.toISOString() : String(row.at),
    handedTo: row.handedTo === null || row.handedTo === undefined ? null : String(row.handedTo),
  };
}

/**
 * The message a reply must thread onto: the prospect's LATEST inbound message.
 *
 * Latest rather than first, because a prospect who wrote twice expects the
 * answer under what they said last. `ue_type === 2` is Instantly's inbound
 * marker — an outbound message of ours is not something to reply to, and
 * threading onto one would open a second branch of the conversation.
 *
 * Returns null when the lead has never written back. That is a refusal, not a
 * reason to send something else.
 */
export function selectReplyTarget(
  records: EmailRecord[],
): { emailId: string; subject: string; eaccount: string | null } | null {
  const inbound = records
    // A staff answer CC'd to the sending mailbox is inbound on the mailbox and is
    // OURS — threading an automated reply onto it answers our own colleague.
    .filter(
      (r) =>
        r.ue_type === 2 &&
        typeof r.id === "string" &&
        r.id !== "" &&
        !isStaffSender(r.from_address_email, r.lead),
    )
    .slice()
    .sort(
      (a, b) =>
        new Date(a.timestamp_email).getTime() - new Date(b.timestamp_email).getTime(),
    );

  const latest = inbound[inbound.length - 1];
  if (!latest) return null;

  return {
    emailId: latest.id,
    subject: latest.subject ?? "",
    eaccount: latest.eaccount || null,
  };
}

/**
 * The subject a reply carries: the conversation's own subject under `Re:`.
 *
 * Reuses `subjectForStep`, which already refuses to double an existing `Re:` —
 * clients render `Re: Re:` verbatim and it reads as machine-sent. An empty
 * subject stays empty rather than becoming a bare `Re:`, which would tell the
 * prospect nothing.
 */
export function replySubject(subject: string): string {
  const trimmed = subject.trim();
  if (!trimmed) return "";
  return subjectForStep(trimmed, 2);
}

/** The sending account as a persona: display name + signature come from it. */
async function loadSelfSendAccount(accountEmail: string): Promise<Account> {
  const result = await db.execute(sql`
    SELECT a.email      AS "email",
           a.first_name AS "firstName",
           a.last_name  AS "lastName"
    FROM instantly_accounts a
    WHERE a.email = ${accountEmail}
    LIMIT 1
  `);

  const row = (result.rows as Record<string, unknown>[])[0];
  return {
    email: accountEmail,
    warmup_status: 1,
    status: 1,
    first_name: typeof row?.firstName === "string" ? row.firstName : undefined,
    last_name: typeof row?.lastName === "string" ? row.lastName : undefined,
  } as Account;
}

interface SelfSendThreadAnchor {
  /** Null only on a new thread the caller asked for. */
  inReplyTo: string | null;
  subject: string;
  /** Every Message-Id of this conversation, oldest first (RFC 5322 References). */
  references: string[];
}

/**
 * The thread anchor for a sequence WE dispatched.
 *
 * `In-Reply-To` is the prospect's latest inbound Message-Id; `References` is the
 * whole conversation — our sends plus their replies — in the order it happened,
 * which is what keeps the exchange collapsed in their client.
 */
async function loadSelfSendAnchor(
  instantlyCampaignId: string,
): Promise<SelfSendThreadAnchor | null> {
  const result = await db.execute(sql`
    SELECT m.message_id AS "messageId",
           m.subject    AS "subject",
           COALESCE(m.received_at, m.polled_at) AS "at"
    FROM imap_messages_raw m
    WHERE m.instantly_campaign_id = ${instantlyCampaignId}
      AND m.kind IN ('reply', 'auto_reply')
    ORDER BY COALESCE(m.received_at, m.polled_at) DESC
    LIMIT 1
  `);

  const latest = (result.rows as Record<string, unknown>[])[0];
  if (!latest) return null;

  const chain = await db.execute(sql`
    SELECT "messageId" FROM (
      SELECT d.message_id AS "messageId", d.dispatched_at AS "at"
      FROM smtp_dispatch_raw d
      WHERE d.instantly_campaign_id = ${instantlyCampaignId}
        AND d.outcome = 'sent'
        AND d.message_id IS NOT NULL

      UNION ALL

      SELECT m.message_id AS "messageId",
             COALESCE(m.received_at, m.polled_at) AS "at"
      FROM imap_messages_raw m
      WHERE m.instantly_campaign_id = ${instantlyCampaignId}
        AND m.kind IN ('reply', 'auto_reply')
    ) t
    ORDER BY t."at"
  `);

  return {
    inReplyTo: String(latest.messageId),
    subject: typeof latest.subject === "string" ? latest.subject : "",
    references: (chain.rows as Record<string, unknown>[]).map((r) =>
      String(r.messageId),
    ),
  };
}

/**
 * Record an Instantly-transport reply in bronze, at step 0.
 *
 * ⚠️ `message_id` is deliberately NULL. On the self-send branch that column
 * holds the RFC 5322 Message-Id we put on the wire, and `loadKnownSends`
 * correlates a prospect's next answer against it through `In-Reply-To` /
 * `References`. Instantly returns its OWN email UUID, which appears in no mail
 * header and could therefore never match — storing it in that column would
 * plant a correlation key that looks usable and silently never fires. The
 * anchor query already filters `message_id IS NOT NULL`, so a null keeps this
 * row out of correlation while still preserving what we wrote. Instantly's id
 * lives in the payload, where it is honestly labelled.
 *
 * Fail-loud on the success path: a reply we cannot record is a reply whose text
 * exists only in Instantly, which is the exact durability gap this closes. The
 * failure path swallows (mirroring the smtp branch) because the caller is
 * already about to throw the real dispatch error, and losing that cause to a
 * bookkeeping error would be worse.
 */
async function recordInstantlyReply(input: {
  campaign: CampaignRow;
  sentBy: ReplySender;
  accountEmail: string;
  subject: string;
  bodyHtml: string;
  cc: string;
  bcc?: string;
  inReplyTo: string;
  outcome: "sent" | "transient";
  instantlyEmailId: string | null;
  error: unknown;
}): Promise<void> {
  const row = {
    instantlyCampaignId: input.campaign.instantlyCampaignId,
    leadEmail: input.campaign.leadEmail,
    accountEmail: input.accountEmail,
    step: MANUAL_REPLY_STEP,
    outcome: input.outcome,
    messageId: null,
    responseCode:
      (input.error as { responseCode?: number } | null)?.responseCode ?? null,
    response: (input.error as { response?: string } | null)?.response ?? null,
    payload: {
      kind: "manual_reply",
      sentBy: input.sentBy,
      transport: "instantly",
      subject: input.subject,
      cc: input.cc,
      ...(input.bcc ? { bcc: input.bcc } : {}),
      ...(input.outcome === "sent" ? { bodyHtml: input.bodyHtml } : {}),
      inReplyTo: input.inReplyTo,
      instantlyEmailId: input.instantlyEmailId,
      ...(input.error
        ? {
            error:
              input.error instanceof Error
                ? input.error.message
                : String(input.error),
          }
        : {}),
    },
  };

  if (input.outcome === "sent") {
    await db.insert(smtpDispatchRaw).values(row);
    return;
  }

  await Promise.resolve(db.insert(smtpDispatchRaw).values(row)).catch(() => {});
}

/**
 * Instantly holds the thread — resolve everything the answer needs from it.
 *
 * Split from the dispatch on purpose. A reply produced outside the prospect's
 * business hours WAITS, and a caller must still learn synchronously that there
 * is no thread to answer into or no mailbox that can answer — deferring those
 * refusals would turn a named 409 into a silent failure hours later.
 */
async function prepareInstantlyReply(
  campaign: CampaignRow,
  input: ReplyToLeadInput,
): Promise<PreparedInstantlyReply> {
  const salesRepCopy = await salesRepCopyList(campaign.brandId, input.orgId);
  const { key } = await resolveInstantlyApiKey(input.orgId, input.userId, CALLER);

  const records = await listEmails(key, {
    campaignId: campaign.instantlyCampaignId,
  });
  const target = selectReplyTarget(records);
  if (!target) {
    throw new ReplyToLeadError(
      "no_reply_to_thread",
      409,
      `No inbound message from ${campaign.leadEmail} on campaign ${campaign.instantlyCampaignId} — there is no thread to reply into`,
    );
  }

  // Persisted first: it is what this service DECIDED at send time. The inbound
  // record's own eaccount covers a historical row written before migration 0025.
  const accountEmail = campaign.accountEmail ?? target.eaccount;
  if (!accountEmail) {
    throw new ReplyToLeadError(
      "sending_account_unresolved",
      409,
      `Cannot tell which mailbox contacted ${campaign.leadEmail} on campaign ${campaign.instantlyCampaignId}`,
    );
  }

  const account = await getAccount(key, accountEmail);
  const subject = fillSenderTokensInSubject(replySubject(target.subject), account);

  return {
    transport: "instantly",
    sentBy: input.sentBy,
    key,
    target,
    accountEmail,
    account,
    subject,
    bodyHtml: signedBody(input, account, campaign),
    ...replyCopy(input, salesRepCopy),
  };
}

/** Send the prepared Instantly reply and record what happened. */
async function deliverInstantlyReply(
  campaign: CampaignRow,
  prepared: PreparedInstantlyReply,
): Promise<ReplyToLeadResult> {
  const { sentBy, key, target, accountEmail, account, subject, bodyHtml, cc, bcc } =
    prepared;

  let sent: EmailRecord;
  try {
    sent = await replyToEmail(key, {
      eaccount: accountEmail,
      replyToUuid: target.emailId,
      subject,
      bodyHtml,
      // Comma-separated string, not an array — that is Instantly's contract for
      // this field, and an array is not silently coerced into one.
      ccAddressEmailList: cc,
      ...(bcc ? { bccAddressEmailList: bcc } : {}),
    });
  } catch (error: unknown) {
    // Recorded even though it never left: the same evidence trail the smtp
    // branch leaves, so a refused reply is not invisible on either transport.
    await recordInstantlyReply({
      campaign,
      sentBy,
      accountEmail,
      subject,
      bodyHtml,
      cc,
      bcc,
      inReplyTo: target.emailId,
      outcome: "transient",
      instantlyEmailId: null,
      error,
    });

    throw new ReplyToLeadError(
      "reply_dispatch_failed",
      502,
      `Instantly refused the reply from ${accountEmail} to ${campaign.leadEmail}: ${
        errorMessage(error)
      }`,
    );
  }

  // Bronze, at step 0, exactly as the self-send branch does. Instantly holds
  // this reply too, but only until the plan is cancelled — its own dialog says
  // cancelling permanently deletes every conversation those mailboxes carried.
  // Writing it here means our own words survive that, at the moment we send
  // them rather than whenever a backfill next runs.
  await recordInstantlyReply({
    campaign,
    sentBy,
    accountEmail,
    subject,
    bodyHtml,
    cc,
    bcc,
    inReplyTo: target.emailId,
    outcome: "sent",
    instantlyEmailId: sent.id == null ? null : String(sent.id),
    error: null,
  });

  return {
    transport: "instantly",
    instantlyCampaignId: campaign.instantlyCampaignId,
    leadEmail: campaign.leadEmail,
    accountEmail,
    from: buildFromHeader(account),
    subject,
    cc,
    bcc,
    messageId: String(sent.id ?? ""),
    inReplyTo: target.emailId,
  };
}

/**
 * We hold the thread — resolve everything the answer needs from bronze.
 *
 * Same split, same reason as the Instantly branch: the refusals stay
 * synchronous even when the dispatch itself waits for the prospect's morning.
 */
async function prepareSmtpReply(
  campaign: CampaignRow,
  input: ReplyToLeadInput,
): Promise<PreparedSmtpReply> {
  const salesRepCopy = await salesRepCopyList(campaign.brandId, input.orgId);
  const anchor = await loadSelfSendAnchor(campaign.instantlyCampaignId);
  if (!anchor) {
    throw new ReplyToLeadError(
      "no_reply_to_thread",
      409,
      `No inbound message from ${campaign.leadEmail} on sequence ${campaign.instantlyCampaignId} — there is no thread to reply into`,
    );
  }

  const accountEmail = campaign.accountEmail;
  if (!accountEmail) {
    throw new ReplyToLeadError(
      "sending_account_unresolved",
      409,
      `Sequence ${campaign.instantlyCampaignId} carries no sending account, so nothing can answer as the mailbox this lead knows`,
    );
  }

  let credential;
  try {
    credential = await resolveMailboxCredential(accountEmail, CALLER);
  } catch (error: unknown) {
    throw new ReplyToLeadError(
      "mailbox_credential_unavailable",
      409,
      `No credential for ${accountEmail}, so we cannot authenticate as the mailbox that contacted ${campaign.leadEmail}: ${
        errorMessage(error)
      }`,
    );
  }

  const account = await loadSelfSendAccount(accountEmail);

  return {
    transport: SEND_TRANSPORT_SMTP,
    sentBy: input.sentBy,
    anchor,
    accountEmail,
    credential,
    account,
    subject: fillSenderTokensInSubject(replySubject(anchor.subject), account),
    bodyHtml: signedBody(input, account, campaign),
    from: buildFromHeader(account),
    ...replyCopy(input, salesRepCopy),
  };
}

/**
 * The answer to a prospect whose newest message went to one of our people's own
 * address (lib/reply-destination): from THAT address, in that person's name,
 * through our own sender, threaded on their message. No credential for that
 * address (a personal Gmail) = refused loudly, never sent from anywhere else.
 */
async function prepareOwnAddressReply(
  campaign: CampaignRow,
  input: ReplyToLeadInput,
  elsewhere: ElsewhereDestination,
): Promise<PreparedSmtpReply> {
  const refuse = (why: string): never => {
    throw new ReplyToLeadError(
      "prospect_wrote_elsewhere",
      409,
      `${campaign.leadEmail} last wrote to ${elsewhere.to} at ${elsewhere.at}, not to a mailbox the outreach sent from, and ${why}; answer them from that mailbox, in that thread`,
    );
  };
  const address = elsewhere.ownAddress;
  if (!address) return refuse("none of those addresses is one of ours");
  if (!elsewhere.messageId.startsWith("<")) return refuse("their message carries no Message-ID to thread on");

  let credential: MailboxCredential;
  try {
    credential = await resolveMailboxCredential(address, CALLER);
  } catch (error: unknown) {
    return refuse(
      `we hold no credential to send as ${address} (${errorMessage(error)})`,
    );
  }

  const salesRepCopy = await salesRepCopyList(campaign.brandId, input.orgId);
  const account = await loadSelfSendAccount(address);
  const copy = replyCopy(input, salesRepCopy);
  const notSelf = (list: string): string =>
    list
      .split(",")
      .map((a) => a.trim())
      .filter((a) => a && a.toLowerCase() !== address)
      .join(", ");

  return {
    transport: SEND_TRANSPORT_SMTP,
    sentBy: input.sentBy,
    anchor: {
      inReplyTo: elsewhere.messageId,
      subject: elsewhere.subject,
      references: elsewhere.references,
    },
    accountEmail: address,
    credential,
    account,
    subject: fillSenderTokensInSubject(replySubject(elsewhere.subject), account),
    bodyHtml: signedBody(input, account, campaign),
    from: buildFromHeader(account),
    cc: notSelf(copy.cc),
    bcc: notSelf(copy.bcc),
  };
}

/**
 * The answer into the thread the caller CHOSE (EVERY-PERSON-ON-THE-THREAD-1109):
 * from the mailbox that holds the chosen message, threaded on it; or a new
 * thread from the sequence's mailbox. Our own sender, either transport.
 */
async function prepareChosenThreadReply(
  campaign: CampaignRow,
  input: ReplyToLeadInput,
  chosen: ChosenThreadMessage | null,
): Promise<PreparedSmtpReply> {
  const accountEmail = chosen ? chosen.mailbox : campaign.accountEmail;
  if (!accountEmail) {
    throw new ReplyToLeadError(
      "sending_account_unresolved",
      409,
      `Sequence ${campaign.instantlyCampaignId} carries no sending account, so nothing can open a new thread as the mailbox this lead knows`,
    );
  }
  let credential: MailboxCredential;
  try {
    credential = await resolveMailboxCredential(accountEmail, CALLER);
  } catch (error: unknown) {
    throw new ReplyToLeadError(
      "mailbox_credential_unavailable",
      409,
      `No credential for ${accountEmail}, so we cannot answer as the mailbox of that thread: ${errorMessage(error)}`,
    );
  }
  const salesRepCopy = await salesRepCopyList(campaign.brandId, input.orgId);
  const account = await loadSelfSendAccount(accountEmail);
  const anchor: SelfSendThreadAnchor = chosen
    ? await loadChosenAnchor(chosen)
    : { inReplyTo: null, subject: input.newThreadSubject ?? "", references: [] };
  return {
    transport: SEND_TRANSPORT_SMTP,
    sentBy: input.sentBy,
    anchor,
    accountEmail,
    credential,
    account,
    subject: fillSenderTokensInSubject(chosen ? replySubject(anchor.subject) : (input.newThreadSubject ?? "").trim(), account),
    bodyHtml: signedBody(input, account, campaign),
    from: buildFromHeader(account),
    ...replyCopy(input, salesRepCopy),
  };
}

/** Each list the caller gave replaces that header; a list left out keeps its default. */
function withChosenRecipients(
  prepared: PreparedSmtpReply,
  recipients: NonNullable<ReplyToLeadInput["recipients"]>,
): PreparedSmtpReply {
  return {
    ...prepared,
    ...(recipients.to !== undefined ? { to: headerList(recipients.to) } : {}),
    ...(recipients.cc !== undefined ? { cc: headerList(recipients.cc) } : {}),
    ...(recipients.bcc !== undefined ? { bcc: headerList(recipients.bcc) } : {}),
  };
}

/** Send the prepared reply ourselves and record what happened. */
async function deliverSmtpReply(
  campaign: CampaignRow,
  prepared: PreparedSmtpReply,
  attachments: ResolvedAttachment[],
): Promise<ReplyToLeadResult> {
  const { sentBy, anchor, accountEmail, credential, subject, bodyHtml, from, cc, bcc } =
    prepared;
  // Recorded only when the caller chose it: a default answer's row is unchanged.
  const chosenTo = prepared.to;

  const message = {
    from,
    to: chosenTo ?? campaign.leadEmail,
    cc,
    ...(bcc ? { bcc } : {}),
    subject,
    html: bodyHtml,
    // No List-Unsubscribe pair: this is a one-to-one answer, not bulk mail, and
    // the original outreach already carried it.
    headers: {} as Record<string, string>,
    ...(anchor.inReplyTo ? { inReplyTo: anchor.inReplyTo } : {}),
    ...(anchor.references.length > 0 ? { references: anchor.references } : {}),
    ...(attachments.length > 0
      ? {
          attachments: attachments.map((a) => ({
            filename: a.filename,
            content: a.content,
            contentType: a.contentType,
          })),
        }
      : {}),
  };
  // What we keep about the files: everything but the bytes (brand-service
  // holds those). Absent when none, so a plain reply's row is unchanged.
  const sentAttachments =
    attachments.length > 0 ? attachments.map(describeAttachment) : undefined;

  try {
    const sent = await dispatchMessage(credential, message);
    const sentAt = new Date();
    const dispatchId = randomUUID();

    // Bronze, at step 0. Two things depend on it: the IMAP poller correlates the
    // prospect's next answer through the Message-Id it records, and the
    // forwarded thread reads our reply back out of it.
    await db.insert(smtpDispatchRaw).values({
      id: dispatchId,
      instantlyCampaignId: campaign.instantlyCampaignId,
      leadEmail: campaign.leadEmail,
      accountEmail,
      step: MANUAL_REPLY_STEP,
      outcome: "sent",
      messageId: sent.messageId,
      responseCode: null,
      response: sent.response,
      payload: {
        kind: "manual_reply",
        sentBy,
        subject,
        ...(chosenTo ? { to: chosenTo } : {}),
        cc,
        ...(bcc ? { bcc } : {}),
        ...(anchor.inReplyTo ? {} : { newThread: true }),
        bodyHtml,
        inReplyTo: anchor.inReplyTo,
        references: anchor.references,
        accepted: sent.accepted,
        ...(sentAttachments ? { attachments: sentAttachments } : {}),
      },
    });

    // The exact message (FACTORY-RECORD-SEND-1028). A one-to-one answer leaves
    // the moment it is asked for: no cadence, cap or window decided it.
    // Fail-soft: the reply is out.
    await recordSentMessage({
      dispatchId,
      instantlyCampaignId: campaign.instantlyCampaignId,
      leadEmail: campaign.leadEmail,
      accountEmail,
      step: MANUAL_REPLY_STEP,
      kind: "manual_reply",
      messageId: sent.messageId,
      rawMessage: sent.rawMessage,
      messageInputs: {
        subject,
        bodyHtml,
        from,
        ...(chosenTo ? { to: chosenTo } : {}),
        cc,
        bcc: bcc || null,
        inReplyTo: anchor.inReplyTo,
        references: anchor.references,
        attachments: sentAttachments ?? [],
      },
      decision: {
        schema: "send-decision/v1",
        trigger: "manual_reply",
        sentBy,
        sentAt: sentAt.toISOString(),
      },
      sentAt,
    });

    return {
      transport: SEND_TRANSPORT_SMTP,
      instantlyCampaignId: campaign.instantlyCampaignId,
      leadEmail: campaign.leadEmail,
      accountEmail,
      from,
      subject,
      cc,
      bcc,
      ...(chosenTo ? { to: chosenTo } : {}),
      messageId: sent.messageId,
      inReplyTo: anchor.inReplyTo,
      ...(sentAttachments ? { attachments: sentAttachments } : {}),
    };
  } catch (error: unknown) {
    // The attempt is recorded whether or not it went out — the same evidence
    // trail the dispatch worker leaves, so a refused reply is not invisible.
    await db
      .insert(smtpDispatchRaw)
      .values({
        instantlyCampaignId: campaign.instantlyCampaignId,
        leadEmail: campaign.leadEmail,
        accountEmail,
        step: MANUAL_REPLY_STEP,
        outcome: "transient",
        messageId: null,
        responseCode:
          (error as { responseCode?: number } | null)?.responseCode ?? null,
        response: (error as { response?: string } | null)?.response ?? null,
        payload: {
          kind: "manual_reply",
          sentBy,
          subject,
          ...(chosenTo ? { to: chosenTo } : {}),
          cc,
          ...(bcc ? { bcc } : {}),
          inReplyTo: anchor.inReplyTo,
          ...(sentAttachments ? { attachments: sentAttachments } : {}),
          error: errorMessage(error),
        },
      })
      .catch(() => {});

    throw new ReplyToLeadError(
      "reply_dispatch_failed",
      502,
      `SMTP refused the reply from ${accountEmail} to ${chosenTo ?? campaign.leadEmail}: ${
        errorMessage(error)
      }`,
    );
  }
}

/**
 * Answer a lead who replied, in their own thread, from the mailbox that
 * contacted them — NOW, whatever time it is in their day.
 *
 * ⚠️ NO BUSINESS-HOURS HOLD. See `ReplyToLeadOutcome`: the answer goes out the
 * moment it is produced, so any relative day the words name ("tomorrow", a
 * weekday) is true when the prospect reads it. Every refusal still runs first.
 *
 * Throws `ReplyToLeadError` on every refusal.
 */
export async function replyToLead(
  input: ReplyToLeadInput,
): Promise<ReplyToLeadOutcome> {
  // ⚠️ A CHOSEN MESSAGE NAMES ITS THREAD (EVERY-PERSON-ON-THE-THREAD-1109):
  // the caller continues the thread that message is in, whoever it is filed
  // on (Markus answering in Thomas's thread is answered there, as himself).
  if (input.threadMessageId !== undefined && input.newThreadSubject !== undefined) {
    throw new ReplyToLeadError(
      "thread_message_not_found",
      400,
      "Name a message to continue (in_reply_to) OR a new thread's subject (new_thread_subject), not both",
    );
  }
  const chosen =
    input.threadMessageId !== undefined
      ? await findThreadMessage(input.orgId, input.threadMessageId)
      : null;
  if (input.threadMessageId !== undefined && !chosen) {
    throw new ReplyToLeadError(
      "thread_message_not_found",
      404,
      `No message ${input.threadMessageId} on a thread of this org (a message we sent, or one we received and filed on a thread); nothing was sent`,
    );
  }
  const campaign = chosen
    ? chosen.campaign
    : await loadCampaign(input.orgId, input.campaignId, input.leadEmail);
  if (!campaign) {
    throw new ReplyToLeadError(
      "campaign_not_found",
      404,
      `No campaign ${input.campaignId} in this org for ${input.leadEmail}`,
    );
  }

  // ⚠️ NO TAKEOVER GATE (EVERY-REPLY-LEAD-EVENT-1105, owner 2026-11-05): a
  // person's hand reply on the thread never refuses an automated one. It stays
  // in the person's Unibox timeline, which the AI reads before deciding to
  // write now, later or nothing (lib/reply-sender).

  // ⚠️ A THREAD HANDED TO A PERSON IS THEIRS. Once an escalation is claimed on
  // this thread (lib/escalate-reply), the responder never writes on it again —
  // the rep answers from their own inbox. The hand-over itself is the one
  // automated write allowed through.
  if (input.sentBy === "automation" && !input.handoff) {
    const handedTo = await findEscalationHandoff(campaign.instantlyCampaignId);
    if (handedTo) {
      throw new ReplyToLeadError(
        "handed_over",
        409,
        `The thread with ${campaign.leadEmail} on sequence ${campaign.instantlyCampaignId} was handed to a person (${handedTo.handedTo ?? "hand-over in progress"}) at ${handedTo.at}; the automated responder does not write on it again`,
      );
    }
  }

  // ⚠️ NO AUTOMATED EMAIL TO A PERSON THE BRAND STATED IS NOT THEIRS
  // (DUALIST-WA-SILENCE-EMAIL, owner 2026-11-04). The responder, the hand-over
  // and the not-a-prospect reassurance are all `automation`; a person of the
  // brand typing a reply (`human`) is never refused for it. After the
  // hand-over gate so its answer (and the route's side effects on it) is
  // unchanged. crm-service unreadable = nothing sent (lib/stated-not-to-brand).
  if (input.sentBy === "automation") {
    const brandIds = campaign.brandIds && campaign.brandIds.length > 0
      ? campaign.brandIds
      : campaign.brandId
        ? [campaign.brandId]
        : [];
    let stated;
    try {
      stated = await findStatedNotToBrand(input.orgId, campaign.leadEmail, brandIds);
    } catch (error: unknown) {
      throw new ReplyToLeadError(
        "stated_not_to_brand_unreadable",
        502,
        `Could not read whether the brand stated ${campaign.leadEmail} is not theirs (${errorMessage(error)}); nothing was sent`,
      );
    }
    if (stated) {
      throw new ReplyToLeadError(
        "stated_not_to_brand",
        409,
        `The brand's team stated that ${campaign.leadEmail} is not someone of brand ${stated.brandId ?? "(unnamed)"}${stated.correctionId ? ` (correction ${stated.correctionId})` : ""}; no automated reply is sent to them until that is undone`,
      );
    }
  }

  // ⚠️ THE RECIPIENTS ARE THE CALLER'S CHOICE (EVERY-PERSON-ON-THE-THREAD-1109),
  // and every one of them is a person the same automated-send gates protect:
  // a person who opted out of this org, or whom the brand stated is not
  // theirs, gets no automated email because someone else's thread named them.
  const recipients = input.recipients;
  if (recipients && input.sentBy === "automation") {
    const lead = campaign.leadEmail.trim().toLowerCase();
    const others = [...new Set(chosenAddresses(recipients))].filter(
      (a) => a !== lead && a !== agencyInbox().trim().toLowerCase() && !isStaffSender(a, lead),
    );
    const optOuts = await findStandingOptOuts(input.orgId, others);
    const optedOut = others.find((a) => optOuts.has(a));
    if (optedOut) {
      throw new ReplyToLeadError(
        "recipient_opted_out",
        409,
        `${optedOut} opted out of this org's emails; no automated email is sent to them, nothing was sent`,
      );
    }
    const brandIds = campaign.brandIds && campaign.brandIds.length > 0
      ? campaign.brandIds
      : campaign.brandId
        ? [campaign.brandId]
        : [];
    for (const address of others) {
      let stated;
      try {
        stated = await findStatedNotToBrand(input.orgId, address, brandIds);
      } catch (error: unknown) {
        throw new ReplyToLeadError(
          "stated_not_to_brand_unreadable",
          502,
          `Could not read whether the brand stated ${address} is not theirs (${errorMessage(error)}); nothing was sent`,
        );
      }
      if (stated) {
        throw new ReplyToLeadError(
          "stated_not_to_brand",
          409,
          `The brand's team stated that ${address} is not someone of brand ${stated.brandId ?? "(unnamed)"}; no automated email is sent to them, nothing was sent`,
        );
      }
    }
  }

  // ⚠️ ANY ANSWER GOES WHERE THEY WROTE LAST (owner 2026-10-13, caps). Their
  // newest message went to one of our people's own address: the answer leaves
  // in that person's name, from that address, threaded on that message, never
  // from the sending alias into the old thread (lib/reply-destination).
  // A caller who CHOSE the thread (a message, or a new one) decided where it goes.
  const threadChosen = !!chosen || input.newThreadSubject !== undefined;
  const elsewhere = threadChosen
    ? null
    : await findProspectWroteElsewhere(input.orgId, campaign.leadEmail);

  // ⚠️ FILES ONLY LEAVE THROUGH OUR OWN SENDER. Instantly's reply endpoint takes
  // no attachment (and the plan is cancelled), so a reply naming a document on
  // that pipe is refused, never sent without the file. Checked before any
  // document is read; every document is then resolved before anything is
  // prepared or sent (lib/offer-documents).
  const documentIds = input.attachmentDocumentIds ?? [];
  const viaOwnSender =
    threadChosen || !!elsewhere || campaign.sendTransport === SEND_TRANSPORT_SMTP;
  if (recipients && !viaOwnSender) {
    throw new ReplyToLeadError(
      "recipients_unsupported_on_transport",
      422,
      `The thread with ${campaign.leadEmail} (sequence ${campaign.instantlyCampaignId}) runs on Instantly, whose reply endpoint cannot take the recipients chosen; nothing was sent`,
    );
  }
  if (documentIds.length > 0 && !viaOwnSender) {
    throw new ReplyToLeadError(
      "attachments_unsupported_on_transport",
      422,
      `The thread with ${campaign.leadEmail} (sequence ${campaign.instantlyCampaignId}) runs on Instantly, whose reply endpoint carries no attachment; nothing was sent`,
    );
  }
  const attachments = await resolveReplyAttachments(input.orgId, documentIds);

  // ⚠️ EVERY REPLY QUOTES THE CONVERSATION (QUOTE-THREAD-1103). A caller that
  // brings its own quote (the hand-over, the not-a-prospect answer) keeps it;
  // every other reply gets the lead's whole conversation, the cold email
  // included, below the signature, like any mail client. Without it the copy
  // in the agency inbox read as OUR thread, not the brand's (Doc Dinners).
  // A NEW thread quotes nothing: it is a fresh conversation.
  const quoted: ReplyToLeadInput =
    input.quotedHtml === undefined && input.newThreadSubject !== undefined
      ? { ...input, quotedHtml: "" }
      : input.quotedHtml === undefined
      ? {
          ...input,
          quotedHtml: await (await import("./quoted-conversation")).loadQuotedConversation({
            orgId: input.orgId,
            userId: input.userId,
            campaignId: campaign.campaignId,
            instantlyCampaignId: campaign.instantlyCampaignId,
            brandId: campaign.brandId,
            leadEmail: campaign.leadEmail,
          }),
        }
      : input;

  const base = threadChosen
    ? await prepareChosenThreadReply(campaign, quoted, chosen)
    : elsewhere
      ? await prepareOwnAddressReply(campaign, quoted, elsewhere)
      : campaign.sendTransport === SEND_TRANSPORT_SMTP
        ? await prepareSmtpReply(campaign, quoted)
        : await prepareInstantlyReply(campaign, quoted);
  const prepared =
    recipients && base.transport === SEND_TRANSPORT_SMTP
      ? withChosenRecipients(base, recipients)
      : base;

  const reply =
    prepared.transport === SEND_TRANSPORT_SMTP
      ? await deliverSmtpReply(campaign, prepared, attachments)
      : await deliverInstantlyReply(campaign, prepared);

  return { status: "sent", reply };
}
