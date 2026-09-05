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

import { sql } from "drizzle-orm";

import { db } from "../db";
import { smtpDispatchRaw } from "../db/schema";
import { agencyInbox } from "./agency-inbox";
import {
  getAccount,
  listEmails,
  replyToEmail,
  type Account,
  type EmailRecord,
} from "./instantly-client";
import { resolveInstantlyApiKey, type CallerInfo } from "./key-client";
import { buildReplyBodyWithSignature } from "./send-lead";
import {
  resolveMailboxCredential,
  type MailboxCredential,
} from "./self-send/mailbox-credentials";
import { buildFromHeader, subjectForStep } from "./self-send/message";
import { dispatchMessage } from "./self-send/smtp";
import {
  resolveTransportForSend,
  SEND_TRANSPORT_SMTP,
  type SendTransport,
} from "./self-send/transport";
import { isSendingDay } from "./sending-calendar";
import {
  isWithinLocalSendWindow,
  nextLocalSendInstant,
  resolveLeadTimezone,
} from "./sending-window";
import { enqueueScheduledReply } from "./scheduled-replies";

const CALLER: CallerInfo = { method: "POST", path: "/orgs/replies" };

/**
 * The `step` a manual reply is recorded under in `smtp_dispatch_raw`.
 *
 * Sequence steps are 1-based everywhere in this repo (`sequence_costs.step`,
 * `sequence_steps.step`), so 0 is unambiguously "not a step of the sequence".
 * The row still has to exist: it is what lets the IMAP poller correlate the
 * prospect's answer to OUR answer back to this lead, and what keeps the
 * forwarded thread complete. It is deliberately NOT a `sequence_steps` row and
 * carries no hold — a reply is not a scheduled step and must never enter the
 * dispatch queue.
 */
export const MANUAL_REPLY_STEP = 0;

/** A refusal a caller can act on, rather than a bare 500. */
export class ReplyToLeadError extends Error {
  constructor(
    public readonly code:
      | "campaign_not_found"
      | "no_reply_to_thread"
      | "sending_account_unresolved"
      | "mailbox_credential_unavailable"
      | "reply_dispatch_failed",
    public readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = "ReplyToLeadError";
  }
}

export interface ReplyToLeadInput {
  orgId: string;
  userId: string;
  /** Logical campaign id — the same key manual qualifications and opt-outs use. */
  campaignId: string;
  leadEmail: string;
  /** The answer itself, HTML. Signed here; never signed by the caller. */
  bodyHtml: string;
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
  /** Identifier of the message we sent (Instantly's email id, or a Message-Id). */
  messageId: string;
  /** The message this one threads onto. Never null — a reply without one fails. */
  inReplyTo: string;
}

/**
 * A reply that has been fully resolved but not yet sent.
 *
 * Producing this is what keeps every named refusal SYNCHRONOUS even when the
 * dispatch itself waits for the prospect's morning: the thread, the mailbox and
 * the credential are all checked while the caller is still on the phone. The
 * prepared values are deliberately DISCARDED when the reply is deferred — the
 * drain re-resolves the anchor, because a newer inbound message may have
 * arrived in the meantime and the answer belongs under whatever they said last.
 */
interface PreparedInstantlyReply {
  transport: "instantly";
  key: string;
  target: { emailId: string; subject: string; eaccount: string | null };
  accountEmail: string;
  account: Account;
  subject: string;
  bodyHtml: string;
  cc: string;
}

interface PreparedSmtpReply {
  transport: typeof SEND_TRANSPORT_SMTP;
  anchor: SelfSendThreadAnchor;
  accountEmail: string;
  credential: MailboxCredential;
  account: Account;
  subject: string;
  bodyHtml: string;
  from: string;
  cc: string;
}

/** A reply that is waiting for the prospect's own business hours to open. */
export interface ScheduledReplyResult {
  transport: SendTransport;
  instantlyCampaignId: string;
  leadEmail: string;
  /** The mailbox that will answer — resolved now, not at dispatch. */
  accountEmail: string;
  subject: string;
  cc: string;
  /** The prospect's timezone the window was resolved in. */
  timezone: string;
  /** The first instant their window opens, ISO 8601 UTC. A LOWER BOUND. */
  scheduledFor: string;
}

/**
 * What happened to the answer: it went out, or it is waiting for the prospect's
 * morning. Both are successes; a refusal throws `ReplyToLeadError`.
 */
export type ReplyToLeadOutcome =
  | { status: "sent"; reply: ReplyToLeadResult }
  | { status: "scheduled"; scheduled: ScheduledReplyResult };

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
 * Matched on `lower(lead_email)` — the same normalization the re-contact window
 * and the serve-side suppression use. `Joe@X.com` and `joe@x.com` are one inbox,
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
    SELECT
      c.campaign_id           AS "campaignId",
      c.instantly_campaign_id AS "instantlyCampaignId",
      c.lead_email            AS "leadEmail",
      c.account_email         AS "accountEmail",
      c.send_transport        AS "sendTransport",
      c.timezone              AS "timezone",
      c.created_at            AS "createdAt"
    FROM instantly_campaigns c
    WHERE c.org_id = ${orgId}
      AND c.campaign_id IN (${idList})
      AND lower(c.lead_email) = lower(${leadEmail})
    ORDER BY c.created_at ASC, c.instantly_campaign_id ASC
  `);

  return (result.rows as Record<string, unknown>[]).map((row) => ({
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
  }));
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
    .filter((r) => r.ue_type === 2 && typeof r.id === "string" && r.id !== "")
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
  inReplyTo: string;
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
  accountEmail: string;
  subject: string;
  bodyHtml: string;
  cc: string;
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
      transport: "instantly",
      subject: input.subject,
      cc: input.cc,
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
  const subject = replySubject(target.subject);

  return {
    transport: "instantly",
    key,
    target,
    accountEmail,
    account,
    subject,
    bodyHtml: buildReplyBodyWithSignature(input.bodyHtml, account),
    cc: agencyInbox(),
  };
}

/** Send the prepared Instantly reply and record what happened. */
async function deliverInstantlyReply(
  campaign: CampaignRow,
  prepared: PreparedInstantlyReply,
): Promise<ReplyToLeadResult> {
  const { key, target, accountEmail, account, subject, bodyHtml, cc } = prepared;

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
    });
  } catch (error: unknown) {
    // Recorded even though it never left: the same evidence trail the smtp
    // branch leaves, so a refused reply is not invisible on either transport.
    await recordInstantlyReply({
      campaign,
      accountEmail,
      subject,
      bodyHtml,
      cc,
      inReplyTo: target.emailId,
      outcome: "transient",
      instantlyEmailId: null,
      error,
    });

    throw new ReplyToLeadError(
      "reply_dispatch_failed",
      502,
      `Instantly refused the reply from ${accountEmail} to ${campaign.leadEmail}: ${
        error instanceof Error ? error.message : String(error)
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
    accountEmail,
    subject,
    bodyHtml,
    cc,
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
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }

  const account = await loadSelfSendAccount(accountEmail);

  return {
    transport: SEND_TRANSPORT_SMTP,
    anchor,
    accountEmail,
    credential,
    account,
    subject: replySubject(anchor.subject),
    bodyHtml: buildReplyBodyWithSignature(input.bodyHtml, account),
    from: buildFromHeader(account),
    cc: agencyInbox(),
  };
}

/** Send the prepared reply ourselves and record what happened. */
async function deliverSmtpReply(
  campaign: CampaignRow,
  prepared: PreparedSmtpReply,
): Promise<ReplyToLeadResult> {
  const { anchor, accountEmail, credential, subject, bodyHtml, from, cc } = prepared;

  const message = {
    from,
    to: campaign.leadEmail,
    cc,
    subject,
    html: bodyHtml,
    // No List-Unsubscribe pair: this is a one-to-one answer, not bulk mail, and
    // the original outreach already carried it.
    headers: {} as Record<string, string>,
    inReplyTo: anchor.inReplyTo,
    references: anchor.references,
  };

  try {
    const sent = await dispatchMessage(credential, message);

    // Bronze, at step 0. Two things depend on it: the IMAP poller correlates the
    // prospect's next answer through the Message-Id it records, and the
    // forwarded thread reads our reply back out of it.
    await db.insert(smtpDispatchRaw).values({
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
        subject,
        cc,
        bodyHtml,
        inReplyTo: anchor.inReplyTo,
        references: anchor.references,
        accepted: sent.accepted,
      },
    });

    return {
      transport: SEND_TRANSPORT_SMTP,
      instantlyCampaignId: campaign.instantlyCampaignId,
      leadEmail: campaign.leadEmail,
      accountEmail,
      from,
      subject,
      cc,
      messageId: sent.messageId,
      inReplyTo: anchor.inReplyTo,
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
          subject,
          cc,
          inReplyTo: anchor.inReplyTo,
          error: error instanceof Error ? error.message : String(error),
        },
      })
      .catch(() => {});

    throw new ReplyToLeadError(
      "reply_dispatch_failed",
      502,
      `SMTP refused the reply from ${accountEmail} to ${campaign.leadEmail}: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }
}

/**
 * True when the prospect can be mailed at this instant — the SAME two gates the
 * sequence dispatch worker applies, in the same order, from the same modules.
 *
 * Nothing here is reply-specific: `isSendingDay` is the fleet's Mon-Fri
 * calendar and `isWithinLocalSendWindow` is the prospect's own 08:00-17:00 in
 * their own timezone, with the fleet default when we hold none. Do NOT add a
 * reply-only rule to either — the queue's existing behaviour IS the answer.
 */
export function canSendReplyNow(
  asOf: Date,
  timezone: string | null | undefined,
): boolean {
  return isSendingDay(asOf) && isWithinLocalSendWindow(asOf, timezone);
}

export interface ReplyToLeadOptions {
  asOf?: Date;
  /**
   * Whether a reply produced outside the prospect's window waits.
   *
   * The drain passes `false`: it has already checked the window itself and is
   * dispatching what it selected, so re-deferring there would put a reply back
   * in the queue it was just taken out of.
   */
  deferOutsideWindow?: boolean;
}

/**
 * Answer a lead who replied, in their own thread, from the mailbox that
 * contacted them — at a sane hour in THEIR day.
 *
 * ⚠️ THE ANSWER WAITS FOR THE PROSPECT'S BUSINESS HOURS, exactly like every
 * other message this service sends. A reply produced at 23:00 their time is
 * enqueued and goes out when their window next opens, drained by the SAME
 * hourly worker that sends the sequence steps. Nothing about the reply becomes
 * a sequence step: it takes no `sequence_steps` row and no `sequence_costs`
 * hold, and it is still recorded in bronze at step 0 when it goes out.
 *
 * Every refusal is raised BEFORE that decision, so a caller learns immediately
 * that there is no thread, no mailbox or no credential rather than hours later.
 *
 * Throws `ReplyToLeadError` on every refusal.
 */
export async function replyToLead(
  input: ReplyToLeadInput,
  options: ReplyToLeadOptions = {},
): Promise<ReplyToLeadOutcome> {
  const asOf = options.asOf ?? new Date();

  const campaign = await loadCampaign(
    input.orgId,
    input.campaignId,
    input.leadEmail,
  );
  if (!campaign) {
    throw new ReplyToLeadError(
      "campaign_not_found",
      404,
      `No campaign ${input.campaignId} in this org for ${input.leadEmail}`,
    );
  }

  const prepared =
    campaign.sendTransport === SEND_TRANSPORT_SMTP
      ? await prepareSmtpReply(campaign, input)
      : await prepareInstantlyReply(campaign, input);

  if (
    options.deferOutsideWindow !== false &&
    !canSendReplyNow(asOf, campaign.timezone)
  ) {
    const timezone = resolveLeadTimezone(campaign.timezone);
    const scheduledFor = nextLocalSendInstant(asOf, timezone);

    const row = await enqueueScheduledReply({
      orgId: input.orgId,
      userId: input.userId,
      campaignId: campaign.campaignId,
      instantlyCampaignId: campaign.instantlyCampaignId,
      leadEmail: campaign.leadEmail,
      bodyHtml: input.bodyHtml,
      timezone: campaign.timezone,
      scheduledFor,
    });

    return {
      status: "scheduled",
      scheduled: {
        transport: campaign.sendTransport,
        instantlyCampaignId: campaign.instantlyCampaignId,
        leadEmail: campaign.leadEmail,
        accountEmail: prepared.accountEmail,
        subject: prepared.subject,
        cc: prepared.cc,
        timezone,
        scheduledFor: (row.scheduledFor ?? scheduledFor).toISOString(),
      },
    };
  }

  const reply =
    prepared.transport === SEND_TRANSPORT_SMTP
      ? await deliverSmtpReply(campaign, prepared)
      : await deliverInstantlyReply(campaign, prepared);

  return { status: "sent", reply };
}
