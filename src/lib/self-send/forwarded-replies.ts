/**
 * Replies that reached a STAFF Gmail instead of the sending mailbox, recovered
 * and ingested exactly like a reply the IMAP poll reads.
 *
 * ⚠️ WHY. A mailbox that forwards every message away WITHOUT keeping a copy is
 * blind (`inbound-blindness.ts`): the poll reads an empty INBOX, and the
 * prospect's answer exists only where the forward pointed. Prod 2026-10-13
 * (PRESSBEAT-RECOVER-1013): the pressbeat.ai and five growthagency.* mailboxes
 * forwarded everything to kevin.lourd@gmail.com until 2026-10-12; 6,481 cold
 * emails out of pressbeat.ai alone, zero replies ingested, Laura
 * (fusionmedcare.com) and Dylan (peakfamilychiropractic.com) answered and
 * nobody here ever knew. That Gmail is mirrored by google-service (staff
 * mailbox), so the words are recoverable from there, and only from there: the
 * Gandi mailbox stored nothing (UIDNEXT 2), and Instantly read the same empty box.
 *
 * HOW, and why it is the poll's own logic, not a second one:
 *  - correlation is the SAME exact key: a Message-Id we put on the wire, read
 *    from the message's In-Reply-To/References (`classifyInbound`,
 *    `correlateSend`, `loadKnownSends` per alias, both transports). Never the
 *    sender or the subject. Ambiguous is declined.
 *  - the row lands in the SAME bronze (`imap_messages_raw`, under the alias that
 *    sent the referenced email, keyed on the message's own RFC Message-ID), so a
 *    copy the poll reads later, or a re-run, is the same row: idempotent. A
 *    message Instantly's mirror already holds (same Message-ID) is skipped.
 *    `payload.recoveredFrom` marks the provenance, and `inbound-blindness.ts`
 *    ignores such rows (a recovered reply proves the mailbox is NOT read).
 *  - promotion and side effects are the poll's (`promoteEvent` +
 *    `actOnInboundReply`): stop, classification, opt-out. A reply older than
 *    {@link LATE_AFTER_MS} is `deliveredLate`: its fact is recorded but nothing
 *    acts as if they had just written (no celebration, no responder, no rep
 *    call); the unanswered-reply sweep then gives it the staff check-in an old
 *    reply gets.
 *  - an answer our staff already sent is recorded first, as `staff_reply`, so
 *    nobody is asked about a prospect who was answered: one sent from Gmail AS
 *    the sending alias (it is in the same mirror read) and one sent from the
 *    staff address itself (google-service's staff conversation read).
 *
 * Which mailboxes: by default every mailbox `inbound-blindness.ts` calls blind
 * now; a caller may name mailboxes or domains (a mailbox that WAS blind, like
 * pressbeat.ai once its forward kept a copy).
 */

import { sql } from "drizzle-orm";

import { db } from "../../db";
import { imapMessagesRaw } from "../../db/schema";
import { promoteEvent } from "../silver-promote";
import type { CallerInfo } from "../key-client";
import { isStaffSender } from "../staff-senders";
import { QUESTION_STALE_AFTER_MS } from "../ask-client-to-answer";
import {
  fetchMirroredMailForAddresses,
  fetchStaffConversation,
  type GoogleCallerIdentity,
  type MirroredMessage,
} from "../google-client";
import {
  classifyInbound,
  correlateSend,
  eventTypeForInbound,
  refileStaffReply,
  STAFF_REPLY_KIND,
  type InboundHeaders,
  type InboundKind,
} from "./inbound";
import { actOnInboundReply, emptyPollSummary, loadKnownSends, type KnownSend, type PollSummary } from "./imap-poller";
import { loadInboundBlindMailboxes } from "./inbound-blindness";
import { loadMailboxLogins } from "./mailbox-credentials";

const CALLER: CallerInfo = { method: "POST", path: "/internal/self-send/forwarded-replies" };

/** The provenance tag on every recovered bronze row. */
export const RECOVERED_FROM_STAFF_MIRROR = "staff_gmail_mirror";

/** Older than this, a recovered reply is `deliveredLate` (same bar as a stale question). */
export const LATE_AFTER_MS = QUESTION_STALE_AFTER_MS;

/** A known send of the mailbox, with the alias that sent it. */
export interface AliasedSend extends KnownSend {
  accountEmail: string;
}

export type ForwardedPlan =
  | {
      action: "file";
      kind: InboundKind;
      send: AliasedSend;
      referencedMessageIds: string[];
    }
  | { action: "skip"; reason: "no_message_id" | "unrelated" | "ambiguous" | "not_to_the_lead" };

function lower(value: string | null | undefined): string {
  return (value ?? "").trim().toLowerCase();
}

/** The message's headers in the classifier's shape (lowercased names). */
export function headersOf(message: MirroredMessage): InboundHeaders {
  const out: Record<string, string> = {};
  for (const [name, value] of Object.entries(message.headers)) out[name.toLowerCase()] = value;
  if (message.messageId) out["message-id"] = message.messageId;
  if (message.inReplyTo) out["in-reply-to"] = message.inReplyTo;
  if (message.references) out["references"] = message.references;
  if (message.from && !out["from"]) out["from"] = message.from;
  if (message.subject && !out["subject"]) out["subject"] = message.subject;
  return out;
}

/**
 * Pure: what one mirrored message is to this mailbox.
 *
 * A message FROM one of the mailbox's own addresses is our answer, sent from
 * Gmail as that alias: filed `staff_reply` when it threads onto one of our sends
 * AND is addressed to that send's lead. Anything else goes through the poll's
 * classification and correlation unchanged.
 */
export function planMirroredMessage(
  message: MirroredMessage,
  aliases: ReadonlySet<string>,
  knownSends: ReadonlyMap<string, AliasedSend>,
): ForwardedPlan {
  if (!message.messageId) return { action: "skip", reason: "no_message_id" };
  const headers = headersOf(message);
  const classified = classifyInbound(headers, message.text, new Set(knownSends.keys()));
  if (classified.kind === "unrelated") return { action: "skip", reason: "unrelated" };
  const correlation = correlateSend(classified.referencedMessageIds, knownSends);
  if (correlation.outcome === "ambiguous") return { action: "skip", reason: "ambiguous" };
  if (correlation.outcome !== "matched") return { action: "skip", reason: "unrelated" };
  const send = correlation.send;

  if (aliases.has(lower(message.from))) {
    const recipients = [...message.to, ...message.cc].map(lower);
    if (!recipients.includes(lower(send.leadEmail))) return { action: "skip", reason: "not_to_the_lead" };
    return { action: "file", kind: STAFF_REPLY_KIND, send, referencedMessageIds: classified.referencedMessageIds };
  }

  return {
    action: "file",
    kind: refileStaffReply(classified.kind, isStaffSender(message.from ?? headers["from"], send.leadEmail)),
    send,
    referencedMessageIds: classified.referencedMessageIds,
  };
}

/** Pure: is a reply dated `date` late as of `asOf`? Undated = late (never act as if fresh). */
export function isDeliveredLate(date: Date | null, asOf: Date): boolean {
  return date === null || asOf.getTime() - date.getTime() > LATE_AFTER_MS;
}

/** One recovered message, as reported. */
export interface RecoveredMessage {
  accountEmail: string;
  leadEmail: string;
  instantlyCampaignId: string;
  kind: string;
  date: string | null;
  deliveredLate: boolean;
  snippet: string;
}

export interface ForwardedRepliesSummary {
  dryRun: boolean;
  mailboxes: string[];
  messagesRead: number;
  /** Correlated to one of our sends and filed (or, dry run, would be). */
  filed: number;
  alreadyIngested: number;
  unrelated: number;
  ambiguous: number;
  /** Our own answers recorded: sent from Gmail as the alias, or from a staff address. */
  staffAnswers: number;
  mailboxesFailed: number;
  poll: PollSummary;
  recovered: RecoveredMessage[];
}

function rowsOf(result: unknown): Record<string, unknown>[] {
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

/** Every address of each target mailbox, keyed by the mailbox login. */
export function groupTargetMailboxes(
  logins: ReadonlyMap<string, string>,
  target: { mailboxes: ReadonlySet<string>; domains: ReadonlySet<string> },
): Map<string, Set<string>> {
  const byLogin = new Map<string, Set<string>>();
  const add = (login: string, address: string): void => {
    const set = byLogin.get(login) ?? new Set<string>([login]);
    set.add(address);
    byLogin.set(login, set);
  };
  // A named alias stands for its whole mailbox; an address the login map does
  // not know is its own mailbox.
  const namedLogins = new Set([...target.mailboxes].map((m) => logins.get(m) ?? m));
  for (const [address, login] of logins) {
    const domain = address.split("@")[1] ?? "";
    if (namedLogins.has(login) || target.domains.has(domain)) add(login, address);
  }
  for (const login of namedLogins) if (!byLogin.has(login)) add(login, login);
  return byLogin;
}

async function loadAliasedSends(aliases: Iterable<string>): Promise<Map<string, AliasedSend>> {
  const out = new Map<string, AliasedSend>();
  for (const alias of aliases) {
    for (const [messageId, send] of await loadKnownSends(alias)) {
      if (!out.has(messageId)) out.set(messageId, { ...send, accountEmail: alias });
    }
  }
  return out;
}

/** A real identity of this mailbox's own work, for google-service's request log. */
async function identityFor(aliases: Iterable<string>): Promise<GoogleCallerIdentity | null> {
  const result = await db.execute(sql`
    SELECT org_id AS "orgId", user_id AS "userId", run_id AS "runId"
    FROM instantly_campaigns
    WHERE lower(account_email) = ANY(${sql.param([...aliases])}::text[])
      AND org_id IS NOT NULL AND user_id IS NOT NULL AND run_id IS NOT NULL
    ORDER BY created_at DESC
    LIMIT 1
  `);
  const row = rowsOf(result)[0];
  return row ? { orgId: String(row.orgId), userId: String(row.userId), runId: String(row.runId) } : null;
}

async function campaignIdentity(instantlyCampaignId: string): Promise<GoogleCallerIdentity | null> {
  const result = await db.execute(sql`
    SELECT org_id AS "orgId", user_id AS "userId", run_id AS "runId"
    FROM instantly_campaigns
    WHERE instantly_campaign_id = ${instantlyCampaignId}
      AND org_id IS NOT NULL AND user_id IS NOT NULL AND run_id IS NOT NULL
  `);
  const row = rowsOf(result)[0];
  return row ? { orgId: String(row.orgId), userId: String(row.userId), runId: String(row.runId) } : null;
}

async function instantlyMirrorHolds(messageId: string): Promise<boolean> {
  const result = await db.execute(sql`
    SELECT 1 FROM instantly_emails_raw
    WHERE payload->>'message_id' = ${messageId} AND payload->>'ue_type' <> '1'
    LIMIT 1
  `);
  return rowsOf(result).length > 0;
}

/** File one row in bronze. Null = already there (re-run, or the poll read it). */
async function fileRow(input: {
  accountEmail: string;
  messageId: string;
  from: string | null;
  subject: string | null;
  kind: string;
  send: AliasedSend;
  referencedMessageIds: string[];
  text: string;
  date: Date | null;
  mirrorId: string;
}): Promise<string | null> {
  const [row] = await db
    .insert(imapMessagesRaw)
    .values({
      accountEmail: input.accountEmail,
      messageId: input.messageId,
      fromAddress: input.from,
      subject: input.subject,
      kind: input.kind,
      instantlyCampaignId: input.send.instantlyCampaignId,
      step: input.send.step,
      payload: {
        subject: input.subject,
        from: input.from,
        referencedMessageIds: input.referencedMessageIds,
        textSnippet: input.text.slice(0, 4000),
        recoveredFrom: RECOVERED_FROM_STAFF_MIRROR,
        mirrorMessageId: input.mirrorId,
      },
      receivedAt: input.date,
    })
    .onConflictDoNothing({ target: [imapMessagesRaw.accountEmail, imapMessagesRaw.messageId] })
    .returning({ id: imapMessagesRaw.id });
  return row?.id ?? null;
}

/**
 * Answers our staff sent the lead from the staff address itself, after `after`,
 * filed as `staff_reply` on the lead's sequence. Returns how many were new.
 */
async function fileStaffAddressAnswers(
  send: AliasedSend,
  after: Date | null,
  summary: ForwardedRepliesSummary,
): Promise<void> {
  const identity = await campaignIdentity(send.instantlyCampaignId);
  if (!identity) return;
  const messages = await fetchStaffConversation(send.leadEmail, identity);
  for (const m of messages) {
    if (m.direction !== "outbound") continue;
    if (!m.to.map(lower).includes(lower(send.leadEmail))) continue;
    if (after && m.date && m.date.getTime() < after.getTime()) continue;
    if (summary.dryRun) {
      summary.staffAnswers += 1;
      continue;
    }
    const id = await fileRow({
      accountEmail: send.accountEmail,
      messageId: `gmail-mirror:${m.mirrorId}`,
      from: m.from,
      subject: m.subject,
      kind: STAFF_REPLY_KIND,
      send,
      referencedMessageIds: [],
      text: m.text,
      date: m.date,
      mirrorId: m.mirrorId,
    });
    if (id) summary.staffAnswers += 1;
  }
}

/** Recover one mailbox. Throws on an unreadable source (counted by the caller). */
async function recoverMailbox(
  login: string,
  aliases: ReadonlySet<string>,
  asOf: Date,
  summary: ForwardedRepliesSummary,
): Promise<void> {
  const identity = await identityFor(aliases);
  if (!identity) {
    console.warn(`[instantly-service] forwarded-replies: mailbox=${login} has no campaign with an identity, skipped`);
    return;
  }
  const knownSends = await loadAliasedSends(aliases);
  const messages = await fetchMirroredMailForAddresses([...aliases], identity);
  summary.messagesRead += messages.length;

  // Our own answers first, so a prospect already answered is never asked about.
  const plans = messages.map((message) => ({ message, plan: planMirroredMessage(message, aliases, knownSends) }));
  const ordered = [
    ...plans.filter((p) => p.plan.action === "file" && p.plan.kind === STAFF_REPLY_KIND),
    ...plans.filter((p) => !(p.plan.action === "file" && p.plan.kind === STAFF_REPLY_KIND)),
  ];

  for (const { message, plan } of ordered) {
    if (plan.action === "skip") {
      if (plan.reason === "ambiguous") summary.ambiguous += 1;
      else summary.unrelated += 1;
      continue;
    }
    const messageId = message.messageId as string;
    if (await instantlyMirrorHolds(messageId)) {
      summary.alreadyIngested += 1;
      continue;
    }
    const deliveredLate = isDeliveredLate(message.date, asOf);

    if (plan.kind === "reply") {
      await fileStaffAddressAnswers(plan.send, message.date, summary);
    }

    if (summary.dryRun) {
      summary.filed += 1;
      if (plan.kind === STAFF_REPLY_KIND) summary.staffAnswers += 1;
      summary.recovered.push(reportOf(plan.send, plan.kind, message, deliveredLate));
      continue;
    }

    const rowId = await fileRow({
      accountEmail: plan.send.accountEmail,
      messageId,
      from: message.from,
      subject: message.subject,
      kind: plan.kind,
      send: plan.send,
      referencedMessageIds: plan.referencedMessageIds,
      text: message.text,
      date: message.date,
      mirrorId: message.mirrorId,
    });
    if (!rowId) {
      summary.alreadyIngested += 1;
      continue;
    }
    summary.filed += 1;
    summary.recovered.push(reportOf(plan.send, plan.kind, message, deliveredLate));
    if (plan.kind === STAFF_REPLY_KIND) {
      summary.staffAnswers += 1;
      continue;
    }

    const eventType = eventTypeForInbound(plan.kind);
    if (!eventType) continue;
    const timestamp = message.date ?? asOf;
    await promoteEvent({
      eventType,
      instantlyCampaignId: plan.send.instantlyCampaignId,
      leadEmail: plan.send.leadEmail,
      accountEmail: plan.send.accountEmail,
      step: plan.send.step,
      variant: null,
      timestamp,
      source: "self_send",
      sourceRowId: rowId,
      ...(deliveredLate ? { deliveredLate: true } : {}),
    });
    if (plan.kind === "reply") {
      await actOnInboundReply({
        send: plan.send,
        accountEmail: plan.send.accountEmail,
        rowId,
        text: message.text,
        subject: message.subject,
        messageId,
        timestamp,
        summary: summary.poll,
        deliveredLate,
      });
    } else if (plan.kind === "auto_reply") summary.poll.autoReplies += 1;
    else if (plan.kind === "bounce") summary.poll.bounces += 1;
  }
}

function reportOf(send: AliasedSend, kind: string, message: MirroredMessage, deliveredLate: boolean): RecoveredMessage {
  return {
    accountEmail: send.accountEmail,
    leadEmail: send.leadEmail,
    instantlyCampaignId: send.instantlyCampaignId,
    kind,
    date: message.date ? message.date.toISOString() : null,
    deliveredLate,
    snippet: message.text.replace(/\s+/g, " ").trim().slice(0, 160),
  };
}

/**
 * Recover the replies of the target mailboxes out of the staff Gmail mirrors.
 * Dry run (the default) reads and plans, writes nothing. Fail-loud PER MAILBOX:
 * one unreadable mailbox is counted and logged, the rest still run.
 */
export async function recoverForwardedReplies(
  options: { dryRun?: boolean; mailboxes?: string[]; domains?: string[]; asOf?: Date } = {},
): Promise<ForwardedRepliesSummary> {
  const dryRun = options.dryRun ?? true;
  const asOf = options.asOf ?? new Date();
  const logins = await loadMailboxLogins(CALLER);
  const named = new Set((options.mailboxes ?? []).map(lower).filter(Boolean));
  const domains = new Set((options.domains ?? []).map((d) => lower(d).replace(/^@/, "")).filter(Boolean));
  const mailboxes = named.size > 0 || domains.size > 0 ? named : await loadInboundBlindMailboxes(logins);
  const groups = groupTargetMailboxes(logins, { mailboxes, domains });

  const summary: ForwardedRepliesSummary = {
    dryRun,
    mailboxes: [...groups.keys()].sort(),
    messagesRead: 0,
    filed: 0,
    alreadyIngested: 0,
    unrelated: 0,
    ambiguous: 0,
    staffAnswers: 0,
    mailboxesFailed: 0,
    poll: emptyPollSummary(),
    recovered: [],
  };

  for (const [login, aliases] of groups) {
    try {
      await recoverMailbox(login, aliases, asOf, summary);
    } catch (error) {
      summary.mailboxesFailed += 1;
      console.error(
        `[instantly-service] forwarded-replies: mailbox=${login} failed: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }
  }
  return summary;
}
