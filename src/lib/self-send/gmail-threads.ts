/**
 * A conversation a person on OUR side holds with a prospect from their OWN
 * mailbox, folded into the prospect's thread and acted on like any reply.
 *
 * ⚠️ WHY. When a person answers a prospect by hand, they do it from their own
 * Gmail, to the prospect directly, and the prospect then answers THEM. None of
 * that touches a sending mailbox, so neither the IMAP poll nor Instantly ever
 * sees it: the Unibox showed four messages of a six-message conversation, and
 * the prospect's "let's set some time to chat, Thurs/Fri?" was classified by
 * nobody and acted on by nothing (STAFF-GMAIL-THREAD-1013: lukas@stankamera.com,
 * answered by kevin@distribute.you on 2026-10-11, wrote back on 2026-10-13).
 * `forwarded-replies.ts` covered only the mailboxes judged BLIND, and only our
 * side's answers; this covers every lead we hold a sequence with, both ways.
 *
 * WHERE THE WORDS ARE: google-service's Gmail mirror (~15 min fresh). Today the
 * source is the staff mirrors (`fetchMirroredMailForAddresses`, called with the
 * LEADS' addresses); a client's own connected Gmail is the same mechanism with
 * the org's own addresses as "ours" (the source is a parameter, not a branch).
 *
 * WHAT COUNTS: a message with the lead on one side and one of OUR addresses on
 * the other (from the lead to us, or from us to the lead). Mail between the lead
 * and a sending alias is the poll's (and is already filed there); mail between
 * the lead and anyone else is not ours to read.
 *
 * WHICH SEQUENCE: our `Message-Id` in its references when there is one (rare:
 * the person usually answers from a new thread); else the lead's sequence that
 * had really emailed them by then, the one they replied to first, latest wins.
 * Sequences of two DIFFERENT orgs with no reference to tell them apart are
 * declined (never file a conversation under another client's brand).
 *
 * HOW IT IS FILED: the same bronze as every inbound (`imap_messages_raw`, under
 * the sequence's sending mailbox, keyed on the RFC Message-ID, provenance
 * {@link OWN_MAILBOX_THREAD}), promoted through the poll's own path
 * (`promoteFiled`): our side's message is `staff_reply`; the lead's is a reply
 * (or an auto-reply) with stop, classification and every side effect a reply
 * gets. Idempotent: a re-run, or a copy another read already filed, is skipped.
 * Older than {@link LATE_AFTER_MS} = `deliveredLate`, exactly like the blind
 * recovery (a backfill never celebrates last month's reply).
 *
 * THE RESPONDER (owner's call delegated, 2026-10-13): a thread a person moved to
 * their own mailbox STAYS theirs. The prospect now writes to that person; an
 * automated answer from the sending alias would arrive from someone else, on
 * another thread. So `human-takeover.ts` holds such a thread for good
 * (`own_mailbox`) and the responder is not asked to take a new reply on it: the
 * person who holds it answers (no email asks them, REPLY-NOTIFS-1013).
 */

import { sql } from "drizzle-orm";

import { db } from "../../db";
import type { CallerInfo } from "../key-client";
import { bareAddress, isStaffSender } from "../staff-senders";
import { fetchMirroredMailForAddresses, type GoogleCallerIdentity, type MirroredMessage } from "../google-client";
import { isAutoReply, isDeliveryStatusNotification, STAFF_REPLY_KIND, type InboundKind } from "./inbound";
import { emptyPollSummary, type PollSummary } from "./imap-poller";
import {
  fileRow,
  headersOf,
  instantlyMirrorHolds,
  isDeliveredLate,
  LATE_AFTER_MS,
  mirrorMessageFiled,
  promoteFiled,
  type AliasedSend,
} from "./forwarded-replies";

export const CALLER: CallerInfo = { method: "POST", path: "/internal/self-send/gmail-threads" };

/** Provenance tag on every bronze row this module files. */
export const OWN_MAILBOX_THREAD = "own_mailbox_thread";

/** The routine window: what the mirror may have learned since the last passes (it lags ~15 min). */
export const GMAIL_THREADS_LOOKBACK_MS = LATE_AFTER_MS;

/** Addresses per mirror read (google-service's cap). */
export const ADDRESS_BATCH = 1000;

export { LATE_AFTER_MS };

function lower(value: string | null | undefined): string {
  return (bareAddress(value) ?? "").trim().toLowerCase();
}

/** One side of an exchange this message is part of. */
export interface ExchangePart {
  leadEmail: string;
  /** `lead` = the lead wrote it to us; `ours` = we wrote it to the lead. */
  side: "lead" | "ours";
}

/**
 * Pure: the lead(s) this message is an exchange with, and which side wrote it.
 * Empty = not a conversation between one of our leads and one of our addresses.
 */
export function planExchange(
  message: MirroredMessage,
  leads: ReadonlySet<string>,
  isOurs: (address: string) => boolean,
): ExchangePart[] {
  const from = lower(message.from);
  if (!from) return [];
  const recipients = [...new Set([...message.to, ...message.cc].map(lower).filter(Boolean))];
  if (leads.has(from) && !isOurs(from)) {
    return recipients.some((r) => r !== from && isOurs(r)) ? [{ leadEmail: from, side: "lead" }] : [];
  }
  if (!isOurs(from)) return [];
  return recipients.filter((r) => leads.has(r) && !isOurs(r)).map((leadEmail) => ({ leadEmail, side: "ours" }));
}

/** Pure: the inbound kind of the LEAD's message, or null when it is a delivery report (not theirs). */
export function leadMessageKind(message: MirroredMessage): InboundKind | null {
  const headers = headersOf(message);
  if (isDeliveryStatusNotification(headers)) return null;
  return isAutoReply(headers) ? "auto_reply" : "reply";
}

/** A sequence of the lead that had really emailed them by the message's date. */
export interface SequenceCandidate {
  instantlyCampaignId: string;
  orgId: string | null;
  accountEmail: string | null;
  /** Highest real `email_sent` step by then. */
  step: number;
  lastSentAt: Date;
  replied: boolean;
}

export type SequenceChoice =
  | { outcome: "matched"; sequence: SequenceCandidate }
  | { outcome: "ambiguous" }
  | { outcome: "none" };

/**
 * Pure: which sequence the exchange belongs to. A referenced send of ours names
 * it exactly; else one org's sequences resolve to the one they replied to, the
 * latest emailed; sequences of two orgs with nothing to tell them apart are
 * declined.
 */
export function chooseSequence(
  candidates: readonly SequenceCandidate[],
  referencedCampaignIds: ReadonlySet<string>,
): SequenceChoice {
  if (candidates.length === 0) return { outcome: "none" };
  const referenced = candidates.filter((c) => referencedCampaignIds.has(c.instantlyCampaignId));
  if (referenced.length === 1) return { outcome: "matched", sequence: referenced[0] };
  const pool = referenced.length > 1 ? referenced : candidates;
  if (new Set(pool.map((c) => c.orgId ?? "")).size > 1) return { outcome: "ambiguous" };
  const replied = pool.filter((c) => c.replied);
  const best = (replied.length > 0 ? replied : pool).reduce((a, b) =>
    b.lastSentAt.getTime() > a.lastSentAt.getTime() ||
    (b.lastSentAt.getTime() === a.lastSentAt.getTime() && b.instantlyCampaignId > a.instantlyCampaignId)
      ? b
      : a,
  );
  return { outcome: "matched", sequence: best };
}

function rowsOf(result: unknown): Record<string, unknown>[] {
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

/** Every address we hold a sequence for (lowercased). Our own people are never leads here. */
async function loadLeadAddresses(): Promise<string[]> {
  const result = await db.execute(sql`
    SELECT DISTINCT lower(trim(lead_email)) AS email
    FROM instantly_campaigns
    WHERE lead_email IS NOT NULL
      AND instantly_campaign_id NOT LIKE 'reserving:%'
  `);
  return rowsOf(result)
    .map((r) => String(r.email))
    .filter((e) => e.includes("@") && !isStaffSender(e));
}

async function loadCandidates(leadEmail: string, at: Date): Promise<SequenceCandidate[]> {
  const result = await db.execute(sql`
    SELECT c.instantly_campaign_id AS "instantlyCampaignId",
           c.org_id AS "orgId",
           COALESCE(c.account_email, s.account) AS "accountEmail",
           s.step AS "step",
           s.last_sent AS "lastSentAt",
           EXISTS (
             SELECT 1 FROM instantly_events r
             WHERE r.campaign_id = c.instantly_campaign_id
               AND r.event_type = 'reply_received' AND NOT r.inferred
           ) AS "replied"
    FROM instantly_campaigns c
    JOIN LATERAL (
      SELECT max(e.step) AS step,
             max(e."timestamp") AS last_sent,
             (array_agg(e.account_email ORDER BY e."timestamp" DESC))[1] AS account
      FROM instantly_events e
      WHERE e.campaign_id = c.instantly_campaign_id
        AND e.event_type = 'email_sent'
        AND NOT e.inferred
        AND e."timestamp" <= ${at.toISOString()}::timestamptz
    ) s ON s.step IS NOT NULL
    WHERE lower(c.lead_email) = ${leadEmail}
      AND c.instantly_campaign_id NOT LIKE 'reserving:%'
  `);
  return rowsOf(result).map((r) => ({
    instantlyCampaignId: String(r.instantlyCampaignId),
    orgId: (r.orgId as string | null) ?? null,
    accountEmail: (r.accountEmail as string | null) ?? null,
    step: Number(r.step),
    lastSentAt: r.lastSentAt instanceof Date ? r.lastSentAt : new Date(String(r.lastSentAt)),
    replied: Boolean(r.replied),
  }));
}

/** Sequences one of these Message-Ids belongs to (our sends, and messages already filed on a thread). */
async function referencedCampaigns(ids: string[]): Promise<Set<string>> {
  if (ids.length === 0) return new Set();
  const result = await db.execute(sql`
    SELECT instantly_campaign_id AS id FROM smtp_dispatch_raw
    WHERE message_id = ANY(${sql.param(ids)}::text[])
    UNION
    SELECT instantly_campaign_id AS id FROM imap_messages_raw
    WHERE message_id = ANY(${sql.param(ids)}::text[]) AND instantly_campaign_id IS NOT NULL
  `);
  return new Set(rowsOf(result).map((r) => String(r.id)));
}

function referencesOf(message: MirroredMessage): string[] {
  const raw = `${message.inReplyTo ?? ""} ${message.references ?? ""}`;
  return [...new Set(raw.match(/<[^<>\s]+>/g) ?? [])];
}

/** A real identity of the fleet's own work, for google-service's request log. */
async function readerIdentity(): Promise<GoogleCallerIdentity | null> {
  const result = await db.execute(sql`
    SELECT org_id AS "orgId", user_id AS "userId", run_id AS "runId"
    FROM instantly_campaigns
    WHERE org_id IS NOT NULL AND user_id IS NOT NULL AND run_id IS NOT NULL
    ORDER BY created_at DESC
    LIMIT 1
  `);
  const row = rowsOf(result)[0];
  return row ? { orgId: String(row.orgId), userId: String(row.userId), runId: String(row.runId) } : null;
}

/** One message folded in, as reported. */
export interface FoldedMessage {
  leadEmail: string;
  instantlyCampaignId: string;
  kind: string;
  from: string | null;
  date: string;
  deliveredLate: boolean;
  snippet: string;
}

export interface GmailThreadsSummary {
  dryRun: boolean;
  since: string;
  leadsRead: number;
  messagesRead: number;
  /** Filed (or, dry run, would be). */
  filed: number;
  staffAnswers: number;
  alreadyIngested: number;
  /** Lead on one side, one of ours on the other, but no sequence had emailed them by then. */
  noSequence: number;
  ambiguous: number;
  /** Mail the read returned that is not a lead <-> us exchange (lead and a sending alias, a delivery report). */
  notAnExchange: number;
  batchesFailed: number;
  poll: PollSummary;
  folded: FoldedMessage[];
}

/**
 * Fold the lead <-> our-people exchanges of the mirror into the leads' threads.
 * Dry run reads and plans, writes nothing. Fail-loud per batch: an unreadable
 * batch is counted and logged, the rest still run.
 */
export async function foldInGmailThreads(
  options: { dryRun?: boolean; since?: Date; leads?: string[]; asOf?: Date } = {},
): Promise<GmailThreadsSummary> {
  const dryRun = options.dryRun ?? true;
  const asOf = options.asOf ?? new Date();
  const since = options.since ?? new Date(asOf.getTime() - GMAIL_THREADS_LOOKBACK_MS);
  const leads =
    options.leads && options.leads.length > 0
      ? [...new Set(options.leads.map((l) => l.trim().toLowerCase()))].filter((e) => !isStaffSender(e))
      : await loadLeadAddresses();
  const summary: GmailThreadsSummary = {
    dryRun,
    since: since.toISOString(),
    leadsRead: leads.length,
    messagesRead: 0,
    filed: 0,
    staffAnswers: 0,
    alreadyIngested: 0,
    noSequence: 0,
    ambiguous: 0,
    notAnExchange: 0,
    batchesFailed: 0,
    poll: emptyPollSummary(),
    folded: [],
  };
  if (leads.length === 0) return summary;

  const identity = await readerIdentity();
  if (!identity) throw new Error("no campaign carries an identity to read the Gmail mirror with");

  const leadSet = new Set(leads);
  const isOurs = (address: string): boolean => isStaffSender(address);

  // Read every batch first, then act in DATE order: our answer is filed before
  // the reply that follows it, so the thread is already held when the reply's
  // side effects ask who answers it.
  const messages = new Map<string, MirroredMessage>();
  for (let i = 0; i < leads.length; i += ADDRESS_BATCH) {
    const batch = leads.slice(i, i + ADDRESS_BATCH);
    try {
      for (const m of await fetchMirroredMailForAddresses(batch, identity, since)) {
        messages.set(m.gmailMessageId ?? m.mirrorId, m);
      }
    } catch (error) {
      summary.batchesFailed += 1;
      console.error(
        `[instantly-service] gmail-threads: mirror read FAILED for batch ${i / ADDRESS_BATCH + 1}: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }
  }
  summary.messagesRead = messages.size;
  const ordered = [...messages.values()].sort(
    (a, b) => (a.date?.getTime() ?? 0) - (b.date?.getTime() ?? 0),
  );

  for (const message of ordered) {
    const parts = planExchange(message, leadSet, isOurs);
    if (parts.length === 0 || !message.date) {
      summary.notAnExchange += 1;
      continue;
    }
    for (const part of parts) {
      await foldOne(message, part, asOf, summary);
    }
  }
  return summary;
}

async function foldOne(
  message: MirroredMessage,
  part: ExchangePart,
  asOf: Date,
  summary: GmailThreadsSummary,
): Promise<void> {
  const date = message.date as Date;
  const kind: InboundKind | null = part.side === "ours" ? STAFF_REPLY_KIND : leadMessageKind(message);
  if (!kind) {
    summary.notAnExchange += 1;
    return;
  }
  const candidates = await loadCandidates(part.leadEmail, date);
  const needsReferences =
    candidates.length > 1 && new Set(candidates.map((c) => c.orgId ?? "")).size > 1;
  const choice = chooseSequence(
    candidates,
    needsReferences ? await referencedCampaigns(referencesOf(message)) : new Set(),
  );
  if (choice.outcome === "none") {
    summary.noSequence += 1;
    return;
  }
  if (choice.outcome === "ambiguous") {
    summary.ambiguous += 1;
    console.warn(
      `[instantly-service] gmail-threads: lead=${part.leadEmail} has sequences in several orgs and nothing names one; message ${message.gmailMessageId ?? message.mirrorId} not filed`,
    );
    return;
  }
  const sequence = choice.sequence;
  if (!sequence.accountEmail) {
    summary.noSequence += 1;
    return;
  }
  const gmailId = message.gmailMessageId ?? message.mirrorId;
  const messageId = message.messageId ?? `gmail-mirror:${gmailId}`;
  if (
    (await instantlyMirrorHolds(sequence.instantlyCampaignId, messageId)) ||
    (await mirrorMessageFiled(sequence.instantlyCampaignId, gmailId))
  ) {
    summary.alreadyIngested += 1;
    return;
  }

  const send: AliasedSend = {
    instantlyCampaignId: sequence.instantlyCampaignId,
    leadEmail: part.leadEmail,
    step: sequence.step,
    orgId: sequence.orgId,
    accountEmail: sequence.accountEmail,
  };
  const deliveredLate = isDeliveredLate(date, asOf);
  const report: FoldedMessage = {
    leadEmail: part.leadEmail,
    instantlyCampaignId: sequence.instantlyCampaignId,
    kind,
    from: message.from,
    date: date.toISOString(),
    deliveredLate,
    snippet: message.text.replace(/\s+/g, " ").trim().slice(0, 160),
  };

  if (summary.dryRun) {
    summary.filed += 1;
    if (kind === STAFF_REPLY_KIND) summary.staffAnswers += 1;
    summary.folded.push(report);
    return;
  }

  const rowId = await fileRow({
    accountEmail: send.accountEmail,
    messageId,
    from: message.from,
    subject: message.subject,
    kind,
    send,
    referencedMessageIds: referencesOf(message),
    text: message.text,
    date,
    mirrorId: message.mirrorId,
    headers: headersOf(message),
    recoveredFrom: OWN_MAILBOX_THREAD,
    to: [...message.to, ...message.cc],
  });
  if (!rowId) {
    summary.alreadyIngested += 1;
    return;
  }
  summary.filed += 1;
  summary.folded.push(report);
  await promoteFiled(
    rowId,
    { action: "file", kind, send, referencedMessageIds: [] },
    message,
    messageId,
    deliveredLate,
    asOf,
    summary,
  );
}

let inFlight: Promise<GmailThreadsSummary> | null = null;

/** The worker's pass: commit mode, routine window; joins a pass already running. */
export function runGmailThreadsPass(): Promise<GmailThreadsSummary> {
  if (inFlight) return inFlight;
  inFlight = foldInGmailThreads({ dryRun: false }).finally(() => {
    inFlight = null;
  });
  return inFlight;
}
