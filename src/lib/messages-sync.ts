/**
 * Messages sync — ONE row per email, every typology (migration 0054).
 *
 * An outbound email lives in four bronze tables depending on what it was for,
 * an inbound one in three, so "everything this mailbox sent last week" was a
 * seven-table UNION nobody had written. `messages` is that union PERSISTED,
 * keyed on the bronze row it came from, so a re-run is a no-op and bronze
 * stays the record (the body is not copied; `source_table` + `source_row_id`
 * point back to it).
 *
 * Pure mappers per source (`mapX`) take the bronze row and answer with a
 * `MessageRow`; `syncMessages` reads a bounded window of each source, maps,
 * and upserts. Nothing here promotes to silver events and nothing decides —
 * state facts (sent / bounced / replied / clicked) stay in `instantly_events`.
 */

import { sql } from "drizzle-orm";
import { db } from "../db";
import { messages } from "../db/schema";
import { parseInstantlySequenceStep } from "./self-send/instantly-sends";

/**
 * Mirror of `reply-to-lead.ts`'s `MANUAL_REPLY_STEP` — a one-to-one reply is
 * recorded at step 0, outside the 1-based sequence. Kept local so this
 * projection does not import the whole reply path; a test pins the two equal.
 */
export const MANUAL_REPLY_STEP = 0;

export type MessageDirection = "out" | "in";
export type MessageKind =
  | "outreach"
  | "manual_reply"
  | "warmup"
  | "warmup_reply"
  | "seed"
  | "reply"
  | "auto_reply"
  | "bounce"
  | "unrelated";
export type MessageTransport = "instantly" | "smtp";
export type MessageOutcome = "sent" | "permanent" | "transient" | "received";

export interface MessageRow {
  sourceTable: string;
  sourceRowId: string;
  messageId: string | null;
  direction: MessageDirection;
  kind: MessageKind;
  transport: MessageTransport;
  accountEmail: string;
  mailboxLogin: string | null;
  counterparty: string | null;
  subject: string | null;
  instantlyCampaignId: string | null;
  step: number | null;
  threadId: string;
  contextRef: string | null;
  orgId: string | null;
  campaignId: string | null;
  outcome: MessageOutcome;
  placement: string | null;
  spfPass: boolean | null;
  dkimPass: boolean | null;
  dmarcPass: boolean | null;
  occurredAt: Date;
}

/** Default re-read window; the unique source index makes the overlap a no-op. */
export const MESSAGES_SYNC_WINDOW_DAYS = 3;

function lower(v: unknown): string | null {
  const s = v === null || v === undefined ? "" : String(v).trim().toLowerCase();
  return s === "" ? null : s;
}

function str(v: unknown): string | null {
  const s = v === null || v === undefined ? "" : String(v);
  return s === "" ? null : s;
}

function toDate(v: unknown): Date | null {
  if (v === null || v === undefined) return null;
  const d = v instanceof Date ? v : new Date(String(v));
  return Number.isNaN(d.getTime()) ? null : d;
}

function bool(v: unknown): boolean | null {
  if (v === null || v === undefined) return null;
  if (typeof v === "boolean") return v;
  if (v === "t" || v === "true") return true;
  if (v === "f" || v === "false") return false;
  return null;
}

// ─── Pure mappers ────────────────────────────────────────────────────────────

export interface InstantlyEmailSource {
  id: string;
  instantlyCampaignId: string | null;
  ueType: string | null;
  messageId: string | null;
  eaccount: string | null;
  fromAddress: string | null;
  toAddresses: string | null;
  subject: string | null;
  stepRaw: string | null;
  timestampEmail: string | null;
  fetchedAt: Date | string;
  leadEmail: string | null;
  orgId: string | null;
  campaignId: string | null;
  mailboxLogin: string | null;
}

/**
 * Instantly's Unibox mirror. `ue_type` 1 is a sequence send, 2 an inbound
 * reply, 3 / 4 our own replies sent through Instantly. The Message-Id is the
 * one Instantly actually put on the wire (100% of outbound rows carry it).
 */
export function mapInstantlyEmail(r: InstantlyEmailSource): MessageRow | null {
  const inbound = r.ueType === "2";
  const account = lower(r.eaccount) ?? (inbound ? lower(r.toAddresses) : lower(r.fromAddress));
  if (!account) return null;
  const occurredAt = toDate(r.timestampEmail) ?? toDate(r.fetchedAt);
  if (!occurredAt) return null;
  const step = inbound ? null : parseInstantlySequenceStep(r.stepRaw);
  const thread = r.instantlyCampaignId ?? r.messageId ?? r.id;
  return {
    sourceTable: "instantly_emails_raw",
    sourceRowId: r.id,
    messageId: str(r.messageId),
    direction: inbound ? "in" : "out",
    kind: inbound ? "reply" : r.ueType === "1" ? "outreach" : "manual_reply",
    transport: "instantly",
    accountEmail: account,
    mailboxLogin: r.mailboxLogin,
    counterparty: inbound
      ? (lower(r.fromAddress) ?? lower(r.leadEmail))
      : (lower(r.toAddresses) ?? lower(r.leadEmail)),
    subject: str(r.subject),
    instantlyCampaignId: r.instantlyCampaignId,
    step,
    threadId: thread,
    contextRef: null,
    orgId: r.orgId,
    campaignId: r.campaignId,
    outcome: inbound ? "received" : "sent",
    placement: null,
    spfPass: null,
    dkimPass: null,
    dmarcPass: null,
    occurredAt,
  };
}

export interface SmtpDispatchSource {
  id: string;
  instantlyCampaignId: string;
  leadEmail: string;
  accountEmail: string;
  step: number;
  outcome: string;
  messageId: string | null;
  subject: string | null;
  dispatchedAt: Date | string;
  orgId: string | null;
  campaignId: string | null;
  mailboxLogin: string | null;
}

/** Our own dispatcher. Step 0 is a one-to-one reply, never a sequence step. */
export function mapSmtpDispatch(r: SmtpDispatchSource): MessageRow | null {
  const occurredAt = toDate(r.dispatchedAt);
  if (!occurredAt) return null;
  const manual = r.step === MANUAL_REPLY_STEP;
  return {
    sourceTable: "smtp_dispatch_raw",
    sourceRowId: r.id,
    messageId: str(r.messageId),
    direction: "out",
    kind: manual ? "manual_reply" : "outreach",
    transport: "smtp",
    accountEmail: r.accountEmail.toLowerCase(),
    mailboxLogin: r.mailboxLogin,
    counterparty: r.leadEmail.toLowerCase(),
    subject: str(r.subject),
    instantlyCampaignId: r.instantlyCampaignId,
    step: manual ? null : r.step,
    threadId: r.instantlyCampaignId,
    contextRef: null,
    orgId: r.orgId,
    campaignId: r.campaignId,
    outcome: r.outcome === "sent" || r.outcome === "permanent" ? r.outcome : "transient",
    placement: null,
    spfPass: null,
    dkimPass: null,
    dmarcPass: null,
    occurredAt,
  };
}

export interface ImapMessageSource {
  id: string;
  accountEmail: string;
  messageId: string;
  fromAddress: string | null;
  subject: string | null;
  kind: string;
  instantlyCampaignId: string | null;
  step: number | null;
  receivedAt: Date | string | null;
  polledAt: Date | string;
  orgId: string | null;
  campaignId: string | null;
  mailboxLogin: string | null;
}

const IMAP_KINDS: Record<string, MessageKind> = {
  reply: "reply",
  auto_reply: "auto_reply",
  bounce: "bounce",
  unrelated: "unrelated",
};

/**
 * What came back into a mailbox we read ourselves. The mapper accepts
 * `unrelated` (so nothing is silently dropped if one arrives), but the sync
 * EXCLUDES it at the read: measured 2026-09-18, 80,697 of 80,891 IMAP rows
 * polled in three days were unrelated — newsletters and notifications on real
 * mailboxes — and none of them is about our outreach or our mailboxes' health.
 * Bronze keeps them; this projection is the mail that concerns the estate.
 */
export function mapImapMessage(r: ImapMessageSource): MessageRow | null {
  const occurredAt = toDate(r.receivedAt) ?? toDate(r.polledAt);
  if (!occurredAt) return null;
  const kind = IMAP_KINDS[r.kind] ?? "unrelated";
  return {
    sourceTable: "imap_messages_raw",
    sourceRowId: r.id,
    messageId: r.messageId,
    direction: "in",
    kind,
    transport: "smtp",
    accountEmail: r.accountEmail.toLowerCase(),
    mailboxLogin: r.mailboxLogin,
    counterparty: lower(r.fromAddress),
    subject: str(r.subject),
    instantlyCampaignId: r.instantlyCampaignId,
    step: r.step,
    threadId: r.instantlyCampaignId ?? r.messageId,
    contextRef: null,
    orgId: r.orgId,
    campaignId: r.campaignId,
    outcome: "received",
    placement: null,
    spfPass: null,
    dkimPass: null,
    dmarcPass: null,
    occurredAt,
  };
}

export interface WarmupDispatchSource {
  id: string;
  senderEmail: string;
  senderMailbox: string;
  receiverEmail: string;
  dayKey: string;
  messageId: string | null;
  subject: string | null;
  outcome: string;
  dispatchedAt: Date | string;
  placement: string | null;
}

/** A warmup send; a reply is a send too and is told apart by its `Re:` subject. */
export function mapWarmupDispatch(r: WarmupDispatchSource): MessageRow | null {
  const occurredAt = toDate(r.dispatchedAt);
  if (!occurredAt) return null;
  const reply = /^re:/i.test((r.subject ?? "").trim());
  return {
    sourceTable: "warmup_dispatches",
    sourceRowId: r.id,
    messageId: str(r.messageId),
    direction: "out",
    kind: reply ? "warmup_reply" : "warmup",
    transport: "smtp",
    accountEmail: r.senderEmail.toLowerCase(),
    mailboxLogin: r.senderMailbox.toLowerCase(),
    counterparty: r.receiverEmail.toLowerCase(),
    subject: str(r.subject),
    instantlyCampaignId: null,
    step: null,
    threadId: r.messageId ?? r.id,
    contextRef: r.dayKey,
    orgId: null,
    campaignId: null,
    outcome: r.outcome === "sent" || r.outcome === "permanent" ? r.outcome : "transient",
    placement: r.placement,
    spfPass: null,
    dkimPass: null,
    dmarcPass: null,
    occurredAt,
  };
}

export interface SeedDispatchSource {
  id: string;
  testId: string;
  senderEmail: string;
  receiverEmail: string;
  messageId: string | null;
  outcome: string;
  dispatchedAt: Date | string;
  placement: string | null;
  spfPass: boolean | string | null;
  dkimPass: boolean | string | null;
  dmarcPass: boolean | string | null;
  mailboxLogin: string | null;
}

/** A placement seed; where it landed and its auth verdict arrive with the observation. */
export function mapSeedDispatch(r: SeedDispatchSource): MessageRow | null {
  const occurredAt = toDate(r.dispatchedAt);
  if (!occurredAt) return null;
  return {
    sourceTable: "seed_placement_dispatches",
    sourceRowId: r.id,
    messageId: str(r.messageId),
    direction: "out",
    kind: "seed",
    transport: "smtp",
    accountEmail: r.senderEmail.toLowerCase(),
    mailboxLogin: r.mailboxLogin,
    counterparty: r.receiverEmail.toLowerCase(),
    subject: null,
    instantlyCampaignId: null,
    step: null,
    threadId: r.messageId ?? r.id,
    contextRef: r.testId,
    orgId: null,
    campaignId: null,
    outcome: r.outcome === "sent" || r.outcome === "permanent" ? r.outcome : "transient",
    placement: r.placement,
    spfPass: bool(r.spfPass),
    dkimPass: bool(r.dkimPass),
    dmarcPass: bool(r.dmarcPass),
    occurredAt,
  };
}

// ─── IO ──────────────────────────────────────────────────────────────────────

function rowsOf<T>(result: unknown): T[] {
  if (Array.isArray(result)) return result as T[];
  const rows = (result as { rows?: unknown })?.rows;
  return Array.isArray(rows) ? (rows as T[]) : [];
}

export interface MessagesSyncSummary {
  windowDays: number;
  read: Record<string, number>;
  upserted: number;
  skipped: number;
}

const UPSERT_CHUNK = 500;

async function upsertRows(rows: MessageRow[], syncedAt: Date): Promise<number> {
  let written = 0;
  for (let i = 0; i < rows.length; i += UPSERT_CHUNK) {
    const chunk = rows.slice(i, i + UPSERT_CHUNK).map((r) => ({ ...r, syncedAt }));
    await db
      .insert(messages)
      .values(chunk)
      .onConflictDoUpdate({
        target: [messages.sourceTable, messages.sourceRowId],
        set: {
          messageId: sql`excluded.message_id`,
          kind: sql`excluded.kind`,
          mailboxLogin: sql`excluded.mailbox_login`,
          counterparty: sql`excluded.counterparty`,
          subject: sql`excluded.subject`,
          instantlyCampaignId: sql`excluded.instantly_campaign_id`,
          step: sql`excluded.step`,
          threadId: sql`excluded.thread_id`,
          orgId: sql`excluded.org_id`,
          campaignId: sql`excluded.campaign_id`,
          outcome: sql`excluded.outcome`,
          placement: sql`excluded.placement`,
          spfPass: sql`excluded.spf_pass`,
          dkimPass: sql`excluded.dkim_pass`,
          dmarcPass: sql`excluded.dmarc_pass`,
          occurredAt: sql`excluded.occurred_at`,
          syncedAt,
        },
      });
    written += chunk.length;
  }
  return written;
}

/**
 * Re-read a window of every source and upsert. `sinceDays` bounds the window
 * on each source's OWN write timestamp; the unique source index makes the
 * overlap a no-op, so there is no cursor to drift. A large `sinceDays` is the
 * backfill.
 */
export async function syncMessages(
  opts: { sinceDays?: number } = {},
): Promise<MessagesSyncSummary> {
  // The interval tick passes 1: a day of every source is a few thousand rows,
  // and the unique source index makes the overlap with the previous tick free.
  const windowDays = Math.max(1, Math.floor(opts.sinceDays ?? MESSAGES_SYNC_WINDOW_DAYS));
  const cutoff = sql`now() - make_interval(days => ${windowDays})`;
  const syncedAt = new Date();
  const read: Record<string, number> = {};
  const rows: MessageRow[] = [];
  let skipped = 0;

  const instantly = rowsOf<Record<string, unknown>>(
    await db.execute(sql`
      SELECT m.id, m.instantly_campaign_id, m.fetched_at,
             m.payload->>'ue_type' AS ue_type,
             m.payload->>'message_id' AS message_id,
             m.payload->>'eaccount' AS eaccount,
             m.payload->>'from_address_email' AS from_address,
             m.payload->>'to_address_email_list' AS to_addresses,
             m.payload->>'subject' AS subject,
             m.payload->>'step' AS step_raw,
             m.payload->>'timestamp_email' AS timestamp_email,
             c.lead_email, c.org_id, c.campaign_id, a.mailbox_login
      FROM instantly_emails_raw m
      LEFT JOIN instantly_campaigns c ON c.instantly_campaign_id = m.instantly_campaign_id
      LEFT JOIN instantly_accounts a ON lower(a.email) = lower(m.payload->>'eaccount')
      WHERE m.fetched_at >= ${cutoff}
    `),
  );
  read.instantly_emails_raw = instantly.length;
  for (const r of instantly) {
    const mapped = mapInstantlyEmail({
      id: String(r.id),
      instantlyCampaignId: str(r.instantly_campaign_id),
      ueType: str(r.ue_type),
      messageId: str(r.message_id),
      eaccount: str(r.eaccount),
      fromAddress: str(r.from_address),
      toAddresses: str(r.to_addresses),
      subject: str(r.subject),
      stepRaw: str(r.step_raw),
      timestampEmail: str(r.timestamp_email),
      fetchedAt: r.fetched_at as Date,
      leadEmail: str(r.lead_email),
      orgId: str(r.org_id),
      campaignId: str(r.campaign_id),
      mailboxLogin: str(r.mailbox_login),
    });
    if (mapped) rows.push(mapped);
    else skipped += 1;
  }

  const smtp = rowsOf<Record<string, unknown>>(
    await db.execute(sql`
      SELECT d.id, d.instantly_campaign_id, d.lead_email, d.account_email, d.step, d.outcome,
             d.message_id, d.dispatched_at,
             COALESCE(s.subject, d.payload->>'subject') AS subject,
             c.org_id, c.campaign_id, a.mailbox_login
      FROM smtp_dispatch_raw d
      LEFT JOIN sequence_steps s
        ON s.instantly_campaign_id = d.instantly_campaign_id AND s.step = d.step
      LEFT JOIN instantly_campaigns c ON c.instantly_campaign_id = d.instantly_campaign_id
      LEFT JOIN instantly_accounts a ON lower(a.email) = lower(d.account_email)
      WHERE d.dispatched_at >= ${cutoff}
    `),
  );
  read.smtp_dispatch_raw = smtp.length;
  for (const r of smtp) {
    const mapped = mapSmtpDispatch({
      id: String(r.id),
      instantlyCampaignId: String(r.instantly_campaign_id),
      leadEmail: String(r.lead_email),
      accountEmail: String(r.account_email),
      step: Number(r.step),
      outcome: String(r.outcome),
      messageId: str(r.message_id),
      subject: str(r.subject),
      dispatchedAt: r.dispatched_at as Date,
      orgId: str(r.org_id),
      campaignId: str(r.campaign_id),
      mailboxLogin: str(r.mailbox_login),
    });
    if (mapped) rows.push(mapped);
    else skipped += 1;
  }

  const imap = rowsOf<Record<string, unknown>>(
    await db.execute(sql`
      SELECT i.id, i.account_email, i.message_id, i.from_address, i.subject, i.kind,
             i.instantly_campaign_id, i.step, i.received_at, i.polled_at,
             c.org_id, c.campaign_id, a.mailbox_login
      FROM imap_messages_raw i
      LEFT JOIN instantly_campaigns c ON c.instantly_campaign_id = i.instantly_campaign_id
      LEFT JOIN instantly_accounts a ON lower(a.email) = lower(i.account_email)
      WHERE i.polled_at >= ${cutoff}
        AND i.kind <> 'unrelated'
    `),
  );
  read.imap_messages_raw = imap.length;
  for (const r of imap) {
    const mapped = mapImapMessage({
      id: String(r.id),
      accountEmail: String(r.account_email),
      messageId: String(r.message_id),
      fromAddress: str(r.from_address),
      subject: str(r.subject),
      kind: String(r.kind),
      instantlyCampaignId: str(r.instantly_campaign_id),
      step: r.step === null || r.step === undefined ? null : Number(r.step),
      receivedAt: (r.received_at as Date | null) ?? null,
      polledAt: r.polled_at as Date,
      orgId: str(r.org_id),
      campaignId: str(r.campaign_id),
      mailboxLogin: str(r.mailbox_login),
    });
    if (mapped) rows.push(mapped);
    else skipped += 1;
  }

  const warmup = rowsOf<Record<string, unknown>>(
    await db.execute(sql`
      SELECT w.id, w.sender_email, w.sender_mailbox, w.receiver_email, w.day_key, w.message_id,
             w.subject, w.outcome, w.dispatched_at, r.placement
      FROM warmup_dispatches w
      LEFT JOIN warmup_receipts r
        ON r.message_id = w.message_id AND r.receiver_email = w.receiver_email
      WHERE w.dispatched_at >= ${cutoff}
    `),
  );
  read.warmup_dispatches = warmup.length;
  for (const r of warmup) {
    const mapped = mapWarmupDispatch({
      id: String(r.id),
      senderEmail: String(r.sender_email),
      senderMailbox: String(r.sender_mailbox),
      receiverEmail: String(r.receiver_email),
      dayKey: String(r.day_key),
      messageId: str(r.message_id),
      subject: str(r.subject),
      outcome: String(r.outcome),
      dispatchedAt: r.dispatched_at as Date,
      placement: str(r.placement),
    });
    if (mapped) rows.push(mapped);
    else skipped += 1;
  }

  const seeds = rowsOf<Record<string, unknown>>(
    await db.execute(sql`
      SELECT d.id, d.test_id, d.sender_email, d.receiver_email, d.message_id, d.outcome,
             d.dispatched_at, o.placement, o.spf_pass, o.dkim_pass, o.dmarc_pass, a.mailbox_login
      FROM seed_placement_dispatches d
      LEFT JOIN seed_placement_observations o
        ON o.message_id = d.message_id AND o.receiver_email = d.receiver_email
      LEFT JOIN instantly_accounts a ON lower(a.email) = lower(d.sender_email)
      WHERE d.dispatched_at >= ${cutoff}
    `),
  );
  read.seed_placement_dispatches = seeds.length;
  for (const r of seeds) {
    const mapped = mapSeedDispatch({
      id: String(r.id),
      testId: String(r.test_id),
      senderEmail: String(r.sender_email),
      receiverEmail: String(r.receiver_email),
      messageId: str(r.message_id),
      outcome: String(r.outcome),
      dispatchedAt: r.dispatched_at as Date,
      placement: str(r.placement),
      spfPass: r.spf_pass as boolean | null,
      dkimPass: r.dkim_pass as boolean | null,
      dmarcPass: r.dmarc_pass as boolean | null,
      mailboxLogin: str(r.mailbox_login),
    });
    if (mapped) rows.push(mapped);
    else skipped += 1;
  }

  const upserted = await upsertRows(rows, syncedAt);
  const summary = { windowDays, read, upserted, skipped };
  console.log(`[instantly-service] messages-sync: ${JSON.stringify(summary)}`);
  return summary;
}
