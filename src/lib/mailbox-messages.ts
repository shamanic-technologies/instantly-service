/**
 * The cross-org feed of our sending mailboxes' messages, `GET /internal/mailbox-messages`
 * (EVERY-PERSON-ON-THE-THREAD-1109, owner 2026-11-09).
 *
 * crm-service judges every conversation it reads (Gmail, WhatsApp) for org,
 * brand, persona and "is this about the brand", but never saw the mailboxes we
 * prospect from: Markus, who answered our email to his colleague Thomas, was
 * filed under Thomas and existed nowhere. This feed plugs those mailboxes into
 * the CRM as one more mail source: every message we sent or received (warmup
 * and seeds excluded), with EVERY address on it and its role
 * (lib/message-participants), the words, the Message-Id chain and the sequence
 * it is filed on. Who each person is gets judged downstream (crm, Jev).
 *
 * HOW IT STAYS EXACT:
 *  - DB triggers (migration 0108) mark every bronze row written or changed in
 *    `mailbox_message_dirty` (`imap_messages_raw` minus plain unrelated noise,
 *    `smtp_dispatch_raw` sent rows, `instantly_emails_raw`): a refile
 *    (unrelated -> reply), a body stored later, a re-key is seen whoever wrote it.
 *  - the worker drains it: rebuilds each message from bronze, hashes what we
 *    serve, and appends a VERSION row only when the hash moved. Versions are
 *    written under one advisory lock, so `seq` commits in order and a reader
 *    paging by cursor never skips one (same reasoning as outreach-facts).
 *  - the reader serves the message rebuilt from bronze at read; the version
 *    gives the order and the thread filter only.
 *
 * Ids: `smtp:<dispatch row>` (what we sent), `imap:<sha of Message-Id>` (what
 * we received: ONE id even when five aliases of one mailbox hold the copy; the
 * copy filed on a thread wins), `iem:<mirror row>` (Instantly-era Unibox).
 */
import { createHash } from "node:crypto";

import { sql, type SQL } from "drizzle-orm";

import { db } from "../db";
import { htmlToText } from "./forward-positive-reply";
import {
  dispatchEnvelope,
  imapEnvelope,
  instantlyEnvelope,
  type MessageAddress,
  type MessageEnvelope,
  type MessageParticipant,
} from "./message-participants";
import { nextFeedCursor } from "./outreach-facts";
import { sentBodyHtml } from "./self-send/sent-body";
import { isStaffSender } from "./staff-senders";

/** Any 64-bit constant unique to this lock. */
const FEED_LOCK_KEY = 726_400_221;

export const MAILBOX_MESSAGES_MAX_LIMIT = 500;
export const MAILBOX_MESSAGES_DEFAULT_LIMIT = 200;

export type MailboxMessageKind =
  | "outreach"
  | "manual_reply"
  | "reply"
  | "auto_reply"
  | "staff_reply"
  | "bounce"
  | "delay"
  | "unrelated";

export interface MailboxMessage {
  id: string;
  version: string;
  mailbox: string;
  direction: "in" | "out";
  kind: MailboxMessageKind;
  messageId: string | null;
  inReplyTo: string | null;
  references: string[];
  threadKey: string | null;
  occurredAt: string;
  subject: string | null;
  from: MessageAddress | null;
  to: MessageAddress[];
  cc: MessageAddress[];
  bcc: MessageAddress[];
  participants: MessageParticipant[];
  text: string | null;
  textUnavailableReason: string | null;
  sentBy: "human" | "automation" | null;
  orgId: string | null;
  brandIds: string[];
  campaignId: string | null;
  instantlyCampaignId: string | null;
  leadEmail: string | null;
}

/** A message before it is placed in the feed (no version yet). */
export type BuiltMessage = Omit<MailboxMessage, "version">;

export const SOURCE_TABLES = ["imap_messages_raw", "smtp_dispatch_raw", "instantly_emails_raw"] as const;
export type SourceTable = (typeof SOURCE_TABLES)[number];

function rowsOf<T>(result: unknown): T[] {
  if (Array.isArray(result)) return result as T[];
  const rows = (result as { rows?: unknown })?.rows;
  return Array.isArray(rows) ? (rows as T[]) : [];
}

function str(v: unknown): string | null {
  if (v === null || v === undefined) return null;
  const s = String(v);
  return s === "" ? null : s;
}

function iso(v: unknown): string {
  if (v instanceof Date) return v.toISOString();
  const d = new Date(String(v));
  return Number.isNaN(d.getTime()) ? "" : d.toISOString();
}

function brandIdsOf(v: unknown): string[] {
  return Array.isArray(v) ? v.map(String) : [];
}

function attribution(r: Record<string, unknown>) {
  return {
    orgId: str(r.c_org_id),
    brandIds: brandIdsOf(r.c_brand_ids),
    campaignId: str(r.c_campaign_id),
    instantlyCampaignId: str(r.c_instantly_campaign_id),
    leadEmail: str(r.c_lead_email)?.toLowerCase() ?? null,
  };
}

function text(value: string | null, reason: string): { text: string | null; textUnavailableReason: string | null } {
  const t = value === null ? "" : value.trim();
  return t === "" ? { text: null, textUnavailableReason: reason } : { text: t, textUnavailableReason: null };
}

function withEnvelope(e: MessageEnvelope) {
  return {
    messageId: e.messageId,
    inReplyTo: e.inReplyTo,
    references: e.references,
    threadKey: e.threadKey,
    from: e.from,
    to: e.to,
    cc: e.cc,
    bcc: e.bcc,
    participants: e.participants,
  };
}

/** The served id of a received message: one per Message-Id, whichever alias holds it. */
export function imapMessageKey(messageId: string): string {
  return `imap:${createHash("sha256").update(messageId.trim()).digest("hex").slice(0, 32)}`;
}

const IMAP_KINDS: Record<string, MailboxMessageKind> = {
  reply: "reply",
  auto_reply: "auto_reply",
  staff_reply: "staff_reply",
  bounce: "bounce",
  delay: "delay",
  unrelated: "unrelated",
};

/** Pure: one IMAP bronze row (+ its sequence) as served. */
export function buildImapMessage(r: Record<string, unknown>): BuiltMessage {
  const accountEmail = String(r.account_email).toLowerCase();
  const env = imapEnvelope({
    accountEmail,
    fromAddress: str(r.from_address),
    messageId: str(r.message_id),
    payload: r.payload,
  });
  const kind = IMAP_KINDS[String(r.kind)] ?? "unrelated";
  const payload = (r.payload ?? {}) as Record<string, unknown>;
  const filed = str(r.instantly_campaign_id) !== null;
  return {
    id: imapMessageKey(String(r.message_id)),
    mailbox: accountEmail,
    direction: kind === "staff_reply" ? "out" : "in",
    kind,
    ...withEnvelope(env),
    occurredAt: iso(r.received_at ?? r.polled_at),
    subject: str(r.subject),
    ...text(typeof payload.textSnippet === "string" ? payload.textSnippet : null, "body_not_stored"),
    sentBy: kind === "staff_reply" ? "human" : null,
    ...(filed
      ? attribution(r)
      : { orgId: null, brandIds: [], campaignId: null, instantlyCampaignId: null, leadEmail: null }),
  };
}

/** Pure: one row of our dispatch log (+ its sequence) as served. */
export function buildDispatchMessage(r: Record<string, unknown>): BuiltMessage {
  const payload = (r.payload ?? {}) as Record<string, unknown>;
  const step = Number(r.step);
  const manual = step === 0;
  const accountEmail = String(r.account_email).toLowerCase();
  const name = [str(r.a_first_name), str(r.a_last_name)].filter(Boolean).join(" ");
  const env = dispatchEnvelope({
    accountEmail,
    accountName: name || null,
    leadEmail: String(r.lead_email),
    messageId: str(r.message_id),
    payload,
  });
  const body = sentBodyHtml({
    bodyHtml: String(r.s_body_html ?? payload.bodyHtml ?? ""),
    sentHtml: typeof r.sent_html === "string" ? r.sent_html : null,
    sequenceStep: !manual,
    dispatchedAt: (r.dispatched_at as Date | string | null) ?? null,
    pinnedSignatureHtml: typeof r.c_signature_html === "string" ? r.c_signature_html : null,
    accountKnown: r.a_email !== null && r.a_email !== undefined,
    accountEmail,
    accountFirstName: str(r.a_first_name),
    accountLastName: str(r.a_last_name),
  });
  const sentBy = payload.sentBy === "human" || payload.sentBy === "automation" ? payload.sentBy : null;
  return {
    id: `smtp:${String(r.id)}`,
    mailbox: accountEmail,
    direction: "out",
    kind: manual ? "manual_reply" : "outreach",
    ...withEnvelope(env),
    occurredAt: iso(r.dispatched_at),
    subject: str(r.s_subject) ?? str(payload.subject),
    ...text(htmlToText(body.bodyHtml), "body_not_stored"),
    sentBy: manual ? sentBy : null,
    ...attribution(r),
  };
}

/** Pure: one row of the Instantly Unibox mirror (+ its sequence) as served. */
export function buildInstantlyMessage(r: Record<string, unknown>): BuiltMessage {
  const p = (r.payload ?? {}) as Record<string, unknown>;
  const env = instantlyEnvelope(p);
  const ue = String(p.ue_type ?? "");
  const leadEmail = str(r.c_lead_email) ?? str(p.lead);
  const staff = ue === "2" && isStaffSender(str(p.from_address_email), leadEmail);
  const inbound = ue === "2" && !staff;
  const body = (p.body ?? {}) as Record<string, unknown>;
  const words =
    typeof body.text === "string" && body.text.trim() !== ""
      ? body.text
      : typeof body.html === "string"
        ? htmlToText(body.html)
        : null;
  const threadId = str(p.thread_id);
  return {
    id: `iem:${String(r.id)}`,
    mailbox: (str(p.eaccount) ?? "").toLowerCase(),
    direction: inbound ? "in" : "out",
    kind: inbound ? "reply" : staff ? "staff_reply" : ue === "1" ? "outreach" : "manual_reply",
    ...withEnvelope(env),
    // Instantly's mirror carries no References: its own thread id groups the thread.
    threadKey: threadId ? `instantly-thread:${threadId}` : env.threadKey,
    occurredAt: iso(p.timestamp_email ?? r.fetched_at),
    subject: str(p.subject),
    ...text(words, "body_not_stored"),
    sentBy: staff ? "human" : null,
    ...attribution(r),
    leadEmail: leadEmail?.toLowerCase() ?? null,
  };
}

/** What a version hashes: everything served. */
export function messageHash(m: BuiltMessage): string {
  return createHash("sha256").update(JSON.stringify(m)).digest("hex").slice(0, 32);
}

const CAMPAIGN_COLUMNS = sql`
  c.org_id AS c_org_id, c.brand_ids AS c_brand_ids, c.campaign_id AS c_campaign_id,
  c.instantly_campaign_id AS c_instantly_campaign_id, c.lead_email AS c_lead_email,
  c.signature_html AS c_signature_html`;

function idList(ids: string[]): SQL {
  return sql.join(ids.map((id) => sql`${id}`), sql`, `);
}

/**
 * The IMAP copies of the given Message-Ids that may be served, the canonical one
 * first per Message-Id: the copy filed on a thread, else the first polled.
 */
async function loadImapCanonical(messageIds: string[]): Promise<Map<string, Record<string, unknown>>> {
  const out = new Map<string, Record<string, unknown>>();
  if (messageIds.length === 0) return out;
  const rows = rowsOf<Record<string, unknown>>(
    await db.execute(sql`
      SELECT DISTINCT ON (i.message_id)
             i.id, i.account_email, i.message_id, i.from_address, i.subject, i.kind,
             i.instantly_campaign_id, i.step, i.payload, i.received_at, i.polled_at, ${CAMPAIGN_COLUMNS}
      FROM imap_messages_raw i
      LEFT JOIN instantly_campaigns c ON c.instantly_campaign_id = i.instantly_campaign_id
      WHERE i.message_id IN (${idList(messageIds)})
        AND (i.kind <> 'unrelated' OR i.payload ? 'orphanCandidate')
      ORDER BY i.message_id, (i.kind = 'unrelated'), i.polled_at, i.id
    `),
  );
  for (const r of rows) out.set(String(r.message_id), r);
  return out;
}

async function loadDispatchRows(ids: string[]): Promise<Record<string, unknown>[]> {
  if (ids.length === 0) return [];
  return rowsOf<Record<string, unknown>>(
    await db.execute(sql`
      SELECT d.id, d.instantly_campaign_id, d.lead_email, d.account_email, d.step, d.outcome,
             d.message_id, d.payload, d.dispatched_at,
             s.subject AS s_subject, s.body_html AS s_body_html,
             sm.message_inputs->'built'->>'html' AS sent_html,
             a.email AS a_email, a.first_name AS a_first_name, a.last_name AS a_last_name,
             ${CAMPAIGN_COLUMNS}
      FROM smtp_dispatch_raw d
      LEFT JOIN sequence_steps s ON s.instantly_campaign_id = d.instantly_campaign_id AND s.step = d.step
      LEFT JOIN smtp_sent_messages sm ON sm.dispatch_id = d.id
      LEFT JOIN instantly_campaigns c ON c.instantly_campaign_id = d.instantly_campaign_id
      LEFT JOIN instantly_accounts a ON a.email = d.account_email
      WHERE d.id IN (${idList(ids)}) AND d.outcome = 'sent'
    `),
  );
}

async function loadInstantlyRows(ids: string[]): Promise<Record<string, unknown>[]> {
  if (ids.length === 0) return [];
  return rowsOf<Record<string, unknown>>(
    await db.execute(sql`
      SELECT m.id, m.instantly_campaign_id, m.payload, m.fetched_at, ${CAMPAIGN_COLUMNS}
      FROM instantly_emails_raw m
      LEFT JOIN instantly_campaigns c ON c.instantly_campaign_id = m.instantly_campaign_id
      WHERE m.id IN (${idList(ids)})
    `),
  );
}

export interface SourceRef {
  sourceTable: string;
  sourceRowId: string;
}

/** The served message of each source row still servable, keyed `table:id`. */
export async function buildMessagesFor(
  refs: SourceRef[],
): Promise<Map<string, { message: BuiltMessage; source: SourceRef }>> {
  const out = new Map<string, { message: BuiltMessage; source: SourceRef }>();
  const ids = (t: SourceTable) => [...new Set(refs.filter((r) => r.sourceTable === t).map((r) => r.sourceRowId))];

  const imapIds = ids("imap_messages_raw");
  if (imapIds.length > 0) {
    const own = rowsOf<Record<string, unknown>>(
      await db.execute(sql`SELECT id, message_id FROM imap_messages_raw WHERE id IN (${idList(imapIds)})`),
    );
    const canonical = await loadImapCanonical([...new Set(own.map((r) => String(r.message_id)))]);
    for (const r of own) {
      const row = canonical.get(String(r.message_id));
      if (!row) continue;
      const source = { sourceTable: "imap_messages_raw", sourceRowId: String(row.id) };
      out.set(`imap_messages_raw:${String(r.id)}`, { message: buildImapMessage(row), source });
    }
  }
  for (const r of await loadDispatchRows(ids("smtp_dispatch_raw"))) {
    out.set(`smtp_dispatch_raw:${String(r.id)}`, {
      message: buildDispatchMessage(r),
      source: { sourceTable: "smtp_dispatch_raw", sourceRowId: String(r.id) },
    });
  }
  for (const r of await loadInstantlyRows(ids("instantly_emails_raw"))) {
    const message = buildInstantlyMessage(r);
    if (!message.mailbox) continue; // no mailbox of ours named: not a message of our mailboxes
    out.set(`instantly_emails_raw:${String(r.id)}`, {
      message,
      source: { sourceTable: "instantly_emails_raw", sourceRowId: String(r.id) },
    });
  }
  return out;
}

export interface DrainSummary {
  drained: number;
  versioned: number;
  unchanged: number;
  notServable: number;
}

/**
 * Take up to `limit` marked rows, rebuild them, append a version where what we
 * serve changed. One transaction under the feed lock: the rows leave the dirty
 * set only with their versions (a crash re-marks nothing and loses nothing).
 */
export async function drainMailboxMessages(limit = 500): Promise<DrainSummary> {
  return db.transaction(async (tx) => {
    await tx.execute(sql`SELECT pg_advisory_xact_lock(${FEED_LOCK_KEY})`);
    const dirty = rowsOf<Record<string, unknown>>(
      await tx.execute(sql`
        DELETE FROM mailbox_message_dirty d
        USING (
          SELECT source_table, source_row_id FROM mailbox_message_dirty
          ORDER BY marked_at LIMIT ${limit}
        ) pick
        WHERE d.source_table = pick.source_table AND d.source_row_id = pick.source_row_id
        RETURNING d.source_table, d.source_row_id
      `),
    );
    const refs = dirty.map((r) => ({ sourceTable: String(r.source_table), sourceRowId: String(r.source_row_id) }));
    const built = await buildMessagesFor(refs);
    const byKey = new Map<string, { message: BuiltMessage; source: SourceRef }>();
    for (const v of built.values()) byKey.set(v.message.id, v);

    const keys = [...byKey.keys()];
    const latest = new Map<string, string>();
    if (keys.length > 0) {
      for (const r of rowsOf<Record<string, unknown>>(
        await tx.execute(sql`
          SELECT DISTINCT ON (message_key) message_key, source_hash
          FROM mailbox_message_versions
          WHERE message_key IN (${idList(keys)})
          ORDER BY message_key, seq DESC
        `),
      )) {
        latest.set(String(r.message_key), String(r.source_hash));
      }
    }

    let versioned = 0;
    const ordered = [...byKey.values()].sort((a, b) => a.message.occurredAt.localeCompare(b.message.occurredAt));
    for (const { message, source } of ordered) {
      const hash = messageHash(message);
      if (latest.get(message.id) === hash) continue;
      await tx.execute(sql`
        INSERT INTO mailbox_message_versions (message_key, source_table, source_row_id, source_hash, thread_key)
        VALUES (${message.id}, ${source.sourceTable}, ${source.sourceRowId}, ${hash}, ${message.threadKey})
      `);
      versioned += 1;
    }
    return {
      drained: refs.length,
      versioned,
      unchanged: byKey.size - versioned,
      notServable: refs.length - built.size,
    };
  });
}

/**
 * Mark the whole history once (first boot of the feed) or a window
 * (`sinceDays`): every servable bronze row goes through the drain like a fresh one.
 */
export async function markMailboxHistory(sinceDays: number | null): Promise<number> {
  const since = (col: SQL) =>
    sinceDays === null ? sql`` : sql`AND ${col} > now() - make_interval(days => ${sinceDays})`;
  const result = await db.execute(sql`
    INSERT INTO mailbox_message_dirty (source_table, source_row_id)
    SELECT 'smtp_dispatch_raw', d.id FROM smtp_dispatch_raw d WHERE d.outcome = 'sent' ${since(sql`d.dispatched_at`)}
    UNION ALL
    SELECT 'instantly_emails_raw', m.id FROM instantly_emails_raw m WHERE true ${since(sql`m.fetched_at`)}
    UNION ALL
    SELECT 'imap_messages_raw', i.id FROM imap_messages_raw i
    WHERE (i.kind <> 'unrelated' OR i.payload ? 'orphanCandidate') ${since(sql`i.polled_at`)}
    ON CONFLICT (source_table, source_row_id) DO NOTHING
  `);
  return Number((result as { rowCount?: number }).rowCount ?? 0);
}

/** True when the feed has never versioned anything nor has anything waiting. */
export async function mailboxFeedIsEmpty(): Promise<boolean> {
  const rows = rowsOf<Record<string, unknown>>(
    await db.execute(sql`
      SELECT (SELECT 1 FROM mailbox_message_versions LIMIT 1) AS v,
             (SELECT 1 FROM mailbox_message_dirty LIMIT 1) AS d
    `),
  );
  return rows.length > 0 && rows[0].v == null && rows[0].d == null;
}

/** Rebuild the messages named by versions, newest version per message, in feed order. */
async function serveVersions(versions: Record<string, unknown>[]): Promise<MailboxMessage[]> {
  const latestByKey = new Map<string, Record<string, unknown>>();
  for (const v of versions) latestByKey.set(String(v.message_key), v);
  const picked = [...latestByKey.values()].sort((a, b) =>
    BigInt(String(a.seq)) < BigInt(String(b.seq)) ? -1 : 1,
  );
  const built = await buildMessagesFor(
    picked.map((v) => ({ sourceTable: String(v.source_table), sourceRowId: String(v.source_row_id) })),
  );
  const out: MailboxMessage[] = [];
  for (const v of picked) {
    const entry = built.get(`${String(v.source_table)}:${String(v.source_row_id)}`);
    if (!entry) {
      console.warn(
        `[instantly-service] mailbox-messages: version ${String(v.seq)} names ${String(v.source_table)}:${String(v.source_row_id)}, no longer servable`,
      );
      continue;
    }
    out.push({ ...entry.message, version: String(v.seq) });
  }
  return out;
}

/** The feed after `after` (exclusive), or one thread's messages (latest version each). */
export async function readMailboxMessages(args: {
  after: number | string;
  limit: number;
  threadKey?: string;
}): Promise<{ messages: MailboxMessage[]; nextCursor: string; hasMore: boolean }> {
  if (args.threadKey) {
    const versions = rowsOf<Record<string, unknown>>(
      await db.execute(sql`
        SELECT * FROM (
          SELECT DISTINCT ON (message_key) seq::text AS seq, message_key, source_table, source_row_id
          FROM mailbox_message_versions
          WHERE thread_key = ${args.threadKey}
          ORDER BY message_key, seq DESC
        ) t ORDER BY seq::bigint
      `),
    );
    const messages = await serveVersions(versions);
    const last = versions.length > 0 ? String(versions[versions.length - 1].seq) : String(args.after);
    return { messages, nextCursor: last, hasMore: false };
  }

  const rows = rowsOf<Record<string, unknown>>(
    await db.execute(sql`
      WITH head AS (SELECT MAX(seq)::text AS feed_head FROM mailbox_message_versions),
      page AS (
        SELECT seq, message_key, source_table, source_row_id FROM mailbox_message_versions
        WHERE seq > ${String(args.after)}::bigint
        ORDER BY seq
        LIMIT ${args.limit + 1}
      )
      SELECT page.seq::text AS seq, page.message_key, page.source_table, page.source_row_id, head.feed_head
      FROM head LEFT JOIN page ON true
      ORDER BY page.seq
    `),
  );
  const feedHead = rows.length > 0 && rows[0].feed_head != null ? String(rows[0].feed_head) : null;
  const versions = rows.filter((r) => r.seq != null);
  const hasMore = versions.length > args.limit;
  const page = versions.slice(0, args.limit);
  const messages = await serveVersions(page);
  const lastSeq = page.length > 0 ? String(page[page.length - 1].seq) : null;
  return {
    messages,
    nextCursor: nextFeedCursor({ since: args.after, lastFactSeq: lastSeq, hasMore, feedHead }),
    hasMore,
  };
}
