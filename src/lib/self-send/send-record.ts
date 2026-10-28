/**
 * The durable record of every email we send (FACTORY-RECORD-SEND-1028): the
 * EXACT message the server received (`rawMessage` of `dispatchMessage`), the
 * inputs it was built from, and why it left when it did (`send-decision.ts`).
 * Table `smtp_sent_messages` (migration `0097`).
 *
 * Written AFTER the send is recorded and promoted, and FAIL-SOFT: the email has
 * left, so a failed record must never throw into the dispatch loop (a throw there
 * skips nothing useful and can re-send). It is logged loud instead.
 *
 * Kept `SENT_MESSAGE_RETENTION_DAYS` (≈1k sends a day, a few kB each, compressed
 * by TOAST), pruned by `sent-message-retention-worker.ts`.
 */

import { createHash } from "node:crypto";
import { and, desc, eq, lt, sql, type SQL } from "drizzle-orm";

import { db } from "../../db";
import { smtpSentMessages } from "../../db/schema";
import { errorMessage } from "../db-error.js";

export const SENT_MESSAGE_RETENTION_DAYS = 180;

export type SentMessageKind = "sequence_step" | "manual_reply";

export interface SentMessageRecord {
  dispatchId: string;
  instantlyCampaignId: string;
  leadEmail: string;
  accountEmail: string;
  step: number;
  kind: SentMessageKind;
  messageId: string;
  rawMessage: Buffer;
  /** What the message was built from (body, subject, signature, sender, thread). */
  messageInputs: unknown;
  /** Why it left at that moment (`SendDecision` for a sequence step). */
  decision: unknown;
  sentAt: Date;
}

export function sha256Hex(bytes: Buffer): string {
  return createHash("sha256").update(bytes).digest("hex");
}

/** Store one sent email. Never throws: the send already happened. */
export async function recordSentMessage(record: SentMessageRecord): Promise<boolean> {
  try {
    await db.insert(smtpSentMessages).values({
      dispatchId: record.dispatchId,
      instantlyCampaignId: record.instantlyCampaignId,
      leadEmail: record.leadEmail,
      accountEmail: record.accountEmail,
      step: record.step,
      kind: record.kind,
      messageId: record.messageId,
      rawMessage: record.rawMessage,
      rawSha256: sha256Hex(record.rawMessage),
      rawBytes: record.rawMessage.length,
      messageInputs: toJson(record.messageInputs),
      decision: toJson(record.decision),
      sentAt: record.sentAt,
    });
    return true;
  } catch (error) {
    console.error(
      `[instantly-service] send-record: sent message NOT recorded campaign=${record.instantlyCampaignId} step=${record.step} messageId=${record.messageId}: ${errorMessage(error)}`,
    );
    return false;
  }
}

/** Dates as ISO strings, Maps/Sets dropped to plain shapes: what jsonb can hold. */
function toJson(value: unknown): object {
  return JSON.parse(JSON.stringify(value ?? null)) as object;
}

export async function pruneSentMessages(now: Date = new Date()): Promise<number> {
  const cutoff = new Date(now.getTime() - SENT_MESSAGE_RETENTION_DAYS * 24 * 60 * 60 * 1000);
  const deleted = await db
    .delete(smtpSentMessages)
    .where(lt(smtpSentMessages.sentAt, cutoff))
    .returning({ id: smtpSentMessages.id });
  return deleted.length;
}

export interface SentMessageQuery {
  instantlyCampaignId?: string;
  leadEmail?: string;
  messageId?: string;
  limit: number;
}

/** Staff read: newest first, the raw message as base64 with its sha256. */
export async function loadSentMessages(query: SentMessageQuery) {
  const filters: SQL[] = [];
  if (query.instantlyCampaignId) {
    filters.push(eq(smtpSentMessages.instantlyCampaignId, query.instantlyCampaignId));
  }
  if (query.leadEmail) {
    filters.push(sql`lower(${smtpSentMessages.leadEmail}) = ${query.leadEmail.trim().toLowerCase()}`);
  }
  if (query.messageId) filters.push(eq(smtpSentMessages.messageId, query.messageId));

  const rows = await db
    .select()
    .from(smtpSentMessages)
    .where(filters.length > 0 ? and(...filters) : undefined)
    .orderBy(desc(smtpSentMessages.sentAt))
    .limit(query.limit);

  return rows.map((row) => ({
    id: row.id,
    dispatchId: row.dispatchId,
    instantlyCampaignId: row.instantlyCampaignId,
    leadEmail: row.leadEmail,
    accountEmail: row.accountEmail,
    step: row.step,
    kind: row.kind,
    messageId: row.messageId,
    sentAt: row.sentAt.toISOString(),
    rawMessageBase64: row.rawMessage.toString("base64"),
    rawSha256: row.rawSha256,
    rawBytes: row.rawBytes,
    messageInputs: row.messageInputs,
    decision: row.decision,
  }));
}
