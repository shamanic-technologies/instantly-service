/**
 * FACTORY-P3 next outcomes (lead_interested): every reply judgment is kept with what it was judged from and the EXACT
 * chat-service request it sent and the whole answer (`reply_qualifications`, migration 0098), on either path: our own
 * code (`service`) or the stored script while switched (`script`). workflow-service's case recorder reads it through
 * `GET /internal/reply-qualifications/recorded` to prove the stored script on fresh production judgments.
 *
 * Written AFTER the judgment and FAIL-SOFT: the verdict is the product, its record is evidence; a failed write is
 * logged and never turns a judgment into a failure. Personal data: never logged.
 */
import { and, asc, getTableColumns, gt, gte, lte, or, sql } from "drizzle-orm";
import { db } from "../db";
import { replyQualifications } from "../db/schema";
import { REPLY_KIND_CLASSIFICATION, type ReplyKind } from "./reply-kind";

export interface ReplyQualificationRecord {
  orgId: string | null;
  instantlyCampaignId: string;
  leadEmail?: string | null;
  source?: string | null;
  path: "service" | "script";
  runId: string | null;
  scriptSlug?: string | null;
  /** What the judgment was made from, before any business rule: the reply's text, its subject, the email we sent. */
  inputs: { replyText: string; subject: string | null; ourOutreach: string | null };
  request: Record<string, unknown> | null;
  response: Record<string, unknown> | null;
  kind: string | null;
  error?: string | null;
}

export async function recordReplyQualification(r: ReplyQualificationRecord): Promise<void> {
  try {
    await db.insert(replyQualifications).values({
      orgId: r.orgId,
      instantlyCampaignId: r.instantlyCampaignId,
      leadEmail: r.leadEmail ?? null,
      source: r.source ?? null,
      path: r.path,
      runId: r.runId,
      scriptSlug: r.scriptSlug ?? null,
      inputs: r.inputs,
      request: r.request,
      response: r.response,
      kind: r.kind,
      error: r.error ? r.error.slice(0, 1000) : null,
    });
  } catch (error) {
    console.error(
      `[instantly-service] reply qualification NOT recorded (the judgment stands) for ${r.instantlyCampaignId}: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}

export interface RecordedPage {
  qualifications: Array<{
    id: string;
    createdAt: string;
    orgId: string | null;
    instantlyCampaignId: string;
    source: string | null;
    path: string;
    runId: string | null;
    scriptSlug: string | null;
    inputs: unknown;
    request: unknown;
    response: unknown;
    kind: string | null;
    /** What the label means for the reply, as the replies table stores it (`REPLY_KIND_CLASSIFICATION`). */
    classification: "positive" | "negative" | "neutral" | null;
    error: string | null;
  }>;
  nextCursor: string | null;
  hasMore: boolean;
}

/**
 * The cursor is the last row's (created_at, id), base64url JSON. created_at is carried as Postgres's own text (its
 * microseconds): a JS Date keeps milliseconds only, so a page boundary inside one millisecond would re-serve rows.
 */
export function encodeCursor(createdAt: string, id: string): string {
  return Buffer.from(JSON.stringify([createdAt, id])).toString("base64url");
}

export function decodeCursor(cursor: string): { createdAt: string; id: string } | null {
  try {
    const [at, id] = JSON.parse(Buffer.from(cursor, "base64url").toString("utf8")) as [unknown, unknown];
    if (typeof at !== "string" || typeof id !== "string") return null;
    return Number.isNaN(new Date(at).getTime()) ? null : { createdAt: at, id };
  } catch {
    return null;
  }
}

/** Recorded judgments in (created_at, id) order, settled a minute (a row is stamped at insert). */
export async function readRecordedQualifications(opts: { from: Date; to?: Date; cursor: { createdAt: string; id: string } | null; limit: number }): Promise<RecordedPage> {
  const settled = sql`${replyQualifications.createdAt} < now() - interval '60 seconds'`;
  const where = and(
    gte(replyQualifications.createdAt, opts.from),
    opts.to ? lte(replyQualifications.createdAt, opts.to) : undefined,
    settled,
    opts.cursor
      ? or(
          sql`${replyQualifications.createdAt} > ${opts.cursor.createdAt}::timestamptz`,
          and(sql`${replyQualifications.createdAt} = ${opts.cursor.createdAt}::timestamptz`, gt(replyQualifications.id, opts.cursor.id)),
        )
      : undefined,
  );
  const rows = await db
    .select({ ...getTableColumns(replyQualifications), createdAtText: sql<string>`${replyQualifications.createdAt}::text` })
    .from(replyQualifications)
    .where(where)
    .orderBy(asc(replyQualifications.createdAt), asc(replyQualifications.id))
    .limit(opts.limit + 1);
  const page = rows.slice(0, opts.limit);
  const last = page.at(-1);
  return {
    qualifications: page.map((r) => ({
      id: r.id,
      createdAt: r.createdAt.toISOString(),
      orgId: r.orgId,
      instantlyCampaignId: r.instantlyCampaignId,
      source: r.source,
      path: r.path,
      runId: r.runId,
      scriptSlug: r.scriptSlug,
      inputs: r.inputs,
      request: r.request,
      response: r.response,
      kind: r.kind,
      classification: r.kind ? (REPLY_KIND_CLASSIFICATION[r.kind as ReplyKind] ?? null) : null,
      error: r.error,
    })),
    nextCursor: rows.length > opts.limit && last ? encodeCursor(last.createdAtText, last.id) : null,
    hasMore: rows.length > opts.limit,
  };
}
