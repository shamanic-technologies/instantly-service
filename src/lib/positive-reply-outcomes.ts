/**
 * COST-ACCOUNTING-1023 (owner 2026-10-23): a reply judged POSITIVE is the
 * fleet's key outcome, Positive reply (catalogue key `conversation`). This
 * declares it to runs-service's ledger, the same way lib/send-outcomes.ts
 * declares every `message_sent`.
 *
 * Grain: one sequence (`instantly_campaigns` row = one person x one campaign
 * thread) whose reply classification is `positive`
 * (`REPLY_KIND_CLASSIFICATION`, whoever judged it: our classifier, Instantly's
 * label, or a person's manual qualification) = ONE `conversation` occurrence
 * of the person, produced by a run of ours (`reply-judged-positive`), which
 * CONSUMES the person's `message_sent`. Subject = the person's lead in
 * lead-service, as everywhere in the chain.
 *
 * Why a run of ours: the classifier's LLM call is chat-service's
 * `/internal/platform-complete`, which opens its OWN platform run and returns no
 * id, so no run of this service ever "judged" the reply. The run here carries no
 * cost; the occurrence's production cost is what it consumed (the chain rolls
 * up), and the judge's own fraction of a cent stays on chat-service's run until
 * that call can nest under ours.
 *
 * Once per sequence: the run id is persisted BEFORE the declaration (a retry
 * reuses the same run, runs-service answers `already_recorded`), and the row is
 * marked `conversation_declared_at` afterwards; a later re-judgment (positive
 * again, another positive kind) never declares a second occurrence. A sequence
 * re-judged NON-positive after it was declared keeps its occurrence: the ledger
 * has no withdrawal, and the reply that was judged positive did happen; the
 * correction lives in the classification, not in the cost books.
 *
 * Never on the reply path: a sweep (driven by send-outcomes-worker) does it; a
 * failure leaves the row undeclared and the next tick retries.
 */
import { sql } from "drizzle-orm";
import { db } from "../db";
import { errorMessage } from "./db-error.js";
import { createRun, updateRun } from "./runs-client";
import {
  SEND_OUTCOME_SUBJECT_SERVICE,
  SEND_PRODUCED_OUTCOME,
  classifyDeclareAnswer,
  postRunOutcomes,
  type DeclarationMark,
  type SendOutcomeDeclaration,
} from "./send-outcomes";

export const POSITIVE_REPLY_OUTCOME = "conversation";
export const POSITIVE_REPLY_TASK = "reply-judged-positive";
export const POSITIVE_REPLY_BATCH_LIMIT = 100;

/** What a sequence's positive reply declares about the person. */
export function buildPositiveReplyDeclaration(leadId: string): SendOutcomeDeclaration {
  const subject = { subjectService: SEND_OUTCOME_SUBJECT_SERVICE, subjectId: leadId };
  return {
    produced: [{ outcomeKey: POSITIVE_REPLY_OUTCOME, ...subject }],
    consumed: [{ outcomeKey: SEND_PRODUCED_OUTCOME, ...subject }],
  };
}

interface PendingPositiveReply {
  id: string;
  orgId: string;
  userId: string | null;
  leadId: string | null;
  brandId: string | null;
  campaignId: string | null;
  runId: string | null;
}

async function loadPending(limit: number): Promise<PendingPositiveReply[]> {
  const result = await db.execute(sql`
    SELECT id, org_id, user_id, lead_id, brand_ids[1] AS brand_id, campaign_id, conversation_run_id
    FROM instantly_campaigns
    WHERE reply_classification = 'positive' AND conversation_declared_at IS NULL
    ORDER BY updated_at DESC
    LIMIT ${limit}
  `);
  return (result.rows as Array<Record<string, unknown>>).map((r) => ({
    id: String(r.id),
    orgId: String(r.org_id),
    userId: (r.user_id as string | null) ?? null,
    leadId: (r.lead_id as string | null) ?? null,
    brandId: (r.brand_id as string | null) ?? null,
    campaignId: (r.campaign_id as string | null) ?? null,
    runId: (r.conversation_run_id as string | null) ?? null,
  }));
}

async function mark(id: string, outcome: DeclarationMark): Promise<void> {
  await db.execute(sql`
    UPDATE instantly_campaigns SET conversation_declared_at = now(), conversation_declaration = ${outcome}
    WHERE id = ${id} AND conversation_declared_at IS NULL
  `);
}

/** The sequence's run: the persisted one, else a new one, persisted before any declaration. */
async function ensureRun(row: PendingPositiveReply): Promise<string> {
  if (row.runId) return row.runId;
  const identity = { orgId: row.orgId, userId: row.userId ?? "" };
  const run = await createRun(
    { serviceName: "instantly-service", taskName: POSITIVE_REPLY_TASK, brandId: row.brandId, campaignId: row.campaignId },
    identity,
  );
  const result = await db.execute(sql`
    UPDATE instantly_campaigns SET conversation_run_id = ${run.id}
    WHERE id = ${row.id} AND conversation_run_id IS NULL
    RETURNING conversation_run_id
  `);
  if ((result.rows as unknown[]).length === 0) {
    // A concurrent pass persisted another run first: close ours, use theirs.
    await updateRun(run.id, "completed", identity).catch(() => {});
    const [existing] = (await db.execute(sql`SELECT conversation_run_id FROM instantly_campaigns WHERE id = ${row.id}`)).rows as Array<Record<string, unknown>>;
    return String(existing.conversation_run_id);
  }
  await updateRun(run.id, "completed", identity);
  return run.id;
}

export interface PositiveReplySummary {
  read: number;
  declared: number;
  runGone: number;
  noLeadId: number;
  refused: number;
  retry: number;
}

async function declareOne(row: PendingPositiveReply, summary: PositiveReplySummary): Promise<void> {
  if (!row.leadId) {
    await mark(row.id, "no_lead_id");
    summary.noLeadId++;
    console.error(`[positive-reply-outcomes] sequence ${row.id}: no lead id held, conversation NOT declared`);
    return;
  }
  let runId: string;
  let answer: { status: number; body: string };
  try {
    runId = await ensureRun(row);
    answer = await postRunOutcomes(runId, row.orgId, buildPositiveReplyDeclaration(row.leadId));
  } catch (error: unknown) {
    summary.retry++;
    console.error(`[positive-reply-outcomes] sequence ${row.id} declaration failed (retried next tick): ${errorMessage(error)}`);
    return;
  }
  const verdict = classifyDeclareAnswer(answer.status, answer.body);
  if (verdict === "retry") {
    summary.retry++;
    console.error(`[positive-reply-outcomes] run ${runId} declaration ${answer.status} (retried next tick): ${answer.body.slice(0, 300)}`);
    return;
  }
  if (verdict !== "declared") {
    console.error(`[positive-reply-outcomes] run ${runId} declaration ${answer.status} -> ${verdict}: ${answer.body.slice(0, 300)}`);
  }
  await mark(row.id, verdict);
  if (verdict === "declared") summary.declared++;
  else if (verdict === "run_gone") summary.runGone++;
  else summary.refused++;
}

let inFlight: Promise<PositiveReplySummary> | null = null;

/** One sweep pass, sequential (tens of rows a day). Concurrent calls join it. */
export function declarePositiveReplies(limit = POSITIVE_REPLY_BATCH_LIMIT): Promise<PositiveReplySummary> {
  if (inFlight) return inFlight;
  inFlight = (async () => {
    const rows = await loadPending(limit);
    const summary: PositiveReplySummary = { read: rows.length, declared: 0, runGone: 0, noLeadId: 0, refused: 0, retry: 0 };
    for (const row of rows) await declareOne(row, summary);
    if (rows.length > 0) console.log(`[positive-reply-outcomes] pass done ${JSON.stringify(summary)}`);
    return summary;
  })().finally(() => {
    inFlight = null;
  });
  return inFlight;
}
