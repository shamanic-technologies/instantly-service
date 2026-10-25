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
 * of the person, which CONSUMES the person's `message_sent`. Subject = the
 * person's lead in lead-service, as everywhere in the chain.
 *
 * The run that produces it is the org's `reply-judging` run of the day
 * (lib/reply-judging-run.ts): every classifier call of that org that day nests
 * its chat-service platform run under it, so the judging spend (non-positive
 * judgments included, normal loss) is split over the day's positive replies.
 * Sequences declared before 2026-10-23 15:00 UTC sit on a per-sequence
 * `reply-judged-positive` run (no cost) and keep it.
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
import { pipeOfRow, replyJudgingRunFor, type ReplyJudgingPipe } from "./reply-judging-run";
import { DECLARED_NO_SEND, sendInputStates } from "./send-input";
import { LEGACY_DEAL_PROGRESS_STATUSES, REPLY_KIND_CLASSIFICATION } from "./reply-kind";
import {
  SEND_OUTCOME_SUBJECT_SERVICE,
  SEND_PRODUCED_OUTCOME,
  classifyDeclareAnswer,
  postOutcomesBatch,
  postRunOutcomes,
  readBatchAnswer,
  type DeclarationMark,
  type SendOutcomeDeclaration,
} from "./send-outcomes";

export const POSITIVE_REPLY_OUTCOME = "conversation";
export const POSITIVE_REPLY_BATCH_LIMIT = 100;

/**
 * What a sequence's positive reply declares about the person. `consumeSend`
 * false = no send of ours to this lead can ever be placed in the ledger
 * (lib/send-input.ts): the reply is declared consuming nothing.
 */
export function buildPositiveReplyDeclaration(leadId: string, consumeSend = true): SendOutcomeDeclaration {
  const subject = { subjectService: SEND_OUTCOME_SUBJECT_SERVICE, subjectId: leadId };
  return {
    produced: [{ outcomeKey: POSITIVE_REPLY_OUTCOME, ...subject }],
    consumed: consumeSend ? [{ outcomeKey: SEND_PRODUCED_OUTCOME, ...subject }] : [],
  };
}

interface PendingPositiveReply {
  id: string;
  orgId: string;
  userId: string | null;
  leadId: string | null;
  brandId: string | null;
  campaignId: string | null;
  pipe: ReplyJudgingPipe;
  runId: string | null;
}

async function loadPending(limit: number): Promise<PendingPositiveReply[]> {
  const result = await db.execute(sql`
    SELECT id, org_id, user_id, lead_id, brand_ids[1] AS brand_id, campaign_id, conversation_run_id,
      workflow_slug, feature_slug, to_jsonb(brand_ids) AS brand_ids
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
    pipe: pipeOfRow(r),
    runId: (r.conversation_run_id as string | null) ?? null,
  }));
}

async function mark(id: string, outcome: DeclarationMark | typeof DECLARED_NO_SEND): Promise<void> {
  await db.execute(sql`
    UPDATE instantly_campaigns SET conversation_declared_at = now(), conversation_declaration = ${outcome}
    WHERE id = ${id} AND conversation_declared_at IS NULL
  `);
}

/**
 * The sequence's run: the persisted one, else the org's `reply-judging` run of
 * the day on the sequence's pipe (the run the classifier's spend nests under),
 * persisted before any declaration so a retry reuses it.
 */
async function ensureRun(row: PendingPositiveReply): Promise<string> {
  if (row.runId) return row.runId;
  const runId = await replyJudgingRunFor(row.orgId, row.pipe);
  const result = await db.execute(sql`
    UPDATE instantly_campaigns SET conversation_run_id = ${runId}
    WHERE id = ${row.id} AND conversation_run_id IS NULL
    RETURNING conversation_run_id
  `);
  if ((result.rows as unknown[]).length === 0) {
    const [existing] = (await db.execute(sql`SELECT conversation_run_id FROM instantly_campaigns WHERE id = ${row.id}`)).rows as Array<Record<string, unknown>>;
    return String(existing.conversation_run_id);
  }
  return runId;
}

export interface PositiveReplySummary {
  read: number;
  declared: number;
  runGone: number;
  noLeadId: number;
  refused: number;
  retry: number;
  /** Declared consuming nothing: no send of ours to the lead can be placed. */
  declaredNoSend: number;
  /** A send of ours to the lead is not declared yet: next tick. */
  waitingForSend: number;
}

async function declareOne(row: PendingPositiveReply, summary: PositiveReplySummary, sendInput: Map<string, string>): Promise<void> {
  if (!row.leadId) {
    await mark(row.id, "no_lead_id");
    summary.noLeadId++;
    console.error(`[positive-reply-outcomes] sequence ${row.id}: no lead id held, conversation NOT declared`);
    return;
  }
  const input = sendInput.get(row.leadId);
  if (input === "pending") {
    summary.waitingForSend++;
    return;
  }
  const consumeSend = input === "declared";
  let runId: string;
  let answer: { status: number; body: string };
  try {
    runId = await ensureRun(row);
    answer = await postRunOutcomes(runId, row.orgId, buildPositiveReplyDeclaration(row.leadId, consumeSend));
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
  await mark(row.id, verdict === "declared" && !consumeSend ? DECLARED_NO_SEND : verdict);
  if (verdict === "declared") {
    summary.declared++;
    if (!consumeSend) summary.declaredNoSend++;
  }
  else if (verdict === "run_gone") summary.runGone++;
  else summary.refused++;
}

let inFlight: Promise<PositiveReplySummary> | null = null;

/** One sweep pass, sequential (tens of rows a day). Concurrent calls join it. */
export function declarePositiveReplies(limit = POSITIVE_REPLY_BATCH_LIMIT): Promise<PositiveReplySummary> {
  if (inFlight) return inFlight;
  inFlight = (async () => {
    const rows = await loadPending(limit);
    const summary: PositiveReplySummary = { read: rows.length, declared: 0, runGone: 0, noLeadId: 0, refused: 0, retry: 0, declaredNoSend: 0, waitingForSend: 0 };
    const sendInput = await sendInputStates(rows.flatMap((r) => (r.leadId ? [r.leadId] : [])));
    for (const row of rows) await declareOne(row, summary, sendInput);
    if (rows.length > 0) console.log(`[positive-reply-outcomes] pass done ${JSON.stringify(summary)}`);
    return summary;
  })().finally(() => {
    inFlight = null;
  });
  return inFlight;
}

/** The silver event types whose classification is `positive` (+ the legacy deal-progress spellings, resolved to `lead_interested`). */
export const POSITIVE_REPLY_EVENT_TYPES: readonly string[] = [
  ...Object.entries(REPLY_KIND_CLASSIFICATION)
    .filter(([, classification]) => classification === "positive")
    .map(([kind]) => kind),
  ...LEGACY_DEAL_PROGRESS_STATUSES,
];

export interface ConversationRedateSummary {
  /** Declared sequences on a spend-free per-sequence `reply-judged-positive` run. */
  read: number;
  /** Runs re-stated dated at the positive reply (runs-service moves an occurrence only earlier). */
  restated: number;
  /** Occurrences runs-service actually moved back. */
  redated: number;
  /** No witnessed reply on the sequence: left at its declaration time, never an invented date. */
  noReplyEvidence: number;
}

/**
 * LEDGER-HISTORY-1024: the positive replies declared before the dated door
 * (2026-10-23, on a spend-free per-sequence `reply-judged-positive` run) sit
 * on their declaration day. Re-state each dated at the reply itself: the
 * earliest positive-kind event of the sequence, else its earliest witnessed
 * reply. A run of the daily `reply-judging` table carries the judging spend
 * (an occurrence there cannot predate the run) and is left as is. Idempotent:
 * runs-service only moves an occurrence earlier; a re-run moves nothing.
 */
export async function redatePastPositiveReplies(opts: { dryRun: boolean }): Promise<ConversationRedateSummary> {
  const result = await db.execute(sql`
    SELECT c.conversation_run_id AS run_id, c.lead_id,
      to_char(COALESCE(p.at, r.at), 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS replied_at
    FROM instantly_campaigns c
    LEFT JOIN LATERAL (
      SELECT min(e."timestamp") AS at FROM instantly_events e
      WHERE e.campaign_id = c.instantly_campaign_id AND lower(e.lead_email) = lower(c.lead_email)
        AND e.event_type = ANY(${`{${POSITIVE_REPLY_EVENT_TYPES.join(",")}}`}::text[])
        AND e.inferred = false AND e.withdrawn_at IS NULL
    ) p ON true
    LEFT JOIN LATERAL (
      SELECT min(e."timestamp") AS at FROM instantly_events e
      WHERE e.campaign_id = c.instantly_campaign_id AND lower(e.lead_email) = lower(c.lead_email)
        AND e.event_type = 'reply_received' AND e.inferred = false AND e.withdrawn_at IS NULL
    ) r ON true
    WHERE c.conversation_declaration = 'declared' AND c.lead_id IS NOT NULL AND c.conversation_run_id IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM reply_judging_runs j WHERE j.run_id = c.conversation_run_id)
    ORDER BY c.id
  `);
  const rows = (result.rows as Array<Record<string, unknown>>).map((r) => ({
    runId: String(r.run_id),
    leadId: String(r.lead_id),
    repliedAt: (r.replied_at as string | null) ?? null,
  }));
  const summary: ConversationRedateSummary = { read: rows.length, restated: 0, redated: 0, noReplyEvidence: 0 };
  const dated = rows.filter((r) => r.repliedAt !== null);
  summary.noReplyEvidence = rows.length - dated.length;
  if (opts.dryRun) {
    summary.restated = dated.length;
    return summary;
  }
  for (let i = 0; i < dated.length; i += 300) {
    const batch = dated.slice(i, i + 300);
    const posted = await postOutcomesBatch(
      batch.map((r) => ({ runId: r.runId, ...buildPositiveReplyDeclaration(r.leadId), occurredAt: r.repliedAt!, redate: true as const })),
    );
    const answer = readBatchAnswer(posted.status, posted.body);
    if (!answer.ok) throw new Error(`runs-service refused the conversation re-date: ${JSON.stringify(answer.invalid).slice(0, 500)}`);
    summary.restated += batch.length;
    summary.redated += Number((JSON.parse(posted.body) as { redatedCount?: unknown }).redatedCount ?? 0);
  }
  console.log(`[positive-reply-outcomes] past positive replies re-dated ${JSON.stringify(summary)}`);
  return summary;
}
