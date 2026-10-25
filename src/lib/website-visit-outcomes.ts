/**
 * LEDGER-HISTORY-1024: a click on a link in one of our emails is a WEBSITE
 * VISIT (CLICK-FIRES-WEBSITE-VISIT-1015), the catalogue outcome `website_visit`
 * (features-service `GET /public/catalogue/steps`). This declares it to
 * runs-service's ledger, today and in the past, the same way
 * lib/send-outcomes.ts declares every `message_sent`.
 *
 * Grain: one sequence (`instantly_campaigns` row = one person x one thread)
 * with at least one REAL click = ONE `website_visit` occurrence of the person,
 * which CONSUMES the person's `message_sent`. Subject = the person's lead in
 * lead-service, as everywhere in the chain. A real click = a measured,
 * non-withdrawn `email_link_clicked` (`inferred = false`): the same clicks that
 * fire the website-visit trigger and stop-on-click; a self-send hit judged a
 * scanner is never promoted, so it is never one.
 *
 * The run: the org's spend-free `website-visit` anchor run of the click's UTC
 * day (lib/outcome-anchor-run.ts); the occurrence is DATED at the first real
 * click, so a June visit counts on its June day. Two sequences of the same lead
 * clicked the same day fold into one occurrence on that run (runs-service is
 * idempotent per run x outcome x subject).
 *
 * Once per sequence: the run id is persisted BEFORE the declaration (a retry
 * reuses it), the row marked afterwards. Never on the click path: a sweep
 * (send-outcomes-worker) does it; a failure leaves the row undeclared and the
 * next tick retries. A row with no lead id is marked `no_lead_id`, never
 * declared under an invented subject.
 */
import { sql } from "drizzle-orm";
import { db } from "../db";
import { errorMessage } from "./db-error.js";
import { outcomeAnchorRun } from "./outcome-anchor-run";
import {
  SEND_OUTCOME_SUBJECT_SERVICE,
  SEND_PRODUCED_OUTCOME,
  classifyDeclareAnswer,
  postRunOutcomes,
  type DeclarationMark,
  type SendOutcomeDeclaration,
} from "./send-outcomes";
import { DECLARED_NO_SEND, sendInputStates } from "./send-input";

export const WEBSITE_VISIT_OUTCOME = "website_visit";
export const WEBSITE_VISIT_TASK = "website-visit";
export const WEBSITE_VISIT_BATCH_LIMIT = 200;

/**
 * What a sequence's website visit declares about the person. `consumeSend`
 * false = no send of ours to this lead can ever be placed in the ledger
 * (lib/send-input.ts): the visit is declared consuming nothing.
 */
export function buildWebsiteVisitDeclaration(leadId: string, consumeSend = true): SendOutcomeDeclaration {
  const subject = { subjectService: SEND_OUTCOME_SUBJECT_SERVICE, subjectId: leadId };
  return {
    produced: [{ outcomeKey: WEBSITE_VISIT_OUTCOME, ...subject }],
    consumed: consumeSend ? [{ outcomeKey: SEND_PRODUCED_OUTCOME, ...subject }] : [],
  };
}

export interface PendingWebsiteVisit {
  id: string;
  orgId: string;
  leadId: string | null;
  runId: string | null;
  /** ISO instant of the sequence's first real click. */
  visitedAt: string;
}

/**
 * Sequences with a real click not yet declared, oldest visit first. Driven from
 * the clicks (`event_type` index, a few thousand rows), never a scan of every
 * sequence. The event column holds UTC without a zone: read as an ISO instant.
 */
async function loadPending(limit: number): Promise<PendingWebsiteVisit[]> {
  const result = await db.execute(sql`
    SELECT c.id, c.org_id, c.lead_id, c.website_visit_run_id,
      to_char(v.visited_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS visited_at
    FROM (
      SELECT e.campaign_id, e.lead_email, min(e."timestamp") AS visited_at
      FROM instantly_events e
      WHERE e.event_type = 'email_link_clicked' AND e.inferred = false AND e.withdrawn_at IS NULL
      GROUP BY e.campaign_id, e.lead_email
    ) v
    JOIN instantly_campaigns c ON c.instantly_campaign_id = v.campaign_id AND lower(c.lead_email) = lower(v.lead_email)
    WHERE c.website_visit_declared_at IS NULL
    ORDER BY v.visited_at, c.id
    LIMIT ${limit}
  `);
  return (result.rows as Array<Record<string, unknown>>).map((r) => ({
    id: String(r.id),
    orgId: String(r.org_id),
    leadId: (r.lead_id as string | null) ?? null,
    runId: (r.website_visit_run_id as string | null) ?? null,
    visitedAt: String(r.visited_at),
  }));
}

async function mark(id: string, outcome: DeclarationMark | typeof DECLARED_NO_SEND): Promise<void> {
  await db.execute(sql`
    UPDATE instantly_campaigns SET website_visit_declared_at = now(), website_visit_declaration = ${outcome}
    WHERE id = ${id} AND website_visit_declared_at IS NULL
  `);
}

/** The persisted run, else the org's anchor run of the visit's UTC day, persisted before any declaration. */
async function ensureRun(row: PendingWebsiteVisit): Promise<string> {
  if (row.runId) return row.runId;
  const runId = await outcomeAnchorRun(WEBSITE_VISIT_TASK, row.orgId, row.visitedAt.slice(0, 10));
  const result = await db.execute(sql`
    UPDATE instantly_campaigns SET website_visit_run_id = ${runId}
    WHERE id = ${row.id} AND website_visit_run_id IS NULL
    RETURNING website_visit_run_id
  `);
  if ((result.rows as unknown[]).length === 0) {
    const [existing] = (await db.execute(sql`SELECT website_visit_run_id FROM instantly_campaigns WHERE id = ${row.id}`))
      .rows as Array<Record<string, unknown>>;
    return String(existing.website_visit_run_id);
  }
  return runId;
}

export interface WebsiteVisitSummary {
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

async function declareOne(row: PendingWebsiteVisit, summary: WebsiteVisitSummary, sendInput: Map<string, string>): Promise<void> {
  if (!row.leadId) {
    await mark(row.id, "no_lead_id");
    summary.noLeadId++;
    console.error(`[website-visit-outcomes] sequence ${row.id}: no lead id held, website_visit NOT declared`);
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
    answer = await postRunOutcomes(runId, row.orgId, buildWebsiteVisitDeclaration(row.leadId, consumeSend), row.visitedAt);
  } catch (error: unknown) {
    summary.retry++;
    console.error(`[website-visit-outcomes] sequence ${row.id} declaration failed (retried next tick): ${errorMessage(error)}`);
    return;
  }
  const verdict = classifyDeclareAnswer(answer.status, answer.body);
  if (verdict === "retry") {
    summary.retry++;
    console.error(`[website-visit-outcomes] run ${runId} declaration ${answer.status} (retried next tick): ${answer.body.slice(0, 300)}`);
    return;
  }
  if (verdict !== "declared") {
    console.error(`[website-visit-outcomes] run ${runId} declaration ${answer.status} -> ${verdict}: ${answer.body.slice(0, 300)}`);
  }
  await mark(row.id, verdict === "declared" && !consumeSend ? DECLARED_NO_SEND : verdict);
  if (verdict === "declared") {
    summary.declared++;
    if (!consumeSend) summary.declaredNoSend++;
  }
  else if (verdict === "run_gone") summary.runGone++;
  else summary.refused++;
}

let inFlight: Promise<WebsiteVisitSummary> | null = null;

/** One sweep pass, sequential (tens of clicks a day; the past drains in a few passes). Concurrent calls join it. */
export function declareWebsiteVisits(limit = WEBSITE_VISIT_BATCH_LIMIT): Promise<WebsiteVisitSummary> {
  if (inFlight) return inFlight;
  inFlight = (async () => {
    const rows = await loadPending(limit);
    const summary: WebsiteVisitSummary = { read: rows.length, declared: 0, runGone: 0, noLeadId: 0, refused: 0, retry: 0, declaredNoSend: 0, waitingForSend: 0 };
    const sendInput = await sendInputStates(rows.flatMap((r) => (r.leadId ? [r.leadId] : [])));
    for (const row of rows) await declareOne(row, summary, sendInput);
    if (rows.length > 0) console.log(`[website-visit-outcomes] pass done ${JSON.stringify(summary)}`);
    return summary;
  })().finally(() => {
    inFlight = null;
  });
  return inFlight;
}
