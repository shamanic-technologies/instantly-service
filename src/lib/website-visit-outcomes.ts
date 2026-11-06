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
 *
 * WHO performed it (OUTCOME-PERFORMER-1106): the anchor run carries no
 * workflow, so every occurrence states its performer on `produced[]`: the
 * script whose email was clicked (`script` + the sequence's own
 * `workflow_slug`, the slug its send runs carry), else (a sequence an Engine
 * sent, no workflow on its row: never a guessed script) the tracker that
 * measured the click (`tracker`, named by the transport that carried the
 * email). Stored by runs-service once per occurrence; a sequence's mark
 * `website_visit_performer` is set only when runs-service's answer PROVES it
 * stored it (`performerStatedCount` served), so the completion pass
 * (`completeWebsiteVisitPerformers`, same worker) states it on every visit
 * declared before, or declared while runs-service did not know the field yet.
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
  type OutcomePerformer,
  type SendOutcomeDeclaration,
} from "./send-outcomes";
import { DECLARED_NO_SEND, sendInputStates } from "./send-input";
import { SEND_TRANSPORT_SMTP } from "./self-send/transport";

export const WEBSITE_VISIT_OUTCOME = "website_visit";
export const WEBSITE_VISIT_TASK = "website-visit";
export const WEBSITE_VISIT_BATCH_LIMIT = 200;

/**
 * What a sequence's website visit declares about the person. `consumeSend`
 * false = no send of ours to this lead can ever be placed in the ledger
 * (lib/send-input.ts): the visit is declared consuming nothing.
 */
export function buildWebsiteVisitDeclaration(
  leadId: string,
  consumeSend = true,
  performer?: OutcomePerformer,
): SendOutcomeDeclaration {
  const subject = { subjectService: SEND_OUTCOME_SUBJECT_SERVICE, subjectId: leadId };
  return {
    produced: [{ outcomeKey: WEBSITE_VISIT_OUTCOME, ...subject, ...(performer ? { performer } : {}) }],
    consumed: consumeSend ? [{ outcomeKey: SEND_PRODUCED_OUTCOME, ...subject }] : [],
  };
}

/** The tool named when no script can be read: the click tracker of the transport that carried the email. */
export const WEBSITE_VISIT_TRACKER_NAMES = { instantly: "instantly", smtp: "distribute.you" } as const;

/**
 * Who performed a sequence's website visit: our script whose email was clicked (the sequence's own workflow slug),
 * else the tracker that measured the click. Never a guessed script.
 */
export function websiteVisitPerformer(row: { workflowSlug: string | null; sendTransport: string | null }): OutcomePerformer {
  const slug = row.workflowSlug?.trim();
  if (slug) return { kind: "script", workflowSlug: slug };
  return {
    kind: "tracker",
    name: row.sendTransport === SEND_TRANSPORT_SMTP ? WEBSITE_VISIT_TRACKER_NAMES.smtp : WEBSITE_VISIT_TRACKER_NAMES.instantly,
  };
}

/** runs-service served `performerStatedCount`: it read and stored the performer (an older build drops the field silently). */
export function answerProvesPerformer(status: number, body: string): boolean {
  return status >= 200 && status < 300 && body.includes('"performerStatedCount"');
}

/** runs-service refused the performer because the occurrence already holds another one (never overwritten). */
export function isPerformerConflict(status: number, body: string): boolean {
  return status === 409 && body.includes("performer_conflict");
}

/** `website_visit_performer` marks: stated (stored at runs-service), conflict (another one stored first), run_gone, refused. */
export type PerformerMark = "stated" | "conflict" | "run_gone" | "refused";

export interface PendingWebsiteVisit {
  id: string;
  orgId: string;
  leadId: string | null;
  runId: string | null;
  /** ISO instant of the sequence's first real click. */
  visitedAt: string;
  workflowSlug: string | null;
  sendTransport: string | null;
  /** Mark already held (`declared` / `declared_no_send` on the completion pass), null on the declaration pass. */
  declaration: string | null;
}

/**
 * Sequences with a real click not yet declared, oldest visit first. Driven from
 * the clicks (`event_type` index, a few thousand rows), never a scan of every
 * sequence. The event column holds UTC without a zone: read as an ISO instant.
 */
async function loadPending(limit: number, mode: "declare" | "performer" = "declare"): Promise<PendingWebsiteVisit[]> {
  const scope =
    mode === "declare"
      ? sql`c.website_visit_declared_at IS NULL`
      : sql`c.website_visit_declaration IN ('declared', ${DECLARED_NO_SEND}) AND c.website_visit_run_id IS NOT NULL
          AND c.lead_id IS NOT NULL AND c.website_visit_performer IS NULL`;
  const result = await db.execute(sql`
    SELECT c.id, c.org_id, c.lead_id, c.website_visit_run_id, c.workflow_slug, c.send_transport, c.website_visit_declaration,
      to_char(v.visited_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS visited_at
    FROM (
      SELECT e.campaign_id, e.lead_email, min(e."timestamp") AS visited_at
      FROM instantly_events e
      WHERE e.event_type = 'email_link_clicked' AND e.inferred = false AND e.withdrawn_at IS NULL
      GROUP BY e.campaign_id, e.lead_email
    ) v
    JOIN instantly_campaigns c ON c.instantly_campaign_id = v.campaign_id AND lower(c.lead_email) = lower(v.lead_email)
    WHERE ${scope}
    ORDER BY v.visited_at, c.id
    LIMIT ${limit}
  `);
  return (result.rows as Array<Record<string, unknown>>).map((r) => ({
    id: String(r.id),
    orgId: String(r.org_id),
    leadId: (r.lead_id as string | null) ?? null,
    runId: (r.website_visit_run_id as string | null) ?? null,
    visitedAt: String(r.visited_at),
    workflowSlug: (r.workflow_slug as string | null) ?? null,
    sendTransport: (r.send_transport as string | null) ?? null,
    declaration: (r.website_visit_declaration as string | null) ?? null,
  }));
}

async function markPerformer(id: string, outcome: PerformerMark): Promise<void> {
  await db.execute(sql`
    UPDATE instantly_campaigns SET website_visit_performer = ${outcome}, website_visit_performer_at = now()
    WHERE id = ${id} AND website_visit_performer IS NULL
  `);
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
  /** runs-service proved it stored the performer. */
  performerStated: number;
  /** The occurrence already held another performer: declared without it (the first one stands). */
  performerConflict: number;
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
  const performer = websiteVisitPerformer(row);
  let runId: string;
  let answer: { status: number; body: string };
  let conflict = false;
  try {
    runId = await ensureRun(row);
    answer = await postRunOutcomes(runId, row.orgId, buildWebsiteVisitDeclaration(row.leadId, consumeSend, performer), row.visitedAt);
    if (isPerformerConflict(answer.status, answer.body)) {
      // Two sequences of one lead clicked the same day fold into one occurrence on this run: the first performer
      // stands, this sequence's consumption is still declared.
      conflict = true;
      answer = await postRunOutcomes(runId, row.orgId, buildWebsiteVisitDeclaration(row.leadId, consumeSend), row.visitedAt);
    }
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
  if (verdict === "declared" && conflict) {
    await markPerformer(row.id, "conflict");
    summary.performerConflict++;
  } else if (verdict === "declared" && answerProvesPerformer(answer.status, answer.body)) {
    await markPerformer(row.id, "stated");
    summary.performerStated++;
  }
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
    const summary: WebsiteVisitSummary = {
      read: rows.length, declared: 0, runGone: 0, noLeadId: 0, refused: 0, retry: 0, declaredNoSend: 0, waitingForSend: 0,
      performerStated: 0, performerConflict: 0,
    };
    const sendInput = await sendInputStates(rows.flatMap((r) => (r.leadId ? [r.leadId] : [])));
    for (const row of rows) await declareOne(row, summary, sendInput);
    if (rows.length > 0) console.log(`[website-visit-outcomes] pass done ${JSON.stringify(summary)}`);
    return summary;
  })().finally(() => {
    inFlight = null;
  });
  return inFlight;
}

export interface PerformerCompletionSummary {
  dryRun: boolean;
  read: number;
  /** Planned performers (dry run and commit): `script:<slug>` / `tracker:<name>` -> occurrences. */
  byPerformer: Record<string, number>;
  stated: number;
  conflict: number;
  runGone: number;
  refused: number;
  retry: number;
  /** runs-service answered without `performerStatedCount`: it does not read the field yet, the pass stops. */
  notServedYet: number;
}

const performerLabel = (p: OutcomePerformer): string => (p.kind === "script" ? `script:${p.workflowSlug}` : `${p.kind}:${p.name ?? ""}`);

/**
 * OUTCOME-PERFORMER-1106: every website visit declared before its performer was stated (or while runs-service did not
 * read the field yet) states it once, on the occurrence already recorded (runs-service: `performer_stated`, a replay =
 * `already_recorded`, another one stored = 409 `performer_conflict`, never overwritten). Re-posts the PRODUCED item
 * only, same run, same instant: nothing else moves (no consumption, no cost). Runs itself on the worker until no row
 * is left; `dryRun` plans and counts, writes nothing.
 */
export async function completeWebsiteVisitPerformers(
  opts: { dryRun: boolean; limit?: number } = { dryRun: false },
): Promise<PerformerCompletionSummary> {
  const rows = await loadPending(opts.limit ?? WEBSITE_VISIT_BATCH_LIMIT, "performer");
  const summary: PerformerCompletionSummary = {
    dryRun: opts.dryRun, read: rows.length, byPerformer: {}, stated: 0, conflict: 0, runGone: 0, refused: 0, retry: 0, notServedYet: 0,
  };
  for (const row of rows) {
    const performer = websiteVisitPerformer(row);
    const label = performerLabel(performer);
    summary.byPerformer[label] = (summary.byPerformer[label] ?? 0) + 1;
    if (opts.dryRun) continue;
    const declaration = { produced: buildWebsiteVisitDeclaration(row.leadId as string, false, performer).produced, consumed: [] };
    let answer: { status: number; body: string };
    try {
      answer = await postRunOutcomes(row.runId as string, row.orgId, declaration, row.visitedAt);
    } catch (error: unknown) {
      summary.retry++;
      console.error(`[website-visit-performers] sequence ${row.id} failed (retried next tick): ${errorMessage(error)}`);
      continue;
    }
    if (answerProvesPerformer(answer.status, answer.body)) {
      await markPerformer(row.id, "stated");
      summary.stated++;
      continue;
    }
    if (isPerformerConflict(answer.status, answer.body)) {
      await markPerformer(row.id, "conflict");
      summary.conflict++;
      console.error(`[website-visit-performers] run ${row.runId}: another performer already stored: ${answer.body.slice(0, 300)}`);
      continue;
    }
    if (answer.status >= 200 && answer.status < 300) {
      summary.notServedYet++;
      console.error(`[website-visit-performers] runs-service does not read performer yet (no performerStatedCount): pass stopped`);
      break;
    }
    const verdict = classifyDeclareAnswer(answer.status, answer.body);
    if (verdict === "retry") {
      summary.retry++;
      console.error(`[website-visit-performers] run ${row.runId} ${answer.status} (retried next tick): ${answer.body.slice(0, 300)}`);
      continue;
    }
    await markPerformer(row.id, verdict === "run_gone" ? "run_gone" : "refused");
    if (verdict === "run_gone") summary.runGone++;
    else summary.refused++;
    console.error(`[website-visit-performers] run ${row.runId} ${answer.status} -> ${verdict}: ${answer.body.slice(0, 300)}`);
  }
  if (rows.length > 0) console.log(`[website-visit-performers] pass done ${JSON.stringify(summary)}`);
  return summary;
}
