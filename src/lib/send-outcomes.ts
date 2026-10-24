/**
 * COST-ACCOUNTING-1023 (owner 2026-10-23): every email we send is declared to
 * runs-service's general ledger as the outcome its run PRODUCED, so the send's
 * cost lands on an outcome instead of "produced nothing".
 *
 * Grain: one ACTUAL step hold (`sequence_costs.status='actual'`, a real
 * `email_sent` at that step) = one `email-send-step-N` run = ONE `message_sent`
 * occurrence of the person (each follow-up step is one more occurrence, on its
 * own run). A bounce is still `message_sent`: an email was sent. The run also
 * states what it CONSUMED: the person's `message_written` and `email_checked`.
 * Subject = the person's lead in lead-service (`instantly_campaigns.lead_id`),
 * the one subject every service of the chain names, so a consumption finds its
 * producer. A row whose lead id we do not hold is never declared under an
 * invented id: it is marked `no_lead_id` (or `ambiguous_lead`) and logged.
 *
 * Never on the send path: the email leaves whatever the ledger says. A sweep
 * (`declareSendOutcomes`, driven by send-outcomes-worker) declares every actual
 * hold not yet declared, newest first, and marks it; a transient failure leaves
 * it NULL and the next tick retries, so the ledger catches up. runs-service is
 * idempotent per (run, direction, outcome, subject): a replay answers
 * `already_recorded` and declares nothing twice.
 *
 * COST-BACKFILL-1023-INSTANTLY: every declaration states WHEN the email left
 * (`occurredAt` = the earliest non-withdrawn `email_sent` of that step), through
 * runs-service's batch door (`POST /v1/platform-runs/outcomes/batch`), so a send
 * of June declared today behaves in the ledger as declared in June (consumption
 * resolution, the day it counts on). Declaring without it dated 79,029 past
 * sends 2026-10-23 (the first hours of this worker). The past is swept by the
 * SAME path (`runSendOutcomesBackfill`): keyset over every undeclared hold,
 * cancelled ones included when their email really left (runs-service charged
 * the run; ~20k such runs from 2026-02 to 2026-10). A hold with no real
 * `email_sent` is never declared (a failure is never an outcome).
 */
import { sql } from "drizzle-orm";
import { db } from "../db";

/** The catalogue keys (features-service `GET /public/catalogue/steps`) a send run declares. */
export const SEND_PRODUCED_OUTCOME = "message_sent";
export const SEND_CONSUMED_OUTCOMES = ["message_written", "email_checked"] as const;
/**
 * LEDGER-HISTORY-1024: the ledger records what a send USED for the address,
 * read from OUR records, never from what another service declared so far. A
 * send used a verdict (`email_checked`) when our send door verified the address
 * before the email left (`email_send_verifications`, accepted), or when its
 * sequence was served after email checks began on served leads
 * (`EMAIL_CHECKS_BEGAN_AT`); otherwise it used the address the reveal found
 * (`email_found`). A verdict whose producer has not declared it yet stays an
 * unplaced `email_checked` (it names the missing producer), never re-stated.
 */
export const SEND_UNCHECKED_ADDRESS_OUTCOME = "email_found";
/** No email check ran on served leads before this instant (human-service + apollo-service, measured 2026-10-24). */
export const EMAIL_CHECKS_BEGAN_AT = "2026-09-24T18:12:00.000Z";

/** Did this send use a verdict on its address? From our own records only. Pure. */
export function sendUsedAVerdict(row: { sequenceCreatedAt: string | null; verifiedBeforeSend: boolean }): boolean {
  if (row.verifiedBeforeSend) return true;
  return row.sequenceCreatedAt !== null && Date.parse(row.sequenceCreatedAt) >= Date.parse(EMAIL_CHECKS_BEGAN_AT);
}
/** A person's occurrences are named by their lead in lead-service. */
export const SEND_OUTCOME_SUBJECT_SERVICE = "lead-service";

export const SEND_OUTCOMES_BATCH_LIMIT = 500;
const DECLARE_TIMEOUT_MS = 15_000;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export interface OutcomeOccurrence {
  outcomeKey: string;
  subjectService: string;
  subjectId: string;
}

export interface SendOutcomeDeclaration {
  produced: OutcomeOccurrence[];
  consumed: OutcomeOccurrence[];
}

/** What one sent step's run declares about the person it emailed (`checked` = a check of the address exists). */
export function buildSendOutcomeDeclaration(leadId: string, checked = true): SendOutcomeDeclaration {
  const subject = { subjectService: SEND_OUTCOME_SUBJECT_SERVICE, subjectId: leadId };
  return {
    produced: [{ outcomeKey: SEND_PRODUCED_OUTCOME, ...subject }],
    consumed: SEND_CONSUMED_OUTCOMES.map((outcomeKey) => ({
      outcomeKey: outcomeKey === "email_checked" && !checked ? SEND_UNCHECKED_ADDRESS_OUTCOME : outcomeKey,
      ...subject,
    })),
  };
}


/** Terminal marks stored in `sequence_costs.outcomes_declaration`. */
export type DeclarationMark = "declared" | "run_gone" | "no_lead_id" | "ambiguous_lead" | "refused";

/**
 * How a runs-service answer ends the row: a terminal mark, or `retry` (left
 * NULL, the next tick asks again). 2xx = recorded or already recorded. 404 `Run
 * not found` = the run is gone (retention), nothing can ever be declared on it. 409 / a 400 that
 * is not `unknown_outcome_key` = runs-service refuses THIS row for good (org
 * mismatch, invalid run id). `unknown_outcome_key`, 401, 5xx = our key or the
 * catalogue is wrong/unreadable right now: retry, logged loud.
 */
export function classifyDeclareAnswer(status: number, body: string): DeclarationMark | "retry" {
  if (status >= 200 && status < 300) return "declared";
  // Only runs-service's own "Run not found" means the run is gone; a bare
  // fall-through 404 (route not served, wrong URL) is retried, never marked.
  if (status === 404) return body.includes("Run not found") ? "run_gone" : "retry";
  if (status === 409) return "refused";
  if (status === 400) return body.includes("unknown_outcome_key") ? "retry" : "refused";
  return "retry";
}

export interface PendingSendOutcome {
  holdId: string;
  runId: string;
  step: number;
  orgId: string | null;
  leadId: string | null;
  matches: number;
  /** ISO instant the step's email left (earliest non-withdrawn `email_sent`), null = no evidence. */
  sentAt: string | null;
  status: string;
  /** When we received the sequence (`instantly_campaigns.created_at`, UTC). */
  sequenceCreatedAt: string | null;
  /** Our send door accepted a verdict on this address (org + address) before the email left. */
  verifiedBeforeSend: boolean;
}

/** Terminal marks a hold can get without a call (never an invented subject or date). */
export type LocalMark = "no_lead_id" | "ambiguous_lead" | "no_send_evidence";

/** What one hold becomes before any call: a local mark, or a run to declare at `sentAt`. */
export function classifyPendingSend(row: PendingSendOutcome): LocalMark | "declare" {
  if (row.matches > 1) return "ambiguous_lead";
  if (!row.leadId) return "no_lead_id";
  if (!row.sentAt) return "no_send_evidence";
  return "declare";
}

/**
 * Shared SELECT: each hold, its per-lead campaign row (by `instantly_campaign_id`,
 * or for a pre-0027 hold by the caller campaign + address, the same fallback as
 * `matchesHoldCampaign`; `matches` > 1 = ambiguous, never guessed), and the
 * instant its step's email left. The column holds UTC without a zone: read as
 * an ISO instant, never a local Date.
 */
const PENDING_SELECT = sql`
  SELECT sc.id AS hold_id, sc.run_id, sc.step, sc.status, m.org_id, m.lead_id, COALESCE(m.n, 0)::int AS matches,
    to_char(e.sent_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS sent_at,
    to_char(m.created_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS sequence_created_at,
    COALESCE(vf.used, false) AS verified_before_send
  FROM sequence_costs sc
  LEFT JOIN LATERAL (
    SELECT (array_agg(c.org_id))[1] AS org_id, (array_agg(c.lead_id))[1] AS lead_id,
      (array_agg(c.instantly_campaign_id))[1] AS icid, (array_agg(c.lead_email))[1] AS lead_email,
      (array_agg(c.created_at))[1] AS created_at, count(*) AS n
    FROM (
      -- Two index-served branches, never one OR (4.8 s vs 1.3 s on prod).
      SELECT c.org_id, c.lead_id, c.instantly_campaign_id, c.lead_email, c.created_at FROM instantly_campaigns c
      WHERE sc.instantly_campaign_id IS NOT NULL AND c.instantly_campaign_id = sc.instantly_campaign_id
      UNION ALL
      SELECT c.org_id, c.lead_id, c.instantly_campaign_id, c.lead_email, c.created_at FROM instantly_campaigns c
      WHERE sc.instantly_campaign_id IS NULL AND c.campaign_id = sc.campaign_id
        AND lower(c.lead_email) = lower(sc.lead_email)
    ) c
  ) m ON true
  LEFT JOIN LATERAL (
    SELECT min(ev."timestamp") AS sent_at FROM instantly_events ev
    WHERE m.n = 1 AND ev.campaign_id = m.icid AND ev.lead_email = m.lead_email
      AND ev.event_type = 'email_sent' AND ev.step = sc.step AND ev.withdrawn_at IS NULL
  ) e ON true
  LEFT JOIN LATERAL (
    -- Served by (org_id, email, created_at); the event column is UTC without a zone.
    SELECT true AS used FROM email_send_verifications v
    WHERE m.n = 1 AND v.org_id = m.org_id AND v.email = lower(m.lead_email) AND v.accepted
      AND v.created_at <= COALESCE(e.sent_at AT TIME ZONE 'UTC', now())
    LIMIT 1
  ) vf ON true
`;

function toPending(rows: Array<Record<string, unknown>>): PendingSendOutcome[] {
  return rows.map((r) => ({
    holdId: String(r.hold_id),
    runId: String(r.run_id),
    step: Number(r.step),
    orgId: (r.org_id as string | null) ?? null,
    leadId: (r.lead_id as string | null) ?? null,
    matches: Number(r.matches),
    sentAt: (r.sent_at as string | null) ?? null,
    status: String(r.status),
    sequenceCreatedAt: (r.sequence_created_at as string | null) ?? null,
    verifiedBeforeSend: r.verified_before_send === true,
  }));
}

/** Live selector: actual holds not yet declared, newest send first. */
async function loadPending(limit: number): Promise<PendingSendOutcome[]> {
  const result = await db.execute(sql`
    ${PENDING_SELECT}
    WHERE sc.status = 'actual' AND sc.outcomes_declared_at IS NULL
    ORDER BY sc.updated_at DESC
    LIMIT ${limit}
  `);
  return toPending(result.rows as Array<Record<string, unknown>>);
}

/**
 * One email, one occurrence: a retried step sits on several runs (one cancelled
 * hold per attempt, then the attempt that sent). A cancelled hold speaks for its
 * step only when no other run holds that step ACTUAL and no newer cancelled hold
 * exists; prod 2026-10-23: 18k of the 20k charged cancelled-hold runs had such a
 * sibling. The losers' spend stays "produced none", never a second message_sent.
 */
const ONLY_HOLD_OF_ITS_STEP = sql`NOT EXISTS (
  SELECT 1 FROM sequence_costs o
  WHERE o.run_id <> sc.run_id AND o.step = sc.step
    AND (o.status = 'actual' OR (o.status = 'cancelled' AND o.created_at > sc.created_at))
    AND ((sc.instantly_campaign_id IS NOT NULL AND o.instantly_campaign_id = sc.instantly_campaign_id)
      OR (sc.instantly_campaign_id IS NULL AND o.campaign_id = sc.campaign_id AND o.lead_email = sc.lead_email))
)`;

/**
 * Backfill selector: every undeclared hold after `afterHoldId` (keyset on id),
 * actual ones, plus cancelled ones whose email really left and that are the
 * only hold of their step (`ONLY_HOLD_OF_ITS_STEP`). A cancelled hold with no
 * `email_sent` is skipped, never marked.
 */
export async function loadBackfillPage(
  afterHoldId: string,
  limit: number,
  redate = false,
): Promise<{ rows: PendingSendOutcome[]; lastHoldId: string | null }> {
  // Re-date mode reads the holds ALREADY declared (79,029 runs were declared on
  // 2026-10-23 without a date): re-stating them with `redate` moves each
  // occurrence back to when the email left; one already on time stays as is.
  const scope = redate
    ? sql`sc.outcomes_declaration = 'declared'`
    : sql`sc.outcomes_declared_at IS NULL AND (sc.status = 'actual' OR (sc.status = 'cancelled' AND ${ONLY_HOLD_OF_ITS_STEP}))`;
  const result = await db.execute(sql`
    SELECT * FROM (
      ${PENDING_SELECT}
      WHERE ${scope} AND sc.id > ${afterHoldId}
      ORDER BY sc.id
      LIMIT ${limit}
    ) page
  `);
  const all = toPending(result.rows as Array<Record<string, unknown>>);
  const lastHoldId = all.length > 0 ? all[all.length - 1].holdId : null;
  return { rows: all.filter((r) => r.status === "actual" || r.sentAt !== null), lastHoldId };
}

async function markHolds(holdIds: string[], outcome: DeclarationMark | LocalMark): Promise<void> {
  if (holdIds.length === 0) return;
  await db.execute(sql`
    UPDATE sequence_costs SET outcomes_declared_at = now(), outcomes_declaration = ${outcome}
    WHERE id = ANY(${`{${holdIds.join(",")}}`}::text[]) AND outcomes_declared_at IS NULL
  `);
}

/**
 * POST one declaration on one run (runs-service platform route; `x-org-id`
 * makes runs-service check the run is that org's), dated `occurredAt` when
 * given. Used by the positive-reply and website-visit sweeps. Returns the raw answer, never throws on an HTTP status.
 */
export async function postRunOutcomes(
  runId: string,
  orgId: string | null,
  declaration: SendOutcomeDeclaration,
  occurredAt?: string,
): Promise<{ status: number; body: string }> {
  const headers = runsHeaders();
  if (orgId && UUID_RE.test(orgId)) headers["x-org-id"] = orgId;
  const res = await fetch(`${runsUrl()}/v1/platform-runs/${encodeURIComponent(runId)}/outcomes`, {
    method: "POST",
    headers,
    body: JSON.stringify(occurredAt ? { ...declaration, occurredAt } : declaration),
    signal: AbortSignal.timeout(DECLARE_TIMEOUT_MS),
  });
  return { status: res.status, body: await res.text() };
}

const runsUrl = () => process.env.RUNS_SERVICE_URL || "http://localhost:3006";
function runsHeaders(): Record<string, string> {
  return {
    "Content-Type": "application/json",
    "x-api-key": process.env.RUNS_SERVICE_API_KEY || "",
    "x-service-name": "instantly-service",
  };
}

/** runs-service's literal for "at the run's start" (a send recorded a hair before its run opened). */
export const OCCURRED_AT_RUN_START = "run_started_at";
/** 3 occurrences per run, runs-service caps a batch at 1000 occurrences / 500 runs. */
export const DECLARE_BATCH_RUNS = 300;
const BATCH_TIMEOUT_MS = 120_000;

export interface BatchRun {
  runId: string;
  leadId: string;
  occurredAt: string;
  holdIds: string[];
  /** The send used a verdict on the address (consume `email_checked`), else `email_found`. */
  checked: boolean;
}

/**
 * One batch answer, read: `ok` = every run recorded or already recorded; `invalid`
 * = the 422 per-run list (nothing was written); anything else throws loud with
 * the status and body (the rows stay undeclared, the next pass retries).
 */
export function readBatchAnswer(status: number, body: string): { ok: true } | { ok: false; invalid: Array<{ runId: string; reason: string }> } {
  if (status === 200 || status === 201) return { ok: true };
  if (status === 422) {
    const parsed = JSON.parse(body) as { error?: string; details?: { invalid?: Array<{ runId: string; reason: string }> } };
    if (parsed.error === "invalid_runs" && Array.isArray(parsed.details?.invalid)) return { ok: false, invalid: parsed.details.invalid };
  }
  throw new Error(`runs-service outcomes batch ${status}: ${body.slice(0, 300)}`);
}

/** What a run refused by the batch becomes: a mark, or one more try dated at the run's start. */
export function markForInvalidRun(reason: string, occurredAt: string): DeclarationMark | "retry_at_run_start" {
  if (reason === "run_not_found") return "run_gone";
  if (reason === "invalid_occurred_at" && occurredAt !== OCCURRED_AT_RUN_START) return "retry_at_run_start";
  return "refused";
}

export interface BatchEntry extends SendOutcomeDeclaration {
  runId: string;
  occurredAt: string;
  redate?: true;
}

/** runs-service's batch door (all or nothing: a 422 `invalid_runs` writes nothing). Never throws on an HTTP status. */
export async function postOutcomesBatch(entries: BatchEntry[]): Promise<{ status: number; body: string }> {
  const res = await fetch(`${runsUrl()}/v1/platform-runs/outcomes/batch`, {
    method: "POST",
    headers: runsHeaders(),
    body: JSON.stringify({ runs: entries }),
    signal: AbortSignal.timeout(BATCH_TIMEOUT_MS),
  });
  return { status: res.status, body: await res.text() };
}

function postBatch(runs: BatchRun[], redate: boolean): Promise<{ status: number; body: string }> {
  return postOutcomesBatch(
    runs.map((r) => ({
      runId: r.runId,
      ...buildSendOutcomeDeclaration(r.leadId, r.checked),
      occurredAt: r.occurredAt,
      ...(redate ? { redate: true as const } : {}),
    })),
  );
}

export interface SendOutcomesSummary {
  read: number;
  declared: number;
  runGone: number;
  noLeadId: number;
  ambiguousLead: number;
  noSendEvidence: number;
  refused: number;
  /** Runs stated a second time at `run_started_at` (their email was stamped before the run opened). */
  datedAtRunStart: number;
  /** Occurrences runs-service moved back to the send time (re-date mode). */
  redated: number;
  /** Up to 5 holds of this pass NOT declared for want of a lead id or a sent email (logged). */
  undeclaredSample: string[];
  /** Runs whose send used no verdict on the address: they consume `email_found`. */
  unchecked: number;
}

export const emptySummary = (): SendOutcomesSummary => ({
  read: 0, declared: 0, runGone: 0, noLeadId: 0, ambiguousLead: 0, noSendEvidence: 0, refused: 0, datedAtRunStart: 0, redated: 0, undeclaredSample: [], unchecked: 0,
});

/**
 * Groups holds into runs (pre-0038 steps carry two holds on one run; a batch
 * states each run once), marks what can never be declared, and returns the runs
 * to declare. A run whose holds disagree on lead = ambiguous, never guessed.
 */
export function planRuns(rows: PendingSendOutcome[]): { runs: BatchRun[]; marks: Array<{ holdIds: string[]; mark: LocalMark }> } {
  const marks: Array<{ holdIds: string[]; mark: LocalMark }> = [];
  const byRun = new Map<string, BatchRun>();
  for (const row of rows) {
    const verdict = classifyPendingSend(row);
    if (verdict !== "declare") {
      marks.push({ holdIds: [row.holdId], mark: verdict });
      continue;
    }
    const existing = byRun.get(row.runId);
    if (!existing) {
      byRun.set(row.runId, { runId: row.runId, leadId: row.leadId!, occurredAt: row.sentAt!, holdIds: [row.holdId], checked: sendUsedAVerdict(row) });
    } else if (existing.leadId !== row.leadId) {
      byRun.delete(row.runId);
      marks.push({ holdIds: [...existing.holdIds, row.holdId], mark: "ambiguous_lead" });
    } else {
      existing.holdIds.push(row.holdId);
      existing.checked = existing.checked || sendUsedAVerdict(row);
      if (row.sentAt! < existing.occurredAt) existing.occurredAt = row.sentAt!;
    }
  }
  return { runs: [...byRun.values()], marks };
}

/** Declares runs in batches; `dryRun` only plans. Throws on an unreadable answer (rows stay NULL). */
export async function declarePlannedRows(
  rows: PendingSendOutcome[],
  summary: SendOutcomesSummary,
  dryRun = false,
  redate = false,
): Promise<void> {
  summary.read += rows.length;
  const { runs, marks } = planRuns(rows);
  for (const m of marks) {
    if (!dryRun) await markHolds(m.holdIds, m.mark);
    if (m.mark === "no_lead_id") summary.noLeadId += m.holdIds.length;
    else if (m.mark === "ambiguous_lead") summary.ambiguousLead += m.holdIds.length;
    else summary.noSendEvidence += m.holdIds.length;
    if (summary.undeclaredSample.length < 5) summary.undeclaredSample.push(m.holdIds[0]);
  }
  if (dryRun) {
    summary.declared += runs.length;
    return;
  }
  summary.unchecked += runs.filter((r) => !r.checked).length;
  for (let i = 0; i < runs.length; i += DECLARE_BATCH_RUNS) {
    let batch = runs.slice(i, i + DECLARE_BATCH_RUNS);
    // A 422 writes nothing: drop/mark the refused runs, re-date the early ones, re-post the rest.
    for (let attempt = 0; batch.length > 0; attempt++) {
      if (attempt > 3) throw new Error(`runs-service outcomes batch still refusing after ${attempt} attempts`);
      const posted = await postBatch(batch, redate);
      if (posted.status === 409 && posted.body.includes("run_marked_support")) {
        // A run marked support (by its owner or staff) cannot declare, and the
        // batch names none of them: state each run alone, a refused one is marked.
        await declareRunByRun(batch, summary);
        break;
      }
      const answer = readBatchAnswer(posted.status, posted.body);
      if (answer.ok) {
        summary.redated += Number((JSON.parse(posted.body) as { redatedCount?: unknown }).redatedCount ?? 0);
        await markHolds(batch.flatMap((r) => r.holdIds), "declared");
        summary.declared += batch.length;
        break;
      }
      const refused = new Map(answer.invalid.map((x) => [x.runId.toLowerCase(), x.reason]));
      const next: BatchRun[] = [];
      for (const run of batch) {
        const reason = refused.get(run.runId.toLowerCase());
        if (!reason) {
          next.push(run);
          continue;
        }
        const verdict = markForInvalidRun(reason, run.occurredAt);
        if (verdict === "retry_at_run_start") {
          next.push({ ...run, occurredAt: OCCURRED_AT_RUN_START });
          summary.datedAtRunStart++;
          continue;
        }
        console.error(`[send-outcomes] run ${run.runId} refused by the batch (${reason}) -> ${verdict}`);
        await markHolds(run.holdIds, verdict);
        if (verdict === "run_gone") summary.runGone++;
        else summary.refused++;
      }
      batch = next;
    }
  }
}

/**
 * The batch's fallback when runs-service refuses it for a support-marked run
 * (409 `run_marked_support`, no run named): each run through the single door,
 * dated the same way; a refusal (409) marks its holds `refused`, a transient
 * answer leaves them undeclared for the next pass.
 */
async function declareRunByRun(runs: BatchRun[], summary: SendOutcomesSummary): Promise<void> {
  for (const run of runs) {
    let answer = await postRunOutcomes(run.runId, null, buildSendOutcomeDeclaration(run.leadId, run.checked), run.occurredAt);
    if (answer.status === 400 && answer.body.includes("occurred") && run.occurredAt !== OCCURRED_AT_RUN_START) {
      answer = await postRunOutcomes(run.runId, null, buildSendOutcomeDeclaration(run.leadId, run.checked), OCCURRED_AT_RUN_START);
      summary.datedAtRunStart++;
    }
    const verdict = classifyDeclareAnswer(answer.status, answer.body);
    if (verdict === "retry") {
      console.error(`[send-outcomes] run ${run.runId} declaration ${answer.status} (retried next pass): ${answer.body.slice(0, 300)}`);
      continue;
    }
    if (verdict !== "declared") console.error(`[send-outcomes] run ${run.runId} declaration ${answer.status} -> ${verdict}: ${answer.body.slice(0, 200)}`);
    await markHolds(run.holdIds, verdict);
    if (verdict === "declared") summary.declared++;
    else if (verdict === "run_gone") summary.runGone++;
    else summary.refused++;
  }
}

function logPass(tag: string, summary: SendOutcomesSummary): void {
  if (summary.noLeadId + summary.ambiguousLead + summary.noSendEvidence > 0) {
    console.error(
      `[${tag}] ${summary.noLeadId} holds with no lead id + ${summary.ambiguousLead} ambiguous + ${summary.noSendEvidence} with no sent email NOT declared (never an invented subject or date), e.g. ${summary.undeclaredSample.join(", ")}`,
    );
  }
  if (summary.read > 0) console.log(`[${tag}] pass done ${JSON.stringify(summary)}`);
}

let inFlight: Promise<SendOutcomesSummary> | null = null;

/** One live sweep pass. Concurrent calls join the pass already running. */
export function declareSendOutcomes(limit = SEND_OUTCOMES_BATCH_LIMIT): Promise<SendOutcomesSummary> {
  if (inFlight) return inFlight;
  inFlight = (async () => {
    const summary = emptySummary();
    await declarePlannedRows(await loadPending(limit), summary);
    logPass("send-outcomes", summary);
    return summary;
  })().finally(() => {
    inFlight = null;
  });
  return inFlight;
}

export const BACKFILL_PAGE_HOLDS = 900;
let backfillInFlight: Promise<SendOutcomesSummary> | null = null;

/**
 * The past, swept once through the same declaration (COST-BACKFILL-1023-INSTANTLY):
 * keyset over every undeclared hold, page by page, until none is left or
 * `limit` holds were walked. Idempotent and resumable: a declared hold is marked
 * and never read again; a replay on runs-service is `already_recorded`. A dry
 * run plans every page and writes nothing.
 */
export function runSendOutcomesBackfill(opts: { dryRun: boolean; limit?: number; redate?: boolean }): Promise<SendOutcomesSummary> {
  if (backfillInFlight) return backfillInFlight;
  backfillInFlight = (async () => {
    const summary = emptySummary();
    let cursor = "";
    let walked = 0;
    for (;;) {
      const page = opts.limit === undefined ? BACKFILL_PAGE_HOLDS : Math.min(BACKFILL_PAGE_HOLDS, opts.limit - walked);
      if (page <= 0) break;
      const { rows, lastHoldId } = await loadBackfillPage(cursor, page, opts.redate === true);
      if (!lastHoldId) break;
      walked += page;
      await declarePlannedRows(rows, summary, opts.dryRun, opts.redate === true);
      cursor = lastHoldId;
    }
    logPass(`send-outcomes-backfill${opts.redate ? " redate" : ""}${opts.dryRun ? " dry-run" : ""}`, summary);
    return summary;
  })().finally(() => {
    backfillInFlight = null;
  });
  return backfillInFlight;
}
