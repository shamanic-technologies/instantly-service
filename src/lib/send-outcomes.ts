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
 */
import { sql } from "drizzle-orm";
import { db } from "../db";
import { errorMessage } from "./db-error.js";

/** The catalogue keys (features-service `GET /public/catalogue/steps`) a send run declares. */
export const SEND_PRODUCED_OUTCOME = "message_sent";
export const SEND_CONSUMED_OUTCOMES = ["message_written", "email_checked"] as const;
/** A person's occurrences are named by their lead in lead-service. */
export const SEND_OUTCOME_SUBJECT_SERVICE = "lead-service";

export const SEND_OUTCOMES_BATCH_LIMIT = 500;
const DECLARE_CONCURRENCY = 5;
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

/** What one sent step's run declares about the person it emailed. */
export function buildSendOutcomeDeclaration(leadId: string): SendOutcomeDeclaration {
  const subject = { subjectService: SEND_OUTCOME_SUBJECT_SERVICE, subjectId: leadId };
  return {
    produced: [{ outcomeKey: SEND_PRODUCED_OUTCOME, ...subject }],
    consumed: SEND_CONSUMED_OUTCOMES.map((outcomeKey) => ({ outcomeKey, ...subject })),
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
}

/**
 * Actual holds whose run has not been declared yet, newest send first, each
 * joined to its per-lead campaign row: by `instantly_campaign_id`, or for a
 * pre-0027 hold (no such id) by the caller campaign + address, the same
 * fallback as `matchesHoldCampaign`. `matches` > 1 = ambiguous, never guessed.
 */
async function loadPending(limit: number): Promise<PendingSendOutcome[]> {
  const result = await db.execute(sql`
    SELECT sc.id AS hold_id, sc.run_id, sc.step, m.org_id, m.lead_id, COALESCE(m.n, 0)::int AS matches
    FROM sequence_costs sc
    LEFT JOIN LATERAL (
      SELECT (array_agg(c.org_id))[1] AS org_id, (array_agg(c.lead_id))[1] AS lead_id, count(*) AS n
      FROM (
        -- Two index-served branches, never one OR (4.8 s vs 1.3 s on prod).
        SELECT c.org_id, c.lead_id FROM instantly_campaigns c
        WHERE sc.instantly_campaign_id IS NOT NULL AND c.instantly_campaign_id = sc.instantly_campaign_id
        UNION ALL
        SELECT c.org_id, c.lead_id FROM instantly_campaigns c
        WHERE sc.instantly_campaign_id IS NULL AND c.campaign_id = sc.campaign_id
          AND lower(c.lead_email) = lower(sc.lead_email)
      ) c
    ) m ON true
    WHERE sc.status = 'actual' AND sc.outcomes_declared_at IS NULL
    ORDER BY sc.updated_at DESC
    LIMIT ${limit}
  `);
  return (result.rows as Array<Record<string, unknown>>).map((r) => ({
    holdId: String(r.hold_id),
    runId: String(r.run_id),
    step: Number(r.step),
    orgId: (r.org_id as string | null) ?? null,
    leadId: (r.lead_id as string | null) ?? null,
    matches: Number(r.matches),
  }));
}

async function mark(holdId: string, outcome: DeclarationMark): Promise<void> {
  await db.execute(sql`
    UPDATE sequence_costs SET outcomes_declared_at = now(), outcomes_declaration = ${outcome}
    WHERE id = ${holdId} AND outcomes_declared_at IS NULL
  `);
}

/**
 * POST the declaration on the run's ledger entry (runs-service platform route:
 * the step runs are ours; `x-org-id` makes runs-service check the run is that
 * org's). Returns the raw answer, never throws on an HTTP status.
 */
export async function postRunOutcomes(
  runId: string,
  orgId: string | null,
  declaration: SendOutcomeDeclaration,
): Promise<{ status: number; body: string }> {
  const url = process.env.RUNS_SERVICE_URL || "http://localhost:3006";
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    "x-api-key": process.env.RUNS_SERVICE_API_KEY || "",
    "x-service-name": "instantly-service",
  };
  if (orgId && UUID_RE.test(orgId)) headers["x-org-id"] = orgId;
  const res = await fetch(`${url}/v1/platform-runs/${encodeURIComponent(runId)}/outcomes`, {
    method: "POST",
    headers,
    body: JSON.stringify(declaration),
    signal: AbortSignal.timeout(DECLARE_TIMEOUT_MS),
  });
  return { status: res.status, body: await res.text() };
}

export interface SendOutcomesSummary {
  read: number;
  declared: number;
  runGone: number;
  noLeadId: number;
  ambiguousLead: number;
  refused: number;
  retry: number;
  /** Up to 5 holds of this pass NOT declared for want of a lead id (logged). */
  undeclaredSample: string[];
}

async function declareOne(row: PendingSendOutcome, summary: SendOutcomesSummary): Promise<void> {
  // Counted and named once per pass (the summary line), not per row: the
  // pre-2026-08 holds carry no lead id by the tens of thousands.
  if (row.matches > 1) {
    await mark(row.holdId, "ambiguous_lead");
    summary.ambiguousLead++;
    summary.undeclaredSample.push(row.holdId);
    return;
  }
  if (!row.leadId) {
    await mark(row.holdId, "no_lead_id");
    summary.noLeadId++;
    summary.undeclaredSample.push(row.holdId);
    return;
  }
  let answer: { status: number; body: string };
  try {
    answer = await postRunOutcomes(row.runId, row.orgId, buildSendOutcomeDeclaration(row.leadId));
  } catch (error: unknown) {
    summary.retry++;
    console.error(`[send-outcomes] run ${row.runId} declaration failed (retried next tick): ${errorMessage(error)}`);
    return;
  }
  const verdict = classifyDeclareAnswer(answer.status, answer.body);
  if (verdict === "retry") {
    summary.retry++;
    console.error(`[send-outcomes] run ${row.runId} declaration ${answer.status} (retried next tick): ${answer.body.slice(0, 300)}`);
    return;
  }
  if (verdict !== "declared") {
    console.error(`[send-outcomes] run ${row.runId} declaration ${answer.status} -> ${verdict}: ${answer.body.slice(0, 300)}`);
  }
  await mark(row.holdId, verdict);
  if (verdict === "declared") summary.declared++;
  else if (verdict === "run_gone") summary.runGone++;
  else summary.refused++;
}

let inFlight: Promise<SendOutcomesSummary> | null = null;

/** One sweep pass. Concurrent calls join the pass already running. */
export function declareSendOutcomes(limit = SEND_OUTCOMES_BATCH_LIMIT): Promise<SendOutcomesSummary> {
  if (inFlight) return inFlight;
  inFlight = (async () => {
    const rows = await loadPending(limit);
    const summary: SendOutcomesSummary = { read: rows.length, declared: 0, runGone: 0, noLeadId: 0, ambiguousLead: 0, refused: 0, retry: 0, undeclaredSample: [] };
    let next = 0;
    const lane = async () => {
      while (next < rows.length) {
        const row = rows[next++];
        await declareOne(row, summary);
      }
    };
    await Promise.all(Array.from({ length: Math.min(DECLARE_CONCURRENCY, rows.length) }, lane));
    summary.undeclaredSample = summary.undeclaredSample.slice(0, 5);
    if (summary.noLeadId + summary.ambiguousLead > 0) {
      console.error(
        `[send-outcomes] ${summary.noLeadId} holds with no lead id + ${summary.ambiguousLead} ambiguous NOT declared (never an invented subject), e.g. ${summary.undeclaredSample.join(", ")}`,
      );
    }
    if (rows.length > 0) console.log(`[send-outcomes] pass done ${JSON.stringify(summary)}`);
    return summary;
  })().finally(() => {
    inFlight = null;
  });
  return inFlight;
}
