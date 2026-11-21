/**
 * OUTREACH-3WAYS-1121: the person is contacted when WE DECIDE to email them, not
 * hours later when the throttled mailbox sends.
 *
 * A client states "at most N new people a day" on a campaign; billing-service
 * counts the distinct people the campaign's runs produced something about, read
 * off runs-service's outcome ledger, and campaign-service holds the next run once
 * N is reached. The only person-keyed fact a send used to state was its
 * `message_sent`, declared by lib/send-outcomes when the email really leaves:
 * prod 2026-11-21, a run queued at 07:24 declared its person at 12:11..14:08, so a
 * day's runs queued far past N before any was counted.
 *
 * So every step `POST /orgs/send` queues states, on the step's own run (the
 * `email-send-step-N` run, tagged with the campaign, the one its `message_sent`
 * lands on later), ONE produced `email_queued` about the SAME subject
 * (lead-service, the lead id `message_sent` uses), at the queue decision.
 * Nothing consumed (what the send uses is `message_sent`'s to state, unchanged).
 *
 * Never changes what or when anything is sent: the send path asks once, bounded,
 * fail-soft; a step it could not declare stays `queued_declaration = 'pending'`
 * and this module's sweep (driven by send-outcomes-worker) declares it, dated at
 * the queue decision (`sequence_costs.created_at`). Only rows the send path wrote
 * are ever 'pending': restores, retry-stuck and every row before migration 0115
 * stay NULL (no backfill).
 */
import { sql } from "drizzle-orm";
import { db } from "../db";
import { errorMessage } from "./db-error.js";
import {
  OCCURRED_AT_RUN_START,
  SEND_OUTCOME_SUBJECT_SERVICE,
  classifyDeclareAnswer,
  postRunOutcomes,
  type DeclarationMark,
  type SendOutcomeDeclaration,
} from "./send-outcomes";

/** The catalogue step a queue decision produces (features-service: "Email queued", person subject, toward `email_sent`). */
export const QUEUED_PRODUCED_OUTCOME = "email_queued";
/** Value the send path writes on the rows it queues; the sweep reads it. */
export const QUEUED_PENDING = "pending";
/** The send path waits at most this long for runs-service; the sweep catches the rest. */
export const QUEUED_DECLARE_SEND_PATH_TIMEOUT_MS = 5_000;
/** A pending row younger than this is still the send path's (in flight). */
const SWEEP_MIN_AGE_SECONDS = 60;
export const QUEUED_OUTCOMES_LIMIT = 500;

export type QueuedMark = Extract<DeclarationMark, "declared" | "run_gone" | "refused" | "no_lead_id">;

/** What one queued step's run states: the person's produced `email_queued`, nothing consumed. */
export function buildQueuedOutcomeDeclaration(leadId: string): SendOutcomeDeclaration {
  return {
    produced: [{ outcomeKey: QUEUED_PRODUCED_OUTCOME, subjectService: SEND_OUTCOME_SUBJECT_SERVICE, subjectId: leadId }],
    consumed: [],
  };
}

export interface QueuedHold {
  holdId: string;
  runId: string;
}

async function markQueued(holdIds: string[], outcome: QueuedMark): Promise<void> {
  if (holdIds.length === 0) return;
  await db.execute(sql`
    UPDATE sequence_costs SET queued_declaration = ${outcome}, queued_declared_at = now()
    WHERE id = ANY(${`{${holdIds.join(",")}}`}::text[]) AND queued_declaration = ${QUEUED_PENDING}
  `);
}

/**
 * Declare one step's `email_queued`, dated `occurredAt` (absent = now). A date
 * runs-service finds before the run's start is re-sent once at the run's start.
 * Returns the mark, or `retry` (left pending). Never throws on an HTTP status.
 */
async function declareOne(
  runId: string,
  orgId: string | null,
  leadId: string,
  occurredAt: string | undefined,
  timeoutMs?: number,
): Promise<QueuedMark | "retry"> {
  const declaration = buildQueuedOutcomeDeclaration(leadId);
  let answer = await postRunOutcomes(runId, orgId, declaration, occurredAt, timeoutMs);
  if (answer.status === 400 && answer.body.includes("invalid_occurred_at") && occurredAt !== OCCURRED_AT_RUN_START) {
    answer = await postRunOutcomes(runId, orgId, declaration, OCCURRED_AT_RUN_START, timeoutMs);
  }
  const outcome = classifyDeclareAnswer(answer.status, answer.body);
  if (outcome === "retry") {
    console.error(
      `[instantly-service] queued-outcomes: runs-service ${answer.status} for run=${runId} (left pending, retried by the sweep): ${answer.body.slice(0, 300)}`,
    );
    return "retry";
  }
  if (outcome === "refused") {
    console.error(`[instantly-service] queued-outcomes: run=${runId} REFUSED (kept, never resent): ${answer.body.slice(0, 300)}`);
  }
  if (outcome === "ambiguous_lead" || outcome === "no_lead_id") return "refused";
  return outcome;
}

/**
 * At the queue decision (`POST /orgs/send`, after the steps are queued): declare
 * each step's `email_queued` now. Fail-soft and bounded: the send is already
 * decided; whatever is not declared stays pending for the sweep.
 */
export async function declareQueuedAtSend(holds: QueuedHold[], orgId: string | null, leadId: string | null | undefined): Promise<void> {
  if (holds.length === 0) return;
  if (!leadId) {
    try {
      await markQueued(holds.map((h) => h.holdId), "no_lead_id");
    } catch (error: unknown) {
      console.error(`[instantly-service] queued-outcomes: marking no_lead_id failed: ${errorMessage(error)}`);
    }
    return;
  }
  for (const h of holds) {
    try {
      const outcome = await declareOne(h.runId, orgId, leadId, undefined, QUEUED_DECLARE_SEND_PATH_TIMEOUT_MS);
      if (outcome !== "retry") await markQueued([h.holdId], outcome);
    } catch (error: unknown) {
      console.error(
        `[instantly-service] queued-outcomes: run=${h.runId} not declared at queue time (left pending, retried by the sweep): ${errorMessage(error)}`,
      );
    }
  }
}

export interface QueuedOutcomesSummary {
  pending: number;
  declared: number;
  noLeadId: number;
  runGone: number;
  refused: number;
  retry: number;
}

let inFlight: Promise<QueuedOutcomesSummary> | null = null;

/** Declare every step the send path left pending (oldest first, dated at its queue decision). Concurrent calls join. */
export function declareQueuedOutcomes(limit = QUEUED_OUTCOMES_LIMIT): Promise<QueuedOutcomesSummary> {
  if (inFlight) return inFlight;
  inFlight = (async () => {
    const result = await db.execute(sql`
      SELECT sc.id, sc.run_id, c.lead_id, c.org_id,
             to_char((sc.created_at::timestamptz) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS queued_at
      FROM sequence_costs sc
      LEFT JOIN instantly_campaigns c ON c.instantly_campaign_id = sc.instantly_campaign_id
      WHERE sc.queued_declaration = ${QUEUED_PENDING}
        AND sc.created_at < now() - make_interval(secs => ${SWEEP_MIN_AGE_SECONDS})
      ORDER BY sc.created_at ASC
      LIMIT ${limit}
    `);
    const rows = (result.rows as Array<Record<string, unknown>>).map((r) => ({
      holdId: String(r.id),
      runId: String(r.run_id),
      leadId: (r.lead_id as string | null) ?? null,
      orgId: (r.org_id as string | null) ?? null,
      queuedAt: String(r.queued_at),
    }));
    const summary: QueuedOutcomesSummary = { pending: rows.length, declared: 0, noLeadId: 0, runGone: 0, refused: 0, retry: 0 };
    for (const row of rows) {
      if (!row.leadId) {
        await markQueued([row.holdId], "no_lead_id");
        summary.noLeadId++;
        continue;
      }
      const outcome = await declareOne(row.runId, row.orgId, row.leadId, row.queuedAt);
      if (outcome === "retry") {
        summary.retry++;
        continue;
      }
      await markQueued([row.holdId], outcome);
      if (outcome === "declared") summary.declared++;
      else if (outcome === "run_gone") summary.runGone++;
      else summary.refused++;
    }
    if (rows.length > 0) console.log(`[instantly-service] queued-outcomes pass ${JSON.stringify(summary)}`);
    return summary;
  })().finally(() => {
    inFlight = null;
  });
  return inFlight;
}
