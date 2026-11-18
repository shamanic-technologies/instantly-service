/**
 * REPLY-SENT-UNIT-1118 (owner 2026-11-18): an answer sent in a thread is an
 * email we sent, whatever thread it goes in. It was declared nowhere.
 *
 * A sequence step is declared by lib/send-outcomes off its `sequence_costs`
 * hold. An answer (lib/reply-to-lead, the door `POST /orgs/replies`) takes no
 * hold and no run of ours, so that sweep never saw it: prod 2026-11-18, 144
 * answers of the lead answerer went out with no `message_sent` on the ledger,
 * and the message written for each linked to nothing.
 *
 * Grain: one answer that went out = one row of `reply_send_outcomes` = ONE
 * `message_sent` (the key every send of the fleet declares) of the thread's
 * lead (`instantly_campaigns.lead_id`), dated when it left.
 *
 * ON THE RUN THAT ASKED FOR IT (the caller's `x-run-id`), never a run of ours:
 *  - that run is the one that sent (a sender script's run, whose Engine tactic
 *    `recorded_by: sender` leaves this declaration to us);
 *  - what the answer was made FROM is that run's to state, and it does (the
 *    script runner declares the written message its run asked for), so the
 *    ledger links written -> sent on one run. We state NO consumption here: a
 *    `message_written` named from this side resolves to the lead's latest one,
 *    which for a caller that wrote inline is an older, unrelated message;
 *  - a caller that also declares its `message_sent` on its run is the SAME
 *    occurrence at runs-service (run, key, subject): `already_recorded`, never
 *    a second Unit.
 * An answer nobody named a run for stays recorded and undeclared (visible as
 * such), never declared under an invented run. A thread whose lead id we do
 * not hold is marked `no_lead_id`, never declared under an invented subject.
 *
 * No cost: an answer declares none, as a sequence step declares none for the
 * mailbox estate (see lib/reply-to-lead).
 *
 * Never on the send path: the answer leaves whatever the ledger says. The sweep
 * (driven by send-outcomes-worker) declares each recorded answer through the
 * batch door, one call per answer (all-or-nothing batches would let one refused
 * run hold the others).
 */
import { sql } from "drizzle-orm";
import { db } from "../db";
import { errorMessage } from "./db-error.js";
import {
  SEND_OUTCOME_SUBJECT_SERVICE,
  SEND_PRODUCED_OUTCOME,
  postOutcomesBatch,
  type BatchEntry,
  type DeclarationMark,
} from "./send-outcomes";

export const REPLY_SEND_OUTCOMES_LIMIT = 200;

export interface ReplySendRecord {
  orgId: string;
  instantlyCampaignId: string;
  leadEmail: string;
  messageId: string;
  transport: string;
  sentBy: string;
  /** The caller's run (`x-run-id`). Absent = nobody named one. */
  runId?: string | null;
}

/**
 * Record an answer that went out. Never throws: the answer is out, a lost
 * record is logged loud (its `message_sent` then stays undeclared).
 */
export async function recordReplySend(r: ReplySendRecord): Promise<void> {
  try {
    await db.execute(sql`
      INSERT INTO reply_send_outcomes (org_id, instantly_campaign_id, lead_email, message_id, transport, sent_by, run_id)
      VALUES (${r.orgId}, ${r.instantlyCampaignId}, ${r.leadEmail}, ${r.messageId}, ${r.transport}, ${r.sentBy}, ${r.runId ?? null})
      ON CONFLICT (instantly_campaign_id, message_id) DO NOTHING
    `);
    if (!r.runId) {
      console.error(
        `[instantly-service] reply-send-outcomes: answer ${r.messageId} (campaign=${r.instantlyCampaignId}) was sent with no run id: message_sent undeclared`,
      );
    }
  } catch (error: unknown) {
    console.error(
      `[instantly-service] reply-send-outcomes: answer ${r.messageId} (campaign=${r.instantlyCampaignId}) went out but its record FAILED (message_sent undeclared): ${errorMessage(error)}`,
    );
  }
}

/** What one answer declares on the run that asked for it: the lead's `message_sent`, nothing consumed. */
export function buildReplySendEntry(row: { runId: string; leadId: string; sentAt: string }): BatchEntry {
  return {
    runId: row.runId,
    produced: [{ outcomeKey: SEND_PRODUCED_OUTCOME, subjectService: SEND_OUTCOME_SUBJECT_SERVICE, subjectId: row.leadId }],
    consumed: [],
    occurredAt: row.sentAt,
  };
}

export type ReplySendMark = Extract<DeclarationMark, "declared" | "run_gone" | "no_lead_id" | "refused">;

/**
 * How runs-service's answer to ONE answer's declaration ends its row. 2xx =
 * recorded or already recorded. 422 `invalid_runs`: `run_not_found` = the run
 * is gone, any other reason = refused for good (kept with the body); a 409 is
 * refused for good too. Anything
 * else (401, 5xx, a 400 from an unreadable catalogue) = retry next tick.
 */
export function classifyReplySendAnswer(status: number, body: string): ReplySendMark | "retry" {
  if (status >= 200 && status < 300) return "declared";
  // runs-service refuses THIS occurrence for good (a run marked support, a conflicting quantity).
  if (status === 409) return "refused";
  if (status !== 422) return "retry";
  try {
    const parsed = JSON.parse(body) as { error?: string; details?: { invalid?: Array<{ reason?: string }> } };
    if (parsed.error !== "invalid_runs" || !Array.isArray(parsed.details?.invalid) || parsed.details.invalid.length === 0) return "retry";
    return parsed.details.invalid[0].reason === "run_not_found" ? "run_gone" : "refused";
  } catch {
    return "retry";
  }
}

export interface PendingReplySend {
  id: string;
  runId: string;
  leadId: string | null;
  /** ISO instant the answer left (the column holds UTC without a zone). */
  sentAt: string;
}

export interface ReplySendSummary {
  pending: number;
  declared: number;
  noLeadId: number;
  runGone: number;
  refused: number;
  retry: number;
}

async function mark(id: string, outcome: ReplySendMark, error: string | null): Promise<void> {
  await db.execute(sql`
    UPDATE reply_send_outcomes
    SET outcomes_declaration = ${outcome}, outcomes_declared_at = now(), outcomes_error = ${error}
    WHERE id = ${id}::uuid AND outcomes_declaration IS NULL
  `);
}

let inFlight: Promise<ReplySendSummary> | null = null;

/** Declare every recorded answer that names its run and is not declared yet (newest first). Concurrent calls join. */
export function declareReplySends(limit = REPLY_SEND_OUTCOMES_LIMIT): Promise<ReplySendSummary> {
  if (inFlight) return inFlight;
  inFlight = (async () => {
    const result = await db.execute(sql`
      SELECT r.id, r.run_id, c.lead_id, to_char(r.sent_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS sent_at
      FROM reply_send_outcomes r
      LEFT JOIN instantly_campaigns c ON c.instantly_campaign_id = r.instantly_campaign_id
      WHERE r.outcomes_declaration IS NULL AND r.run_id IS NOT NULL
      ORDER BY r.sent_at DESC
      LIMIT ${limit}
    `);
    const rows: PendingReplySend[] = (result.rows as Array<Record<string, unknown>>).map((r) => ({
      id: String(r.id),
      runId: String(r.run_id),
      leadId: (r.lead_id as string | null) ?? null,
      sentAt: String(r.sent_at),
    }));
    const summary: ReplySendSummary = { pending: rows.length, declared: 0, noLeadId: 0, runGone: 0, refused: 0, retry: 0 };
    for (const row of rows) {
      if (!row.leadId) {
        await mark(row.id, "no_lead_id", null);
        summary.noLeadId++;
        continue;
      }
      const answer = await postOutcomesBatch([buildReplySendEntry({ runId: row.runId, leadId: row.leadId, sentAt: row.sentAt })]);
      const outcome = classifyReplySendAnswer(answer.status, answer.body);
      if (outcome === "retry") {
        console.error(`[instantly-service] reply-send-outcomes: runs-service ${answer.status} for run=${row.runId} (retried next tick): ${answer.body.slice(0, 300)}`);
        summary.retry++;
        continue;
      }
      await mark(row.id, outcome, outcome === "declared" ? null : answer.body.slice(0, 1000));
      if (outcome === "declared") summary.declared++;
      else if (outcome === "run_gone") summary.runGone++;
      else {
        summary.refused++;
        console.error(`[instantly-service] reply-send-outcomes: run=${row.runId} REFUSED (kept, never resent): ${answer.body.slice(0, 300)}`);
      }
    }
    if (rows.length > 0) console.log(`[instantly-service] reply-send-outcomes pass ${JSON.stringify(summary)}`);
    return summary;
  })().finally(() => {
    inFlight = null;
  });
  return inFlight;
}
