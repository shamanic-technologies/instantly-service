/**
 * LEDGER-HISTORY-1024: the AI Instant Call rings this service placed
 * (2026-10-07..10-12, `ai-instant-call` runs; the ring code is gone since the
 * reply trigger door took over) are parents of twilio-service's place-call
 * runs, and declared nothing, so the call spend read undeclared. Each ring
 * placed a booking call for a person who had just replied positively: the
 * catalogue outcome `booking_call`, consuming that person's `conversation`.
 *
 * Built from our own records only: the ring stamped
 * `instantly_campaigns.sales_interest_call_at` on the sequence a moment before
 * opening its run, so a ring run is the one `ai-instant-call` run of the org
 * that started within `RING_MATCH_MS` of a stamp; a run matching zero or
 * several stamps is reported, never guessed. Dated at the run's start. One-shot
 * and idempotent: runs-service answers a replay `already_recorded`. twilio-service
 * declares nothing on its children (that would count the call twice).
 */
import { sql } from "drizzle-orm";
import { db } from "../db";
import { listRuns } from "./runs-client";
import { OCCURRED_AT_RUN_START, SEND_OUTCOME_SUBJECT_SERVICE, postOutcomesBatch, readBatchAnswer, type SendOutcomeDeclaration } from "./send-outcomes";

export const RING_RUN_TASK = "ai-instant-call";
export const BOOKING_CALL_OUTCOME = "booking_call";
export const POSITIVE_REPLY_OUTCOME_KEY = "conversation";
/** The stamp is written just before the run is opened (milliseconds apart in prod). */
export const RING_MATCH_MS = 5_000;

export function buildBookingCallDeclaration(leadId: string): SendOutcomeDeclaration {
  const subject = { subjectService: SEND_OUTCOME_SUBJECT_SERVICE, subjectId: leadId };
  return {
    produced: [{ outcomeKey: BOOKING_CALL_OUTCOME, ...subject }],
    consumed: [{ outcomeKey: POSITIVE_REPLY_OUTCOME_KEY, ...subject }],
  };
}

export interface RingStamp {
  leadId: string;
  calledAt: number;
}

/** The lead a ring run was for: the ONE stamp within the window, else null. Pure. */
export function matchRing(runStartedAt: number, stamps: RingStamp[]): string | null {
  const hits = stamps.filter((s) => Math.abs(s.calledAt - runStartedAt) <= RING_MATCH_MS);
  const leads = new Set(hits.map((h) => h.leadId));
  return leads.size === 1 ? hits[0].leadId : null;
}

export interface BookingCallSummary {
  rings: number;
  declared: number;
  unmatched: string[];
}

export async function declarePastBookingCalls(opts: { dryRun: boolean }): Promise<BookingCallSummary> {
  const result = await db.execute(sql`
    SELECT org_id, max(user_id) AS user_id,
      jsonb_agg(jsonb_build_object('leadId', lead_id, 'calledAt', to_char(sales_interest_call_at, 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'))) AS stamps
    FROM instantly_campaigns
    WHERE sales_interest_call_at IS NOT NULL AND lead_id IS NOT NULL
    GROUP BY org_id
  `);
  const summary: BookingCallSummary = { rings: 0, declared: 0, unmatched: [] };
  const entries: Array<{ runId: string; leadId: string }> = [];
  for (const org of result.rows as Array<Record<string, unknown>>) {
    const stamps = (Array.isArray(org.stamps) ? (org.stamps as Array<{ leadId: string; calledAt: string }>) : []).map((s) => ({
      leadId: s.leadId,
      calledAt: Date.parse(s.calledAt),
    }));
    const times = stamps.map((s) => s.calledAt);
    const runs = await listRuns(
      {
        serviceName: "instantly-service",
        startedAfter: new Date(Math.min(...times) - RING_MATCH_MS).toISOString(),
        startedBefore: new Date(Math.max(...times) + RING_MATCH_MS).toISOString(),
      },
      { orgId: String(org.org_id), userId: org.user_id ? String(org.user_id) : "" },
    );
    for (const run of runs.filter((r) => r.taskName === RING_RUN_TASK)) {
      summary.rings++;
      const leadId = matchRing(Date.parse(run.startedAt), stamps);
      if (leadId) entries.push({ runId: run.id, leadId });
      else summary.unmatched.push(run.id);
    }
  }
  if (summary.unmatched.length > 0) {
    console.error(`[booking-call-outcomes] ${summary.unmatched.length} ring run(s) with no single sequence stamp, NOT declared: ${summary.unmatched.join(", ")}`);
  }
  if (opts.dryRun || entries.length === 0) {
    summary.declared = opts.dryRun ? entries.length : 0;
    return summary;
  }
  const posted = await postOutcomesBatch(
    entries.map((e) => ({ runId: e.runId, ...buildBookingCallDeclaration(e.leadId), occurredAt: OCCURRED_AT_RUN_START })),
  );
  const answer = readBatchAnswer(posted.status, posted.body);
  if (!answer.ok) throw new Error(`runs-service refused the booking-call declarations: ${JSON.stringify(answer.invalid).slice(0, 500)}`);
  summary.declared = entries.length;
  console.log(`[booking-call-outcomes] done ${JSON.stringify(summary)}`);
  return summary;
}
