/**
 * COST-ACCOUNTING-1023: the run of ours that judging a reply is done FOR.
 *
 * One `reply-judging` run per (org, UTC day), held in `reply_judging_runs`.
 * Every reply-classifier call (`qualifyReply`) for that org that day sends its
 * id to chat-service as `x-run-id`, so chat-service's platform run (the LLM
 * spend, still platform-billed) becomes its child. The same day's positive
 * replies declare their `conversation` on it (lib/positive-reply-outcomes.ts),
 * so the judging spend rolls into Positive reply; a reply judged non-positive
 * declares nothing and its cost rides the positive ones (normal loss).
 *
 * The run carries no cost of its own and is closed `completed` at creation: it
 * is a cost anchor, not a job. When it cannot be resolved the judgment is not
 * asked (throws): every judging call states its run (PLATFORM-CALLER-RUN-1024).
 */
import { sql } from "drizzle-orm";
import { db } from "../db";
import { createRun, updateRun } from "./runs-client";

export const REPLY_JUDGING_TASK = "reply-judging";

/** UTC calendar day `YYYY-MM-DD`. */
export function utcDay(asOf: Date): string {
  return asOf.toISOString().slice(0, 10);
}

const memo = new Map<string, string>();

/** Test hook. */
export function resetReplyJudgingRunMemo(): void {
  memo.clear();
}

/** The org's judging run for the UTC day of `asOf`, created once. Throws on failure. */
export async function replyJudgingRunForOrg(orgId: string, asOf: Date = new Date()): Promise<string> {
  const day = utcDay(asOf);
  const key = `${orgId}|${day}`;
  const cached = memo.get(key);
  if (cached) return cached;

  const read = async (): Promise<string | null> => {
    const result = await db.execute(sql`
      SELECT run_id FROM reply_judging_runs WHERE org_id = ${orgId} AND day = ${day}::date
    `);
    const row = (result.rows as Array<Record<string, unknown>>)[0];
    return row ? String(row.run_id) : null;
  };

  let runId = await read();
  if (!runId) {
    const identity = { orgId, userId: "" };
    const run = await createRun({ serviceName: "instantly-service", taskName: REPLY_JUDGING_TASK }, identity);
    await updateRun(run.id, "completed", identity);
    const inserted = await db.execute(sql`
      INSERT INTO reply_judging_runs (org_id, day, run_id) VALUES (${orgId}, ${day}::date, ${run.id})
      ON CONFLICT (org_id, day) DO NOTHING
      RETURNING run_id
    `);
    // A concurrent caller stored another run first: theirs is the day's run.
    runId = (inserted.rows as unknown[]).length > 0 ? run.id : await read();
    if (!runId) throw new Error(`reply_judging_runs row for ${key} vanished after a conflict`);
  }
  memo.set(key, runId);
  return runId;
}

/**
 * The judging run for a reply on this sequence (`instantly_campaign_id`).
 * Throws when it cannot be resolved: judging a reply is production, it is
 * never sent to chat-service as nobody's work (PLATFORM-CALLER-RUN-1024).
 */
export async function replyJudgingRunForSequence(instantlyCampaignId: string): Promise<string> {
  const result = await db.execute(sql`
    SELECT org_id FROM instantly_campaigns WHERE instantly_campaign_id = ${instantlyCampaignId} LIMIT 1
  `);
  const row = (result.rows as Array<Record<string, unknown>>)[0];
  const orgId = row?.org_id ? String(row.org_id) : null;
  if (!orgId) {
    throw new Error(`[reply-judging-run] sequence ${instantlyCampaignId} has no org: no reply-judging run to judge it for`);
  }
  return replyJudgingRunForOrg(orgId);
}
