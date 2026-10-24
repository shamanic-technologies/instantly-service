/**
 * LEDGER-HISTORY-1024: the spend-free run of ours a DATED outcome is declared on.
 *
 * One run per (task, org, UTC day of the outcome), held in `outcome_anchor_runs`.
 * The run carries no cost and no child, and is closed `completed` at creation:
 * it is a production order whose only cost is what it CONSUMES (the person's
 * `message_sent`, at its standard cost). Because it is spend-free, runs-service
 * lets it hold occurrences dated BEFORE its start (a June click declared today
 * counts on its June day). Never give it a cost or a child: runs-service
 * refuses both on a run holding facts dated before its start.
 */
import { sql } from "drizzle-orm";
import { db } from "../db";
import { createRun, updateRun } from "./runs-client";

const memo = new Map<string, string>();

/** Test hook. */
export function resetOutcomeAnchorRunMemo(): void {
  memo.clear();
}

/** The org's anchor run of `task` for UTC `day` (`YYYY-MM-DD`), created once. Throws on failure. */
export async function outcomeAnchorRun(taskName: string, orgId: string, day: string): Promise<string> {
  const key = `${taskName}|${orgId}|${day}`;
  const cached = memo.get(key);
  if (cached) return cached;

  const read = async (): Promise<string | null> => {
    const result = await db.execute(sql`
      SELECT run_id FROM outcome_anchor_runs WHERE task_name = ${taskName} AND org_id = ${orgId} AND day = ${day}::date
    `);
    const row = (result.rows as Array<Record<string, unknown>>)[0];
    return row ? String(row.run_id) : null;
  };

  let runId = await read();
  if (!runId) {
    const identity = { orgId, userId: "" };
    const run = await createRun({ serviceName: "instantly-service", taskName }, identity);
    await updateRun(run.id, "completed", identity);
    const inserted = await db.execute(sql`
      INSERT INTO outcome_anchor_runs (task_name, org_id, day, run_id) VALUES (${taskName}, ${orgId}, ${day}::date, ${run.id})
      ON CONFLICT (task_name, org_id, day) DO NOTHING
      RETURNING run_id
    `);
    // A concurrent caller stored another run first: theirs is the day's run.
    runId = (inserted.rows as unknown[]).length > 0 ? run.id : await read();
    if (!runId) throw new Error(`outcome_anchor_runs row for ${key} vanished after a conflict`);
  }
  memo.set(key, runId);
  return runId;
}
