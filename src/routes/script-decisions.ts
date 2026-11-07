/**
 * MIGRATE-1105: the decisions of this service a stored factory Script can take, recorded on the live path either way
 * (`lib/service-script.ts`), served to workflow-service's case recorder. Same family as
 * `GET /internal/dispatch-selections/recorded`:
 *   - `GET /internal/script-decisions/email_queued/recorded?after=<seq>&limit=` (former name message_queued)
 *   - `GET /internal/script-decisions/email_reply_received/recorded?after=<seq>&limit=[&task=]` (former reply_received)
 * `task` (MIGRATE-1105 S2b) narrows to one decision of the outcome (`inbound-decision`, `orphan-judgment`); none = every
 * row of the outcome (rows recorded before tasks carry none). A paid decision's row carries `calls` (its vendor calls in
 * the script's words).
 * oldest first (60 s settle), `{decisions[], nextAfter, hasMore}`. `GET /internal/script-decisions/<outcome>/proof` =
 * decisions per path, same vs different.
 */
import { Router, type Request, type Response } from "express";
import { and, asc, eq, gt, inArray, sql } from "drizzle-orm";
import { db } from "../db";
import { scriptDecisions } from "../db/schema";
import { errorMessage } from "../lib/db-error.js";
import { MESSAGE_QUEUED_OUTCOME } from "../lib/queued-steps-script";
import { REPLY_RECEIVED_OUTCOME } from "../lib/self-send/inbound-decision";

export const RECORDED_OUTCOMES: readonly string[] = [MESSAGE_QUEUED_OUTCOME, REPLY_RECEIVED_OUTCOME];

/**
 * MIGRATE-1105 S5: the name each outcome's decisions were recorded under before its Engine was re-homed onto its finest
 * WHAT. A read under either name serves both (the recorder of a re-homed dynasty keeps every case recorded before).
 */
const FORMER_NAMES: Readonly<Record<string, string>> = { [MESSAGE_QUEUED_OUTCOME]: "message_queued", [REPLY_RECEIVED_OUTCOME]: "reply_received" };
const CURRENT_OF: Readonly<Record<string, string>> = Object.fromEntries(Object.entries(FORMER_NAMES).map(([now, was]) => [was, now]));
export const recordedUnder = (outcome: string): string[] => [outcome, FORMER_NAMES[outcome]].filter((x): x is string => !!x);

const router = Router();

function outcomeOf(req: Request, res: Response): string | null {
  const asked = String(req.params.outcome);
  const outcome = CURRENT_OF[asked] ?? asked;
  if (!RECORDED_OUTCOMES.includes(outcome)) {
    res.status(404).json({ error: `no decisions are recorded for ${outcome}`, outcomes: RECORDED_OUTCOMES });
    return null;
  }
  return outcome;
}

router.get("/:outcome/recorded", async (req: Request, res: Response) => {
  const outcome = outcomeOf(req, res);
  if (!outcome) return;
  const after = req.query.after === undefined ? 0 : Number(req.query.after);
  const limit = req.query.limit === undefined ? 20 : Number(req.query.limit);
  const task = typeof req.query.task === "string" && req.query.task ? req.query.task : null;
  if (!Number.isInteger(after) || after < 0 || !Number.isInteger(limit) || limit < 1 || limit > 100) {
    return res.status(400).json({ error: "after (a seq >= 0) and limit (1..100) must be integers" });
  }
  try {
    const rows = await db
      .select()
      .from(scriptDecisions)
      .where(and(inArray(scriptDecisions.outcome, recordedUnder(outcome)), task ? eq(scriptDecisions.task, task) : undefined, gt(scriptDecisions.seq, after), sql`${scriptDecisions.createdAt} < now() - interval '60 seconds'`))
      .orderBy(asc(scriptDecisions.seq))
      .limit(limit);
    return res.json({ decisions: rows.map((r) => ({ ...r, createdAt: r.createdAt.toISOString() })), nextAfter: rows.length > 0 ? rows[rows.length - 1]!.seq : after, hasMore: rows.length === limit });
  } catch (error) {
    console.error(`[instantly-service] GET /internal/script-decisions/${outcome}/recorded failed: ${errorMessage(error)}`);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/:outcome/proof", async (req: Request, res: Response) => {
  const outcome = outcomeOf(req, res);
  if (!outcome) return;
  const task = typeof req.query.task === "string" && req.query.task ? req.query.task : null;
  try {
    const rows = await db.execute(sql`
      SELECT path,
        count(*)::int AS decisions,
        count(*) FILTER (WHERE same)::int AS same,
        count(*) FILTER (WHERE same = false)::int AS different,
        count(*) FILTER (WHERE error IS NOT NULL)::int AS errors,
        max(created_at) AS last_at
      FROM script_decisions WHERE outcome IN (${sql.join(recordedUnder(outcome).map((o) => sql`${o}`), sql`, `)}) ${task ? sql`AND task = ${task}` : sql``} GROUP BY path ORDER BY path
    `);
    return res.json({ outcome, paths: (rows as unknown as { rows: unknown[] }).rows ?? rows });
  } catch (error) {
    return res.status(500).json({ error: errorMessage(error) });
  }
});

export default router;
