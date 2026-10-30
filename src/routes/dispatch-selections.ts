/**
 * `GET /internal/dispatch-selections/recorded?after=<seq>&limit=` (service key, read-only; FACTORY-P3 next outcomes,
 * message_sent): each dispatch selection the stored send-selection script was asked, oldest first (60 s settle), with
 * its material, our selection and the script's, compared. Read by workflow-service's case recorder. `GET
 * /internal/dispatch-selections/proof` = the live proof: selections and emails compared, and how many differed.
 */
import { Router, type Request, type Response } from "express";
import { and, asc, gt, sql } from "drizzle-orm";
import { db } from "../db";
import { dispatchSelections } from "../db/schema";
import { errorMessage } from "../lib/db-error.js";

const router = Router();

router.get("/recorded", async (req: Request, res: Response) => {
  const after = req.query.after === undefined ? 0 : Number(req.query.after);
  const limit = req.query.limit === undefined ? 20 : Number(req.query.limit);
  if (!Number.isInteger(after) || after < 0 || !Number.isInteger(limit) || limit < 1 || limit > 100) {
    return res.status(400).json({ error: "after (a seq >= 0) and limit (1..100) must be integers" });
  }
  try {
    const rows = await db
      .select()
      .from(dispatchSelections)
      .where(and(gt(dispatchSelections.seq, after), sql`${dispatchSelections.createdAt} < now() - interval '60 seconds'`))
      .orderBy(asc(dispatchSelections.seq))
      .limit(limit);
    return res.json({ decisions: rows.map((r) => ({ ...r, createdAt: r.createdAt.toISOString() })), nextAfter: rows.length > 0 ? rows[rows.length - 1]!.seq : after, hasMore: rows.length === limit });
  } catch (error) {
    console.error(`[instantly-service] GET /internal/dispatch-selections/recorded failed: ${errorMessage(error)}`);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.get("/proof", async (_req: Request, res: Response) => {
  try {
    const rows = await db.execute(sql`
      SELECT path,
        count(*)::int AS selections,
        count(*) FILTER (WHERE same)::int AS same,
        count(*) FILTER (WHERE same = false)::int AS different,
        count(*) FILTER (WHERE error IS NOT NULL)::int AS errors,
        coalesce(sum(sends) FILTER (WHERE same), 0)::int AS emails_same,
        coalesce(sum(sends) FILTER (WHERE same = false), 0)::int AS emails_different,
        max(created_at) AS last_at
      FROM dispatch_selections GROUP BY path ORDER BY path
    `);
    return res.json({ paths: (rows as unknown as { rows: unknown[] }).rows ?? rows });
  } catch (error) {
    return res.status(500).json({ error: errorMessage(error) });
  }
});

export default router;
