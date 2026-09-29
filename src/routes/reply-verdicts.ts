/**
 * `POST /orgs/reply-verdicts/query` — per-reply verdicts for a set of leads.
 *
 * Auth: `serviceAuth` + `requireOrgId`; org scope is in the query itself.
 * Fails loud: an empty list on a read failure would claim these leads never
 * replied, the one wrong answer that looks exactly like a correct one.
 */
import { Router, Request, Response } from "express";

import { readReplyVerdicts } from "../lib/reply-verdicts";
import { ReplyVerdictsQuerySchema } from "../schemas";

const router = Router();

router.post("/query", async (req: Request, res: Response) => {
  const orgId = res.locals.orgId as string;
  const parsed = ReplyVerdictsQuerySchema.safeParse(req.body ?? {});
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.message });
  }
  try {
    const replies = await readReplyVerdicts({ orgId, ...parsed.data });
    return res.status(200).json({ replies });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[instantly-service] reply-verdicts: failed for org=${orgId}: ${message}`);
    return res.status(500).json({ error: message });
  }
});

export default router;
