/**
 * The outreach fact feed (lib/outreach-facts). Platform-scoped (service key
 * only, fleet-wide), read-only and DB-only (no cost).
 *
 *  - `GET  /internal/outreach-facts?since=&limit=&orgId=&brandId=&email=` — the
 *    feed after the cursor `since` (exclusive), in feed order.
 *  - `POST /internal/outreach-facts/sync` — one judge + emission pass by hand
 *    (`{ judgeLimit?, sinceDays? }`; sinceDays null = whole history).
 */
import { Router, Request, Response } from "express";

import { readOutreachFacts, syncOutreachFacts } from "../lib/outreach-facts";
import { judgePendingReplies } from "../lib/reply-judgments";
import { OutreachFactsQuerySchema, OutreachFactsSyncSchema } from "../schemas";

const router = Router();

router.get("/", async (req: Request, res: Response) => {
  const parsed = OutreachFactsQuerySchema.safeParse(req.query);
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.issues.map((i) => i.message).join("; ") });
  }
  try {
    const { since, limit, orgId, brandId, email } = parsed.data;
    const page = await readOutreachFacts({ since: since ? Number(since) : 0, limit: limit ?? 500, orgId, brandId, email });
    return res.status(200).json(page);
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[instantly-service] outreach-facts read failed: ${message}`);
    return res.status(500).json({ error: message });
  }
});

router.post("/sync", async (req: Request, res: Response) => {
  const parsed = OutreachFactsSyncSchema.safeParse(req.body ?? {});
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.message });
  }
  try {
    const judged = await judgePendingReplies(parsed.data.judgeLimit ?? 100);
    const summary = await syncOutreachFacts({ sinceDays: parsed.data.sinceDays ?? null });
    return res.status(200).json({ judged, summary });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[instantly-service] outreach-facts sync failed: ${message}`);
    return res.status(500).json({ error: "outreach-facts sync failed", detail: message });
  }
});

export default router;
