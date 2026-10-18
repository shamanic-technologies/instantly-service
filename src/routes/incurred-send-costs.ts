/**
 * POST /internal/incurred-send-costs — declare to runs-service what the sends
 * of 2026-08-24 → 2026-10-02 really cost, never charged (lib/incurred-send-costs-backfill.ts).
 *
 * Platform-scoped, service-auth, manual only. Body `{dryRun (default TRUE), limit}`:
 * dry run prices every item and writes nothing (synchronous); `dryRun:false`
 * answers 202 and applies in the background (log `incurred-send-costs: done`).
 * Idempotent: a re-run records nothing twice.
 */
import { Router, Request, Response } from "express";
import { runIncurredSendCostsBackfill } from "../lib/incurred-send-costs-backfill";

const router = Router();

router.post("/", async (req: Request, res: Response) => {
  const body = (req.body ?? {}) as { dryRun?: unknown; limit?: unknown };
  const dryRun = body.dryRun !== false;
  const limit = typeof body.limit === "number" && body.limit > 0 ? Math.floor(body.limit) : undefined;

  if (dryRun) {
    try {
      return res.json(await runIncurredSendCostsBackfill({ dryRun: true, limit }));
    } catch (error: any) {
      console.error(`[instantly-service] incurred-send-costs dry-run failed: ${error?.message ?? error}`);
      return res.status(500).json({ error: error?.message ?? "dry run failed" });
    }
  }

  res.status(202).json({ accepted: true, dryRun: false, limit: limit ?? null });
  runIncurredSendCostsBackfill({ dryRun: false, limit })
    .then((summary) => console.log(`[instantly-service] incurred-send-costs: done ${JSON.stringify(summary)}`))
    .catch((error) => console.error(`[instantly-service] incurred-send-costs: FAILED ${error?.message ?? error}`));
});

export default router;
