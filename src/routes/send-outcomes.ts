/**
 * POST /internal/send-outcomes/backfill — declare to runs-service's ledger the
 * `message_sent` of every PAST email-send-step run, dated when the email left
 * (lib/send-outcomes.ts, COST-BACKFILL-1023-INSTANTLY).
 *
 * Platform-scoped, service-auth, manual only. Body `{dryRun (default TRUE), limit, redate}`
 * (`redate:true` re-states the holds ALREADY declared so runs-service moves each
 * occurrence back to the send time; one already on time is left as is).
 * dry run plans every page and writes nothing (synchronous); `dryRun:false`
 * answers 202 and declares in the background (log `send-outcomes-backfill: done`).
 * Idempotent and resumable: a re-run declares nothing twice.
 */
import { Router, Request, Response } from "express";
import { runSendOutcomesBackfill } from "../lib/send-outcomes";

const router = Router();

router.post("/backfill", async (req: Request, res: Response) => {
  const body = (req.body ?? {}) as { dryRun?: unknown; limit?: unknown; redate?: unknown };
  const dryRun = body.dryRun !== false;
  const redate = body.redate === true;
  const limit = typeof body.limit === "number" && body.limit > 0 ? Math.floor(body.limit) : undefined;

  if (dryRun) {
    try {
      return res.json(await runSendOutcomesBackfill({ dryRun: true, limit, redate }));
    } catch (error: any) {
      console.error(`[instantly-service] send-outcomes-backfill dry-run failed: ${error?.message ?? error}`);
      return res.status(500).json({ error: error?.message ?? "dry run failed" });
    }
  }

  res.status(202).json({ accepted: true, dryRun: false, redate, limit: limit ?? null });
  runSendOutcomesBackfill({ dryRun: false, limit, redate })
    .then((summary) => console.log(`[instantly-service] send-outcomes-backfill: done ${JSON.stringify(summary)}`))
    .catch((error) => console.error(`[instantly-service] send-outcomes-backfill: FAILED ${error?.message ?? error}`));
});

export default router;
