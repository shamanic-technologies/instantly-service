/**
 * Staff read: lead sources ranked by the bounces, refusals and blocklist hits
 * their people produce (SOURCE-YIELD-1016, lib/lead-source-yield*.ts).
 * Platform-scoped (no org), service-auth, read-only. Cached 10 min per replica
 * with refresh-ahead at 5 min.
 */
import { Router, Request, Response } from "express";

import {
  LEAD_SOURCE_YIELD_DEFAULT_DAYS,
  LEAD_SOURCE_YIELD_MAX_DAYS,
  loadLeadSourceYield,
} from "../lib/lead-source-yield-read";
import { syncLeadProvenance } from "../lib/lead-provenance-sync";
import { deleteCachedStatsWhere, getOrSetCachedStats, statsCacheKey } from "../lib/stats-cache";

const router = Router();

export const LEAD_SOURCE_YIELD_TTL_MS = 10 * 60 * 1000;
export const LEAD_SOURCE_YIELD_REFRESH_AFTER_MS = 5 * 60 * 1000;

router.get("/lead-source-yield", async (req: Request, res: Response) => {
  const raw = req.query.days;
  const days = raw === undefined ? LEAD_SOURCE_YIELD_DEFAULT_DAYS : Number(raw);
  if (!Number.isInteger(days) || days < 1 || days > LEAD_SOURCE_YIELD_MAX_DAYS) {
    return res.status(400).json({ error: `days must be an integer 1..${LEAD_SOURCE_YIELD_MAX_DAYS}` });
  }
  try {
    const payload = await getOrSetCachedStats(
      statsCacheKey("lead-source-yield", { days }),
      () => loadLeadSourceYield(days),
      LEAD_SOURCE_YIELD_TTL_MS,
      { refreshAfterMs: LEAD_SOURCE_YIELD_REFRESH_AFTER_MS },
    );
    return res.json(payload);
  } catch (error: any) {
    console.error(`[instantly-service] lead-source-yield read failed: ${error?.message ?? error}`);
    return res.status(500).json({ error: error?.message ?? "lead-source-yield read failed" });
  }
});

/** Run one provenance pass now (the worker runs it every 30 min). */
router.post("/lead-source-yield/sync", async (_req: Request, res: Response) => {
  try {
    const summary = await syncLeadProvenance();
    deleteCachedStatsWhere((key) => key.startsWith("lead-source-yield"));
    return res.json(summary);
  } catch (error: any) {
    console.error(`[instantly-service] lead-provenance-sync failed: ${error?.message ?? error}`);
    return res.status(500).json({ error: error?.message ?? "lead-provenance sync failed" });
  }
});

export default router;
