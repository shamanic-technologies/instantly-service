/**
 * GET /internal/delivery-by-verdict — fleet delivery split by the verdict our
 * verifier held on the address when we first emailed it (lib/delivery-by-verdict.ts).
 *
 * Platform-scoped (no org), service-auth. Cached 10 min per replica with
 * refresh-ahead at 5 min: the consumer (features-service) sits it behind its own
 * 15-minute public cache. 503 `verification_history_not_synced` until the mirror
 * of apollo-service's verification history has reached the end of the feed once:
 * before that every address would read "never verified", which is not a fact.
 */
import { Router, Request, Response } from "express";

import { loadDeliveryByVerdict } from "../lib/delivery-by-verdict";
import { getOrSetCachedStats, statsCacheKey } from "../lib/stats-cache";
import { readMirrorCaughtUpAt, syncVerificationHistory } from "../lib/verification-history-mirror";

const router = Router();

export const DELIVERY_BY_VERDICT_TTL_MS = 10 * 60 * 1000;
export const DELIVERY_BY_VERDICT_REFRESH_AFTER_MS = 5 * 60 * 1000;

router.get("/delivery-by-verdict", async (_req: Request, res: Response) => {
  try {
    // One-row read, outside the cache so a 503 is never served from memory.
    if (!(await readMirrorCaughtUpAt())) {
      return res.status(503).json({
        code: "verification_history_not_synced",
        error: "The copy of the verifier's history has not reached its end yet; no split can be served without it.",
      });
    }
    const payload = await getOrSetCachedStats(
      statsCacheKey("delivery-by-verdict", {}),
      async () => {
        const caughtUpAt = await readMirrorCaughtUpAt();
        if (!caughtUpAt) throw new Error("verification history mirror lost its sync state");
        return loadDeliveryByVerdict(caughtUpAt);
      },
      DELIVERY_BY_VERDICT_TTL_MS,
      { refreshAfterMs: DELIVERY_BY_VERDICT_REFRESH_AFTER_MS },
    );
    return res.json(payload);
  } catch (error: any) {
    console.error(`[instantly-service] delivery-by-verdict failed: ${error?.message ?? error}`);
    return res.status(500).json({ error: error?.message ?? "delivery-by-verdict failed" });
  }
});

/** POST /internal/delivery-by-verdict/sync — run one mirror pass by hand (the worker runs it every 5 min). */
router.post("/delivery-by-verdict/sync", async (_req: Request, res: Response) => {
  try {
    return res.json(await syncVerificationHistory());
  } catch (error: any) {
    console.error(`[instantly-service] verification-history-mirror sync failed: ${error?.message ?? error}`);
    return res.status(500).json({ error: error?.message ?? "sync failed" });
  }
});

export default router;
