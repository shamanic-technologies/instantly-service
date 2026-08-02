/**
 * Provider-infrastructure routes (platform-scoped, no org) — issue #555.
 *
 * Mounted at `/internal/infra` behind `serviceAuth`, the same tier as
 * `/internal/audit`. PR 1 exposes the ingestion trigger only; the gold reads
 * (`/domains`, `/waste`, `/spend`) land in PR 2 once a first sync has shown what
 * the vendors actually return.
 */

import { Router, Request, Response } from "express";
import { syncProviderInfra } from "../lib/infra-sync";

const router = Router();

/**
 * POST /internal/infra/sync
 *
 * Polls Gandi (three organisations), Mailforge, Primeforge and Instantly DFY
 * into bronze, upserts silver, and flags rows a vendor stopped reporting.
 * Read-only against every vendor and free of metered spend, so it is safe to
 * run daily.
 *
 * 202 + background; watch logs for `infra-sync: done`. A single vendor failing
 * is counted in `failures` and does not stop the others; the run only throws
 * when every vendor failed.
 */
router.post("/sync", async (_req: Request, res: Response) => {
  const runId = crypto.randomUUID();
  res.status(202).json({ accepted: true, runId });
  console.log(`[infra] infra-sync: dispatched run=${runId}`);

  (async () => {
    const summary = await syncProviderInfra({
      method: "POST",
      path: "/internal/infra/sync",
    });
    console.log(`[infra] infra-sync: done run=${runId} ${JSON.stringify(summary)}`);
  })().catch((error: unknown) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[infra] infra-sync run=${runId} failed: ${message}`);
  });
});

export default router;
