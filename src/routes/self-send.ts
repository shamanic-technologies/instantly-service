/**
 * Self-send ops trigger (platform-scoped, no org — same `serviceAuth` tier as
 * `/internal/audit`).
 *
 * Kept behind an explicit env kill-switch on top of the per-account
 * `send_transport` flag. Two independent switches is not belt-and-braces: the
 * account column decides WHICH mailboxes are eligible, while this one can stop
 * the whole sweep in a single env change without touching any account row, which
 * is what you want at 3am when something looks wrong. Mirrors the existing
 * `RETRY_STUCK_WORKER_ENABLED` / `PLACEMENT_TESTS_ENABLED` convention.
 */

import { Router, type Request, type Response } from "express";

import { runDispatch } from "../lib/self-send/dispatch-worker";

const router = Router();

/** Exactly `"true"`; anything else, including unset, is OFF. */
export function isSelfSendDispatchEnabled(): boolean {
  return process.env.SELF_SEND_DISPATCH_ENABLED === "true";
}

router.post("/dispatch", async (req: Request, res: Response) => {
  if (!isSelfSendDispatchEnabled()) {
    res.status(409).json({
      error: "Self-send dispatch is disabled (SELF_SEND_DISPATCH_ENABLED is not 'true')",
    });
    return;
  }

  const limit =
    typeof req.body?.limit === "number" && Number.isFinite(req.body.limit)
      ? req.body.limit
      : undefined;

  // 202 + background, like every other ops sweep here: a full run walks the
  // fleet and talks to an external mail server, which is far past any sane
  // request timeout. Watch the logs for `self-send-dispatch: done`.
  res.status(202).json({ accepted: true });

  runDispatch({ limit }).catch((error) => {
    console.error(
      `[instantly-service] self-send-dispatch failed: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  });
});

export default router;
