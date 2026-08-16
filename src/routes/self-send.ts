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
import { runPoll } from "../lib/self-send/imap-poller";

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

  // Poll the mailboxes first, awaited inside the same background run, so a
  // reply that landed since the last sweep stops its sequence BEFORE we select
  // what to send. Two separate 202 endpoints could not guarantee that ordering.
  runDispatch({ limit, pollFirst: true }).catch((error) => {
    console.error(
      `[instantly-service] self-send-dispatch failed: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  });
});

/**
 * Read every self-send mailbox and ingest what came back.
 *
 * Gated on the SAME switch as dispatch, deliberately. The two are one feature:
 * sending without reading would keep emailing people who have already replied,
 * which is worse than not sending at all.
 */
router.post("/poll", async (_req: Request, res: Response) => {
  if (!isSelfSendDispatchEnabled()) {
    res.status(409).json({
      error: "Self-send is disabled (SELF_SEND_DISPATCH_ENABLED is not 'true')",
    });
    return;
  }

  res.status(202).json({ accepted: true });

  runPoll().catch((error) => {
    console.error(
      `[instantly-service] self-send-poll failed: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  });
});

export default router;
