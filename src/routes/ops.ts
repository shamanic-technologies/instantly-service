/**
 * Ops reads and syncs over the unified model (platform-scoped, no org).
 *
 * Mounted at `/internal/ops` behind `serviceAuth`, the same tier as
 * `/internal/audit`. PR 1 exposes the mailbox sync only; the gold reads land
 * once the silver objects exist.
 */

import { Router, Request, Response } from "express";
import { syncMailboxes } from "../lib/mailboxes-sync";

const router = Router();

/**
 * POST /internal/ops/mailboxes-sync
 *
 * Re-derives the address → real-mailbox grouping from the credential map,
 * `infra_mailboxes`, `infra_domains` and `instantly_accounts`, and upserts
 * `mailboxes` + `instantly_accounts.mailbox_login`. Also runs at the end of
 * every `POST /internal/infra/sync`. Synchronous — a few hundred rows.
 */
router.post("/mailboxes-sync", async (_req: Request, res: Response) => {
  try {
    const summary = await syncMailboxes({ method: "POST", path: "/internal/ops/mailboxes-sync" });
    res.json(summary);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[instantly-service] mailboxes-sync failed: ${message}`);
    res.status(500).json({ error: "mailboxes-sync failed", detail: message });
  }
});

export default router;
