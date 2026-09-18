/**
 * Ops reads and syncs over the unified model (platform-scoped, no org).
 *
 * Mounted at `/internal/ops` behind `serviceAuth`, the same tier as
 * `/internal/audit`. PR 1 exposes the mailbox sync only; the gold reads land
 * once the silver objects exist.
 */

import { Router, Request, Response } from "express";
import { syncMailboxes } from "../lib/mailboxes-sync";
import { syncMessages } from "../lib/messages-sync";
import { syncDomainDns } from "../lib/domain-dns-sync";

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

/**
 * POST /internal/ops/messages-sync
 *
 * Re-reads a window of every message source (`sinceDays`, default 3) and
 * upserts the `messages` projection. Also runs on an in-process interval.
 * A large `sinceDays` is the backfill; the unique source index makes any
 * overlap a no-op.
 */
router.post("/messages-sync", async (req: Request, res: Response) => {
  const raw = (req.body ?? {}) as { sinceDays?: unknown };
  const sinceDays = raw.sinceDays === undefined ? undefined : Number(raw.sinceDays);
  if (sinceDays !== undefined && (!Number.isFinite(sinceDays) || sinceDays < 1)) {
    res.status(400).json({ error: "sinceDays must be a number >= 1" });
    return;
  }
  try {
    const summary = await syncMessages({ sinceDays });
    res.json(summary);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[instantly-service] messages-sync failed: ${message}`);
    res.status(500).json({ error: "messages-sync failed", detail: message });
  }
});

/**
 * POST /internal/ops/dns-sync
 *
 * Photographs SPF / DMARC / DKIM (probed selectors) / MX for every domain we
 * own or send from into `domain_dns_raw` (append-only). Also runs at the end
 * of every `POST /internal/infra/sync`. Synchronous — ~100 domains at 8 wide.
 */
router.post("/dns-sync", async (_req: Request, res: Response) => {
  try {
    const summary = await syncDomainDns();
    res.json(summary);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[instantly-service] dns-sync failed: ${message}`);
    res.status(500).json({ error: "dns-sync failed", detail: message });
  }
});

export default router;
