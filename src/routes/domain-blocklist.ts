/**
 * Staff reads and actions on the sending-domain blocklists (DOMAIN-BLOCKLIST-1016,
 * lib/domain-blocklist*.ts). Platform-scoped (no org), service-auth.
 */
import { Router, Request, Response } from "express";

import { blocklistByKey, type BlocklistKey } from "../lib/domain-blocklist";
import {
  DOMAIN_BLOCKLIST_DEFAULT_DAYS,
  DOMAIN_BLOCKLIST_MAX_DAYS,
  loadDomainBlocklistRead,
} from "../lib/domain-blocklist-read";
import { recordRemovalRequest, runBlocklistCheck } from "../lib/domain-blocklist-sync";
import { deliverPendingBlocklistAlerts } from "../lib/domain-blocklist-alert";
import { fileDueRemovals } from "../lib/blocklist-removal";
import { loadOwnerContact } from "../lib/owner-identity-client";
import { getOrSetCachedStats, statsCacheKey, deleteCachedStatsWhere } from "../lib/stats-cache";

const router = Router();

export const DOMAIN_BLOCKLIST_READ_TTL_MS = 5 * 60 * 1000;

router.get("/domain-blocklist", async (req: Request, res: Response) => {
  const raw = req.query.days;
  const days = raw === undefined ? DOMAIN_BLOCKLIST_DEFAULT_DAYS : Number(raw);
  if (!Number.isInteger(days) || days < 1 || days > DOMAIN_BLOCKLIST_MAX_DAYS) {
    return res.status(400).json({ error: `days must be an integer 1..${DOMAIN_BLOCKLIST_MAX_DAYS}` });
  }
  try {
    const payload = await getOrSetCachedStats(
      statsCacheKey("domain-blocklist", { days }),
      () => loadDomainBlocklistRead(days),
      DOMAIN_BLOCKLIST_READ_TTL_MS,
    );
    return res.json(payload);
  } catch (error: any) {
    console.error(`[instantly-service] domain-blocklist read failed: ${error?.message ?? error}`);
    return res.status(500).json({ error: error?.message ?? "domain-blocklist read failed" });
  }
});

/** Run one full check now (the worker runs it daily), then deliver pending alerts. */
router.post("/domain-blocklist/check", async (_req: Request, res: Response) => {
  try {
    const summary = await runBlocklistCheck();
    const alerts = await deliverPendingBlocklistAlerts();
    deleteCachedStatsWhere((key) => key.startsWith("domain-blocklist"));
    return res.json({ ...summary, alerts });
  } catch (error: any) {
    console.error(`[instantly-service] domain-blocklist check failed: ${error?.message ?? error}`);
    return res.status(500).json({ error: error?.message ?? "domain-blocklist check failed" });
  }
});

/**
 * File every DUE automated removal request now (SURBL; one per listed domain per
 * week, the daily worker does the same after its check). Synchronous: answers
 * the per-outcome counts once every request is stored.
 */
router.post("/domain-blocklist/removals", async (_req: Request, res: Response) => {
  try {
    const summary = await fileDueRemovals({ loadContact: loadOwnerContact, spacingMs: 5_000 });
    deleteCachedStatsWhere((key) => key.startsWith("domain-blocklist"));
    return res.json(summary);
  } catch (error: any) {
    console.error(`[instantly-service] domain-blocklist removals failed: ${error?.message ?? error}`);
    return res.status(500).json({ error: error?.message ?? "removal requests failed" });
  }
});

/** Record a removal request a person filed on the list's own form. Append-only. */
router.post("/domain-blocklist/removal-requests", async (req: Request, res: Response) => {
  const body = (req.body ?? {}) as { domain?: unknown; list?: unknown; note?: unknown; requestedBy?: unknown };
  if (typeof body.domain !== "string" || body.domain.trim() === "") {
    return res.status(400).json({ error: "domain is required" });
  }
  if (typeof body.list !== "string" || !blocklistByKey(body.list)) {
    return res.status(400).json({ error: "list must be one of surbl, spamhaus_dbl, uribl" });
  }
  try {
    const row = await recordRemovalRequest({
      domain: body.domain,
      list: body.list as BlocklistKey,
      note: typeof body.note === "string" ? body.note : null,
      requestedBy: typeof body.requestedBy === "string" ? body.requestedBy : null,
    });
    deleteCachedStatsWhere((key) => key.startsWith("domain-blocklist"));
    return res.status(201).json(row);
  } catch (error: any) {
    console.error(`[instantly-service] domain-blocklist removal request failed: ${error?.message ?? error}`);
    return res.status(500).json({ error: error?.message ?? "could not record the removal request" });
  }
});

export default router;
