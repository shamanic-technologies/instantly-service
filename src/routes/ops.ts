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
import { lifecycleRules } from "../lib/ops/lifecycle-rules";
import {
  readAddresses,
  readDomains,
  readInfra,
  readMailboxes,
  readMessageBody,
  readMessages,
  readThreads,
  type ThreadFilters,
} from "../lib/ops/reads";
import { getOrSetCachedStats } from "../lib/stats-cache";

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

// ─── Gold reads ──────────────────────────────────────────────────────────────

function fail(res: Response, what: string, error: unknown): void {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`[instantly-service] ops ${what} failed: ${message}`);
  res.status(500).json({ error: `${what} failed`, detail: message });
}

/** GET /internal/ops/lifecycle-rules — the constants `deriveLifecycle` decides on, as data. */
router.get("/lifecycle-rules", (_req: Request, res: Response) => {
  res.json(lifecycleRules());
});

/** GET /internal/ops/domains — one row per (provider, domain): purchase, renewal, DNS, delivery, addresses, cost. Cached 60s. */
router.get("/domains", async (_req: Request, res: Response) => {
  try {
    res.json(await getOrSetCachedStats("ops-domains", () => readDomains()));
  } catch (error) {
    fail(res, "domains", error);
  }
});

/** GET /internal/ops/mailboxes — one row per real mailbox: vendor, pool, subscription, dates, addresses, cap + ramp, volume, delivery, cost. Cached 60s. */
router.get("/mailboxes", async (_req: Request, res: Response) => {
  try {
    res.json(await getOrSetCachedStats("ops-mailboxes", () => readMailboxes()));
  } catch (error) {
    fail(res, "mailboxes", error);
  }
});

/** GET /internal/ops/addresses — the account-health rows plus mailbox, transport, evidence expiry, next seed test, ramp, volume, lifecycle history. Cached 60s. */
router.get("/addresses", async (_req: Request, res: Response) => {
  try {
    res.json(
      await getOrSetCachedStats("ops-addresses", () =>
        readAddresses({ method: "GET", path: "/internal/ops/addresses" }),
      ),
    );
  } catch (error) {
    fail(res, "addresses", error);
  }
});

/** GET /internal/ops/infra — the fleet and each pool: capacity, lifecycle counts, queue, volume, placement, exclusions. Cached 60s. */
router.get("/infra", async (_req: Request, res: Response) => {
  try {
    res.json(
      await getOrSetCachedStats("ops-infra", () => readInfra({ method: "GET", path: "/internal/ops/infra" })),
    );
  } catch (error) {
    fail(res, "infra", error);
  }
});

const DIRECTIONS = new Set(["in", "out"]);

function parseListFilters(q: Record<string, unknown>): ThreadFilters | { error: string } {
  const limit = Number(q.limit);
  if (!Number.isInteger(limit) || limit < 1 || limit > 500) {
    return { error: "limit is required and must be an integer between 1 and 500" };
  }
  const str = (k: string) => (typeof q[k] === "string" && (q[k] as string) !== "" ? (q[k] as string) : undefined);
  const direction = str("direction");
  if (direction !== undefined && !DIRECTIONS.has(direction)) return { error: "direction must be in|out" };
  for (const k of ["since", "until"]) {
    const v = str(k);
    if (v !== undefined && Number.isNaN(new Date(v).getTime())) return { error: `${k} must be an ISO timestamp` };
  }
  const hasInbound = str("hasInbound");
  if (hasInbound !== undefined && hasInbound !== "true" && hasInbound !== "false") {
    return { error: "hasInbound must be true|false" };
  }
  return {
    limit,
    cursor: str("cursor") ?? null,
    kind: str("kind"),
    account: str("account"),
    mailbox: str("mailbox"),
    domain: str("domain"),
    counterparty: str("counterparty"),
    orgId: str("orgId"),
    campaignId: str("campaignId"),
    direction: direction as "in" | "out" | undefined,
    since: str("since"),
    until: str("until"),
    placement: str("placement"),
    hasInbound: hasInbound === undefined ? undefined : hasInbound === "true",
  };
}

/** GET /internal/ops/threads?limit=&cursor=&… — one row per thread, newest activity first. `limit` is required. */
router.get("/threads", async (req: Request, res: Response) => {
  const parsed = parseListFilters(req.query as Record<string, unknown>);
  if ("error" in parsed) {
    res.status(400).json({ error: parsed.error });
    return;
  }
  try {
    res.json(await readThreads(parsed));
  } catch (error) {
    fail(res, "threads", error);
  }
});

/** GET /internal/ops/messages?limit=&threadId=&… — one row per message, newest first. `limit` is required. */
router.get("/messages", async (req: Request, res: Response) => {
  const parsed = parseListFilters(req.query as Record<string, unknown>);
  if ("error" in parsed) {
    res.status(400).json({ error: parsed.error });
    return;
  }
  const threadId = typeof req.query.threadId === "string" && req.query.threadId !== "" ? req.query.threadId : undefined;
  const { hasInbound: _h, ...rest } = parsed;
  try {
    res.json(await readMessages({ ...rest, threadId }));
  } catch (error) {
    fail(res, "messages", error);
  }
});

/** GET /internal/ops/messages/:id/body — the body from the bronze row the message came from. */
router.get("/messages/:id/body", async (req: Request, res: Response) => {
  try {
    const body = await readMessageBody(String(req.params.id));
    if (!body) {
      res.status(404).json({ error: "message_not_found" });
      return;
    }
    res.json(body);
  } catch (error) {
    fail(res, "message body", error);
  }
});

export default router;
