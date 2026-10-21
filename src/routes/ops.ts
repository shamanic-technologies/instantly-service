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
import { readSentPerPeriod, SENT_GRAINS, type SentGrain } from "../lib/ops/sent-per-period";
import { parseEntityScope } from "../lib/ops/entity-scope";
import { inScope, scopeIsFleet } from "../lib/ops/entity-scope";
import { decodeTestCursor, readPlacementTest, readPlacementTests } from "../lib/ops/placement-tests";
import { CAPACITY_HORIZON_DEFAULT_DAYS, CAPACITY_HORIZON_MAX_DAYS, readCapacityDaily } from "../lib/ops/capacity-daily";
import {
  LIFECYCLE_FILTERS,
  SCHEDULE_HORIZON_DEFAULT_DAYS,
  SCHEDULE_HORIZON_MAX_DAYS,
  loadLifecycleOf,
  readQueuedSteps,
  readSchedule,
  type LifecycleFilter,
} from "../lib/ops/schedule";
import { readPlacementAlerts, runPlacementAlert } from "../lib/placement-alert";
import { backfillReplyVerdicts, planReplyVerdicts, syncReplyVerdicts } from "../lib/reply-verdicts";

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
 * POST /internal/ops/reply-verdicts-sync
 *
 * One pass of the per-reply verdict projection (lib/reply-verdicts), over the
 * WHOLE history unless `{sinceDays}` bounds the event scan. Synchronous.
 */
router.post("/reply-verdicts-sync", async (req: Request, res: Response) => {
  const raw = (req.body ?? {}) as { sinceDays?: unknown };
  const sinceDays = raw.sinceDays === undefined ? null : Number(raw.sinceDays);
  if (sinceDays !== null && (!Number.isFinite(sinceDays) || sinceDays < 1)) {
    res.status(400).json({ error: "sinceDays must be a number >= 1" });
    return;
  }
  try {
    res.json(await syncReplyVerdicts({ sinceDays }));
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[instantly-service] reply-verdicts-sync failed: ${message}`);
    res.status(500).json({ error: "reply-verdicts-sync failed", detail: message });
  }
});

/**
 * POST /internal/ops/reply-verdicts-backfill
 *
 * Classify every stored reply no producer ever judged, recording the verdict in
 * BRONZE ONLY — no silver event, so no forward, escalation, rep call or
 * follow-up fires and the per-(campaign × lead) value does not move.
 * `{dryRun}` DEFAULTS TO TRUE: projects, then answers the plan (counts + cost)
 * synchronously. `dryRun:false` runs synchronously and returns the summary.
 */
router.post("/reply-verdicts-backfill", async (req: Request, res: Response) => {
  const raw = (req.body ?? {}) as { dryRun?: unknown; sinceDays?: unknown; limit?: unknown };
  const sinceDays = raw.sinceDays === undefined ? null : Number(raw.sinceDays);
  const limit = raw.limit === undefined ? null : Number(raw.limit);
  try {
    if (raw.dryRun !== false) {
      await syncReplyVerdicts({ sinceDays: null });
      res.json({ dryRun: true, plan: await planReplyVerdicts(sinceDays) });
      return;
    }
    const summary = await backfillReplyVerdicts({ sinceDays, limit });
    res.json({ dryRun: false, summary, plan: await planReplyVerdicts(sinceDays) });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[instantly-service] reply-verdicts-backfill failed: ${message}`);
    res.status(500).json({ error: "reply-verdicts-backfill failed", detail: message });
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

/**
 * GET /internal/ops/sent-per-period?grain=day|week|month&since=<ISO>&account=&mailbox=&domain=&groupBy=account|mailbox|domain
 * Emails SENT per UTC period, split by purpose (to leads / manual replies /
 * warmup / warmup replies / seeds) + distinct leads emailed. Every period from
 * the first send (or `since`) to the current one, zeros included; the current
 * one is flagged `inProgress`. Cached 60s per (grain, since).
 */
router.get("/sent-per-period", async (req: Request, res: Response) => {
  const grain = req.query.grain;
  if (typeof grain !== "string" || !(SENT_GRAINS as readonly string[]).includes(grain)) {
    res.status(400).json({ error: "grain is required and must be day|week|month" });
    return;
  }
  const rawSince = typeof req.query.since === "string" && req.query.since !== "" ? req.query.since : null;
  let since: string | null = null;
  if (rawSince !== null) {
    const d = new Date(rawSince);
    if (Number.isNaN(d.getTime())) {
      res.status(400).json({ error: "since must be an ISO timestamp" });
      return;
    }
    since = d.toISOString();
  }
  const parsedScope = parseEntityScope(req.query as Record<string, unknown>);
  if ("error" in parsedScope) {
    res.status(400).json({ error: parsedScope.error });
    return;
  }
  const { scope, groupBy } = parsedScope;
  try {
    res.json(
      await getOrSetCachedStats(
        `ops-sent-per-period:${grain}:${since ?? "all"}:${JSON.stringify(scope)}:${groupBy ?? "-"}`,
        () => readSentPerPeriod({ grain: grain as SentGrain, since, scope, groupBy }),
        // The fleet series keeps its 60 s; a scoped / grouped one is a drill-down
        // read, served for 10 min and refreshed in the background after 5.
        scopeIsFleet(scope) && !groupBy ? undefined : MONITORING_TTL_MS,
        scopeIsFleet(scope) && !groupBy ? {} : { refreshAfterMs: MONITORING_REFRESH_MS },
      ),
    );
  } catch (error) {
    fail(res, "sent-per-period", error);
  }
});

// ─── Monitoring reads (MAILBOX-MONITORING-1019) ─────────────────────────────

/**
 * The monitoring reads aggregate since inception (the capacity reconstruction
 * alone is ~38k as-of lookups): served for 10 min, refreshed in the background
 * after 5, so a dashboard never waits on a recompute after the first load.
 */
const MONITORING_TTL_MS = 10 * 60 * 1000;
const MONITORING_REFRESH_MS = 5 * 60 * 1000;

function intParam(raw: unknown, def: number, min: number, max: number): number | null {
  if (raw === undefined || raw === "") return def;
  const n = Number(raw);
  return Number.isInteger(n) && n >= min && n <= max ? n : null;
}

/** `?lifecycle=` narrows a read to addresses whose CURRENT lifecycle status is that one (absent = every address). */
function lifecycleOr400(req: Request, res: Response): { lifecycle: LifecycleFilter | null } | null {
  const v = req.query.lifecycle;
  if (v === undefined || v === "") return { lifecycle: null };
  if (typeof v !== "string" || !(LIFECYCLE_FILTERS as readonly string[]).includes(v)) {
    res.status(400).json({ error: `lifecycle must be one of ${LIFECYCLE_FILTERS.join("|")}` });
    return null;
  }
  return { lifecycle: v as LifecycleFilter };
}

function scopeOr400(req: Request, res: Response) {
  const parsed = parseEntityScope(req.query as Record<string, unknown>);
  if ("error" in parsed) {
    res.status(400).json({ error: parsed.error });
    return null;
  }
  return parsed;
}

/** GET /internal/ops/placement-tests?account=&mailbox=&domain=&limit=&cursor= — tests, newest first, counts per inbox provider. */
router.get("/placement-tests", async (req: Request, res: Response) => {
  const parsed = scopeOr400(req, res);
  if (!parsed) return;
  const limit = intParam(req.query.limit, 50, 1, 500);
  if (limit === null) {
    res.status(400).json({ error: "limit must be an integer between 1 and 500" });
    return;
  }
  const rawCursor = typeof req.query.cursor === "string" && req.query.cursor !== "" ? req.query.cursor : null;
  const cursor = rawCursor ? decodeTestCursor(rawCursor) : null;
  if (rawCursor && !cursor) {
    res.status(400).json({ error: "cursor is not one this read issued" });
    return;
  }
  try {
    res.json({ scope: parsed.scope, ...(await readPlacementTests({ scope: parsed.scope, limit, cursor })) });
  } catch (error) {
    fail(res, "placement-tests", error);
  }
});

/** GET /internal/ops/placement-tests/:testId?account=&mailbox=&domain= — one test, seed by seed, per inbox provider. */
router.get("/placement-tests/:testId", async (req: Request, res: Response) => {
  const parsed = scopeOr400(req, res);
  if (!parsed) return;
  try {
    const detail = await readPlacementTest(String(req.params.testId), parsed.scope);
    if (!detail) {
      res.status(404).json({ error: "placement_test_not_found" });
      return;
    }
    res.json(detail);
  } catch (error) {
    fail(res, "placement-test", error);
  }
});

/** GET /internal/ops/capacity-daily?account=&mailbox=&domain=&groupBy=&horizonDays= — capacity per day since inception + the ramp ahead. Cached 60s. */
router.get("/capacity-daily", async (req: Request, res: Response) => {
  const parsed = scopeOr400(req, res);
  if (!parsed) return;
  const horizonDays = intParam(req.query.horizonDays, CAPACITY_HORIZON_DEFAULT_DAYS, 0, CAPACITY_HORIZON_MAX_DAYS);
  if (horizonDays === null) {
    res.status(400).json({ error: `horizonDays must be an integer 0..${CAPACITY_HORIZON_MAX_DAYS}` });
    return;
  }
  try {
    res.json(
      await getOrSetCachedStats(
        `ops-capacity-daily:${JSON.stringify(parsed.scope)}:${parsed.groupBy ?? "-"}:${horizonDays}`,
        () => readCapacityDaily({ scope: parsed.scope, groupBy: parsed.groupBy, horizonDays }),
        MONITORING_TTL_MS,
        { refreshAfterMs: MONITORING_REFRESH_MS },
      ),
    );
  } catch (error) {
    fail(res, "capacity-daily", error);
  }
});

/** GET /internal/ops/schedule?account=&mailbox=&domain=&groupBy=&horizonDays=&lifecycle= — per day from today: queued steps at their theoretical date, planned warmups and seed tests. Cached 60s. */
router.get("/schedule", async (req: Request, res: Response) => {
  const parsed = scopeOr400(req, res);
  if (!parsed) return;
  const lc = lifecycleOr400(req, res);
  if (!lc) return;
  const horizonDays = intParam(req.query.horizonDays, SCHEDULE_HORIZON_DEFAULT_DAYS, 0, SCHEDULE_HORIZON_MAX_DAYS);
  if (horizonDays === null) {
    res.status(400).json({ error: `horizonDays must be an integer 0..${SCHEDULE_HORIZON_MAX_DAYS}` });
    return;
  }
  try {
    res.json(
      await getOrSetCachedStats(
        `ops-schedule:${JSON.stringify(parsed.scope)}:${parsed.groupBy ?? "-"}:${horizonDays}:${lc.lifecycle ?? "-"}`,
        () => readSchedule({ scope: parsed.scope, groupBy: parsed.groupBy, horizonDays, lifecycle: lc.lifecycle }),
        MONITORING_TTL_MS,
        { refreshAfterMs: MONITORING_REFRESH_MS },
      ),
    );
  } catch (error) {
    fail(res, "schedule", error);
  }
});

/** GET /internal/ops/queued-steps?account=&mailbox=&domain=&lifecycle=&limit=&offset=&overdue=true|false — every queued step at its theoretical date, oldest first. */
router.get("/queued-steps", async (req: Request, res: Response) => {
  const parsed = scopeOr400(req, res);
  if (!parsed) return;
  const lc = lifecycleOr400(req, res);
  if (!lc) return;
  const limit = intParam(req.query.limit, 100, 1, 1000);
  const offset = intParam(req.query.offset, 0, 0, 10_000_000);
  if (limit === null || offset === null) {
    res.status(400).json({ error: "limit must be 1..1000 and offset >= 0" });
    return;
  }
  const overdue = req.query.overdue;
  if (overdue !== undefined && overdue !== "true" && overdue !== "false") {
    res.status(400).json({ error: "overdue must be true|false" });
    return;
  }
  try {
    const lifecycleOf = lc.lifecycle
      ? await getOrSetCachedStats("ops-lifecycle-of", () => loadLifecycleOf(), MONITORING_TTL_MS, { refreshAfterMs: MONITORING_REFRESH_MS })
      : null;
    const { loginOf, steps } = await getOrSetCachedStats("ops-queued-steps", () => readQueuedSteps(), MONITORING_TTL_MS, {
      refreshAfterMs: MONITORING_REFRESH_MS,
    });
    const filtered = steps.filter(
      (s) =>
        inScope(parsed.scope, s.account, loginOf.get(s.account) ?? s.mailbox) &&
        (!lifecycleOf || (lifecycleOf.get(s.account.toLowerCase()) ?? null) === lc.lifecycle) &&
        (overdue === undefined || s.overdue === (overdue === "true")),
    );
    res.json({
      scope: parsed.scope,
      ...(lc.lifecycle ? { lifecycle: lc.lifecycle } : {}),
      total: filtered.length,
      offset,
      steps: filtered.slice(offset, offset + limit),
      nextOffset: offset + limit < filtered.length ? offset + limit : null,
    });
  } catch (error) {
    fail(res, "queued-steps", error);
  }
});

/** GET /internal/ops/placement-alerts?days= — the daily evaluation behind the inbox-placement alert, its bar and sources, the episodes. */
router.get("/placement-alerts", async (req: Request, res: Response) => {
  const days = intParam(req.query.days, 30, 1, 400);
  if (days === null) {
    res.status(400).json({ error: "days must be an integer 1..400" });
    return;
  }
  try {
    res.json(await getOrSetCachedStats(`ops-placement-alerts:${days}`, () => readPlacementAlerts(days), MONITORING_TTL_MS, {
      refreshAfterMs: MONITORING_REFRESH_MS,
    }));
  } catch (error) {
    fail(res, "placement-alerts", error);
  }
});

/** POST /internal/ops/placement-alerts/run {dryRun (default true)} — one pass of the alert now. */
router.post("/placement-alerts/run", async (req: Request, res: Response) => {
  const dryRun = (req.body ?? {}).dryRun !== false;
  try {
    res.json({ dryRun, ...(await runPlacementAlert({ dryRun })) });
  } catch (error) {
    fail(res, "placement-alerts run", error);
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
