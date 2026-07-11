import { Router, Request, Response } from "express";
import { sql } from "drizzle-orm";
import { db } from "../db";
import { listAccounts } from "../lib/instantly-client";
import { resolvePlatformInstantlyApiKey } from "../lib/key-client";
import {
  computeCapacitySummary,
  projectDailySchedule,
  type PendingLead,
} from "../lib/sending-forecast";
import { buildAccountHealth } from "../lib/account-health";
import {
  snapshotAccounts,
  reconcileLifecycle,
  fetchLifecycleByEmail,
} from "../lib/account-lifecycle-sync";
import { fetchCapacityHistory } from "../lib/capacity-history";
import { syncInProductionDailyLimit } from "../lib/sync-daily-limit";
import {
  fetchSentTodayByAccount,
  fetchSentYesterdayByAccount,
  fetchQueueSizeByAccount,
  fetchQueueBreakdownByAccount,
} from "../lib/account-sending-stats";
import {
  syncPlacement,
  ensurePlacementSchedule,
  runOneTimeFleetPlacementTest,
  fetchLatestPlacementByAccount,
  fetchPlacementHistory,
  isPlacementSchedulingEnabled,
} from "../lib/placement-sync";
import {
  buildReconciliation,
  isSnapshotStale,
  type LocalReconcileCounts,
} from "../lib/reconcile-audit";
import {
  readInstantlySnapshot,
  refreshInstantlySnapshot,
  maybeTriggerRefresh,
} from "../lib/reconcile-snapshot";
import { getOrSetCachedStats, statsCacheKey } from "../lib/stats-cache";

const router = Router();

/**
 * Load every active-campaign lead that still carries un-sent (provisioned)
 * sequence steps. A `sequence_costs` row is `provisioned` until its
 * `email_sent` webhook actualizes it, so provisioned steps on a live campaign
 * are exactly the future scheduled sends. Gated to genuinely-live campaigns
 * (`status='active'` and a non-terminal `delivery_status`) so a replied /
 * bounced / paused lead whose holds have not yet been reconciled is excluded.
 *
 * `stepDelays` carries the REAL per-step `delay` (calendar days) from the
 * campaign's LATEST bronze sequence config (config-ordered 0-based, same path +
 * indexing the per-account queue breakdown uses:
 * `payload->'sequences'->0->'steps'->i->>'delay'`). This makes the forecast's
 * inter-step cadence identical to the queue-bucket projection — one cadence
 * source of truth. A campaign whose config is absent yields an empty array; the
 * pure projection then falls back per-gap to `STEP_GAP_CALENDAR_DAYS`.
 */
async function loadPendingLeads(): Promise<PendingLead[]> {
  const result = await db.execute(sql`
    WITH pending AS (
      SELECT
        sc.campaign_id,
        sc.lead_email,
        ARRAY_AGG(DISTINCT sc.step) FILTER (WHERE sc.status = 'provisioned') AS provisioned_steps,
        MAX(sc.step) FILTER (WHERE sc.status = 'actual') AS last_sent_step,
        MAX(sc.updated_at) FILTER (WHERE sc.status = 'actual') AS last_sent_at,
        (
          SELECT MIN(c.instantly_campaign_id)
          FROM instantly_campaigns c
          WHERE c.lead_email = sc.lead_email
            AND c.campaign_id IS NOT DISTINCT FROM sc.campaign_id
            AND c.status = 'active'
            AND c.delivery_status IN ('contacted', 'sent')
        ) AS instantly_campaign_id
      FROM sequence_costs sc
      WHERE EXISTS (
        SELECT 1 FROM instantly_campaigns c
        WHERE c.lead_email = sc.lead_email
          AND c.campaign_id IS NOT DISTINCT FROM sc.campaign_id
          AND c.status = 'active'
          AND c.delivery_status IN ('contacted', 'sent')
      )
      GROUP BY sc.campaign_id, sc.lead_email
      HAVING COUNT(*) FILTER (WHERE sc.status = 'provisioned') > 0
    )
    SELECT
      p.provisioned_steps AS "provisionedSteps",
      p.last_sent_step AS "lastSentStep",
      p.last_sent_at AS "lastSentAt",
      cfg.step_delays AS "stepDelays"
    FROM pending p
    LEFT JOIN LATERAL (
      -- Emit the per-step delays as a JSONB array, NOT a Postgres numeric[]
      -- (the ARRAY(...) form). node-postgres reliably parses jsonb into a JS
      -- array, but returns a numeric array as its raw text (brace form) - a
      -- string - which blew up rawDelays.map in the mapper below (prod 500).
      -- Picks the LATEST config row (unchanged), then builds the array from its
      -- steps; a null / non-array steps yields '[]' (never throws, never fabricates).
      SELECT COALESCE((
        SELECT jsonb_agg((elem->>'delay')::numeric ORDER BY t.ord)
        FROM jsonb_array_elements(
          CASE WHEN jsonb_typeof(r.payload->'sequences'->0->'steps') = 'array'
               THEN r.payload->'sequences'->0->'steps'
               ELSE '[]'::jsonb END
        ) WITH ORDINALITY AS t(elem, ord)
      ), '[]'::jsonb) AS step_delays
      FROM instantly_campaigns_config_raw r
      WHERE r.instantly_campaign_id = p.instantly_campaign_id
      ORDER BY r.fetched_at DESC
      LIMIT 1
    ) cfg ON true
  `);

  const rows = Array.isArray(result) ? result : ((result as any).rows ?? []);
  return rows.map((r: Record<string, unknown>): PendingLead => {
    const provisionedSteps = (r.provisionedSteps as number[] | null) ?? [];
    const lastSentStep =
      r.lastSentStep === null || r.lastSentStep === undefined
        ? null
        : Number(r.lastSentStep);
    const lastSentAt = r.lastSentAt ? new Date(r.lastSentAt as string) : null;
    // stepDelays now arrives as a parsed JSONB array. Guard against any
    // non-array shape (defensive — a mis-parse must never 500 the whole fleet
    // forecast; an unusable value degrades that one lead to the per-gap
    // STEP_GAP fallback, never crashes).
    const raw = r.stepDelays;
    const rawDelays = Array.isArray(raw) ? (raw as (number | string | null)[]) : [];
    const stepDelays = rawDelays.map((d) =>
      d === null || d === undefined ? null : Number(d),
    );
    return {
      provisionedSteps: provisionedSteps.map((s) => Number(s)),
      lastSentStep,
      lastSentAt,
      stepDelays,
    };
  });
}

/**
 * GET /internal/audit/sending-forecast
 *
 * Platform-scoped (no org). Returns the fleet's available daily capacity AND a
 * per-day projection of upcoming scheduled send volume from today forward.
 * Fails loud (500) on any missing source — no silent zero fallbacks.
 */
router.get("/sending-forecast", async (_req: Request, res: Response) => {
  try {
    // Wrap the whole computation in the 60s stats cache. This route
    // live-aggregates over the cost ledger (loadPendingLeads: a fleet-wide
    // scan + a per-lead config LATERAL) AND makes a live paginated listAccounts
    // call to Instantly — all uncached. email-gateway retries the SAME request
    // on its ~10s AbortSignal timeout, and the abort does NOT cancel the
    // server-side query, so each retry piled another identical heavy
    // aggregation onto the pool (max 20) → saturation → the timeout flood that
    // 500'd the staff forecast page (same failure class as /orgs/status,
    // v0.57.2). The in-flight dedup collapses the retry storm to ONE loader run;
    // the short TTL serves subsequent polls instantly. Platform-scoped, no
    // params → a single fixed key. Fail-loud preserved: a loader throw rejects
    // (nothing cached) and still 500s below.
    const payload = await getOrSetCachedStats(
      statsCacheKey("sending-forecast", {}),
      async () => {
        const asOf = new Date();

        const apiKey = await resolvePlatformInstantlyApiKey({
          method: "GET",
          path: "/internal/audit/sending-forecast",
        });
        const [accounts, lifecycleByEmail] = await Promise.all([
          listAccounts(apiKey),
          fetchLifecycleByEmail(),
        ]);
        const capacity = computeCapacitySummary(accounts, lifecycleByEmail);

        const pendingLeads = await loadPendingLeads();
        const days = projectDailySchedule(pendingLeads, asOf);

        return {
          asOf: asOf.toISOString(),
          dailyCapacity: capacity.dailyCapacity,
          healthyAccountCount: capacity.healthyAccountCount,
          totalAccountCount: capacity.totalAccountCount,
          blockedDomainCount: capacity.blockedDomainCount,
          days,
        };
      },
    );

    res.json(payload);
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[audit] sending-forecast failed: ${message}`);
    res.status(500).json({ error: message });
  }
});

/**
 * GET /internal/audit/account-health
 *
 * Platform-scoped (no org). Returns per sending account its deliverability
 * health: identity (email/domain), sending config (status/warmupScore/
 * dailyLimit), and lifecycle state (lifecycleStatus/Reason/UpdatedAt + blocked/
 * blockReason from the SAME auto-derived lifecycle the live send path reads —
 * send-eligible ⇔ lifecycle_status == 'in_production').
 *
 * `inboxPlacement` is null for every account: the Instantly V2 API does not
 * expose inbox placement as a per-account property (it exists only as
 * test-scoped, subscription-gated, point-in-time inbox-placement-test results —
 * see lib/account-health.ts). Never fabricated.
 *
 * `sentToday` / `queueSize` are derived from OUR silver + cost-hold data
 * (Instantly's account object exposes neither) — see lib/account-sending-stats.ts.
 * `accountType` maps Instantly's provider_code (google/microsoft/imap).
 *
 * Fails loud (500) on any missing source; no silent fallbacks.
 */
router.get("/account-health", async (_req: Request, res: Response) => {
  try {
    const asOf = new Date();

    const apiKey = await resolvePlatformInstantlyApiKey({
      method: "GET",
      path: "/internal/audit/account-health",
    });
    // Account list (Instantly) + latest placement, sent-today, and queue-size
    // per account (our silver + cost holds) run independently — parallelize.
    // Placement/sent/queue are best-effort per contract (null/0 when absent); a
    // live account list is required (fail loud).
    const [
      accounts,
      placementByEmail,
      sentTodayByEmail,
      sentYesterdayByEmail,
      queueSizeByEmail,
      queueBreakdownByEmail,
      lifecycleByEmail,
    ] = await Promise.all([
      listAccounts(apiKey),
      fetchLatestPlacementByAccount(),
      fetchSentTodayByAccount(),
      fetchSentYesterdayByAccount(),
      fetchQueueSizeByAccount(),
      fetchQueueBreakdownByAccount(asOf),
      fetchLifecycleByEmail(),
    ]);

    res.json({
      asOf: asOf.toISOString(),
      accounts: buildAccountHealth(
        accounts,
        placementByEmail,
        sentTodayByEmail,
        queueSizeByEmail,
        lifecycleByEmail,
        sentYesterdayByEmail,
        queueBreakdownByEmail,
      ),
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[audit] account-health failed: ${message}`);
    res.status(500).json({ error: message });
  }
});

/**
 * GET /internal/audit/account-health/history?email=<sender>
 *
 * Platform-scoped. Per-account inbox-placement history (one blended entry per
 * test, newest first) from our silver placement results. Empty array when the
 * account has never been in a test. `email` query param is required.
 */
router.get("/account-health/history", async (req: Request, res: Response) => {
  try {
    const email = typeof req.query.email === "string" ? req.query.email.trim() : "";
    if (!email) {
      return res.status(400).json({ error: "email query param is required" });
    }
    const history = await fetchPlacementHistory(email);
    res.json({ email, history });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[audit] account-health/history failed: ${message}`);
    res.status(500).json({ error: message });
  }
});

/**
 * GET /internal/audit/capacity-history?days=N
 *
 * Platform-scoped. Reconstructs the fleet's `in_production` daily capacity for
 * each of the last N days (default 30, clamped 1-365) from the append-only Bronze
 * layers (lifecycle events + account snapshots). One point per UTC day.
 */
router.get("/capacity-history", async (req: Request, res: Response) => {
  try {
    const raw = typeof req.query.days === "string" ? parseInt(req.query.days, 10) : NaN;
    const days = Number.isFinite(raw) && raw > 0 ? raw : 30;
    const series = await fetchCapacityHistory(days);
    res.json({ series });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[audit] capacity-history failed: ${message}`);
    res.status(500).json({ error: message });
  }
});

/**
 * POST /internal/audit/accounts-sync
 *
 * Platform-scoped. Snapshots Instantly GET /accounts to Bronze + refreshes the
 * Silver health columns, then runs reconcileLifecycle (health refreshed → some
 * accounts may flip state). Read-only against Instantly except the warmup PATCHes
 * reconcile issues on real transitions. 202 + background; watch logs for
 * `accounts-sync: done`. Wired into the placement cron (every 6h).
 */
router.post("/accounts-sync", async (_req: Request, res: Response) => {
  const runId = crypto.randomUUID();
  res.status(202).json({ accepted: true, runId });
  console.log(`[audit] accounts-sync: dispatched run=${runId}`);

  (async () => {
    const apiKey = await resolvePlatformInstantlyApiKey({
      method: "POST",
      path: "/internal/audit/accounts-sync",
    });
    const snapshot = await snapshotAccounts(apiKey);
    const lifecycle = await reconcileLifecycle(apiKey);
    console.log(
      `[audit] accounts-sync: done run=${runId} ${JSON.stringify({ snapshot, lifecycle })}`,
    );
  })().catch((error: unknown) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[audit] accounts-sync run=${runId} failed: ${message}`);
  });
});

/**
 * POST /internal/audit/daily-limit-sync
 *
 * Platform-scoped. One-time (idempotent, resumable) sweep that PATCHes every
 * currently-`in_production` account's Instantly campaign `daily_limit` to
 * `IN_PRODUCTION_DAILY_LIMIT`. Needed after bumping that constant: reconcile only
 * PATCHes daily_limit on a state FLIP, so accounts already in_production keep the
 * OLD cap until they re-flip — this closes that gap across the live-send pool.
 * Only PATCHes accounts whose silver daily_limit differs from the target
 * (skips the aligned ones), fail-loud per account. Optional `{limit}` bounds the
 * batch. 202 + background; watch logs for `daily-limit-sync: done`.
 */
router.post("/daily-limit-sync", async (req: Request, res: Response) => {
  const runId = crypto.randomUUID();
  const rawLimit = (req.body as { limit?: unknown } | undefined)?.limit;
  const limit = typeof rawLimit === "number" && rawLimit > 0 ? rawLimit : undefined;
  res.status(202).json({ accepted: true, runId });
  console.log(`[audit] daily-limit-sync: dispatched run=${runId} limit=${limit ?? "all"}`);

  (async () => {
    const apiKey = await resolvePlatformInstantlyApiKey({
      method: "POST",
      path: "/internal/audit/daily-limit-sync",
    });
    const summary = await syncInProductionDailyLimit(apiKey, limit);
    console.log(
      `[audit] daily-limit-sync: done run=${runId} ${JSON.stringify(summary)}`,
    );
  })().catch((error: unknown) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[audit] daily-limit-sync run=${runId} failed: ${message}`);
  });
});

/**
 * POST /internal/audit/placement-test/sync
 *
 * Platform-scoped. Polls every Instantly inbox-placement test + its analytics
 * rows, mirrors them to bronze, and promotes to silver (so account-health +
 * history reflect the latest results). Read-only against Instantly (spends no
 * quota) — safe to run any time. 202 + background (the sweep can outlast the
 * proxy timeout); watch logs for `placement-sync: done`.
 */
router.post("/placement-test/sync", async (_req: Request, res: Response) => {
  const runId = crypto.randomUUID();
  res.status(202).json({ accepted: true, runId });
  console.log(`[audit] placement-sync: dispatched run=${runId}`);

  (async () => {
    const apiKey = await resolvePlatformInstantlyApiKey({
      method: "POST",
      path: "/internal/audit/placement-test/sync",
    });
    const summary = await syncPlacement(apiKey);
    // Delivery just refreshed → recompute the lifecycle (accounts that hit 100%
    // inbox may promote to in_production; regressions demote to in_recovery).
    const lifecycle = await reconcileLifecycle(apiKey);
    console.log(
      `[audit] placement-sync: done run=${runId} ${JSON.stringify({ ...summary, lifecycle })}`,
    );
  })().catch((error: unknown) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[audit] placement-sync run=${runId} failed: ${message}`);
  });
});

/**
 * POST /internal/audit/placement-test/run
 *
 * Platform-scoped. Creates ONE one-time (type 1) fleet inbox-placement test that
 * runs immediately — the plan-compatible recurring path (the cron calls this
 * every 6h; automated type-2 tests are HyperGrowth-gated, see /ensure). SPENDS
 * Growth-sub quota → gated behind `PLACEMENT_TESTS_ENABLED=true`; returns 409
 * when disabled. Fails loud on a create rejection (402 quota / 400).
 */
router.post("/placement-test/run", async (_req: Request, res: Response) => {
  try {
    if (!isPlacementSchedulingEnabled()) {
      return res.status(409).json({
        error: "placement testing disabled — set PLACEMENT_TESTS_ENABLED=true to arm",
      });
    }
    const apiKey = await resolvePlatformInstantlyApiKey({
      method: "POST",
      path: "/internal/audit/placement-test/run",
    });
    const summary = await runOneTimeFleetPlacementTest(apiKey);
    res.json({ ...summary });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[audit] placement-test/run failed: ${message}`);
    res.status(500).json({ error: message });
  }
});

/**
 * POST /internal/audit/placement-test/ensure
 *
 * Platform-scoped. Ensures the recurring AUTOMATED (type 2) placement tests exist
 * so Instantly runs them on a schedule server-side. ⚠️ Automated tests require an
 * Instantly HyperGrowth plan — on the Growth sub this 402s; use /run instead
 * (one-time test per cron tick). Kept for when the workspace is on HyperGrowth.
 * SPENDS Growth-sub quota → gated behind `PLACEMENT_TESTS_ENABLED=true`; returns
 * 409 when disabled. Fails loud on a create rejection (402 quota / 400).
 */
router.post("/placement-test/ensure", async (_req: Request, res: Response) => {
  try {
    if (!isPlacementSchedulingEnabled()) {
      return res.status(409).json({
        error: "placement scheduling disabled — set PLACEMENT_TESTS_ENABLED=true to arm",
      });
    }
    const apiKey = await resolvePlatformInstantlyApiKey({
      method: "POST",
      path: "/internal/audit/placement-test/ensure",
    });
    const summary = await ensurePlacementSchedule(apiKey);
    res.json({ ...summary });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[audit] placement-test/ensure failed: ${message}`);
    res.status(500).json({ error: message });
  }
});

/**
 * GET /internal/audit/reconcile
 *
 * Platform-scoped (no org). For each countable fact, returns OUR local number
 * next to INSTANTLY's number + the `delta` (local − instantly), so an operator
 * can spot a divergence on the dashboard and investigate the underlying bug
 * (lost webhook, lagging reconcile, a pause/throttle we missed). See
 * lib/reconcile-audit.ts for why this reconciles COUNTS, not the forecast's
 * future dates (which exist nowhere and cannot be reconciled).
 *
 * The local counts (silver DB) are read live in one fast round-trip. The
 * Instantly side is NOT swept live here — a fleet-wide throttled Instantly sweep
 * (analytics + per-campaign sequence lengths across thousands of campaigns) runs
 * for minutes and blew past the gateway/browser timeout, leaving the staff
 * dashboard stuck on a loading skeleton. Instead the Instantly counts are
 * pre-aggregated into a single-row snapshot by a background refresh; this handler
 * reads that snapshot in one fast query (local + snapshot in parallel), so it
 * returns in a few seconds cold. Stale-while-revalidate: a stale snapshot is
 * still served while a background refresh is kicked; a missing snapshot fails
 * loud (503) and kicks the seeding refresh — never a fabricated Instantly number.
 */
router.get("/reconcile", async (_req: Request, res: Response) => {
  try {
    const asOf = new Date();

    const apiKey = await resolvePlatformInstantlyApiKey({
      method: "GET",
      path: "/internal/audit/reconcile",
    });

    const [localResult, snapshot] = await Promise.all([
      // One round-trip: all five local counts as scalar subqueries. `pendingSends`
      // reuses the EXACT gate `loadPendingLeads` uses, so its number equals the
      // total steps the sending-forecast projects.
      db.execute(sql`
        SELECT
          (SELECT COUNT(*) FROM instantly_campaigns WHERE status = 'active')::int
            AS "activeCampaigns",
          (SELECT COUNT(*) FROM instantly_campaigns)::int
            AS "contactsStored",
          (SELECT COUNT(*) FROM instantly_events WHERE event_type = 'email_sent')::int
            AS "emailsSent",
          (SELECT COUNT(DISTINCT (campaign_id, lead_email))
             FROM instantly_events WHERE event_type = 'email_sent')::int
            AS "contactedDispatched",
          -- Count STEP-SENDS, not cost rows. Each step provisions TWO
          -- sequence_costs rows (account + domain), so COUNT(*) double-counts
          -- vs Instantly's remaining = stepCount − sent (one per step). Collapse
          -- the account/domain pair with DISTINCT (campaign, lead, step) so the
          -- unit matches Instantly's derived pendingSends.
          (SELECT COUNT(DISTINCT (sc.campaign_id, sc.lead_email, sc.step))
             FROM sequence_costs sc
             WHERE sc.status = 'provisioned'
               AND EXISTS (
                 SELECT 1 FROM instantly_campaigns c
                 WHERE c.lead_email = sc.lead_email
                   AND c.campaign_id IS NOT DISTINCT FROM sc.campaign_id
                   AND c.status = 'active'
                   AND c.delivery_status IN ('contacted', 'sent')
               ))::int
            AS "pendingSends"
      `),
      // Fast single-row read of the pre-aggregated Instantly counts.
      readInstantlySnapshot(),
    ]);

    if (!snapshot) {
      // Cold path: the Instantly snapshot has never been computed. Kick a
      // background refresh so the next request succeeds, and fail loud now —
      // never fabricate an Instantly count.
      maybeTriggerRefresh(apiKey, "cold-read");
      return res.status(503).json({
        error:
          "reconcile snapshot not yet computed — refreshing in background, retry shortly",
      });
    }

    // Stale-while-revalidate: serve the snapshot immediately; if it is older than
    // the TTL, kick a background refresh (guarded against stacking).
    if (isSnapshotStale(snapshot.refreshedAt, asOf)) {
      maybeTriggerRefresh(apiKey, "stale-read");
    }

    const rows = Array.isArray(localResult)
      ? localResult
      : ((localResult as any).rows ?? []);
    const r = (rows[0] ?? {}) as Record<string, unknown>;
    const local: LocalReconcileCounts = {
      activeCampaigns: Number(r.activeCampaigns),
      emailsSent: Number(r.emailsSent),
      contactedDispatched: Number(r.contactedDispatched),
      contactsStored: Number(r.contactsStored),
      pendingSends: Number(r.pendingSends),
    };

    res.json({
      asOf: asOf.toISOString(),
      metrics: buildReconciliation(local, snapshot.counts),
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[audit] reconcile failed: ${message}`);
    res.status(500).json({ error: message });
  }
});

/**
 * POST /internal/audit/reconcile/refresh
 *
 * Platform-scoped. Runs the fleet-wide Instantly sweep and upserts the reconcile
 * snapshot the GET reads. 202 + background (the sweep can run for minutes, far
 * past the proxy timeout); watch logs for `reconcile-snapshot: done`. Use this to
 * SEED the snapshot right after deploy (so the first dashboard GET is 200) and as
 * a manual/cron freshness trigger. Explicit refreshes always run (bypass the
 * on-read in-flight guard). Read-only against our DB except the single snapshot
 * upsert; fail loud in the background on any Instantly error.
 */
router.post("/reconcile/refresh", async (_req: Request, res: Response) => {
  const runId = crypto.randomUUID();
  res.status(202).json({ accepted: true, runId });
  console.log(`[audit] reconcile-snapshot: dispatched run=${runId}`);

  (async () => {
    const apiKey = await resolvePlatformInstantlyApiKey({
      method: "POST",
      path: "/internal/audit/reconcile/refresh",
    });
    const counts = await refreshInstantlySnapshot(apiKey);
    console.log(
      `[audit] reconcile-snapshot: done run=${runId} ${JSON.stringify(counts)}`,
    );
  })().catch((error: unknown) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[audit] reconcile-snapshot: run=${runId} failed: ${message}`);
  });
});

export default router;
