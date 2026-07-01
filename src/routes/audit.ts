import { Router, Request, Response } from "express";
import { sql } from "drizzle-orm";
import { db } from "../db";
import {
  listAccounts,
  listAllCampaignAnalytics,
  listAllCampaignSequenceLengths,
} from "../lib/instantly-client";
import { resolvePlatformInstantlyApiKey } from "../lib/key-client";
import {
  computeCapacitySummary,
  projectDailySchedule,
  type PendingLead,
} from "../lib/sending-forecast";
import { buildAccountHealth } from "../lib/account-health";
import {
  syncPlacement,
  ensurePlacementSchedule,
  fetchLatestPlacementByAccount,
  fetchPlacementHistory,
  isPlacementSchedulingEnabled,
} from "../lib/placement-sync";
import {
  summarizeInstantlyCounts,
  buildReconciliation,
  type LocalReconcileCounts,
} from "../lib/reconcile-audit";

const router = Router();

/**
 * Load every active-campaign lead that still carries un-sent (provisioned)
 * sequence steps. A `sequence_costs` row is `provisioned` until its
 * `email_sent` webhook actualizes it, so provisioned steps on a live campaign
 * are exactly the future scheduled sends. Gated to genuinely-live campaigns
 * (`status='active'` and a non-terminal `delivery_status`) so a replied /
 * bounced / paused lead whose holds have not yet been reconciled is excluded.
 */
async function loadPendingLeads(): Promise<PendingLead[]> {
  const result = await db.execute(sql`
    SELECT
      ARRAY_AGG(DISTINCT sc.step) FILTER (WHERE sc.status = 'provisioned') AS "provisionedSteps",
      MAX(sc.step) FILTER (WHERE sc.status = 'actual') AS "lastSentStep",
      MAX(sc.updated_at) FILTER (WHERE sc.status = 'actual') AS "lastSentAt"
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
  `);

  const rows = Array.isArray(result) ? result : ((result as any).rows ?? []);
  return rows.map((r: Record<string, unknown>): PendingLead => {
    const provisionedSteps = (r.provisionedSteps as number[] | null) ?? [];
    const lastSentStep =
      r.lastSentStep === null || r.lastSentStep === undefined
        ? null
        : Number(r.lastSentStep);
    const lastSentAt = r.lastSentAt ? new Date(r.lastSentAt as string) : null;
    return {
      provisionedSteps: provisionedSteps.map((s) => Number(s)),
      lastSentStep,
      lastSentAt,
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
    const asOf = new Date();

    const apiKey = await resolvePlatformInstantlyApiKey({
      method: "GET",
      path: "/internal/audit/sending-forecast",
    });
    const accounts = await listAccounts(apiKey);
    const capacity = computeCapacitySummary(accounts);

    const pendingLeads = await loadPendingLeads();
    const days = projectDailySchedule(pendingLeads, asOf);

    res.json({
      asOf: asOf.toISOString(),
      dailyCapacity: capacity.dailyCapacity,
      healthyAccountCount: capacity.healthyAccountCount,
      totalAccountCount: capacity.totalAccountCount,
      blockedDomainCount: capacity.blockedDomainCount,
      days,
    });
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
 * dailyLimit), and blocked state (blocked/blockReason from the SAME gate the
 * live send path uses — `classifyAccountBlock`/`filterHealthyAccounts`).
 *
 * `inboxPlacement` is null for every account: the Instantly V2 API does not
 * expose inbox placement as a per-account property (it exists only as
 * test-scoped, subscription-gated, point-in-time inbox-placement-test results —
 * see lib/account-health.ts). Never fabricated.
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
    // Account list (Instantly) + latest placement per account (our silver) run
    // independently — parallelize. Placement is best-effort per contract (null
    // when never tested); a live account list is required (fail loud).
    const [accounts, placementByEmail] = await Promise.all([
      listAccounts(apiKey),
      fetchLatestPlacementByAccount(),
    ]);

    res.json({
      asOf: asOf.toISOString(),
      accounts: buildAccountHealth(accounts, placementByEmail),
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
    console.log(`[audit] placement-sync: done run=${runId} ${JSON.stringify(summary)}`);
  })().catch((error: unknown) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[audit] placement-sync run=${runId} failed: ${message}`);
  });
});

/**
 * POST /internal/audit/placement-test/ensure
 *
 * Platform-scoped. Ensures the recurring automated placement tests exist (so
 * Instantly runs the fleet placement test on a schedule server-side). SPENDS
 * Growth-sub quota → gated behind `PLACEMENT_TESTS_ENABLED=true`; returns 409
 * when disabled. Fails loud on a create rejection (402 quota / 400).
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
 * Local counts (silver DB) and the Instantly fleet analytics are fetched in
 * parallel; either failing → fail loud (500), no fabricated zero.
 */
router.get("/reconcile", async (_req: Request, res: Response) => {
  try {
    const asOf = new Date();

    const apiKey = await resolvePlatformInstantlyApiKey({
      method: "GET",
      path: "/internal/audit/reconcile",
    });

    const [localResult, analytics, campaignSequences] = await Promise.all([
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
          (SELECT COUNT(*) FROM sequence_costs sc
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
      listAllCampaignAnalytics(apiKey),
      // Sequence lengths (paginated /campaigns) → Instantly's own remaining-sends
      // count (stepCount − sent) for the pendingSends reconciliation.
      listAllCampaignSequenceLengths(apiKey),
    ]);

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

    const instantly = summarizeInstantlyCounts(analytics, campaignSequences);

    res.json({
      asOf: asOf.toISOString(),
      metrics: buildReconciliation(local, instantly),
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[audit] reconcile failed: ${message}`);
    res.status(500).json({ error: message });
  }
});

export default router;
