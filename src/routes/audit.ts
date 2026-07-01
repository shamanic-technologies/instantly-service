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
    const accounts = await listAccounts(apiKey);

    res.json({
      asOf: asOf.toISOString(),
      accounts: buildAccountHealth(accounts),
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[audit] account-health failed: ${message}`);
    res.status(500).json({ error: message });
  }
});

export default router;
