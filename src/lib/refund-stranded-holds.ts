/**
 * One-time (idempotent, resumable) backlog refund for the provisioned-hold leak
 * fixed forward in reconcile (PR #334, issue #335).
 *
 * Before the forward fix, a campaign that reached a terminal Instantly status
 * (paused/completed) had its remaining `provisioned` `sequence_costs` stranded
 * forever: never actualized (the steps never send), never cancelled (no
 * SEQUENCE_STOP_EVENT fires), and — once the local row is terminal — never
 * revisited by `reconcileAll` (it skips locally-terminal rows). Those open holds
 * permanently depress the org's spendable balance (`balance_cents`).
 *
 * This sweep finds every locally-terminal campaign (status paused/completed)
 * that still has `provisioned` holds and cancels them (refund) via the SAME
 * tested helper the forward fix uses — `cancelRemainingProvisions`. It is:
 *   - idempotent  — only touches `status='provisioned'` rows; a re-run no-ops.
 *   - resumable   — re-selects live state each run; cancelled rows drop out.
 *   - in-cluster  — cancelling calls runs-service (`*.railway.internal`), so this
 *                   MUST run inside Railway (the `/internal/campaigns/refund-
 *                   stranded-holds` endpoint), NOT a laptop shell.
 *
 * NOTE: keyed on the LOCAL terminal status, which is what `deleteFinishedContact`
 * persists. Platform sends (`campaignId IS NULL`) are skipped — their holds
 * cannot be matched by the `(campaignId, leadEmail)` lookup (pre-existing gap).
 */
import { db } from "../db";
import { sql } from "drizzle-orm";
import { cancelRemainingProvisions } from "./silver-promote";

/** Local `instantly_campaigns.status` values that mark a row terminal. */
const TERMINAL_LOCAL_STATUSES = ["paused", "completed"] as const;

export interface StrandedCampaign {
  campaignId: string;
  instantlyCampaignId: string;
  orgId: string | null;
  userId: string | null;
  leadEmail: string;
}

export interface RefundSummary {
  campaignsProcessed: number;
  campaignsFailed: number;
}

/**
 * Select distinct (campaignId, leadEmail) campaigns that are locally terminal
 * AND still carry at least one `provisioned` hold. `limit` bounds the batch
 * (campaign count); omit to sweep all.
 */
export async function selectStrandedCampaigns(
  limit?: number,
): Promise<StrandedCampaign[]> {
  const limitClause = limit && limit > 0 ? sql`LIMIT ${limit}` : sql``;
  const result = await db.execute(sql`
    SELECT DISTINCT
      ic.campaign_id          AS "campaignId",
      ic.instantly_campaign_id AS "instantlyCampaignId",
      ic.org_id               AS "orgId",
      ic.user_id              AS "userId",
      ic.lead_email           AS "leadEmail"
    FROM sequence_costs sc
    JOIN instantly_campaigns ic
      ON ic.campaign_id = sc.campaign_id
     AND ic.lead_email  = sc.lead_email
    WHERE sc.status = 'provisioned'
      AND ic.status IN ('paused', 'completed')
      AND ic.campaign_id IS NOT NULL
      AND ic.lead_email IS NOT NULL
    ORDER BY ic.campaign_id
    ${limitClause}
  `);
  const rows = Array.isArray(result)
    ? result
    : (result as { rows?: unknown[] }).rows ?? [];
  return rows as StrandedCampaign[];
}

/**
 * Cancel all stranded provisioned holds on terminal campaigns. Each campaign is
 * processed independently — one failure is counted and does not abort the sweep
 * (re-run picks it up). Logs progress so a Railway-log watcher can track it.
 */
export async function refundStrandedHolds(opts: { limit?: number } = {}): Promise<RefundSummary> {
  const startedAt = Date.now();
  const campaigns = await selectStrandedCampaigns(opts.limit);
  console.log(
    `[instantly-service] refund-stranded-holds: starting, candidates=${campaigns.length}` +
      (opts.limit ? ` (limit=${opts.limit})` : ""),
  );

  let processed = 0;
  let failed = 0;

  for (const c of campaigns) {
    try {
      await cancelRemainingProvisions(
        {
          campaignId: c.campaignId,
          instantlyCampaignId: c.instantlyCampaignId,
          orgId: c.orgId,
          userId: c.userId,
          runId: null,
        },
        c.leadEmail,
      );
      processed++;
    } catch (error: unknown) {
      failed++;
      const message = error instanceof Error ? error.message : String(error);
      console.error(
        `[instantly-service] refund-stranded-holds: campaign=${c.instantlyCampaignId} ` +
          `lead=${c.leadEmail} failed: ${message}`,
      );
    }
  }

  console.log(
    `[instantly-service] refund-stranded-holds: done, processed=${processed} ` +
      `failed=${failed} durationMs=${Date.now() - startedAt}`,
  );
  return { campaignsProcessed: processed, campaignsFailed: failed };
}

export { TERMINAL_LOCAL_STATUSES };
