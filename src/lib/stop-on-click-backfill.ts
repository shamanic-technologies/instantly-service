/**
 * One-shot catch-up for the clicks stop-on-click missed while its gate was blind.
 *
 * The gate tested a `visit_` PREFIX against a vocabulary campaign-service had renamed, so from the
 * rename until this fix EVERY visit-led campaign returned false and no sequence was ever stopped.
 * Fixing the gate only helps a lead who clicks AGAIN — and a prospect who already clicked through
 * to the landing page usually does not, while their follow-ups keep going out. So the backlog is
 * the customer-visible half of the bug and it does not drain by itself.
 *
 * This sweep re-asks the SAME question for every lead who already clicked and whose sequence is
 * still live, through the SAME `maybeStopOnClickForFunnel` the webhook path calls — no second
 * implementation, no second gate, no local status write. campaign-service is still the one that
 * says whether the funnel opens on a visit, so a reply-led or funnel-less campaign is left running
 * exactly as it would be on a live click.
 *
 * Idempotent and resumable: a campaign paused by a previous run leaves `status='active'` at the
 * next reconcile (and a re-pause of an already-paused Instantly campaign is a no-op anyway), and
 * the helper is fail-soft per lead, so one bad campaign cannot stop the sweep.
 */
import { sql } from "drizzle-orm";

import { db } from "../db";
import { maybeStopOnClickForFunnel } from "./stop-on-click";

/** One lead who clicked and whose sequence is still running. */
export interface ClickedActiveCampaign {
  campaignId: string;
  instantlyCampaignId: string;
  orgId: string | null;
  userId: string | null;
  runId: string | null;
  leadEmail: string;
}

export interface StopOnClickBackfillSummary {
  candidates: number;
  processed: number;
  failed: number;
}

function rowsOf(result: unknown): unknown[] {
  if (Array.isArray(result)) return result;
  return (result as { rows?: unknown[] }).rows ?? [];
}

/**
 * Leads with a REAL click on a still-active, org-scoped sequence.
 *
 * `inferred = false` is load-bearing: an inferred click is a synthetic predecessor projected from a
 * downstream event, so pausing a live sequence on one would act on a click nobody made. A platform
 * send (`campaign_id IS NULL`) belongs to no caller campaign and runs no funnel, so it is out of
 * scope here exactly as it is in the live path.
 */
export async function selectClickedActiveCampaigns(
  limit?: number,
): Promise<ClickedActiveCampaign[]> {
  const limitClause = limit && limit > 0 ? sql`LIMIT ${limit}` : sql``;
  const result = await db.execute(sql`
    SELECT
      c.campaign_id           AS "campaignId",
      c.instantly_campaign_id AS "instantlyCampaignId",
      c.org_id                AS "orgId",
      c.user_id               AS "userId",
      c.run_id                AS "runId",
      c.lead_email            AS "leadEmail"
    FROM instantly_campaigns c
    WHERE c.status = 'active'
      AND c.campaign_id IS NOT NULL
      AND c.org_id IS NOT NULL
      AND EXISTS (
        SELECT 1
        FROM instantly_events e
        WHERE e.campaign_id = c.instantly_campaign_id
          AND e.event_type = 'email_link_clicked'
          AND e.inferred = false
      )
    ORDER BY c.instantly_campaign_id
    ${limitClause}
  `);
  return rowsOf(result) as ClickedActiveCampaign[];
}

/** Re-ask stop-on-click for every already-clicked live sequence. `limit` bounds the batch. */
export async function backfillStopOnClick(
  opts: { limit?: number } = {},
): Promise<StopOnClickBackfillSummary> {
  const startedAt = Date.now();
  const candidates = await selectClickedActiveCampaigns(opts.limit);
  console.log(
    `[instantly-service] stop-on-click-backfill: starting, candidates=${candidates.length}` +
      (opts.limit ? ` (limit=${opts.limit})` : ""),
  );

  let processed = 0;
  let failed = 0;

  for (const c of candidates) {
    try {
      await maybeStopOnClickForFunnel(
        {
          instantlyCampaignId: c.instantlyCampaignId,
          campaignId: c.campaignId,
          orgId: c.orgId,
          userId: c.userId,
          runId: c.runId,
        },
        c.leadEmail,
      );
      processed++;
    } catch (error: unknown) {
      // maybeStopOnClickForFunnel is itself fail-soft, so reaching here means something outside it
      // broke. Count it and keep going: one campaign must not cost the rest of the sweep.
      failed++;
      const message = error instanceof Error ? error.message : String(error);
      console.error(
        `[instantly-service] stop-on-click-backfill: campaign=${c.instantlyCampaignId} ` +
          `lead=${c.leadEmail} failed: ${message}`,
      );
    }
  }

  console.log(
    `[instantly-service] stop-on-click-backfill: done, candidates=${candidates.length} ` +
      `processed=${processed} failed=${failed} durationMs=${Date.now() - startedAt}`,
  );
  return { candidates: candidates.length, processed, failed };
}
