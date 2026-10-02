/**
 * Put back in the queue the self-send sequences a link SCANNER stopped.
 *
 * `stop-on-click` stops a sequence when its lead clicks (the lead is on the
 * brand's site, the cold sequence would only distract). On the self-send
 * transport that stop is local and final: `stopSelfSendSequence` cancels the
 * remaining holds and marks the row `paused`. When the click is later ruled a
 * scanner's (`click-scanner-backfill.ts`, e.g. after the 2026-10-02 network rule
 * caught Microsoft Defender's ordinary-looking user-agent), the stats are fixed
 * but the prospect still never gets the follow-ups — a robot decided that, not
 * them. Owner decision 2026-10-02: resume them.
 *
 * ⚠️ A SEQUENCE IS RESUMED ONLY WHEN NOTHING ELSE COULD HAVE STOPPED IT. Every
 * condition below is evidence that the pause was the scanner's and nobody else's:
 *   - self-send, `paused`, not closed by the stalled-first-email sweep;
 *   - at least one click on it ruled `scanner`, and NO click on it still `human`
 *     (a real click stands, so does its stop) and no real silver click either;
 *   - the person never answered anywhere in the org (reply, auto-reply, bounce,
 *     unsubscribe), was never hand-qualified, holds no standing opt-out;
 *   - no NEWER sequence holds them for the same brand (resuming would double up);
 *   - it has holds cancelled AT OR AFTER the first scanner click, on steps never
 *     really sent — those are the steps the stop took away, and the only ones put
 *     back.
 *
 * ⚠️ BILLED HOLDS ARE NOT REOPENED. A hold carrying runs-service cost ids was
 * cancelled THERE too; flipping it back locally would queue an email whose
 * charge stays cancelled. Every hold this was written for dates from the
 * unbilled era (2026-08-24 → 2026-10-02, no cost ids), so they are reopened
 * locally, exactly as they settle locally. A sequence with a billed reopenable
 * hold is counted `skippedBilled` and left paused.
 *
 * `dryRun` (the default) decides and counts without writing. Idempotent: a
 * resumed row is `active`, so it leaves the candidate set.
 */

import { sql } from "drizzle-orm";

import { db } from "../../db";
import { announceEvidenceChanged } from "../evidence-changed";
import { findStandingOptOut } from "../lead-optouts";
import { refreshLeadStatusCurrent } from "../status-gold";
import { SELF_SEND_CAMPAIGN_LIKE, SEND_TRANSPORT_SMTP } from "./transport";

/** node-postgres resolves `db.execute` to a QueryResult OBJECT, never an array. */
function rowsOf(result: unknown): Record<string, unknown>[] {
  if (Array.isArray(result)) return result as Record<string, unknown>[];
  const rows = (result as { rows?: unknown })?.rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

export interface ScannerPausedCandidate {
  instantlyCampaignId: string;
  orgId: string | null;
  leadEmail: string;
  createdAt: Date;
  /** Steps whose hold the scanner stop cancelled and that were never sent. */
  reopenSteps: number[];
  /** A reopenable hold carries runs-service cost ids (see header). */
  billed: boolean;
}

export interface ScannerPausedSummary {
  dryRun: boolean;
  candidates: number;
  reactivated: number;
  stepsReopened: number;
  skippedOptedOut: number;
  skippedBilled: number;
  /** Rows that moved off `paused` between the read and the write. */
  skippedRaced: number;
  failed: number;
  /** Candidate count per assignment month (`YYYY-MM`), to read how old they are. */
  byMonth: Record<string, number>;
}

const DEFAULT_LIMIT = 500;

export async function loadScannerPausedCandidates(limit: number): Promise<ScannerPausedCandidate[]> {
  const result = await db.execute(sql`
    WITH scanner_click AS (
      SELECT h.instantly_campaign_id, MIN(h.received_at) AS first_at
      FROM tracking_hits_raw h
      WHERE h.kind = 'click' AND h.classification = 'scanner'
      GROUP BY h.instantly_campaign_id
    )
    SELECT
      c.instantly_campaign_id,
      c.org_id,
      c.lead_email,
      c.created_at,
      COALESCE(jsonb_agg(DISTINCT sc.step ORDER BY sc.step), '[]'::jsonb) AS reopen_steps,
      bool_or(sc.cost_id IS NOT NULL OR sc.domain_cost_id IS NOT NULL) AS billed
    FROM instantly_campaigns c
    JOIN scanner_click k ON k.instantly_campaign_id = c.instantly_campaign_id
    JOIN sequence_costs sc
      ON sc.instantly_campaign_id = c.instantly_campaign_id
     AND sc.status = 'cancelled'
     AND sc.updated_at >= k.first_at
     AND NOT EXISTS (
       SELECT 1 FROM instantly_events e
       WHERE e.campaign_id = c.instantly_campaign_id
         AND e.event_type = 'email_sent'
         AND e.inferred = FALSE
         AND e.step = sc.step
     )
    WHERE c.instantly_campaign_id LIKE ${SELF_SEND_CAMPAIGN_LIKE}
      AND c.send_transport = ${SEND_TRANSPORT_SMTP}
      AND c.status = 'paused'
      AND c.lead_email IS NOT NULL
      AND NOT (COALESCE(c.metadata, '{}'::jsonb) ? 'stalledFirstEmailClosed')
      AND NOT EXISTS (
        SELECT 1 FROM tracking_hits_raw o
        WHERE o.kind = 'click'
          AND o.instantly_campaign_id = c.instantly_campaign_id
          AND (o.classification IS NULL OR o.classification IN ('human', 'legacy'))
      )
      AND NOT EXISTS (
        SELECT 1 FROM instantly_events e
        WHERE e.campaign_id = c.instantly_campaign_id
          AND e.event_type = 'email_link_clicked'
          AND e.inferred = FALSE
      )
      AND NOT EXISTS (
        SELECT 1
        FROM instantly_campaigns o
        JOIN instantly_events e ON e.campaign_id = o.instantly_campaign_id
        WHERE lower(o.lead_email) = lower(c.lead_email)
          AND o.org_id IS NOT DISTINCT FROM c.org_id
          AND e.event_type IN ('reply_received', 'auto_reply_received', 'email_bounced', 'lead_unsubscribed')
      )
      AND NOT EXISTS (
        SELECT 1 FROM instantly_manual_qualifications_raw q
        WHERE q.org_id IS NOT DISTINCT FROM c.org_id
          AND lower(q.lead_email) = lower(c.lead_email)
      )
      AND NOT EXISTS (
        SELECT 1 FROM instantly_campaigns o
        WHERE lower(o.lead_email) = lower(c.lead_email)
          AND o.id <> c.id
          AND o.org_id IS NOT DISTINCT FROM c.org_id
          AND o.created_at > c.created_at
          AND o.instantly_campaign_id NOT LIKE 'reserving:%'
          AND (
            o.brand_ids && c.brand_ids
            OR (COALESCE(cardinality(c.brand_ids), 0) = 0
                AND o.campaign_id IS NOT DISTINCT FROM c.campaign_id)
          )
      )
    GROUP BY c.instantly_campaign_id, c.org_id, c.lead_email, c.created_at
    ORDER BY c.created_at
    LIMIT ${limit}
  `);

  return rowsOf(result).map((row) => ({
    instantlyCampaignId: String(row.instantly_campaign_id),
    orgId: row.org_id === null || row.org_id === undefined ? null : String(row.org_id),
    leadEmail: String(row.lead_email),
    createdAt: new Date(String(row.created_at)),
    reopenSteps: Array.isArray(row.reopen_steps) ? (row.reopen_steps as unknown[]).map(Number) : [],
    billed: row.billed === true,
  }));
}

/** Flip the row back to `active` and its stolen holds back to `provisioned`. */
async function reopen(c: ScannerPausedCandidate, asOf: Date): Promise<boolean> {
  const entry = JSON.stringify({ at: asOf.toISOString(), steps: c.reopenSteps });
  return db.transaction(async (tx) => {
    const updated = await tx.execute(sql`
      UPDATE instantly_campaigns
      SET status = 'active',
          metadata = jsonb_set(
            COALESCE(metadata, '{}'::jsonb),
            '{reactivatedAfterScannerClick}',
            ${entry}::jsonb
          ),
          updated_at = now()
      WHERE instantly_campaign_id = ${c.instantlyCampaignId}
        AND status = 'paused'
      RETURNING id
    `);
    if (rowsOf(updated).length === 0) return false;

    await tx.execute(sql`
      UPDATE sequence_costs
      SET status = 'provisioned', updated_at = now()
      WHERE instantly_campaign_id = ${c.instantlyCampaignId}
        AND status = 'cancelled'
        AND cost_id IS NULL
        AND domain_cost_id IS NULL
        AND step = ANY(${sql.param(c.reopenSteps)}::int[])
    `);
    return true;
  });
}

export async function reactivateScannerPausedSequences(
  options: { dryRun?: boolean; limit?: number; asOf?: Date } = {},
): Promise<ScannerPausedSummary> {
  const dryRun = options.dryRun !== false;
  const limit = options.limit && options.limit > 0 ? Math.floor(options.limit) : DEFAULT_LIMIT;
  const asOf = options.asOf ?? new Date();

  const candidates = await loadScannerPausedCandidates(limit);
  const summary: ScannerPausedSummary = {
    dryRun,
    candidates: candidates.length,
    reactivated: 0,
    stepsReopened: 0,
    skippedOptedOut: 0,
    skippedBilled: 0,
    skippedRaced: 0,
    failed: 0,
    byMonth: {},
  };

  for (const c of candidates) {
    const month = c.createdAt.toISOString().slice(0, 7);
    summary.byMonth[month] = (summary.byMonth[month] ?? 0) + 1;

    try {
      if (c.billed) {
        summary.skippedBilled += 1;
        continue;
      }
      if (c.orgId && (await findStandingOptOut(c.orgId, c.leadEmail)) !== null) {
        summary.skippedOptedOut += 1;
        continue;
      }
      if (dryRun) {
        summary.reactivated += 1;
        summary.stepsReopened += c.reopenSteps.length;
        continue;
      }

      if (!(await reopen(c, asOf))) {
        summary.skippedRaced += 1;
        continue;
      }
      summary.reactivated += 1;
      summary.stepsReopened += c.reopenSteps.length;
      await refreshLeadStatusCurrent(c.instantlyCampaignId, c.leadEmail);
      void announceEvidenceChanged(c.orgId, [c.leadEmail], "scanner_click_stop_reverted");
      console.log(
        `[instantly-service] reactivate-scanner-paused: resumed campaign=${c.instantlyCampaignId} lead=${c.leadEmail} steps=${c.reopenSteps.join(",")}`,
      );
    } catch (error: unknown) {
      summary.failed += 1;
      console.error(
        `[instantly-service] reactivate-scanner-paused: campaign=${c.instantlyCampaignId} failed: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }
  }

  return summary;
}
