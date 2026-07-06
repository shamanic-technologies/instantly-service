/**
 * One-time (idempotent, resumable) repair for ORPHANED provisioned holds — the
 * mirror of refund-stranded-holds (that cancels un-sent holds; this actualizes
 * sent-but-unbilled ones).
 *
 * Root cause: `handleEmailSent` (silver-promote) actualizes a step's two
 * `provisioned` sequence_costs rows (account + domain) by first PATCHing
 * runs-service, then flipping the local row. If the runs-service call throws
 * transiently (cold-start ECONNRESET, 5xx), the `catch` logs and moves on — the
 * silver `email_sent` event still commits, but the hold stays `provisioned`.
 * reconcile then never revisits it: its drift gate keys on event COUNTS
 * (sent/reply/bounce/unsub/opens), and the event IS present, so `remote == local`
 * → no drift → the stuck hold is never retried. The send happened but is never
 * billed, and it inflates the reconcile `pendingSends` count.
 *
 * This sweep finds every `provisioned` hold whose step ALREADY has a real
 * (`inferred=false`) `email_sent` silver event and re-runs the SAME tested
 * actualize helper the live path uses — `handleEmailSent`. It is:
 *   - idempotent  — only touches `status='provisioned'` rows; a re-run no-ops.
 *   - resumable   — re-selects live state each run; actualized rows drop out.
 *   - in-cluster  — actualizing calls runs-service (`*.railway.internal`), so this
 *                   MUST run inside Railway (the `/internal/campaigns/actualize-
 *                   orphaned-sends` endpoint), NOT a laptop shell.
 *
 * A hold whose run is GONE (runs-service 404 — retention purged the run) can
 * never be actualized. `handleEmailSent` now detects that terminal 404 and
 * cancels the hold locally (the send fired, it's unbillable); the sweep tallies
 * those under `costsCancelled` instead of re-stranding them. A TRANSIENT error
 * (5xx / timeout / 403) leaves the hold provisioned (`costsTransient`) for the
 * next run. Never fabricates.
 *
 * NOTE: keyed on `(campaignId, leadEmail)` like refund-stranded-holds. Platform
 * sends (`campaignId IS NULL`) are skipped (same pre-existing gap).
 */
import { db } from "../db";
import { sql } from "drizzle-orm";
import { handleEmailSent } from "./silver-promote";

export interface OrphanedSend {
  campaignId: string;
  instantlyCampaignId: string;
  orgId: string | null;
  userId: string | null;
  leadEmail: string;
  step: number;
}

export interface ActualizeSummary {
  /** Candidate steps iterated (both cost rows handled per step). */
  stepsProcessed: number;
  /** Cost rows converted provisioned→actual (send billed). */
  costsActualized: number;
  /** Cost rows cancelled locally — runs-service 404, run purged (unbillable). */
  costsCancelled: number;
  /** Cost rows left provisioned — transient error, retried next run. */
  costsTransient: number;
  /** Steps where `handleEmailSent` threw (e.g. local DB write failure). */
  stepsFailed: number;
}

/**
 * Select distinct `(campaign, lead, step)` where a `provisioned` hold exists AND
 * that step already has a real `email_sent` silver event — i.e. the send fired
 * but the hold was never actualized. One row per step (both cost rows for that
 * step are actualized together by `handleEmailSent`). `limit` bounds the batch.
 */
export async function selectOrphanedSends(
  limit?: number,
): Promise<OrphanedSend[]> {
  const limitClause = limit && limit > 0 ? sql`LIMIT ${limit}` : sql``;
  const result = await db.execute(sql`
    SELECT DISTINCT
      ic.campaign_id           AS "campaignId",
      ic.instantly_campaign_id AS "instantlyCampaignId",
      ic.org_id                AS "orgId",
      ic.user_id               AS "userId",
      ic.lead_email            AS "leadEmail",
      sc.step                  AS "step"
    FROM sequence_costs sc
    JOIN instantly_campaigns ic
      ON ic.campaign_id = sc.campaign_id
     AND ic.lead_email  = sc.lead_email
    WHERE sc.status = 'provisioned'
      AND ic.campaign_id IS NOT NULL
      AND ic.lead_email IS NOT NULL
      AND EXISTS (
        SELECT 1 FROM instantly_events e
        WHERE e.campaign_id = ic.instantly_campaign_id
          AND e.lead_email  = sc.lead_email
          AND e.step        = sc.step
          AND e.event_type  = 'email_sent'
          AND e.inferred    = false
      )
    ORDER BY ic.campaign_id
    ${limitClause}
  `);
  const rows = Array.isArray(result)
    ? result
    : (result as { rows?: unknown[] }).rows ?? [];
  return rows as OrphanedSend[];
}

/**
 * Actualize every orphaned (sent-but-provisioned) hold via `handleEmailSent`.
 * Each step is processed independently — one failure is counted and does not
 * abort the sweep (re-run picks it up). Logs progress for a Railway-log watcher.
 */
export async function actualizeOrphanedSends(
  opts: { limit?: number } = {},
): Promise<ActualizeSummary> {
  const startedAt = Date.now();
  const orphans = await selectOrphanedSends(opts.limit);
  console.log(
    `[instantly-service] actualize-orphaned-sends: starting, candidates=${orphans.length}` +
      (opts.limit ? ` (limit=${opts.limit})` : ""),
  );

  let processed = 0;
  let actualized = 0;
  let cancelled = 0;
  let transient = 0;
  let failed = 0;

  for (const o of orphans) {
    try {
      const outcome = await handleEmailSent(
        {
          campaignId: o.campaignId,
          instantlyCampaignId: o.instantlyCampaignId,
          orgId: o.orgId,
          userId: o.userId,
          runId: null,
        },
        o.leadEmail,
        o.step,
      );
      processed++;
      actualized += outcome.actualized;
      cancelled += outcome.cancelled;
      transient += outcome.transient;
    } catch (error: unknown) {
      failed++;
      const message = error instanceof Error ? error.message : String(error);
      console.error(
        `[instantly-service] actualize-orphaned-sends: campaign=${o.instantlyCampaignId} ` +
          `lead=${o.leadEmail} step=${o.step} failed: ${message}`,
      );
    }
  }

  console.log(
    `[instantly-service] actualize-orphaned-sends: done, steps=${processed} ` +
      `actualized=${actualized} cancelled=${cancelled} transient=${transient} ` +
      `failed=${failed} durationMs=${Date.now() - startedAt}`,
  );
  return {
    stepsProcessed: processed,
    costsActualized: actualized,
    costsCancelled: cancelled,
    costsTransient: transient,
    stepsFailed: failed,
  };
}
