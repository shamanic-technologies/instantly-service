/**
 * Unified, evidence-based reconcile for stranded `provisioned` sequence_costs.
 *
 * A provisioned hold is a pre-call reservation that must resolve AFTER the send:
 * actualize when the email actually dispatched, cancel when the sequence can no
 * longer send that step. The live path (`handleEmailSent` / `cancelRemaining
 * Provisions` in silver-promote) does this — but two gaps stranded holds forever:
 *   1. Platform sends (`campaign_id` NULL) never matched the caller-campaign_id
 *      resolver → their holds never actualized/cancelled. (Fixed forward by mig
 *      0027 persisting `instantly_campaign_id`; this sweep drains the backlog.)
 *   2. A transient runs-service error during actualize was swallowed → the hold
 *      stayed provisioned and reconcile's event-count drift gate never retried it.
 *
 * This sweep decides each hold's fate from instantly-service's OWN send/delivery
 * evidence (real, `inferred=false` silver events), NEVER from an assumption:
 *   - real `email_sent` at the hold's step  → ACTUALIZE (the send fired, bill it).
 *   - else the send is terminal (lead replied/bounced/unsubscribed, or its
 *     Instantly campaign is paused/completed / the lead has no active platform
 *     campaign left) → CANCEL (it will never send that step; refund the hold).
 *   - else the send is still in flight (active campaign, step not yet dispatched)
 *     → SKIP. Cancelling here would under-bill if the step later sends.
 *
 * It acts on each hold's OWN `run_id` + `cost_id` (no campaign match needed), so
 * it covers org AND platform, resolved AND historical rows. It is:
 *   - idempotent  — only touches `status='provisioned'`; a re-run no-ops.
 *   - resumable   — re-selects live state each run; resolved rows drop out.
 *   - in-cluster  — actualize/cancel PATCH runs-service (`*.railway.internal`),
 *                   so it MUST run inside Railway (the endpoint), not a laptop.
 *   - dry-runnable — `dryRun` returns the per-action plan counts without mutating.
 *
 * Supersedes the two single-purpose sweeps (actualize-orphaned-sends = the
 * ACTUALIZE case for org; refund-stranded-holds = the CANCEL case for org
 * finished campaigns); those remain for compatibility but this covers all cases.
 */
import { db } from "../db";
import { sql } from "drizzle-orm";
import { eq } from "drizzle-orm";
import { sequenceCosts } from "../db/schema";
import { updateCostStatus, isRunGoneError, type IdentityContext } from "./runs-client";

const NIL_USER_UUID = "00000000-0000-0000-0000-000000000000";

export type HoldAction = "actualize" | "cancel" | "skip";

/** Real send/delivery evidence for one provisioned hold. */
export interface HoldEvidence {
  /** A real (`inferred=false`) `email_sent` exists for this send at this step. */
  hasSent: boolean;
  /** The lead has a real reply/bounce/unsubscribe on this send (any step). */
  leadStopped: boolean;
  /** The send's Instantly campaign is terminal (paused/completed), or — for an
   *  unresolvable historical platform hold — the lead has no active platform
   *  campaign left, so no further step can dispatch. */
  campTerminal: boolean;
}

/**
 * Decide a hold's fate from real evidence. Pure — unit-tested exhaustively.
 * Order is load-bearing: a SENT step is billable even if the lead later stopped
 * (the email already went out), so `hasSent` wins over `leadStopped`.
 */
export function classifyHold(e: HoldEvidence): HoldAction {
  if (e.hasSent) return "actualize";
  if (e.leadStopped || e.campTerminal) return "cancel";
  return "skip";
}

export interface HoldRow {
  id: string;
  runId: string;
  costId: string;
  orgId: string | null;
  userId: string | null;
  action: HoldAction;
}

export interface ReconcileHoldsSummary {
  /** Total provisioned holds classified. */
  holdsClassified: number;
  /** Holds whose evidence says "sent" (planned actualize). */
  planActualize: number;
  /** Holds whose evidence says "terminal" (planned cancel). */
  planCancel: number;
  /** Holds still in flight (left provisioned). */
  planSkip: number;
  /** Cost rows flipped provisioned→actual (send billed). */
  actualized: number;
  /** Cost rows flipped provisioned→cancelled (refunded / unbillable). */
  cancelled: number;
  /** Cost rows left provisioned — transient runs-service error, retried next run. */
  transient: number;
  /** Holds where the local flip / PATCH threw unexpectedly. */
  failed: number;
  /** True when no mutation was performed (plan only). */
  dryRun: boolean;
}

/**
 * Select every `provisioned` hold with its real-evidence-derived action. One row
 * per cost row (each step has 2 — account + domain — each acted on independently
 * by its own run_id + cost_id). `limit` bounds the batch.
 */
export async function selectHoldActions(limit?: number): Promise<HoldRow[]> {
  const limitClause = limit && limit > 0 ? sql`LIMIT ${limit}` : sql``;
  const result = await db.execute(sql`
    WITH prov AS (
      SELECT
        sc.id, sc.run_id, sc.cost_id, sc.lead_email, sc.step,
        sc.campaign_id,
        COALESCE(sc.instantly_campaign_id, ic.instantly_campaign_id) AS icid,
        ic.org_id, ic.user_id
      FROM sequence_costs sc
      LEFT JOIN instantly_campaigns ic
        ON sc.campaign_id IS NOT NULL
       AND ic.campaign_id = sc.campaign_id
       AND ic.lead_email  = sc.lead_email
      WHERE sc.status = 'provisioned'
    )
    SELECT
      p.id                                        AS "id",
      p.run_id                                    AS "runId",
      p.cost_id                                   AS "costId",
      p.org_id                                    AS "orgId",
      p.user_id                                   AS "userId",
      -- has_sent: real email_sent at this step for this send
      CASE
        WHEN p.icid IS NOT NULL THEN EXISTS (
          SELECT 1 FROM instantly_events e
          WHERE e.campaign_id = p.icid AND e.lead_email = p.lead_email
            AND e.step = p.step AND e.event_type = 'email_sent' AND e.inferred = false)
        ELSE EXISTS (
          SELECT 1 FROM instantly_events e
          JOIN instantly_campaigns ic2 ON e.campaign_id = ic2.instantly_campaign_id
          WHERE ic2.campaign_id IS NULL AND ic2.lead_email = p.lead_email
            AND e.step = p.step AND e.event_type = 'email_sent' AND e.inferred = false)
      END AS has_sent,
      -- lead_stopped: real reply/bounce/unsub on this send (any step)
      CASE
        WHEN p.icid IS NOT NULL THEN EXISTS (
          SELECT 1 FROM instantly_events e
          WHERE e.campaign_id = p.icid AND e.lead_email = p.lead_email
            AND e.event_type IN ('reply_received','email_bounced','lead_unsubscribed')
            AND e.inferred = false)
        ELSE EXISTS (
          SELECT 1 FROM instantly_events e
          JOIN instantly_campaigns ic2 ON e.campaign_id = ic2.instantly_campaign_id
          WHERE ic2.campaign_id IS NULL AND ic2.lead_email = p.lead_email
            AND e.event_type IN ('reply_received','email_bounced','lead_unsubscribed')
            AND e.inferred = false)
      END AS lead_stopped,
      -- camp_terminal: send's campaign is paused/completed, OR (unresolvable
      -- platform hold) the lead has no active platform campaign left
      CASE
        WHEN p.icid IS NOT NULL THEN COALESCE((
          SELECT ic3.status IN ('paused','completed')
          FROM instantly_campaigns ic3 WHERE ic3.instantly_campaign_id = p.icid), false)
        WHEN p.campaign_id IS NULL THEN NOT EXISTS (
          SELECT 1 FROM instantly_campaigns ic4
          WHERE ic4.campaign_id IS NULL AND ic4.lead_email = p.lead_email
            AND ic4.status = 'active')
        ELSE false
      END AS camp_terminal
    FROM prov p
    ORDER BY p.id
    ${limitClause}
  `);
  const rows = (Array.isArray(result)
    ? result
    : (result as { rows?: unknown[] }).rows ?? []) as Array<{
    id: string;
    runId: string;
    costId: string;
    orgId: string | null;
    userId: string | null;
    has_sent: boolean;
    lead_stopped: boolean;
    camp_terminal: boolean;
  }>;

  return rows.map((r) => ({
    id: r.id,
    runId: r.runId,
    costId: r.costId,
    orgId: r.orgId,
    userId: r.userId,
    action: classifyHold({
      hasSent: r.has_sent === true,
      leadStopped: r.lead_stopped === true,
      campTerminal: r.camp_terminal === true,
    }),
  }));
}

/**
 * Reconcile stranded provisioned holds. `dryRun` (default true) returns the plan
 * counts without mutating. On commit, each non-skip hold is PATCHed in runs-service
 * then flipped locally; a terminal 404 (run purged) flips the local row to
 * `cancelled` (unbillable — mirrors handleEmailSent); a transient error leaves it
 * provisioned for the next run. Fail-soft per hold: one failure never aborts the
 * sweep. Logs progress for a Railway-log watcher (`reconcile-provisioned-holds: done`).
 */
export async function reconcileProvisionedHolds(
  opts: { dryRun?: boolean; limit?: number } = {},
): Promise<ReconcileHoldsSummary> {
  const dryRun = opts.dryRun !== false; // default true
  const startedAt = Date.now();
  const holds = await selectHoldActions(opts.limit);

  const planActualize = holds.filter((h) => h.action === "actualize").length;
  const planCancel = holds.filter((h) => h.action === "cancel").length;
  const planSkip = holds.filter((h) => h.action === "skip").length;

  console.log(
    `[instantly-service] reconcile-provisioned-holds: ${dryRun ? "DRY-RUN " : ""}` +
      `classified=${holds.length} actualize=${planActualize} cancel=${planCancel} ` +
      `skip=${planSkip}` + (opts.limit ? ` (limit=${opts.limit})` : ""),
  );

  const summary: ReconcileHoldsSummary = {
    holdsClassified: holds.length,
    planActualize,
    planCancel,
    planSkip,
    actualized: 0,
    cancelled: 0,
    transient: 0,
    failed: 0,
    dryRun,
  };

  if (dryRun) return summary;

  for (const h of holds) {
    if (h.action === "skip") continue;
    const identity: IdentityContext = {
      orgId: h.orgId || "system",
      userId: h.userId || NIL_USER_UUID,
      runId: h.runId,
    };
    const target = h.action === "actualize" ? "actual" : "cancelled";
    try {
      await updateCostStatus(h.runId, h.costId, target, identity);
      await db
        .update(sequenceCosts)
        .set({ status: target, updatedAt: new Date() })
        .where(eq(sequenceCosts.id, h.id));
      if (target === "actual") summary.actualized++;
      else summary.cancelled++;
    } catch (error: unknown) {
      if (isRunGoneError(error)) {
        // Run purged (retention 404). Actualize can never bill it and a cancel
        // has nothing to refund — the hold was never actualized. Either way the
        // hold is terminal + unbillable: flip local to cancelled so it stops
        // being re-swept. No runs-service PATCH (it 404s).
        await db
          .update(sequenceCosts)
          .set({ status: "cancelled", updatedAt: new Date() })
          .where(eq(sequenceCosts.id, h.id));
        summary.cancelled++;
      } else {
        // Transient (5xx / timeout / 403 / cold-start) — leave provisioned.
        summary.transient++;
        const message = error instanceof Error ? error.message : String(error);
        console.error(
          `[instantly-service] reconcile-provisioned-holds: hold=${h.id} ` +
            `action=${h.action} transient, left provisioned: ${message}`,
        );
      }
    }
  }

  console.log(
    `[instantly-service] reconcile-provisioned-holds: done, classified=${holds.length} ` +
      `actualized=${summary.actualized} cancelled=${summary.cancelled} ` +
      `transient=${summary.transient} failed=${summary.failed} ` +
      `durationMs=${Date.now() - startedAt}`,
  );
  return summary;
}
