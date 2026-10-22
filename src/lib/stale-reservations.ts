/**
 * What a send interrupted mid-flight leaves behind, taken back (STALE-RESERVATION-1016).
 *
 * `POST /orgs/send` (and restore-stopped-followups) works in this order: RESERVE the
 * (campaign, lead) row with a `reserving:<uuid>` sentinel, create one runs-service
 * step run per email and PROVISION its two costs, AUTHORIZE, pick the mailbox, then
 * phase-2 swaps the sentinel for the real id and the holds are written to
 * `sequence_costs` (the send queue). A failure inside the handler gives everything
 * back (`releaseReservation`, `abandonStepHolds`). A process that DIES in between
 * (OOM 2026-10-16 14:49:46 UTC, a deploy) gives nothing back:
 *
 *   - the row keeps its sentinel. `classifyHeldRow` answers `in_flight` to every
 *     duplicate forever, and nothing is queued, so nothing is ever emailed.
 *   - the step runs stay `running` with their costs `provisioned`, on no local hold
 *     (their ids lived in memory only): the org is held money for emails that will
 *     never leave.
 *
 * ── Two sweeps ────────────────────────────────────────────────────────────────
 * RELEASE: a `reserving:` row older than `STALE_RESERVATION_RECLAIM_MS` is deleted,
 * exactly as the handler's own failure path does (`releaseReservation`), so the lead
 * is servable again and lead-service is told (`announceEvidenceChanged`). It is NOT
 * retried here: the row holds no subject, body or sequence (those are written at
 * phase-2), so only the caller can send it again, through every door of
 * `POST /orgs/send` (verification, billing, opt-out).
 *
 * Never released when an email may have left. On our own sender nothing can leave
 * before phase-2 (the dispatcher reads `sequence_costs`, written after it). On the
 * Instantly transport the campaign is created at Instantly BEFORE phase-2, so a
 * crash there can leave a live campaign we hold no id for. Evidence of that:
 * `metadata.instantlyCreateStartedAt` (stamped by send.ts just before the Instantly
 * call), or a `send-campaign-created` trace naming a non-`self:` id on the row's run
 * (legacy rows, before the stamp). Such a row is KEPT, marked
 * `metadata.staleReservationKept` (logged once, never re-examined): a held claim is
 * a duplicate, never a second email. A run runs-service no longer has (404,
 * retention) carries no trace to read: our own records are then the only evidence,
 * and they hold none.
 *
 * SETTLE: every `email-send-step-*` run of ours still `running` after the same delay
 * whose provisioned costs sit on NO `sequence_costs` row is an orphan: its costs are
 * cancelled and the run failed (`settleOrphanStepRun`). Keyed on runs-service, not on
 * the reservation row, because a retry that reclaims the row (`setWhere` stale)
 * overwrites its `run_id`: the crashed attempt's step runs are then reachable from
 * nowhere else (prod 2026-10-13 and 10-16). A run carrying an `actual` cost is never
 * touched (legacy runs were never completed).
 *
 * The retry-stuck worker is NOT re-armed for this (`RETRY_STUCK_WORKER_ENABLED=false`
 * in prod: re-arming it redispatches hundreds of never-contacted Instantly sequences
 * fleet-wide, a volume decision; and it only acts on committed rows anyway).
 */

import { errorMessage as dbErrorMessage } from "./db-error.js";
import { sql } from "drizzle-orm";

import { db } from "../db";
import { announceEvidenceChanged } from "./evidence-changed";
import {
  getRunWithCosts,
  isRunGoneError,
  listRunEvents,
  listRuns,
  updateCostStatus,
  updateRun,
  type IdentityContext,
  type RunCost,
  type RunEvent,
} from "./runs-client";

/**
 * How old a sentinel / a running step run must be before it is taken back. The
 * handler's whole reserve -> phase-2 window is seconds and the longest request any
 * caller waits on is 300 s (Caddy `read_timeout` 310 s): 15 minutes cannot be a
 * live send.
 */
export const STALE_RESERVATION_RECLAIM_MS = 15 * 60 * 1000;

/** How far back the orphan step-run sweep looks (runs-service `startedAfter`). */
export const ORPHAN_STEP_RUN_LOOKBACK_DAYS = 30;

const RESERVATION_PREFIX = "reserving:";
const SELF_SEND_PREFIX = "self:";
const STEP_RUN_TASK_PREFIX = "email-send-step-";

export interface StaleReservationRow {
  id: string;
  instantlyCampaignId: string;
  campaignId: string | null;
  leadEmail: string;
  orgId: string | null;
  userId: string | null;
  runId: string | null;
  createdAt: string;
  instantlyCreateStartedAt: string | null;
}

export type StaleReservationDecision =
  | { action: "release" }
  | { action: "keep"; reason: string };

/**
 * Pure: may this stale reservation be released? `events` = the traces on the row's
 * run, null when runs-service no longer has the run.
 */
export function decideStaleReservation(
  row: Pick<StaleReservationRow, "instantlyCreateStartedAt">,
  events: RunEvent[] | null,
): StaleReservationDecision {
  if (row.instantlyCreateStartedAt) {
    return {
      action: "keep",
      reason: `an Instantly campaign was being created at ${row.instantlyCreateStartedAt}: an email may have left`,
    };
  }
  const created = (events ?? []).find(
    (e) => e.event === "send-campaign-created" && !createdCampaignIdOf(e).startsWith(SELF_SEND_PREFIX),
  );
  if (created) {
    return {
      action: "keep",
      reason: `the run traced send-campaign-created (${created.detail ?? "no detail"}) at ${created.createdAt}: an email may have left`,
    };
  }
  return { action: "release" };
}

/** The `instantlyCampaignId=<id>` the send-campaign-created trace names ("" when absent). */
function createdCampaignIdOf(event: RunEvent): string {
  const match = /instantlyCampaignId=([^,\s]+)/.exec(event.detail ?? "");
  return match ? match[1]! : "";
}

export type OrphanVerdict =
  | { orphan: true; costIds: string[] }
  | { orphan: false; reason: string };

/**
 * Pure: is this `running` step run an orphan, and which costs does it give back?
 * `queuedCostIds` = the cost ids some `sequence_costs` row carries.
 */
export function classifyStepRun(costs: RunCost[], queuedCostIds: ReadonlySet<string>): OrphanVerdict {
  if (costs.some((c) => c.status === "actual")) return { orphan: false, reason: "carries an actual cost" };
  const provisioned = costs.filter((c) => c.status === "provisioned");
  if (provisioned.length === 0) return { orphan: false, reason: "no provisioned cost" };
  if (provisioned.some((c) => queuedCostIds.has(c.id))) {
    return { orphan: false, reason: "a queued hold carries its cost" };
  }
  return { orphan: true, costIds: provisioned.map((c) => c.id) };
}

function rowsOf<T>(result: unknown): T[] {
  if (Array.isArray(result)) return result as T[];
  const rows = (result as { rows?: unknown } | null)?.rows;
  return Array.isArray(rows) ? (rows as T[]) : [];
}

function errorMessage(error: unknown): string {
  return dbErrorMessage(error);
}

export interface ReleaseSummary {
  stale: number;
  released: number;
  kept: number;
  skipped: number;
}

/** RELEASE sweep (see the header). Per-row failures are logged and retried next tick. */
export async function releaseStaleReservations(
  opts: { limit?: number; staleMs?: number } = {},
): Promise<ReleaseSummary> {
  const limit = opts.limit ?? 100;
  const staleSecs = Math.floor((opts.staleMs ?? STALE_RESERVATION_RECLAIM_MS) / 1000);
  const rows = rowsOf<StaleReservationRow>(
    await db.execute(sql`
      SELECT
        c.id,
        c.instantly_campaign_id AS "instantlyCampaignId",
        c.campaign_id AS "campaignId",
        c.lead_email AS "leadEmail",
        c.org_id AS "orgId",
        c.user_id AS "userId",
        c.run_id AS "runId",
        to_char(c.created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS "createdAt",
        c.metadata->>'instantlyCreateStartedAt' AS "instantlyCreateStartedAt"
      FROM instantly_campaigns c
      WHERE c.instantly_campaign_id LIKE ${RESERVATION_PREFIX + "%"}
        AND c.created_at < now() - make_interval(secs => ${staleSecs})
        AND NOT (COALESCE(c.metadata, '{}'::jsonb) ? 'staleReservationKept')
      ORDER BY c.created_at
      LIMIT ${limit}
    `),
  );

  const summary: ReleaseSummary = { stale: rows.length, released: 0, kept: 0, skipped: 0 };
  for (const row of rows) {
    const label = `row=${row.id} sentinel=${row.instantlyCampaignId} lead=${row.leadEmail} org=${row.orgId ?? "none"} campaign=${row.campaignId ?? "none"} run=${row.runId ?? "none"} reservedAt=${row.createdAt}`;
    try {
      let events: RunEvent[] | null = null;
      if (row.runId) {
        if (!row.orgId || !row.userId) {
          console.error(`[stale-reservation] SKIPPED ${label}: no org/user to read its run with`);
          summary.skipped++;
          continue;
        }
        events = await listRunEvents(row.runId, { orgId: row.orgId, userId: row.userId });
      }
      const decision = decideStaleReservation(row, events);

      if (decision.action === "keep") {
        const marked = rowsOf<{ id: string }>(
          await db.execute(sql`
            UPDATE instantly_campaigns
            SET metadata = COALESCE(metadata, '{}'::jsonb)
                  || jsonb_build_object('staleReservationKept', jsonb_build_object('reason', ${decision.reason}::text, 'at', now()))
            WHERE id = ${row.id} AND instantly_campaign_id = ${row.instantlyCampaignId}
            RETURNING id
          `),
        );
        if (marked.length > 0) {
          summary.kept++;
          console.error(`[stale-reservation] KEPT (never re-sent) ${label}: ${decision.reason}`);
        }
        continue;
      }

      // Same sentinel AND still stale: a retry that reclaimed the row meanwhile
      // carries a new sentinel and a fresh clock, and is left alone.
      const deleted = rowsOf<{ id: string }>(
        await db.execute(sql`
          DELETE FROM instantly_campaigns
          WHERE id = ${row.id}
            AND instantly_campaign_id = ${row.instantlyCampaignId}
            AND created_at < now() - make_interval(secs => ${staleSecs})
          RETURNING id
        `),
      );
      if (deleted.length === 0) continue;
      summary.released++;
      console.warn(
        `[stale-reservation] RELEASED ${label}: interrupted before anything was queued, nothing sent; the lead is servable again`,
      );
      void announceEvidenceChanged(row.orgId, [row.leadEmail], "stale_reservation_released");
    } catch (error) {
      summary.skipped++;
      console.error(`[stale-reservation] FAILED ${label} (retried next tick): ${errorMessage(error)}`);
    }
  }
  return summary;
}

export interface SettleSummary {
  orgs: number;
  candidates: number;
  settled: number;
  costsCancelled: number;
  failed: number;
}

/** SETTLE sweep (see the header). Per-org / per-run failures are logged and retried next tick. */
export async function settleOrphanStepRuns(
  opts: { asOf?: Date; staleMs?: number; lookbackDays?: number } = {},
): Promise<SettleSummary> {
  const asOf = opts.asOf ?? new Date();
  const staleMs = opts.staleMs ?? STALE_RESERVATION_RECLAIM_MS;
  const lookbackDays = opts.lookbackDays ?? ORPHAN_STEP_RUN_LOOKBACK_DAYS;
  const startedBefore = new Date(asOf.getTime() - staleMs).toISOString();
  const startedAfter = new Date(asOf.getTime() - lookbackDays * 86_400_000).toISOString();

  // Every org that wrote a send row in the window, read with a REAL user of that
  // org (never a placeholder identity).
  const orgs = rowsOf<{ orgId: string; userId: string }>(
    await db.execute(sql`
      SELECT DISTINCT ON (org_id) org_id AS "orgId", user_id AS "userId"
      FROM instantly_campaigns
      WHERE org_id IS NOT NULL AND user_id IS NOT NULL
        AND COALESCE(updated_at, created_at) > now() - make_interval(days => ${lookbackDays})
      ORDER BY org_id, COALESCE(updated_at, created_at) DESC
    `),
  );

  const summary: SettleSummary = { orgs: orgs.length, candidates: 0, settled: 0, costsCancelled: 0, failed: 0 };
  for (const org of orgs) {
    const orgIdentity: IdentityContext = { orgId: org.orgId, userId: org.userId };
    let runs;
    try {
      runs = await listRuns(
        { serviceName: "instantly-service", status: "running", startedAfter, startedBefore },
        orgIdentity,
      );
    } catch (error) {
      summary.failed++;
      console.error(`[stale-reservation] orphan sweep: could not list runs of org ${org.orgId}: ${errorMessage(error)}`);
      continue;
    }
    const candidates = runs.filter(
      (r) =>
        r.taskName.startsWith(STEP_RUN_TASK_PREFIX) &&
        Number(r.ownProvisionedCostInUsdCents) > 0 &&
        Date.parse(r.startedAt) < Date.parse(startedBefore),
    );
    for (const candidate of candidates) {
      summary.candidates++;
      try {
        const cancelled = await settleOrphanStepRun(candidate.id, orgIdentity);
        if (cancelled > 0) {
          summary.settled++;
          summary.costsCancelled += cancelled;
        }
      } catch (error) {
        summary.failed++;
        console.error(
          `[stale-reservation] orphan sweep: could not settle step run ${candidate.id} of org ${org.orgId} (retried next tick): ${errorMessage(error)}`,
        );
      }
    }
  }
  return summary;
}

/**
 * Cancel every provisioned cost of one orphan step run and fail the run. Returns how
 * many costs were cancelled (0 = not an orphan, left untouched). Throws on a
 * transient runs-service error; a 404 (run or cost gone) needs nothing more.
 */
async function settleOrphanStepRun(stepRunId: string, orgIdentity: IdentityContext): Promise<number> {
  const run = await getRunWithCosts(stepRunId, orgIdentity);
  if (!run || run.status !== "running") return 0;

  const costIds = run.costs.map((c) => c.id);
  const queued = rowsOf<{ id: string }>(
    await db.execute(sql`
      SELECT cost_id AS id FROM sequence_costs WHERE cost_id = ANY(${sql.param(costIds)}::text[])
      UNION
      SELECT domain_cost_id AS id FROM sequence_costs WHERE domain_cost_id = ANY(${sql.param(costIds)}::text[])
    `),
  );
  const verdict = classifyStepRun(run.costs, new Set(queued.map((q) => q.id)));
  if (!verdict.orphan) return 0;

  const identity: IdentityContext = { ...orgIdentity, runId: stepRunId };
  for (const costId of verdict.costIds) {
    try {
      await updateCostStatus(stepRunId, costId, "cancelled", identity);
    } catch (error) {
      if (!isRunGoneError(error)) throw error;
    }
  }
  try {
    await updateRun(stepRunId, "failed", identity, "interrupted send: no hold ever queued this step, its costs are cancelled");
  } catch (error) {
    if (!isRunGoneError(error)) throw error;
  }
  console.warn(
    `[stale-reservation] SETTLED orphan step run ${stepRunId} (${run.taskName}, parent ${run.parentRunId ?? "none"}, org ${orgIdentity.orgId}, started ${run.startedAt}): ${verdict.costIds.length} provisioned cost(s) cancelled, nothing was sent`,
  );
  return verdict.costIds.length;
}

let inFlight: Promise<{ release: ReleaseSummary; settle: SettleSummary }> | null = null;

/** Both sweeps; concurrent callers join the one in flight. */
export function reclaimInterruptedSends(): Promise<{ release: ReleaseSummary; settle: SettleSummary }> {
  if (inFlight) return inFlight;
  inFlight = (async () => {
    const release = await releaseStaleReservations();
    const settle = await settleOrphanStepRuns();
    if (release.stale > 0 || settle.candidates > 0 || settle.failed > 0) {
      console.log(`[stale-reservation] done ${JSON.stringify({ release, settle })}`);
    }
    return { release, settle };
  })().finally(() => {
    inFlight = null;
  });
  return inFlight;
}
