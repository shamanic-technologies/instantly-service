/**
 * Put back, fleet-wide, the follow-ups of contacted leads that were cut when
 * nothing about the LEAD said stop (OLIVE-FOLLOWUPS-1103).
 *
 * Owner rule (2026-10-06, again 2026-11-03): a lead who got our first email gets
 * every planned follow-up, whether the campaign is ongoing or stopped; a
 * follow-up is paid for at the first email, so an empty balance is never a
 * reason to skip it. Cuts found in prod against that rule:
 *   - a payment stop (`payment_declined` / `no_payment_method`) cut every queued
 *     sequence of the campaign (`stopped-campaigns.ts`, fixed);
 *   - a bounce later retracted as "the receiver refused the MESSAGE, the address
 *     is fine" (`bounce-retraction.ts`) had already cancelled the rest of the
 *     sequence (Olive: 22 leads, step 1 only);
 *   (a sequence whose mailbox we no longer have is NOT owed anything: owner
 *   2026-11-03, "cancel them when we dont have the mailbox anymore". The
 *   2026-10-22 stop of 374 sequences on 19 deleted Primeforge mailboxes stands.)
 *
 * The population is one RULE, never a list: a self-sent sequence
 *   - that really emailed the lead (a real `email_sent`);
 *   - with steps after the last sent one whose hold was cancelled and never
 *     sent, no live hold on that step, and a body to send;
 *   - cut since {@link CUT_SINCE} (the day after the owner rule's restore,
 *     2026-10-06: earlier cuts were judged then), or on a sequence whose bounce
 *     was retracted (any date: the cut rested on a fact we took back);
 *   - with NO stop evidence for the lead: reply / bounce / unsubscribe / click
 *     / any reply kind on the sequence, a standing opt-out, a manual
 *     qualification, a follow-up cut naming them (LEAK-1012), the one-time
 *     follow-up cut of FOLLOWUP-JIT-1107 (`followup_jit_cuts`: the follow-up
 *     is written on its due day by lead-service now), a refused latest
 *     verdict, a newer sequence of the same org to the same address, a row
 *     closed by the Instantly plan cut or as a stalled first email;
 *   - whose campaign was not stopped for an org teardown (campaign-service);
 *   - whose sending mailbox we still hold a credential for (else nothing could
 *     send it, and its follow-ups are cancelled, never moved).
 * A person who booked a meeting is stopped again by the booked sweep before
 * anything leaves (`booked-stops-cold.ts`, top of every dispatch tick).
 *
 * Each one goes back through `restoreOne` (fresh provisioned holds billed to
 * the org, row back to `active`); the dispatcher then sends at its normal pace,
 * from the lead's mailbox (or a production mailbox when that one, still ours,
 * is refused or mostly in spam there). Idempotent: a restored step holds a provisioned row, so it is
 * no longer a candidate. Dry-run by default.
 */

import { errorMessage } from "./db-error.js";
import { sql } from "drizzle-orm";

import { db } from "../db";
import { listCampaignStatuses } from "./campaign-client";
import type { CallerInfo } from "./key-client";
import { restoreOne, type RestoreCandidate } from "./restore-stopped-followups";
import { loadCredentialedMailboxes } from "./self-send/mailbox-credentials";
import { stopCutsStartedSequences, stoppedCampaignIds } from "./stopped-campaigns";

/** Cuts before this were judged at the 2026-10-06 restore; only later ones are taken back by date. */
export const CUT_SINCE = "2026-10-07T00:00:00Z";

function rowsOf(result: unknown): Record<string, unknown>[] {
  if (Array.isArray(result)) return result as Record<string, unknown>[];
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

export interface CutFollowupCandidate extends RestoreCandidate {
  /** The row was marked `paused` by the cut (else still `active`, holds cancelled). */
  paused: boolean;
  /** The mailbox the sequence is pinned to. */
  accountEmail: string | null;
}

/**
 * The candidates, optionally narrowed to some sequences (`instantlyCampaignIds`)
 * or one org. Pure SQL; the org-teardown exclusion is applied by the caller.
 */
export async function loadCutFollowupCandidates(
  filter: { instantlyCampaignIds?: string[]; orgId?: string; awaitingVerdict?: boolean } = {},
): Promise<CutFollowupCandidate[]> {
  const ids = filter.instantlyCampaignIds ?? null;
  if (ids !== null && ids.length === 0) return [];
  const idsParam = ids === null ? null : sql.param(ids);
  const orgParam = filter.orgId ?? null;
  const result = await db.execute(sql`
    WITH retracted AS (
      SELECT DISTINCT r.event->>'campaign_id' AS id
      FROM instantly_events_retracted r
      WHERE r.action = 'retracted' AND r.event->>'event_type' = 'email_bounced'
    ),
    seqs AS (
      SELECT c.*
      FROM instantly_campaigns c
      WHERE c.status IN ('active', 'paused')
        AND c.instantly_campaign_id LIKE 'self:%'
        AND c.org_id IS NOT NULL
        AND c.campaign_id IS NOT NULL
        AND c.lead_email IS NOT NULL
        AND NOT (COALESCE(c.metadata, '{}'::jsonb) ? 'instantlyPlanRetired')
        AND NOT (COALESCE(c.metadata, '{}'::jsonb) ? 'stalledFirstEmailClosed')
        AND (${idsParam}::text[] IS NULL OR c.instantly_campaign_id = ANY(${idsParam}::text[]))
        AND (${orgParam}::text IS NULL OR c.org_id = ${orgParam}::text)
        AND EXISTS (
          SELECT 1 FROM sequence_costs x
          WHERE x.instantly_campaign_id = c.instantly_campaign_id AND x.status = 'cancelled'
        )
    ),
    contacted AS (
      SELECT s.id, MAX(e.step) AS last_sent_step
      FROM seqs s
      JOIN instantly_events e
        ON e.campaign_id = s.instantly_campaign_id
       AND e.event_type = 'email_sent'
       AND e.inferred = false
       AND e.withdrawn_at IS NULL
      GROUP BY s.id
    ),
    cut AS (
      SELECT s.id, jsonb_agg(DISTINCT sc.step ORDER BY sc.step) AS steps
      FROM seqs s
      JOIN contacted k ON k.id = s.id
      JOIN sequence_costs sc ON sc.instantly_campaign_id = s.instantly_campaign_id
      WHERE sc.status = 'cancelled'
        AND sc.step > k.last_sent_step
        AND (sc.updated_at >= ${CUT_SINCE}::timestamptz
             OR s.instantly_campaign_id IN (SELECT id FROM retracted))
        AND EXISTS (
          SELECT 1 FROM sequence_steps st
          WHERE st.instantly_campaign_id = s.instantly_campaign_id
            AND st.step = sc.step AND st.body_html <> ''
        )
        AND NOT EXISTS (
          SELECT 1 FROM sequence_costs live
          WHERE live.instantly_campaign_id = s.instantly_campaign_id
            AND live.step = sc.step
            AND live.status IN ('provisioned', 'actual')
        )
        AND NOT EXISTS (
          SELECT 1 FROM instantly_events se
          WHERE se.campaign_id = s.instantly_campaign_id
            AND se.event_type = 'email_sent'
            AND se.step = sc.step
            AND se.inferred = false
        )
      GROUP BY s.id
    )
    SELECT s.id                    AS "rowId",
           s.instantly_campaign_id AS "instantlyCampaignId",
           s.campaign_id           AS "campaignId",
           s.org_id                AS "orgId",
           s.user_id               AS "userId",
           s.run_id                AS "runId",
           s.brand_ids             AS "brandIds",
           s.lead_email            AS "leadEmail",
           s.account_email         AS "accountEmail",
           (s.status = 'paused')   AS "paused",
           cut.steps               AS "steps"
    FROM seqs s
    JOIN cut ON cut.id = s.id
    WHERE NOT EXISTS (
        SELECT 1 FROM instantly_events x
        WHERE x.campaign_id = s.instantly_campaign_id
          AND x.withdrawn_at IS NULL
          AND (x.event_type IN ('reply_received', 'email_bounced', 'lead_unsubscribed', 'email_link_clicked')
               OR x.event_type LIKE 'lead\\_%')
      )
      AND NOT EXISTS (
        SELECT 1 FROM instantly_lead_optouts_raw o
        WHERE o.org_id = s.org_id
          AND lower(o.lead_email) = lower(s.lead_email)
          AND NOT EXISTS (
            SELECT 1 FROM instantly_lead_optout_withdrawals w WHERE w.optout_id = o.id
          )
      )
      AND NOT EXISTS (
        SELECT 1 FROM instantly_manual_qualifications_raw q
        WHERE q.org_id = s.org_id AND lower(q.lead_email) = lower(s.lead_email)
      )
      AND NOT EXISTS (
        SELECT 1 FROM instantly_followup_cuts f
        WHERE f.instantly_campaign_id = s.instantly_campaign_id
      )
      AND NOT EXISTS (
        SELECT 1 FROM followup_jit_cuts j
        WHERE j.instantly_campaign_id = s.instantly_campaign_id
      )
      AND NOT EXISTS (
        SELECT 1 FROM instantly_campaigns n
        WHERE n.org_id = s.org_id
          AND lower(n.lead_email) = lower(s.lead_email)
          AND n.created_at > s.created_at
          AND n.instantly_campaign_id <> s.instantly_campaign_id
          AND n.instantly_campaign_id NOT LIKE 'reserving:%'
          AND (
            EXISTS (
              SELECT 1 FROM instantly_events ne
              WHERE ne.campaign_id = n.instantly_campaign_id
                AND ne.event_type = 'email_sent' AND ne.inferred = false
            )
            OR EXISTS (
              SELECT 1 FROM sequence_costs np
              WHERE np.instantly_campaign_id = n.instantly_campaign_id AND np.status = 'provisioned'
            )
          )
      )
      AND ${
        filter.awaitingVerdict
          ? sql`(
        SELECT v.reason FROM email_send_verifications v
        WHERE v.org_id = s.org_id AND v.email = lower(s.lead_email)
        ORDER BY v.created_at DESC LIMIT 1
      ) = 'verifier_failed'`
          : sql`COALESCE((
        SELECT v.accepted FROM email_send_verifications v
        WHERE v.org_id = s.org_id AND v.email = lower(s.lead_email)
        ORDER BY v.created_at DESC LIMIT 1
      ), true)`
      }
    ORDER BY s.org_id, s.campaign_id, s.lead_email
  `);
  return rowsOf(result).map((r) => ({
    rowId: String(r.rowId),
    instantlyCampaignId: String(r.instantlyCampaignId),
    campaignId: String(r.campaignId),
    orgId: String(r.orgId),
    userId: r.userId === null || r.userId === undefined ? null : String(r.userId),
    runId: r.runId === null || r.runId === undefined ? null : String(r.runId),
    brandIds: Array.isArray(r.brandIds) ? (r.brandIds as string[]) : null,
    leadEmail: String(r.leadEmail),
    paused: r.paused === true,
    accountEmail: typeof r.accountEmail === "string" ? r.accountEmail : null,
    steps: Array.isArray(r.steps) ? (r.steps as unknown[]).map(Number) : [],
  }));
}

/** Drop the candidates whose campaign campaign-service stopped for an org teardown. Pure. */
export function withoutTornDownOrgs<T extends { campaignId: string }>(
  candidates: T[],
  stopped: Map<string, string | null>,
): T[] {
  return candidates.filter((c) => !stopCutsStartedSequences(stopped.get(c.campaignId) ?? null));
}

/** Keep only the candidates whose mailbox we still hold a credential for. Pure. */
export function onHeldMailboxes<T extends { accountEmail: string | null }>(
  candidates: T[],
  held: ReadonlySet<string>,
): T[] {
  return candidates.filter((c) => c.accountEmail !== null && held.has(c.accountEmail));
}

export interface CutFollowupRestoreSummary {
  dryRun: boolean;
  candidates: number;
  steps: number;
  /** Per org: leads and steps put back (dry run: that would be). */
  byOrg: Record<string, { leads: number; steps: number }>;
  restored: number;
  restoredSteps: number;
  failed: number;
}

/**
 * Restore the cut follow-ups. `instantlyCampaignIds` narrows to given sequences
 * (the bounce retraction passes the ones it just took back); `orgId` to one org.
 */
export async function restoreCutFollowups(options: {
  dryRun: boolean;
  limit?: number;
  orgId?: string;
  instantlyCampaignIds?: string[];
  /**
   * Only the candidates whose address got NO verdict last time (the verifier
   * failed: vendor down, or the org could not pay the check). Owner 2026-11-03
   * (option 1): they wait and go out once the check can be made; the worker
   * asks again (`restore-cut-followups-worker.ts`).
   */
  awaitingVerdict?: boolean;
  caller: CallerInfo;
}): Promise<CutFollowupRestoreSummary> {
  const loaded = await loadCutFollowupCandidates({
    instantlyCampaignIds: options.instantlyCampaignIds,
    awaitingVerdict: options.awaitingVerdict,
    orgId: options.orgId,
  });
  const all =
    loaded.length === 0
      ? loaded
      : onHeldMailboxes(
          withoutTornDownOrgs(loaded, stoppedCampaignIds(await listCampaignStatuses())),
          await loadCredentialedMailboxes(options.caller),
        );
  const candidates = options.limit ? all.slice(0, options.limit) : all;

  const byOrg: CutFollowupRestoreSummary["byOrg"] = {};
  let steps = 0;
  for (const c of candidates) {
    const b = (byOrg[c.orgId] ??= { leads: 0, steps: 0 });
    b.leads += 1;
    b.steps += c.steps.length;
    steps += c.steps.length;
  }
  const summary: CutFollowupRestoreSummary = {
    dryRun: options.dryRun,
    candidates: candidates.length,
    steps,
    byOrg,
    restored: 0,
    restoredSteps: 0,
    failed: 0,
  };
  if (options.dryRun) return summary;

  for (const c of candidates) {
    try {
      await restoreOne(c, options.caller);
      summary.restored += 1;
      summary.restoredSteps += c.steps.length;
    } catch (error: unknown) {
      summary.failed += 1;
      console.error(
        `[instantly-service] restore-cut-followups: failed campaign=${c.instantlyCampaignId} lead=${c.leadEmail}: ${errorMessage(error)}`,
      );
    }
  }
  console.log(`[instantly-service] restore-cut-followups: done ${JSON.stringify(summary)}`);
  return summary;
}
