/**
 * What a stopped campaign stops: NEW first emails always; the queued follow-ups
 * only when the org cannot go on (torn down, or billing cannot charge it).
 *
 * ⚠️ OWNER RULE (2026-10-06, verbatim): "stopping a campaign SHOULD NOT pause
 * the followups!!!" A customer stop (`stop_reason` `manual`, or none) means no
 * new prospect is emailed. A lead we ALREADY emailed keeps receiving the
 * sequence they were promised, on its own schedule, billed as before. The
 * first version of this sweep (2026-10-02) cut every queued step of every
 * stopped campaign; ~3,800 contacted leads lost their follow-ups, and a resume
 * restored none of them (`restore-stopped-followups.ts` put them back).
 *
 * campaign-service OWNS whether a campaign is running, and why it stopped
 * (`stop_reason`, its closed vocabulary in campaign-service `stop-reason.ts`).
 * It pushes the status to nobody; our send queue (`provisioned` holds) outlives
 * the campaign by up to a sequence length, and the dispatcher reads only local
 * state. So the queue ASKS the owner, once per dispatch tick, BEFORE selecting
 * (`runDispatch`). One `GET /campaigns/list` answers for the whole fleet.
 *
 * Which queued sequences of a stopped campaign are stopped here:
 *   - stop reason in {@link CUT_ALL_STOP_REASONS} (`org_teardown`,
 *     `payment_declined`, `no_payment_method`): EVERY queued sequence. The org
 *     is gone or cannot pay. Prod 2026-10-02: a torn-down throwaway org's first
 *     emails left three minutes after the teardown.
 *   - any other reason (`manual`, null): ONLY a sequence that never sent a real
 *     email (its first email is a NEW first email). A contacted lead is left
 *     alone and its follow-ups go out.
 * Stopping one:
 *   - self-send (`self:`): `stopSelfSendSequence` — holds cancelled (refund),
 *     row marked `paused`, out of the dispatcher's reach.
 *   - Instantly: the per-lead Instantly campaign is PAUSED first (that is what
 *     stops the actual email); only once that succeeded are the holds cancelled
 *     and the row marked `paused`. A failed pause leaves the row untouched and
 *     the next tick retries. Marking the row locally is safe HERE although
 *     `reconcileAll` then skips it: the only thing its `finish` would have done
 *     that matters — cancel the holds — is done here, and the contact on a
 *     paused Instantly campaign is freed by `cleanup:finished-contacts`.
 *
 * ⚠️ A STOPPED ROW IS NOT A STOPPED CAMPAIGN. campaign-service keeps an
 * ancestor row per workflow change (`campaign-identity.ts`), so a sequence can
 * sit under a stopped ancestor of a campaign that is running. A campaign id
 * counts as stopped only when no row of its family is anything but `stopped`.
 *
 * A local campaign id campaign-service does not know is NOT stopped (absence
 * proves nothing); it is counted in the summary. Platform sends
 * (`campaign_id IS NULL`) belong to no campaign and are never touched.
 */

import { sql } from "drizzle-orm";

import { db } from "../db";
import { listCampaignStatuses, type CampaignStatusRow } from "./campaign-client";
import { identityKeyOf } from "./campaign-identity";
import { resolveInstantlyApiKey, type CallerInfo } from "./key-client";
import { updateCampaignStatus } from "./instantly-client";
import { isSelfSendCampaignId } from "./self-send/transport";
import { stopSelfSendSequence } from "./self-send/stop-sequence";

/**
 * The campaign ids whose campaign is stopped, as campaign-service states it.
 *
 * Pure. A row is stopped iff its own status is `stopped` AND every row sharing
 * its identity key is `stopped` too; a row stating too little to pool is judged
 * on its own status alone.
 */
export function stoppedCampaignIds(rows: CampaignStatusRow[]): Map<string, string | null> {
  const liveFamilies = new Set<string>();
  for (const row of rows) {
    if (row.status === "stopped") continue;
    const key = identityKeyOf(row);
    if (key !== null) liveFamilies.add(key);
  }

  const stopped = new Map<string, string | null>();
  for (const row of rows) {
    if (!row.id || row.status !== "stopped") continue;
    const key = identityKeyOf(row);
    if (key !== null && liveFamilies.has(key)) continue;
    stopped.set(row.id, row.stopReason ?? null);
  }
  return stopped;
}

/**
 * The stop reasons that cut a campaign's ALREADY-STARTED sequences too: the org
 * is torn down, or billing cannot charge it. Every other stop (a person's
 * `manual` decision, or no reason) stops only NEW first emails — owner rule
 * 2026-10-06. campaign-service owns the vocabulary; read verbatim.
 */
export const CUT_ALL_STOP_REASONS: ReadonlySet<string> = new Set([
  "org_teardown",
  "payment_declined",
  "no_payment_method",
]);

/** Whether a stop with this reason cuts a sequence that already emailed its lead. */
export function stopCutsStartedSequences(stopReason: string | null): boolean {
  return stopReason !== null && CUT_ALL_STOP_REASONS.has(stopReason);
}

/** One local sequence still holding a provisioned step. */
export interface QueuedSequence {
  instantlyCampaignId: string;
  campaignId: string;
  orgId: string | null;
  userId: string | null;
  runId: string | null;
  leadEmail: string;
  /** A real (`inferred=false`) email has been sent to this lead on this sequence. */
  contacted: boolean;
}

/**
 * At most this many sequences are stopped per tick. Each stop settles every
 * remaining hold against runs-service, and the sweep runs inside the dispatch
 * mutex: the first prod run met ~4,000 sequences. What is left over is not sent
 * meanwhile — it comes back in `notYetStopped` and the dispatcher skips it.
 */
export const STOPPED_CAMPAIGN_SWEEP_LIMIT = 200;

export interface StoppedCampaignSweepSummary {
  campaignsRead: number;
  /** Sequences of a stopped campaign left for a later tick (limit reached or failed). */
  deferred: number;
  queuedSequences: number;
  /** Contacted sequences of a customer-stopped campaign, left to send their follow-ups. */
  keptFollowups: number;
  /** Queued sequences whose campaign id campaign-service does not know (left running). */
  unknownCampaign: number;
  stoppedSelfSend: number;
  stoppedInstantly: number;
  failed: number;
}

function rowsOf(result: unknown): Record<string, unknown>[] {
  if (Array.isArray(result)) return result as Record<string, unknown>[];
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

/**
 * Every active sequence of a campaign that still has a step queued. The hold
 * match mirrors `matchesHoldCampaign` (historical org holds carry no per-lead id).
 */
async function loadQueuedSequences(): Promise<QueuedSequence[]> {
  const result = await db.execute(sql`
    SELECT c.instantly_campaign_id AS "instantlyCampaignId",
           c.campaign_id           AS "campaignId",
           c.org_id                AS "orgId",
           c.user_id               AS "userId",
           c.run_id                AS "runId",
           c.lead_email            AS "leadEmail",
           EXISTS (
             SELECT 1 FROM instantly_events e
             WHERE e.campaign_id = c.instantly_campaign_id
               AND e.event_type = 'email_sent'
               AND e.inferred = false
           ) OR EXISTS (
             SELECT 1 FROM sequence_costs sa
             WHERE sa.status = 'actual'
               AND sa.lead_email = c.lead_email
               AND sa.instantly_campaign_id = c.instantly_campaign_id
           )                       AS "contacted"
    FROM instantly_campaigns c
    WHERE c.status = 'active'
      AND c.campaign_id IS NOT NULL
      AND c.lead_email IS NOT NULL
      AND c.instantly_campaign_id NOT LIKE 'reserving:%'
      AND EXISTS (
        SELECT 1 FROM sequence_costs sc
        WHERE sc.status = 'provisioned'
          AND sc.lead_email = c.lead_email
          AND (sc.instantly_campaign_id = c.instantly_campaign_id
               OR (sc.instantly_campaign_id IS NULL AND sc.campaign_id = c.campaign_id))
      )
  `);
  return rowsOf(result) as unknown as QueuedSequence[];
}

/**
 * Ask campaign-service which campaigns are stopped and stop every queued
 * sequence of theirs, on its own transport.
 *
 * Fails LOUD when campaign-service cannot be read or the queue cannot be
 * loaded: the dispatcher must not send on a campaign it could not confirm is
 * running. A failure on ONE sequence is logged and counted; the rest proceed.
 */
export interface StoppedCampaignSweep {
  summary: StoppedCampaignSweepSummary;
  /**
   * `instantly_campaign_id`s of a stopped campaign still queued after this run
   * (over the limit, or their stop failed). The dispatcher MUST NOT send them.
   */
  notYetStopped: Set<string>;
}

export async function stopQueuedSequencesOfStoppedCampaigns(
  caller: CallerInfo,
  limit: number = STOPPED_CAMPAIGN_SWEEP_LIMIT,
): Promise<StoppedCampaignSweep> {
  const campaigns = await listCampaignStatuses();
  const known = new Set(campaigns.map((c) => c.id));
  const stopped = stoppedCampaignIds(campaigns);
  const queued = await loadQueuedSequences();

  const summary: StoppedCampaignSweepSummary = {
    campaignsRead: campaigns.length,
    deferred: 0,
    queuedSequences: queued.length,
    keptFollowups: 0,
    unknownCampaign: 0,
    stoppedSelfSend: 0,
    stoppedInstantly: 0,
    failed: 0,
  };

  const targets: QueuedSequence[] = [];
  for (const seq of queued) {
    if (!known.has(seq.campaignId)) {
      summary.unknownCampaign += 1;
      continue;
    }
    if (!stopped.has(seq.campaignId)) continue;
    // A customer stop never cuts a lead we already emailed: their follow-ups go out.
    if (seq.contacted && !stopCutsStartedSequences(stopped.get(seq.campaignId) ?? null)) {
      summary.keptFollowups += 1;
      continue;
    }
    targets.push(seq);
  }

  // Instantly first: a self-send sequence left over is held back by the
  // dispatcher (`notYetStopped`), an Instantly one keeps sending until paused.
  targets.sort(
    (a, b) =>
      Number(isSelfSendCampaignId(a.instantlyCampaignId)) -
      Number(isSelfSendCampaignId(b.instantlyCampaignId)),
  );

  const notYetStopped = new Set<string>(targets.slice(limit).map((t) => t.instantlyCampaignId));
  summary.deferred = notYetStopped.size;

  for (const seq of targets.slice(0, limit)) {
    const reason = `campaign ${seq.campaignId} stopped in campaign-service (${stopped.get(seq.campaignId) ?? "no reason"})`;
    try {
      if (isSelfSendCampaignId(seq.instantlyCampaignId)) {
        await stopSelfSendSequence(seq, seq.leadEmail, reason);
        summary.stoppedSelfSend += 1;
        continue;
      }
      const { key } = await resolveInstantlyApiKey(seq.orgId ?? "system", "system", caller);
      await updateCampaignStatus(key, seq.instantlyCampaignId, "paused");
      await stopSelfSendSequence(seq, seq.leadEmail, reason);
      summary.stoppedInstantly += 1;
    } catch (error: unknown) {
      summary.failed += 1;
      notYetStopped.add(seq.instantlyCampaignId);
      const message = error instanceof Error ? error.message : String(error);
      console.error(
        `[instantly-service] stopped-campaigns: could not stop campaign=${seq.instantlyCampaignId} lead=${seq.leadEmail} — ${message}`,
      );
    }
  }

  if (targets.length > 0 || summary.unknownCampaign > 0) {
    console.log(`[instantly-service] stopped-campaigns: done ${JSON.stringify(summary)}`);
  }
  return { summary, notYetStopped };
}
