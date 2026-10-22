/**
 * Close every open Instantly-transport sequence: the Instantly Email Outreach
 * plan is cancelled (owner 2026-10-11, effective 2026-10-12).
 *
 * Once the plan is gone Instantly sends nothing more, so an `active` Instantly
 * row is a sequence that will never finish: its unsent steps stay `provisioned`
 * forever (held from the client's balance, read by every ops surface as "due"),
 * and nothing on our side would ever settle them. The owner's call for those
 * follow-ups is STOP AND REFUND (not "resend from one of our mailboxes": a
 * follow-up from a different sender, in a new thread, weeks late, reads as spam).
 *
 * Per row, in this order:
 *   1. PAUSE the campaign on Instantly (a 404 = already gone, fine). This must run
 *      while the API still answers: it is the only thing that guarantees
 *      Instantly cannot send a step after we refunded it.
 *   2. Cancel every remaining hold (refund) through `cancelRemainingProvisions`.
 *   3. Mark the row `paused` (`metadata.instantlyPlanRetired`) and refresh gold.
 *
 * The local writes are ours to do, unlike a normal Instantly pause: the nightly
 * reconcile that would usually settle the holds reads Instantly, which is about
 * to stop answering.
 *
 * Fail-loud per row: a failed pause leaves the row exactly as it was (counted
 * `failed`, logged), so a re-run picks it up. Idempotent: a closed row is no
 * longer `active` and is never selected again.
 */

import { errorMessage, messageOf } from "../db-error.js";
import { sql } from "drizzle-orm";

import { db } from "../../db";
import { updateCampaignStatus } from "../instantly-client";
import {
  resolveInstantlyApiKey,
  resolvePlatformInstantlyApiKey,
  type CallerInfo,
} from "../key-client";
import { cancelRemainingProvisions } from "../silver-promote";
import { refreshLeadStatusCurrent } from "../status-gold";
import { announceEvidenceChanged } from "../evidence-changed";
import { SELF_SEND_CAMPAIGN_LIKE, SEND_TRANSPORT_INSTANTLY } from "./transport";

const CALLER: CallerInfo = {
  method: "POST",
  path: "/internal/self-send/retire-instantly-sequences",
};

export interface OpenInstantlySequence {
  instantlyCampaignId: string;
  campaignId: string | null;
  orgId: string | null;
  userId: string | null;
  leadEmail: string;
  /** Steps still `provisioned`: what the client is refunded. */
  pendingSteps: number;
}

export interface RetireSummary {
  dryRun: boolean;
  open: number;
  withPendingSteps: number;
  pendingSteps: number;
  closed: number;
  refundedSteps: number;
  failed: number;
  skippedConcurrent: boolean;
}

function rowsOf(result: unknown): Record<string, unknown>[] {
  return ((result as { rows?: Record<string, unknown>[] }).rows ?? []) as Record<string, unknown>[];
}

/** Every Instantly-transport sequence still `active`, oldest first. */
export async function loadOpenInstantlySequences(): Promise<OpenInstantlySequence[]> {
  const result = await db.execute(sql`
    SELECT c.instantly_campaign_id AS "instantlyCampaignId",
           c.campaign_id           AS "campaignId",
           c.org_id                AS "orgId",
           c.user_id               AS "userId",
           c.lead_email            AS "leadEmail",
           (
             SELECT COUNT(DISTINCT sc.step)
             FROM sequence_costs sc
             WHERE sc.status = 'provisioned'
               AND (sc.instantly_campaign_id = c.instantly_campaign_id
                    OR (sc.instantly_campaign_id IS NULL
                        AND sc.lead_email = c.lead_email
                        AND sc.campaign_id IS NOT DISTINCT FROM c.campaign_id))
           ) AS "pendingSteps"
    FROM instantly_campaigns c
    WHERE c.send_transport = ${SEND_TRANSPORT_INSTANTLY}
      AND c.status = 'active'
      AND c.lead_email IS NOT NULL
      AND c.instantly_campaign_id NOT LIKE 'reserving:%'
      AND c.instantly_campaign_id NOT LIKE ${SELF_SEND_CAMPAIGN_LIKE}
    ORDER BY c.created_at
  `);
  return rowsOf(result).map((r) => ({
    instantlyCampaignId: String(r.instantlyCampaignId),
    campaignId: r.campaignId === null ? null : String(r.campaignId),
    orgId: r.orgId === null ? null : String(r.orgId),
    userId: r.userId === null ? null : String(r.userId),
    leadEmail: String(r.leadEmail),
    pendingSteps: Number(r.pendingSteps ?? 0),
  }));
}

/** True for an Instantly 404: the campaign is gone, so it cannot send either. */
function isNotFound(error: unknown): boolean {
  return error instanceof Error && / failed: 404 /.test(messageOf(error));
}

async function closeRow(row: OpenInstantlySequence, asOf: Date): Promise<void> {
  await cancelRemainingProvisions(
    {
      instantlyCampaignId: row.instantlyCampaignId,
      campaignId: row.campaignId,
      orgId: row.orgId,
      userId: row.userId,
      runId: null,
    },
    row.leadEmail,
  );
  const entry = JSON.stringify({ at: asOf.toISOString(), refundedSteps: row.pendingSteps });
  await db.execute(sql`
    UPDATE instantly_campaigns
    SET status = 'paused',
        metadata = jsonb_set(COALESCE(metadata, '{}'::jsonb), '{instantlyPlanRetired}', ${entry}::jsonb),
        updated_at = now()
    WHERE instantly_campaign_id = ${row.instantlyCampaignId}
      AND status = 'active'
  `);
  await refreshLeadStatusCurrent(row.instantlyCampaignId, row.leadEmail);
  void announceEvidenceChanged(row.orgId, [row.leadEmail], "instantly_plan_retired");
}

function emptySummary(dryRun: boolean): RetireSummary {
  return {
    dryRun,
    open: 0,
    withPendingSteps: 0,
    pendingSteps: 0,
    closed: 0,
    refundedSteps: 0,
    failed: 0,
    skippedConcurrent: false,
  };
}

let sweepInFlight = false;

/**
 * Close up to `limit` open Instantly sequences. `dryRun` (the default) counts
 * without calling Instantly or writing anything.
 */
export async function retireInstantlySequences(
  options: { asOf?: Date; limit?: number; dryRun?: boolean } = {},
): Promise<RetireSummary> {
  const dryRun = options.dryRun ?? true;
  if (sweepInFlight) return { ...emptySummary(dryRun), skippedConcurrent: true };
  sweepInFlight = true;
  try {
    const asOf = options.asOf ?? new Date();
    const summary = emptySummary(dryRun);
    const rows = await loadOpenInstantlySequences();
    summary.open = rows.length;
    summary.withPendingSteps = rows.filter((r) => r.pendingSteps > 0).length;
    summary.pendingSteps = rows.reduce((n, r) => n + r.pendingSteps, 0);
    if (dryRun) return summary;

    const keys = new Map<string, string>();
    const keyFor = async (orgId: string | null): Promise<string> => {
      const k = orgId ?? "";
      const cached = keys.get(k);
      if (cached) return cached;
      const key = orgId
        ? (await resolveInstantlyApiKey(orgId, "system", CALLER)).key
        : await resolvePlatformInstantlyApiKey(CALLER);
      keys.set(k, key);
      return key;
    };

    for (const row of rows.slice(0, options.limit ?? rows.length)) {
      try {
        try {
          await updateCampaignStatus(await keyFor(row.orgId), row.instantlyCampaignId, "paused");
        } catch (error) {
          if (!isNotFound(error)) throw error;
        }
        await closeRow(row, asOf);
        summary.closed += 1;
        summary.refundedSteps += row.pendingSteps;
      } catch (error) {
        summary.failed += 1;
        console.error(
          `[instantly-service] retire-instantly-sequences: campaign=${row.instantlyCampaignId} failed: ${
            errorMessage(error)
          }`,
        );
      }
    }

    console.log(`[instantly-service] retire-instantly-sequences: done ${JSON.stringify(summary)}`);
    return summary;
  } finally {
    sweepInFlight = false;
  }
}
