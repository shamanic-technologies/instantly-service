/**
 * A person who booked a meeting with a brand gets no more cold email from that brand.
 *
 * Prod 2026-10-07 (fernanda@chirohealthspa.com, Doc Dinners): her meeting was booked in the
 * client's CRM at 16:04 UTC and on lead-service's conversion ledger at 16:17; step 3 of her cold
 * sequence still went out on 10-08 12:03. Fleet-wide, 15 people received 23 emails AFTER a live
 * booked outcome was recorded for them. A reply, a click, an opt-out and a decline all stopped the
 * sequence; a booking, the best outcome of all, stopped nothing, because nobody here ever learned
 * of it.
 *
 * lead-service OWNS the fact (its ledger: CRM, the brand's tracker, a person's statement; a
 * withdrawn statement is not live). This sweep ASKS it, once per dispatch tick, BEFORE anything is
 * selected (`runDispatch`), for every brand that still has a sequence queued here, and stops every
 * queued sequence of a booked person at that brand, whichever campaign it belongs to (a platform
 * send included: it is still a cold sequence of the brand).
 *
 * A PER-PERSON stop, deliberately not a campaign stop: the campaign keeps running for everybody
 * else, and the owner rule that a stopped campaign keeps its queued follow-ups
 * (`stopped-campaigns.ts`) is untouched.
 *
 * A person matches on lead-service's lead id OR the canonical email it serves (lowercased): lead
 * identity can be repointed (`send.ts`, "Lead identity"), the address is the identity here.
 *
 * Stopping one, the same two pipes as `stopped-campaigns.ts`:
 *   - self-send (`self:`): `stopSelfSendSequence` (holds cancelled, row `paused`).
 *   - Instantly: the per-lead Instantly campaign is PAUSED first (that is what stops the email);
 *     only then are the holds cancelled and the row marked `paused`. A failed pause leaves the row
 *     queued and the next tick retries.
 *
 * FAILS LOUD when the queue cannot be loaded or ANY brand's ledger cannot be read: an unreadable
 * ledger is not "nobody booked", and the dispatcher must not send on that guess (the whole tick
 * fails, logged). A failure on one sequence's stop is logged and counted; the rest proceed.
 */

import { sql } from "drizzle-orm";

import { db } from "../db";
import { listBookedPeople, type BookedPeople } from "./lead-client";
import { resolveInstantlyApiKey, type CallerInfo } from "./key-client";
import { updateCampaignStatus } from "./instantly-client";
import { isSelfSendCampaignId } from "./self-send/transport";
import { stopSelfSendSequence } from "./self-send/stop-sequence";

/** One local sequence still holding a provisioned (scheduled, not sent) step. */
export interface QueuedBrandSequence {
  instantlyCampaignId: string;
  campaignId: string | null;
  orgId: string | null;
  userId: string | null;
  runId: string | null;
  leadId: string | null;
  leadEmail: string;
  brandIds: string[];
}

/** Same bound and reasoning as `STOPPED_CAMPAIGN_SWEEP_LIMIT`: the rest waits, held back. */
export const BOOKED_SWEEP_LIMIT = 200;

export interface BookedSweepSummary {
  brandsRead: number;
  queuedSequences: number;
  /** Queued sequences of a person with a live booked outcome at one of the sequence's brands. */
  booked: number;
  /** Booked sequences left for a later tick (limit reached or stop failed); never sent meanwhile. */
  deferred: number;
  stoppedSelfSend: number;
  stoppedInstantly: number;
  failed: number;
}

export interface BookedSweep {
  summary: BookedSweepSummary;
  /** `instantly_campaign_id`s of a booked person still queued after this run. MUST NOT be sent. */
  notYetStopped: Set<string>;
}

function rowsOf(result: unknown): Record<string, unknown>[] {
  if (Array.isArray(result)) return result as Record<string, unknown>[];
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

/** Is this sequence's person booked at any brand the sequence is sent for? Pure. */
export function isBookedSequence(
  seq: Pick<QueuedBrandSequence, "leadId" | "leadEmail" | "brandIds">,
  bookedByBrand: Map<string, BookedPeople>,
): boolean {
  const email = seq.leadEmail.trim().toLowerCase();
  return seq.brandIds.some((brandId) => {
    const booked = bookedByBrand.get(brandId);
    if (!booked) return false;
    return booked.emails.has(email) || (seq.leadId !== null && booked.leadIds.has(seq.leadId));
  });
}

/**
 * Every active sequence still holding a provisioned step, with the brands it is sent for. The hold
 * match mirrors `matchesHoldCampaign` (historical org holds carry no per-lead id).
 */
async function loadQueuedBrandSequences(): Promise<QueuedBrandSequence[]> {
  const result = await db.execute(sql`
    SELECT c.instantly_campaign_id AS "instantlyCampaignId",
           c.campaign_id           AS "campaignId",
           c.org_id                AS "orgId",
           c.user_id               AS "userId",
           c.run_id                AS "runId",
           c.lead_id               AS "leadId",
           c.lead_email            AS "leadEmail",
           to_jsonb(c.brand_ids)   AS "brandIds"
    FROM instantly_campaigns c
    WHERE c.status = 'active'
      AND c.lead_email IS NOT NULL
      AND cardinality(c.brand_ids) > 0
      AND c.instantly_campaign_id NOT LIKE 'reserving:%'
      AND EXISTS (
        SELECT 1 FROM sequence_costs sc
        WHERE sc.status = 'provisioned'
          AND sc.lead_email = c.lead_email
          AND (sc.instantly_campaign_id = c.instantly_campaign_id
               OR (sc.instantly_campaign_id IS NULL AND sc.campaign_id = c.campaign_id))
      )
  `);
  return rowsOf(result).map((r) => ({
    ...(r as unknown as QueuedBrandSequence),
    brandIds: Array.isArray(r.brandIds) ? (r.brandIds as string[]) : [],
  }));
}

/**
 * Ask lead-service who is booked at each brand with a queued sequence, and stop every queued
 * sequence of a booked person, on its own transport.
 */
export async function stopQueuedSequencesOfBookedPeople(
  caller: CallerInfo,
  limit: number = BOOKED_SWEEP_LIMIT,
): Promise<BookedSweep> {
  const queued = await loadQueuedBrandSequences();
  const brands = [...new Set(queued.flatMap((s) => s.brandIds))];

  // Sequential per brand (three reads each): bounded load on lead-service, and the first failure
  // aborts the whole tick, which is the point.
  const bookedByBrand = new Map<string, BookedPeople>();
  for (const brandId of brands) {
    bookedByBrand.set(brandId, await listBookedPeople(brandId));
  }

  const targets = queued.filter((seq) => isBookedSequence(seq, bookedByBrand));

  const summary: BookedSweepSummary = {
    brandsRead: brands.length,
    queuedSequences: queued.length,
    booked: targets.length,
    deferred: 0,
    stoppedSelfSend: 0,
    stoppedInstantly: 0,
    failed: 0,
  };

  // Instantly first: a self-send sequence left over is held back by the dispatcher
  // (`notYetStopped`), an Instantly one keeps sending until paused.
  targets.sort(
    (a, b) =>
      Number(isSelfSendCampaignId(a.instantlyCampaignId)) -
      Number(isSelfSendCampaignId(b.instantlyCampaignId)),
  );

  const notYetStopped = new Set<string>(targets.slice(limit).map((t) => t.instantlyCampaignId));
  summary.deferred = notYetStopped.size;

  for (const seq of targets.slice(0, limit)) {
    const reason = `meeting booked at brand (lead-service ledger)`;
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
        `[instantly-service] booked-stops-cold: could not stop campaign=${seq.instantlyCampaignId} lead=${seq.leadEmail} — ${message}`,
      );
    }
  }

  if (targets.length > 0) {
    console.log(`[instantly-service] booked-stops-cold: done ${JSON.stringify(summary)}`);
  }
  return { summary, notYetStopped };
}
