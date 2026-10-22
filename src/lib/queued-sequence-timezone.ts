/**
 * A queued self-send sequence with no timezone takes the PERSON's zone from lead-service.
 *
 * The send window is the lead's own Mon-Sat 07:00-19:00 (lib/sending-window), read from
 * `instantly_campaigns.timezone`, which is whatever the caller put on `POST /orgs/send`. Many
 * callers put nothing: measured 2026-10-14, 510 queued sequences carried no zone, whole cold
 * email workflows never map it, and a person a client pushes had none at lead-service at all.
 * Every one of them was emailed on America/Chicago hours (a UK employer at night, an Indian
 * lead at 18:30-06:30 local), although lead-service held a zone for ~80% of them.
 *
 * So the queue does not depend on each caller remembering: before the dispatcher selects, every
 * ACTIVE self-send sequence with a provisioned step and no zone asks lead-service for the
 * person's zone (stated > enrichment > inferred is lead-service's precedence, not ours) and
 * persists it. A zone already on the row is NEVER overwritten.
 *
 * A person lead-service holds no zone for is asked again after `RECHECK_AFTER_MS` (its backfill
 * keeps inferring), not on every tick. Fail-soft per sequence and LOUD: an unreachable
 * lead-service leaves that sequence on the default zone (the behaviour before this existed)
 * rather than blocking every send of the tick.
 */

import { errorMessage } from "./db-error.js";
import { sql } from "drizzle-orm";
import { db } from "../db";
import { findLeadTimezoneOnCampaignByEmail } from "./lead-client";

/** A person with no zone at lead-service is asked again after this long. */
export const RECHECK_AFTER_MS = 6 * 60 * 60 * 1000;
/** Most sequences resolved in one pass; the rest wait for the next tick. */
export const MAX_PER_PASS = 200;
/** Parallel lead-service reads. */
export const CONCURRENCY = 4;

export interface ZonelessSequence {
  instantlyCampaignId: string;
  orgId: string;
  campaignId: string;
  leadEmail: string;
}

export interface TimezoneHealSummary {
  candidates: number;
  resolved: number;
  unknown: number;
  failed: number;
}

/** instantlyCampaignId -> when lead-service last answered "no zone". Per replica, reset on boot. */
const lastUnknownAt = new Map<string, number>();

export function resetTimezoneHealMemory(): void {
  lastUnknownAt.clear();
}

/** True for a zone the send window can use. */
export function isUsableZone(zone: string): boolean {
  try {
    new Intl.DateTimeFormat("en-US", { timeZone: zone });
    return true;
  } catch {
    return false;
  }
}

function rowsOf(result: unknown): Record<string, unknown>[] {
  return ((result as { rows?: Record<string, unknown>[] }).rows ?? []);
}

export async function loadZonelessQueuedSequences(): Promise<ZonelessSequence[]> {
  const result = await db.execute(sql`
    SELECT c.instantly_campaign_id AS "instantlyCampaignId",
           c.org_id AS "orgId",
           c.campaign_id AS "campaignId",
           c.lead_email AS "leadEmail"
    FROM instantly_campaigns c
    WHERE c.status = 'active'
      AND c.timezone IS NULL
      AND c.instantly_campaign_id LIKE 'self:%'
      AND c.org_id IS NOT NULL
      AND c.campaign_id IS NOT NULL
      AND EXISTS (
        SELECT 1 FROM sequence_costs s
        WHERE s.instantly_campaign_id = c.instantly_campaign_id
          AND s.status = 'provisioned'
      )
    ORDER BY c.created_at DESC
  `);
  return rowsOf(result).map((r) => ({
    instantlyCampaignId: String(r.instantlyCampaignId),
    orgId: String(r.orgId),
    campaignId: String(r.campaignId),
    leadEmail: String(r.leadEmail),
  }));
}

async function persistZone(instantlyCampaignId: string, zone: string): Promise<void> {
  await db.execute(sql`
    UPDATE instantly_campaigns
    SET timezone = ${zone}, updated_at = now()
    WHERE instantly_campaign_id = ${instantlyCampaignId}
      AND timezone IS NULL
  `);
}

let inFlight: Promise<TimezoneHealSummary> | null = null;

/** Heal the zoneless queue. Concurrent callers join the pass already running. */
export function healQueuedSequenceTimezones(asOf: Date = new Date()): Promise<TimezoneHealSummary> {
  if (!inFlight) {
    inFlight = runHeal(asOf).finally(() => {
      inFlight = null;
    });
  }
  return inFlight;
}

async function runHeal(asOf: Date): Promise<TimezoneHealSummary> {
  const now = asOf.getTime();
  const due = (await loadZonelessQueuedSequences())
    .filter((s) => {
      const at = lastUnknownAt.get(s.instantlyCampaignId);
      return at === undefined || now - at >= RECHECK_AFTER_MS;
    })
    .slice(0, MAX_PER_PASS);

  const summary: TimezoneHealSummary = { candidates: due.length, resolved: 0, unknown: 0, failed: 0 };
  if (due.length === 0) return summary;

  let next = 0;
  const worker = async () => {
    while (next < due.length) {
      const s = due[next++];
      try {
        const zone = await findLeadTimezoneOnCampaignByEmail({
          orgId: s.orgId,
          campaignId: s.campaignId,
          email: s.leadEmail,
        });
        if (zone && isUsableZone(zone)) {
          await persistZone(s.instantlyCampaignId, zone);
          lastUnknownAt.delete(s.instantlyCampaignId);
          summary.resolved++;
        } else {
          if (zone) {
            console.warn(
              `[instantly-service] queued-sequence-timezone: lead-service serves an unusable zone "${zone}" for ${s.leadEmail} on campaign=${s.instantlyCampaignId}; left on the default zone`,
            );
          }
          lastUnknownAt.set(s.instantlyCampaignId, now);
          summary.unknown++;
        }
      } catch (error) {
        summary.failed++;
        console.warn(
          `[instantly-service] queued-sequence-timezone: could not read the zone of ${s.leadEmail} on campaign=${s.instantlyCampaignId}, left on the default zone this tick — ${errorMessage(error)}`,
        );
      }
    }
  };
  await Promise.all(Array.from({ length: Math.min(CONCURRENCY, due.length) }, worker));

  console.log(
    `[instantly-service] queued-sequence-timezone: ${summary.candidates} zoneless queued sequence(s) checked, ${summary.resolved} given the person's zone, ${summary.unknown} unknown at lead-service, ${summary.failed} failed`,
  );
  return summary;
}
