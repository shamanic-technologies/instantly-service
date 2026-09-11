/**
 * Promote the clicks that were people.
 *
 * The `/c/` redirect records every hit in bronze and promotes NOTHING. This is
 * the other half: once a hit is older than the pairing window, its verdict is
 * available, and a `human` one becomes the `email_link_clicked` silver event the
 * gold stats and `stop-on-click` read.
 *
 * ⚠️ THE SWEEP IS THE ONLY PROMOTION PATH FOR SELF-SEND CLICKS. It carries no
 * kill-switch for that reason: disabling it would not degrade click tracking, it
 * would silently end it.
 *
 * Idempotent without a cursor. The promoted event's timestamp is the hit's own
 * `received_at`, so a re-promotion of the same hit collides on
 * `instantly_events_dedupe_idx` and is a no-op with its side effects skipped —
 * which is what makes it safe to promote FIRST and mark the bronze row second. A
 * hit whose promotion throws stays `classification IS NULL` and is retried on the
 * next tick; nothing is lost and nothing is silently swallowed.
 */

import { sql } from "drizzle-orm";

import { db } from "../../db";
import { promoteEvent } from "../silver-promote";
import {
  CLICK_DECISION_HOLD_SECONDS,
  PAIRED_UNSUBSCRIBE_WINDOW_SECONDS,
  classifyClickHit,
} from "./click-classification";

/** node-postgres resolves `db.execute` to a QueryResult OBJECT, never an array. */
function rowsOf(result: unknown): Record<string, unknown>[] {
  if (Array.isArray(result)) return result as Record<string, unknown>[];
  const rows = (result as { rows?: unknown })?.rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

export interface PendingClickHit {
  id: string;
  instantlyCampaignId: string;
  leadEmail: string;
  step: number | null;
  method: string | null;
  userAgent: string | null;
  receivedAt: Date;
  hasPairedUnsubscribeFetch: boolean;
}

export interface ClickPromotionSummary {
  /** Hits whose hold had expired and which were therefore decided this run. */
  decided: number;
  promoted: number;
  scanner: number;
  failed: number;
  /** Scanner reason → count. A human hit contributes to none of them. */
  reasons: Record<string, number>;
}

const DEFAULT_BATCH = 500;

/**
 * Candidate query: undecided clicks past their hold, each carrying whether the
 * same (campaign, lead) also fetched the opt-out link inside the pairing window.
 *
 * The lead is matched case-folded, the same normalisation every other lead
 * lookup in this service applies — `Joe@X.com` and `joe@x.com` are one inbox,
 * and a case-sensitive match here would miss the pair that proves the scanner.
 */
export async function loadPendingClickHits(
  limit: number,
  asOf: Date,
): Promise<PendingClickHit[]> {
  // Intervals are built from our OWN integer constants with `sql.raw`, never
  // bound: `$1 * interval '1 second'` leaves the parameter untyped and Postgres
  // rejects it as an ambiguous operator.
  const pairingWindow = sql.raw(`interval '${PAIRED_UNSUBSCRIBE_WINDOW_SECONDS} seconds'`);
  const decisionHold = sql.raw(`interval '${CLICK_DECISION_HOLD_SECONDS} seconds'`);

  const result = await db.execute(sql`
    SELECT
      h.id,
      h.instantly_campaign_id,
      h.lead_email,
      h.step,
      h.method,
      h.user_agent,
      h.received_at,
      EXISTS (
        SELECT 1
        FROM tracking_hits_raw u
        WHERE u.kind = 'unsubscribe'
          AND u.instantly_campaign_id = h.instantly_campaign_id
          AND lower(u.lead_email) = lower(h.lead_email)
          AND u.received_at BETWEEN
            h.received_at - ${pairingWindow}
            AND h.received_at + ${pairingWindow}
      ) AS has_paired_unsubscribe
    FROM tracking_hits_raw h
    WHERE h.kind = 'click'
      AND h.classification IS NULL
      AND h.received_at <= ${asOf.toISOString()}::timestamp - ${decisionHold}
    ORDER BY h.received_at ASC
    LIMIT ${limit}
  `);

  return rowsOf(result).map((row) => ({
    id: String(row.id),
    instantlyCampaignId: String(row.instantly_campaign_id),
    leadEmail: String(row.lead_email),
    step: row.step === null || row.step === undefined ? null : Number(row.step),
    method: row.method === null || row.method === undefined ? null : String(row.method),
    userAgent:
      row.user_agent === null || row.user_agent === undefined ? null : String(row.user_agent),
    receivedAt: new Date(String(row.received_at)),
    hasPairedUnsubscribeFetch: row.has_paired_unsubscribe === true,
  }));
}

async function markHit(
  hitId: string,
  classification: string,
  reason: string | null,
  promoted: boolean,
): Promise<void> {
  await db.execute(sql`
    UPDATE tracking_hits_raw
    SET classification = ${classification},
        classification_reason = ${reason},
        promoted_at = ${promoted ? sql`now()` : sql`NULL`}
    WHERE id = ${hitId}
  `);
}

/**
 * Decide and promote every click whose hold has expired.
 *
 * Fail-loud per hit: an error is logged with the hit id and counted, the hit
 * keeps its undecided state, and the run continues. One unreachable campaign
 * must not stop the fleet's clicks from being recorded.
 */
export async function promotePendingClicks(
  options: { limit?: number; asOf?: Date } = {},
): Promise<ClickPromotionSummary> {
  const limit = options.limit && options.limit > 0 ? Math.floor(options.limit) : DEFAULT_BATCH;
  const asOf = options.asOf ?? new Date();

  const hits = await loadPendingClickHits(limit, asOf);

  const summary: ClickPromotionSummary = {
    decided: 0,
    promoted: 0,
    scanner: 0,
    failed: 0,
    reasons: {},
  };

  for (const hit of hits) {
    const verdict = classifyClickHit(hit);

    try {
      if (verdict.verdict === "human") {
        // The timestamp is the hit's own, not `now()`: it is when the person
        // actually clicked, and it is what makes a re-promotion idempotent.
        await promoteEvent({
          eventType: "email_link_clicked",
          instantlyCampaignId: hit.instantlyCampaignId,
          leadEmail: hit.leadEmail,
          accountEmail: null,
          step: hit.step,
          variant: null,
          timestamp: hit.receivedAt,
          source: "self_send",
          sourceRowId: hit.id,
        });
        await markHit(hit.id, "human", null, true);
        summary.promoted += 1;
      } else {
        await markHit(hit.id, "scanner", verdict.reason, false);
        summary.scanner += 1;
        if (verdict.reason) {
          summary.reasons[verdict.reason] = (summary.reasons[verdict.reason] ?? 0) + 1;
        }
      }
      summary.decided += 1;
    } catch (error: unknown) {
      summary.failed += 1;
      const message = error instanceof Error ? error.message : String(error);
      console.error(
        `[instantly-service] click-promotion: hit=${hit.id} campaign=${hit.instantlyCampaignId} failed: ${message}`,
      );
    }
  }

  return summary;
}
