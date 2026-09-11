/**
 * Take the scanners back out of the stats.
 *
 * Every `/c/` hit recorded before the classification shipped was promoted
 * straight into an `email_link_clicked` silver event, so a customer paying for
 * website visits has been shown corporate link scanners as visits since the
 * self-send transport went live (one brand: 131 of 337 smtp leads "clicked",
 * against 95 of 2049 on the Instantly transport). Silver is derived and
 * rebuildable, so the scanner-shaped ones are removed and gold is rebuilt from
 * what remains.
 *
 * ⚠️ SCOPED TO `source = 'self_send'` CLICKS. An Instantly-webhook click is
 * Instantly's own observation through its own tracking domain, and that era is
 * clean (851 of 924 clicked steps sat on a mail carrying a tracked link, no
 * clicker ever unsubscribed). Nothing here reads or writes a webhook-sourced
 * event.
 *
 * ⚠️ IT DOES NOT REACTIVATE ANYTHING. `stop-on-click` paused those leads'
 * Instantly campaigns, and resuming outreach at people on the strength of a
 * correction is a decision nobody made. The pauses stand.
 *
 * ⚠️ THE CANDIDATE SET INCLUDES HITS ALREADY DECIDED `human`, NOT ONLY THE
 * `legacy` ONES MIGRATION 0051 MARKED. A verdict is only as good as the rule
 * that produced it, and the rule gets sharpened: the first pass ran before
 * `isUnreducedChromeVersion` existed and called 80 of one brand's 131 clickers
 * human, where the tightened rule calls 18. Scoping the sweep to `legacy` would
 * leave those 62 permanently mislabelled, because nothing else ever re-opens a
 * decided hit. So this re-applies the CURRENT rule to everything not yet ruled
 * a scanner, and is the instrument to reach for whenever the rule moves.
 *
 * A `scanner` verdict is never re-opened, deliberately and asymmetrically: this
 * sweep can demote a click out of silver but cannot put one back (nothing here
 * promotes), so reversing one would leave bronze claiming a human click that
 * silver does not have. A hit wrongly called a scanner is corrected by fixing
 * the rule and re-running the PROMOTION path against a re-opened hit, which is
 * a deliberate act, not a side effect of a sweep.
 *
 * Idempotent: re-running re-decides the same hits to the same verdicts, the
 * delete matches no row the second time, and gold is rebuilt to the same state.
 */

import { sql } from "drizzle-orm";

import { db } from "../../db";
import { refreshLeadStatusCurrent } from "../status-gold";
import {
  PAIRED_UNSUBSCRIBE_WINDOW_SECONDS,
  classifyClickHit,
} from "./click-classification";

/** node-postgres resolves `db.execute` to a QueryResult OBJECT, never an array. */
function rowsOf(result: unknown): Record<string, unknown>[] {
  if (Array.isArray(result)) return result as Record<string, unknown>[];
  const rows = (result as { rows?: unknown })?.rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

interface LegacyClickHit {
  id: string;
  instantlyCampaignId: string;
  leadEmail: string;
  method: string | null;
  userAgent: string | null;
  brandId: string | null;
  hasPairedUnsubscribeFetch: boolean;
}

export interface BrandBreakdown {
  brandId: string | null;
  scannerHits: number;
  scannerLeads: number;
  humanHits: number;
  humanLeads: number;
}

export interface ClickScannerBackfillSummary {
  dryRun: boolean;
  hitsExamined: number;
  scannerHits: number;
  humanHits: number;
  /** Distinct (campaign, lead) pairs losing every self-send click they had. */
  leadsDemoted: number;
  silverEventsRemoved: number;
  inferredEventsRemoved: number;
  goldRowsRefreshed: number;
  reasons: Record<string, number>;
  byBrand: BrandBreakdown[];
}

const DEFAULT_LIMIT = 5000;

async function loadLegacyClickHits(limit: number): Promise<LegacyClickHit[]> {
  const pairingWindow = sql.raw(`interval '${PAIRED_UNSUBSCRIBE_WINDOW_SECONDS} seconds'`);

  const result = await db.execute(sql`
    SELECT
      h.id,
      h.instantly_campaign_id,
      h.lead_email,
      h.method,
      h.user_agent,
      c.brand_ids[1] AS brand_id,
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
    LEFT JOIN instantly_campaigns c
      ON c.instantly_campaign_id = h.instantly_campaign_id
    WHERE h.kind = 'click'
      AND h.classification IN ('legacy', 'human')
    ORDER BY h.received_at ASC
    LIMIT ${limit}
  `);

  return rowsOf(result).map((row) => ({
    id: String(row.id),
    instantlyCampaignId: String(row.instantly_campaign_id),
    leadEmail: String(row.lead_email),
    method: row.method === null || row.method === undefined ? null : String(row.method),
    userAgent:
      row.user_agent === null || row.user_agent === undefined ? null : String(row.user_agent),
    brandId: row.brand_id === null || row.brand_id === undefined ? null : String(row.brand_id),
    hasPairedUnsubscribeFetch: row.has_paired_unsubscribe === true,
  }));
}

/**
 * Remove the silver click a scanner hit produced, plus anything the inference
 * rules projected FROM it.
 *
 * A click infers an `email_opened` and an `email_sent` predecessor. Only rows
 * still `inferred = true` are removed: a real send upgrades that row in place
 * (`inferred = false`), and deleting it would erase a dispatch that genuinely
 * happened. Scoped by `source_row_id`, the bronze hit's own id, so it can only
 * ever touch the event this exact hit created.
 */
async function removeScannerClickEvent(
  hitId: string,
): Promise<{ clicks: number; inferred: number }> {
  const deletedClicks = rowsOf(
    await db.execute(sql`
      DELETE FROM instantly_events
      WHERE event_type = 'email_link_clicked'
        AND source = 'self_send'
        AND source_row_id = ${hitId}
      RETURNING id
    `),
  ).map((row) => String(row.id));

  if (deletedClicks.length === 0) return { clicks: 0, inferred: 0 };

  const inferredIds = sql.join(
    deletedClicks.map((id) => sql`${id}`),
    sql`, `,
  );

  const deletedInferred = rowsOf(
    await db.execute(sql`
      DELETE FROM instantly_events
      WHERE inferred = TRUE
        AND inferred_from_event_id IN (${inferredIds})
      RETURNING id
    `),
  );

  return { clicks: deletedClicks.length, inferred: deletedInferred.length };
}

async function markDecidedHit(hitId: string, classification: string, reason: string | null) {
  await db.execute(sql`
    UPDATE tracking_hits_raw
    SET classification = ${classification},
        classification_reason = ${reason},
        promoted_at = ${classification === "human" ? sql`promoted_at` : sql`NULL`}
    WHERE id = ${hitId}
  `);
}

export async function backfillScannerClicks(
  options: { dryRun?: boolean; limit?: number } = {},
): Promise<ClickScannerBackfillSummary> {
  const dryRun = options.dryRun !== false;
  const limit = options.limit && options.limit > 0 ? Math.floor(options.limit) : DEFAULT_LIMIT;

  const hits = await loadLegacyClickHits(limit);

  const summary: ClickScannerBackfillSummary = {
    dryRun,
    hitsExamined: hits.length,
    scannerHits: 0,
    humanHits: 0,
    leadsDemoted: 0,
    silverEventsRemoved: 0,
    inferredEventsRemoved: 0,
    goldRowsRefreshed: 0,
    reasons: {},
    byBrand: [],
  };

  const brands = new Map<
    string,
    { brandId: string | null; scannerHits: number; humanHits: number; scannerLeads: Set<string>; humanLeads: Set<string> }
  >();
  const scannerLeads = new Map<string, { campaignId: string; leadEmail: string }>();
  const humanLeadKeys = new Set<string>();

  for (const hit of hits) {
    const verdict = classifyClickHit(hit);
    const leadKey = `${hit.instantlyCampaignId}::${hit.leadEmail.toLowerCase()}`;
    const brandKey = hit.brandId ?? "";
    const brand =
      brands.get(brandKey) ??
      {
        brandId: hit.brandId,
        scannerHits: 0,
        humanHits: 0,
        scannerLeads: new Set<string>(),
        humanLeads: new Set<string>(),
      };
    brands.set(brandKey, brand);

    if (verdict.verdict === "scanner") {
      summary.scannerHits += 1;
      brand.scannerHits += 1;
      brand.scannerLeads.add(leadKey);
      scannerLeads.set(leadKey, {
        campaignId: hit.instantlyCampaignId,
        leadEmail: hit.leadEmail,
      });
      if (verdict.reason) {
        summary.reasons[verdict.reason] = (summary.reasons[verdict.reason] ?? 0) + 1;
      }

      if (!dryRun) {
        const removed = await removeScannerClickEvent(hit.id);
        summary.silverEventsRemoved += removed.clicks;
        summary.inferredEventsRemoved += removed.inferred;
        await markDecidedHit(hit.id, "scanner", verdict.reason);
      }
    } else {
      summary.humanHits += 1;
      brand.humanHits += 1;
      brand.humanLeads.add(leadKey);
      humanLeadKeys.add(leadKey);
      if (!dryRun) {
        await markDecidedHit(hit.id, "human", null);
      }
    }
  }

  // A lead keeps its `clicked` flag if ANY of its clicks survived, so gold is
  // rebuilt from the remaining events rather than toggled — the same reason
  // `refreshLeadStatusCurrent` derives instead of incrementing. Only leads that
  // lost every self-send click they had are reported as demoted.
  for (const [leadKey, lead] of scannerLeads) {
    if (!humanLeadKeys.has(leadKey)) summary.leadsDemoted += 1;
    if (!dryRun) {
      await refreshLeadStatusCurrent(lead.campaignId, lead.leadEmail);
      summary.goldRowsRefreshed += 1;
    }
  }

  summary.byBrand = [...brands.values()]
    .map((brand) => ({
      brandId: brand.brandId,
      scannerHits: brand.scannerHits,
      scannerLeads: brand.scannerLeads.size,
      humanHits: brand.humanHits,
      humanLeads: brand.humanLeads.size,
    }))
    .sort((a, b) => b.scannerLeads - a.scannerLeads);

  return summary;
}
