/**
 * Judge the clicks Instantly recorded by the one fingerprint they can show
 * (CLICK-HUMANS-1018).
 *
 * A self-send click is decided on its own request (user-agent, method, address,
 * paired opt-out fetch: `self-send/click-classification.ts`). Instantly's
 * `link_clicked` webhook and its lead poll carry none of that: lead, step and a
 * timestamp. So the same rule book applies to them through the only rule their
 * data can answer, the click burst: `CLICK_BURST_MIN` clicks of the same
 * (campaign, lead) within `CLICK_BURST_WINDOW_SECONDS` either side, a program
 * sweeping every link of the message. Every other Instantly click keeps the
 * benefit of the doubt (owner rule: only a positive fingerprint removes one).
 *
 * A burst click is RETRACTED from silver like a reclassified bounce: the whole
 * event archived in `instantly_events_retracted` (reason `scanner:click_burst`,
 * undo = re-insert from the archive), deleted with the opens/sends it inferred,
 * and the lead's gold row rebuilt (it stays `clicked` if another click
 * survived). The outreach fact feed then withdraws the `link_clicked` fact on
 * its own (source gone). Idempotent: a retracted click is no longer in silver.
 * Runs in the 6-hourly click repair and by hand
 * (`POST /internal/audit/vendor-click-burst`, dry-run by default).
 */

import { sql } from "drizzle-orm";

import { db } from "../db";
import { instantlyEventsRetracted } from "../db/schema";
import { CLICK_BURST_MIN, CLICK_BURST_WINDOW_SECONDS } from "./self-send/click-classification";
import { refreshLeadStatusCurrent } from "./status-gold";

export const VENDOR_CLICK_BURST_REASON = "scanner:click_burst";

/** node-postgres resolves `db.execute` to a QueryResult OBJECT, never an array. */
function rowsOf(result: unknown): Record<string, unknown>[] {
  if (Array.isArray(result)) return result as Record<string, unknown>[];
  const rows = (result as { rows?: unknown })?.rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

export interface VendorClickBurstSummary {
  dryRun: boolean;
  burstClicks: number;
  /** Distinct (campaign, lead) pairs owning at least one burst click. */
  leadsTouched: number;
  /** Of those, the ones left with no click at all: no longer a website visit. */
  leadsDemoted: number;
  inferredEventsRemoved: number;
}

interface BurstClick {
  id: string;
  campaignId: string;
  leadEmail: string;
  survivingClicks: number;
}

async function loadBurstClicks(): Promise<BurstClick[]> {
  const window = sql.raw(`interval '${CLICK_BURST_WINDOW_SECONDS} seconds'`);
  const result = await db.execute(sql`
    WITH clicks AS (
      SELECT id, campaign_id, lower(lead_email) AS lead, lead_email, "timestamp"
      FROM instantly_events
      WHERE event_type = 'email_link_clicked'
        AND source <> 'self_send'
        AND inferred = false
    ),
    burst AS (
      SELECT c.*
      FROM clicks c
      WHERE (
        SELECT COUNT(*) FROM clicks o
        WHERE o.campaign_id = c.campaign_id
          AND o.lead = c.lead
          AND o."timestamp" BETWEEN c."timestamp" - ${window} AND c."timestamp" + ${window}
      ) >= ${sql.raw(String(CLICK_BURST_MIN))}
    )
    SELECT b.id, b.campaign_id, b.lead_email,
      (SELECT COUNT(*) FROM instantly_events o
        WHERE o.event_type = 'email_link_clicked'
          AND o.campaign_id = b.campaign_id
          AND lower(o.lead_email) = b.lead
          AND o.id NOT IN (SELECT id FROM burst)) AS surviving_clicks
    FROM burst b
  `);
  return rowsOf(result).map((row) => ({
    id: String(row.id),
    campaignId: String(row.campaign_id),
    leadEmail: String(row.lead_email),
    survivingClicks: Number(row.surviving_clicks),
  }));
}

export async function retractVendorBurstClicks(
  options: { dryRun?: boolean } = {},
): Promise<VendorClickBurstSummary> {
  const dryRun = options.dryRun !== false;
  const clicks = await loadBurstClicks();

  const leads = new Map<string, BurstClick>();
  for (const click of clicks) leads.set(`${click.campaignId}::${click.leadEmail.toLowerCase()}`, click);

  const summary: VendorClickBurstSummary = {
    dryRun,
    burstClicks: clicks.length,
    leadsTouched: leads.size,
    leadsDemoted: [...leads.values()].filter((l) => l.survivingClicks === 0).length,
    inferredEventsRemoved: 0,
  };
  if (dryRun) return summary;

  for (const click of clicks) {
    const archived = rowsOf(
      await db.execute(sql`SELECT to_jsonb(e.*) AS event FROM instantly_events e WHERE e.id = ${click.id}`),
    );
    if (archived.length === 0) continue;
    await db
      .insert(instantlyEventsRetracted)
      .values({
        eventId: click.id,
        action: "retracted",
        reason: VENDOR_CLICK_BURST_REASON,
        event: archived[0].event as Record<string, unknown>,
        priorDeliveryStatus: null,
        replacementSourceRowId: null,
      })
      .onConflictDoNothing({ target: instantlyEventsRetracted.eventId });
    const inferred = rowsOf(
      await db.execute(sql`
        DELETE FROM instantly_events
        WHERE inferred = TRUE AND inferred_from_event_id = ${click.id}
        RETURNING id
      `),
    );
    summary.inferredEventsRemoved += inferred.length;
    await db.execute(sql`DELETE FROM instantly_events WHERE id = ${click.id}`);
  }

  for (const lead of leads.values()) {
    await refreshLeadStatusCurrent(lead.campaignId, lead.leadEmail);
  }
  return summary;
}
