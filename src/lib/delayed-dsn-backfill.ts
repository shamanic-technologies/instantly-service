/**
 * Take back the `email_bounced` events a TEMPORARY delivery delay produced.
 *
 * Until `isTransientDeliveryReport` existed, the self-send IMAP poller promoted
 * every delivery-status notification as a bounce — including Gmail's
 * "** Delivery incomplete **" notices, which say the opposite ("Gmail will retry
 * for N more hours. You'll be notified if the delivery fails permanently").
 * Measured 2026-09-24: 251 of 952 stored DSNs were delays, and 122 of them had
 * become `email_bounced` events.
 *
 * The rule applied here is the SAME pure function the live poller now uses, so
 * backfill and go-forward cannot drift. Two outcomes per wrong event:
 *
 *  - **reattributed** — a PERMANENT-failure DSN for the same send arrived later
 *    (the delay turned into a real bounce). The bounce is true, only its
 *    provenance and date are wrong: the event keeps its row and moves its
 *    `source_row_id` / `timestamp` onto the permanent notice. It could not have
 *    been promoted from that notice on its own: `email_bounced` is one-shot per
 *    (campaign, lead, step), so the permanent DSN collided with the delay's row.
 *  - **retracted** — nothing permanent ever arrived. The event is deleted, the
 *    campaign row's `delivery_status` goes back from `bounced` to `sent` (a DSN
 *    is itself proof the message was sent), gold is rebuilt and lead-service is
 *    told the address's evidence changed.
 *
 * ⚠️ REVERSIBLE BY CONSTRUCTION. Every touched event is copied WHOLE into
 * `instantly_events_retracted` first (with the prior delivery status), and each
 * bronze DSN row keeps `payload.reclassifiedFrom = 'bounce'`. Nothing is lost.
 *
 * ⚠️ WHAT IT DOES NOT UNDO: the bounce already stopped those sequences and
 * cancelled their remaining holds. Restarting outreach is a new decision, not a
 * correction — same posture as withdrawing an opt-out.
 *
 * Inferred rows projected FROM a retracted bounce (`bounced_implies_sent`) are
 * kept: the send they assert is real (the DSN proves it).
 *
 * Idempotent: a reclassified DSN row is `kind = 'delay'` and leaves the
 * candidate set, so a second run finds nothing.
 */

import { sql } from "drizzle-orm";

import { db } from "../db";
import { archiveSilverEvent, retractBouncedEvents } from "./bounce-retraction";
import { isTransientDeliveryReport, type InboundHeaders } from "./self-send/inbound";
import { refreshLeadStatusCurrent } from "./status-gold";

export const DELAYED_DSN_REASON = "delay_dsn_not_a_bounce";

/** node-postgres resolves `db.execute` to a QueryResult object, never an array. */
function rowsOf(result: unknown): Record<string, unknown>[] {
  if (Array.isArray(result)) return result as Record<string, unknown>[];
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

export interface DsnRow {
  id: string;
  instantlyCampaignId: string | null;
  step: number | null;
  headers: InboundHeaders;
  text: string;
  /** When the notice arrived — the bounce's true date for a permanent one. */
  at: Date;
}

export interface BouncedEvent {
  id: string;
  campaignId: string | null;
  leadEmail: string | null;
  step: number | null;
  sourceRowId: string;
}

export interface DelayedDsnPlan {
  /** DSN rows stored as `bounce` that report a temporary delay. */
  transientRowIds: string[];
  retract: BouncedEvent[];
  reattribute: Array<{ event: BouncedEvent; replacement: DsnRow }>;
}

/** Pure: which DSN rows were delays, and what each wrong event becomes. */
export function planDelayedDsnBackfill(
  dsnRows: readonly DsnRow[],
  events: readonly BouncedEvent[],
): DelayedDsnPlan {
  const transient = new Set<string>();
  const permanentByCampaign = new Map<string, DsnRow[]>();
  for (const row of dsnRows) {
    if (isTransientDeliveryReport(row.headers, row.text)) {
      transient.add(row.id);
    } else if (row.instantlyCampaignId) {
      const list = permanentByCampaign.get(row.instantlyCampaignId) ?? [];
      list.push(row);
      permanentByCampaign.set(row.instantlyCampaignId, list);
    }
  }

  const plan: DelayedDsnPlan = {
    transientRowIds: [...transient],
    retract: [],
    reattribute: [],
  };

  for (const event of events) {
    if (!transient.has(event.sourceRowId)) continue;
    const candidates = (event.campaignId && permanentByCampaign.get(event.campaignId)) || [];
    // Same step first (it is the notice about THIS email), then the earliest.
    const replacement = [...candidates].sort((a, b) => {
      const sa = a.step === event.step ? 0 : 1;
      const sb = b.step === event.step ? 0 : 1;
      return sa - sb || a.at.getTime() - b.at.getTime();
    })[0];
    if (replacement) plan.reattribute.push({ event, replacement });
    else plan.retract.push(event);
  }

  return plan;
}

export interface DelayedDsnBackfillSummary {
  dsnRowsRead: number;
  transientRows: number;
  eventsRetracted: number;
  eventsReattributed: number;
  /** Leads whose bounce was retracted (named, so the report is auditable). */
  retractedLeads: string[];
}

async function loadDsnRows(): Promise<DsnRow[]> {
  const result = await db.execute(sql`
    SELECT id, instantly_campaign_id, step, payload->'headers' AS headers,
           COALESCE(payload->>'textSnippet', '') AS text,
           COALESCE(received_at, polled_at) AS at
    FROM imap_messages_raw
    WHERE kind = 'bounce'
  `);
  return rowsOf(result).map((r) => ({
    id: String(r.id),
    instantlyCampaignId: (r.instantly_campaign_id as string | null) ?? null,
    step: r.step == null ? null : Number(r.step),
    headers: (r.headers ?? {}) as InboundHeaders,
    text: String(r.text ?? ""),
    at: new Date(r.at as string | Date),
  }));
}

async function loadBouncedEvents(sourceRowIds: string[]): Promise<BouncedEvent[]> {
  if (sourceRowIds.length === 0) return [];
  const result = await db.execute(sql`
    SELECT id, campaign_id, lead_email, step, source_row_id
    FROM instantly_events
    WHERE event_type = 'email_bounced'
      AND source = 'self_send'
      AND source_row_id = ANY(${sql.param(sourceRowIds)}::text[])
  `);
  return rowsOf(result).map((r) => ({
    id: String(r.id),
    campaignId: (r.campaign_id as string | null) ?? null,
    leadEmail: (r.lead_email as string | null) ?? null,
    step: r.step == null ? null : Number(r.step),
    sourceRowId: String(r.source_row_id),
  }));
}

export async function backfillDelayedDsns(
  options: { dryRun?: boolean } = {},
): Promise<DelayedDsnBackfillSummary> {
  const dryRun = options.dryRun !== false;
  const dsnRows = await loadDsnRows();
  const transientIds = dsnRows
    .filter((r) => isTransientDeliveryReport(r.headers, r.text))
    .map((r) => r.id);
  const events = await loadBouncedEvents(transientIds);
  const plan = planDelayedDsnBackfill(dsnRows, events);

  const summary: DelayedDsnBackfillSummary = {
    dsnRowsRead: dsnRows.length,
    transientRows: plan.transientRowIds.length,
    eventsRetracted: plan.retract.length,
    eventsReattributed: plan.reattribute.length,
    retractedLeads: plan.retract.map((e) => `${e.leadEmail} (${e.campaignId})`),
  };
  if (dryRun) return summary;

  const reattributed = new Map<string, string | null>(); // campaignId -> leadEmail

  for (const { event, replacement } of plan.reattribute) {
    await archiveSilverEvent(event.id, "reattributed", DELAYED_DSN_REASON, replacement.id);
    await db.execute(sql`
      UPDATE instantly_events
      SET source_row_id = ${replacement.id},
          timestamp = (
            SELECT COALESCE(received_at, polled_at) FROM imap_messages_raw
            WHERE id = ${replacement.id}
          )
      WHERE id = ${event.id}
    `);
    if (event.campaignId) reattributed.set(event.campaignId, event.leadEmail);
  }

  await retractBouncedEvents(plan.retract, DELAYED_DSN_REASON, "retract:delayed-dsn");

  // Bronze last: while a row is still `bounce` the sweep can be re-run and will
  // find the same work, so a crash mid-way leaves nothing half-done.
  if (plan.transientRowIds.length > 0) {
    await db.execute(sql`
      UPDATE imap_messages_raw
      SET kind = 'delay',
          payload = payload || jsonb_build_object(
            'reclassifiedFrom', 'bounce',
            'reclassifiedAt', now(),
            'reclassifiedReason', ${DELAYED_DSN_REASON}::text)
      WHERE kind = 'bounce' AND id = ANY(${sql.param(plan.transientRowIds)}::text[])
    `);
    await db.execute(sql`
      UPDATE messages SET kind = 'delay'
      WHERE source_table = 'imap_messages_raw'
        AND source_row_id = ANY(${sql.param(plan.transientRowIds)}::text[])
    `);
  }

  for (const [campaignId, leadEmail] of reattributed) {
    await refreshLeadStatusCurrent(campaignId, leadEmail);
  }

  return summary;
}
