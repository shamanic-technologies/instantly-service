/**
 * A prospect's question still unanswered a day later: tell staff, whatever the
 * responder's status says.
 *
 * `maybeAlertUnansweredInfoRequest` asks campaign-service whether a responder
 * campaign is `ongoing` at the moment the question lands. `ongoing` is not
 * "it will answer": campaign-service skips a run that is out of budget
 * (`global_sales_budget_reached`, `budgets_unreadable`) and keeps the campaign
 * ongoing, and a stuck DAG answers nothing either. Measured 2026-10-11 over 45
 * days of positive replies: median answer 5 h, p90 7 days, 12 of 28 never.
 *
 * So this sweep reads the OUTCOME, not the status: every info request we
 * learned of more than `INFO_REQUEST_ANSWER_DEADLINE_MS` ago (and less than
 * `INFO_REQUEST_LOOKBACK` ago), whose thread is still an info request, with no
 * answer sent into it since (our dispatcher's step 0, or an Instantly Unibox
 * message), and not alerted yet, gets the staff alert. Same claim as the
 * live-path alert, so ONE alert per thread across both.
 *
 * Fails loud on the read (the worker logs it, the next tick retries); each
 * alert is fail-soft on its own.
 */

import { sql } from "drizzle-orm";

import { db } from "../db";
import { INFO_REQUEST_KIND, maybeAlertUnansweredInfoRequest } from "./alert-unanswered-info-request";

/** A working day: past the responder's normal answer time (median 5 h). */
export const INFO_REQUEST_ANSWER_DEADLINE_MS = 24 * 60 * 60 * 1000;
/** How far back a still-open question is chased. */
export const INFO_REQUEST_LOOKBACK = "14 days";

export interface OverdueInfoRequest {
  instantlyCampaignId: string;
  leadEmail: string;
  campaignId: string | null;
  orgId: string;
  userId: string;
  runId: string;
  brandIds: string[] | null;
  knownAt: Date;
}

function rowsOf(result: unknown): Record<string, unknown>[] {
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

/** Info requests nobody answered within the deadline and nobody was told about. */
export async function findOverdueInfoRequests(asOf: Date = new Date()): Promise<OverdueInfoRequest[]> {
  const cutoff = new Date(asOf.getTime() - INFO_REQUEST_ANSWER_DEADLINE_MS).toISOString();
  const result = await db.execute(sql`
    WITH q AS (
      SELECT e.campaign_id AS instantly_campaign_id, max(e.created_at) AS known_at
      FROM instantly_events e
      WHERE e.event_type = ${INFO_REQUEST_KIND}
        AND e.inferred = false
        AND e.withdrawn_at IS NULL
        AND e.created_at > now() - ${INFO_REQUEST_LOOKBACK}::interval
      GROUP BY e.campaign_id
    )
    SELECT c.instantly_campaign_id, c.lead_email, c.campaign_id, c.org_id, c.user_id, c.run_id,
           c.brand_ids, q.known_at
    FROM q
    JOIN instantly_campaigns c ON c.instantly_campaign_id = q.instantly_campaign_id
    JOIN instantly_lead_status_current g ON g.instantly_campaign_id = c.instantly_campaign_id
    WHERE q.known_at <= ${cutoff}::timestamp
      AND g.reply_kind = ${INFO_REQUEST_KIND}
      AND c.org_id IS NOT NULL AND c.user_id IS NOT NULL AND c.run_id IS NOT NULL
      AND (c.metadata->'infoRequestUnansweredAlert') IS NULL
      AND NOT EXISTS (
        SELECT 1 FROM smtp_dispatch_raw d
        WHERE d.instantly_campaign_id = c.instantly_campaign_id
          AND d.step = 0
          AND d.dispatched_at >= q.known_at
      )
      AND NOT EXISTS (
        SELECT 1 FROM instantly_emails_raw m
        WHERE m.instantly_campaign_id = c.instantly_campaign_id
          AND m.payload->>'ue_type' = '3'
          AND (m.payload->>'timestamp_email')::timestamptz >= q.known_at AT TIME ZONE 'UTC'
      )
    ORDER BY q.known_at
  `);
  return rowsOf(result).map((r) => ({
    instantlyCampaignId: String(r.instantly_campaign_id),
    leadEmail: String(r.lead_email),
    campaignId: (r.campaign_id as string | null) ?? null,
    orgId: String(r.org_id),
    userId: String(r.user_id),
    runId: String(r.run_id),
    brandIds: Array.isArray(r.brand_ids) ? (r.brand_ids as string[]) : null,
    knownAt: new Date(String(r.known_at)),
  }));
}

/** Pure: "on Oct 8" for the alert's why line. */
export function sinceLabel(knownAt: Date): string {
  return `on ${knownAt.toLocaleDateString("en-US", { month: "short", day: "numeric", timeZone: "UTC" })}`;
}

let inFlight: Promise<{ overdue: number; alerted: number }> | null = null;

/** One sweep. Concurrent calls join the one in flight. Throws when the read fails. */
export function alertOverdueInfoRequests(asOf: Date = new Date()): Promise<{ overdue: number; alerted: number }> {
  if (inFlight) return inFlight;
  inFlight = (async () => {
    const overdue = await findOverdueInfoRequests(asOf);
    let alerted = 0;
    for (const o of overdue) {
      const outcome = await maybeAlertUnansweredInfoRequest(
        {
          instantlyCampaignId: o.instantlyCampaignId,
          campaignId: o.campaignId,
          orgId: o.orgId,
          userId: o.userId,
          runId: o.runId,
          brandIds: o.brandIds,
        },
        o.leadEmail,
        INFO_REQUEST_KIND,
        { overdueSince: sinceLabel(o.knownAt) },
      );
      if (outcome.sent) alerted += 1;
    }
    if (overdue.length > 0) {
      console.log(`[instantly-service] info-request-deadline: ${overdue.length} overdue, ${alerted} alerted`);
    }
    return { overdue: overdue.length, alerted };
  })().finally(() => {
    inFlight = null;
  });
  return inFlight;
}
