/**
 * What we hold for one (campaign, lead) pair — answered on a duplicate
 * `POST /orgs/send` so the caller can tell "still queued with us" from "lost".
 *
 * ⚠️ Exists because the duplicate used to be a bare idempotent 200. A caller
 * that had inferred "lost" from `contacted: true, sent: false` getting old
 * re-served the lead every hour and got the same silent 200 each time (prod
 * 2026-09-24: 1,643 attempts in 24h over 145 people on one campaign, one lead
 * retried 103 times), each attempt burning a run slot that would have bought a
 * new lead. The answer now says what is held.
 */

import { sql } from "drizzle-orm";

import { db } from "../db";

export type HeldState = "in_flight" | "queued" | "finished";

export interface HeldLead {
  state: HeldState;
  awaitingFirstEmail: boolean;
  queuedSince: string | null;
  remainingSteps: number;
}

export interface HeldRow {
  instantlyCampaignId: string;
  status: string | null;
  createdAt: Date | null;
  provisionedSteps: number;
  sentSteps: number;
}

/**
 * Pure: the state of one claimed row.
 *
 * `in_flight` = the row still carries the `reserving:` sentinel (a concurrent
 * peer is creating the sequence). `queued` = active with at least one step
 * still provisioned — the dispatcher's own definition of the queue. Anything
 * else has nothing left to send.
 */
export function classifyHeldRow(row: HeldRow, reservationPrefix: string): HeldLead {
  const queuedSince = row.createdAt ? row.createdAt.toISOString() : null;
  if (row.instantlyCampaignId.startsWith(reservationPrefix)) {
    return { state: "in_flight", awaitingFirstEmail: true, queuedSince, remainingSteps: 0 };
  }
  const queued = row.status === "active" && row.provisionedSteps > 0;
  return {
    state: queued ? "queued" : "finished",
    awaitingFirstEmail: queued && row.sentSteps === 0,
    queuedSince: queued ? queuedSince : null,
    remainingSteps: queued ? row.provisionedSteps : 0,
  };
}

function toDate(value: unknown): Date | null {
  if (value instanceof Date) return value;
  if (typeof value !== "string" || value.trim() === "") return null;
  const iso = value.includes("T") ? value : value.replace(" ", "T");
  const parsed = new Date(/[Zz]|[+-]\d{2}:?\d{2}$/.test(iso) ? iso : `${iso}Z`);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

/**
 * The claimed row for this send's idempotency key — `(campaign_id, lead_email)`
 * for a campaign send, `(run_id, lead_email)` among ACTIVE rows for a platform
 * send (the same arbiters the reservation upserts on). Null when nothing is
 * found, which a caller reports as an absence rather than a guess.
 */
export async function readHeldLead(
  key: { campaignId: string | null; runId: string | null; leadEmail: string },
  reservationPrefix: string,
): Promise<HeldLead | null> {
  if (key.campaignId === null && key.runId === null) return null;
  const match =
    key.campaignId !== null
      ? sql`c.campaign_id = ${key.campaignId}`
      : sql`c.campaign_id IS NULL AND c.run_id = ${key.runId} AND c.status = 'active'`;
  const result = await db.execute(sql`
    SELECT
      c.instantly_campaign_id AS "instantlyCampaignId",
      c.status                AS "status",
      c.created_at            AS "createdAt",
      (SELECT COUNT(DISTINCT sc.step) FROM sequence_costs sc
        WHERE sc.instantly_campaign_id = c.instantly_campaign_id
          AND sc.status = 'provisioned')::int AS "provisionedSteps",
      (SELECT COUNT(DISTINCT sc.step) FROM sequence_costs sc
        WHERE sc.instantly_campaign_id = c.instantly_campaign_id
          AND sc.status = 'actual')::int AS "sentSteps"
    FROM instantly_campaigns c
    WHERE ${match}
      AND c.lead_email = ${key.leadEmail}
    ORDER BY c.created_at DESC
    LIMIT 1
  `);
  const rows = Array.isArray(result)
    ? (result as Record<string, unknown>[])
    : ((result as { rows?: Record<string, unknown>[] }).rows ?? []);
  const row = rows[0];
  if (!row) return null;
  return classifyHeldRow(
    {
      instantlyCampaignId: String(row.instantlyCampaignId),
      status: row.status === null || row.status === undefined ? null : String(row.status),
      createdAt: toDate(row.createdAt),
      provisionedSteps: Number(row.provisionedSteps ?? 0),
      sentSteps: Number(row.sentSteps ?? 0),
    },
    reservationPrefix,
  );
}
