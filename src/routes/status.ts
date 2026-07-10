import { Router, Request, Response } from "express";
import { db } from "../db";
import { sql } from "drizzle-orm";
import { StatusRequestSchema } from "../schemas";

const router = Router();

// 4-stage funnel:
//   1. Pre-Instantly queue (not modeled in this service)
//   2. contacted = row exists in instantly_campaigns (lead pushed to Instantly)
//   3. sent      = instantly_events.event_type='email_sent' (Instantly dispatched)
//   4. delivered = sent AND NOT bounced (derived in queries, never a status)
// Terminal markers (bounced/unsubscribed/replied) derive from events directly.

interface AggRow {
  key: string;
  campaignId: string | null;
  contacted: boolean | null;
  sent: boolean | null;
  delivered: boolean | null;
  opened: boolean | null;
  clicked: boolean | null;
  replied: boolean | null;
  replyClassification: string | null;
  bounced: boolean | null;
  unsubscribed: boolean | null;
  cancelled: boolean | null;
  // Per-scope count of emails actually sent to this recipient (email_sent events,
  // distinct steps). pg returns COUNT/SUM as bigint → string; coerce with Number().
  sentCount: number | string | null;
  lastDeliveredAt: string | null;
  // Per-event first-occurrence (MIN) timestamps — mirror of lastDeliveredAt (MAX).
  firstContactedAt: string | null;
  firstSentAt: string | null;
  firstDeliveredAt: string | null;
  firstOpenedAt: string | null;
  firstClickedAt: string | null;
  firstRepliedAt: string | null;
  firstBouncedAt: string | null;
  firstUnsubscribedAt: string | null;
}

function extractRows(result: unknown): AggRow[] {
  const raw = Array.isArray(result) ? result : (result as any).rows ?? [];
  return raw as AggRow[];
}

function emptyScoped() {
  return { contacted: false, sent: false, delivered: false, opened: false, clicked: false, replied: false, replyClassification: null, bounced: false, unsubscribed: false, cancelled: false, sentCount: 0, lastDeliveredAt: null, firstContactedAt: null, firstSentAt: null, firstDeliveredAt: null, firstOpenedAt: null, firstClickedAt: null, firstRepliedAt: null, firstBouncedAt: null, firstUnsubscribedAt: null };
}

function formatTimestamp(val: string | null | undefined): string | null {
  return val ? new Date(val).toISOString() : null;
}

function buildScopedStatus(row: AggRow | undefined) {
  return row
    ? {
        contacted: row.contacted === true,
        sent: row.sent === true,
        delivered: row.delivered === true,
        opened: row.opened === true,
        clicked: row.clicked === true,
        replied: row.replied === true,
        replyClassification: row.replyClassification ?? null,
        bounced: row.bounced === true,
        unsubscribed: row.unsubscribed === true,
        cancelled: row.cancelled === true,
        sentCount: Number(row.sentCount ?? 0),
        lastDeliveredAt: formatTimestamp(row.lastDeliveredAt),
        firstContactedAt: formatTimestamp(row.firstContactedAt),
        firstSentAt: formatTimestamp(row.firstSentAt),
        firstDeliveredAt: formatTimestamp(row.firstDeliveredAt),
        firstOpenedAt: formatTimestamp(row.firstOpenedAt),
        firstClickedAt: formatTimestamp(row.firstClickedAt),
        firstRepliedAt: formatTimestamp(row.firstRepliedAt),
        firstBouncedAt: formatTimestamp(row.firstBouncedAt),
        firstUnsubscribedAt: formatTimestamp(row.firstUnsubscribedAt),
      }
    : emptyScoped();
}

function sqlIn(values: string[]) {
  return sql.join(values.map((v) => sql`${v}`), sql`, `);
}

/**
 * Pre-aggregated send-count per (Instantly campaign, recipient), bounded by the
 * request's emails. `sentCount` (contract v1.2.0) = the number of distinct
 * sequence steps that produced an `email_sent` event for a recipient — i.e. how
 * many emails of the sequence actually went out (1 = initial, 2 = first
 * follow-up, ...). Counts inferred sends too: an inferred `email_sent` marks a
 * provably-dispatched predecessor (sequence cascade / lost-webhook backfill), so
 * including it yields the true sequence position rather than only webhook-observed
 * dispatches. `instantly_events.campaign_id` holds the Instantly campaign id, so
 * the caller joins `sc.campaign_id = s.instantly_campaign_id`. Emitted as a
 * subquery joined 1:1 with the status rows (no fan-out of the BOOL_OR/MIN/MAX
 * aggregates). Absent recipient → no `sc` row → COALESCE(SUM(...), 0) = 0.
 */
function sentCountSubquery(emails: string[]) {
  return sql`
    SELECT e.campaign_id, e.lead_email, COUNT(DISTINCT e.step) AS cnt
    FROM instantly_events e
    WHERE e.event_type = 'email_sent'
      AND e.lead_email IN (${sqlIn(emails)})
    GROUP BY e.campaign_id, e.lead_email
  `;
}

/** Scoped query grouped by email only — used for campaign mode */
function scopedQueryByEmail(orgId: string, filterClause: ReturnType<typeof sql>, emails: string[]) {
  return db.execute(sql`
    SELECT
      s.lead_email AS "key",
      CAST(NULL AS text) AS "campaignId",
      BOOL_OR(s.contacted) AS "contacted",
      BOOL_OR(s.sent) AS "sent",
      (BOOL_OR(s.sent) AND NOT BOOL_OR(s.bounced)) AS "delivered",
      BOOL_OR(s.opened) AS "opened",
      BOOL_OR(s.clicked) AS "clicked",
      BOOL_OR(s.replied) AS "replied",
      (array_agg(s.reply_classification ORDER BY s.updated_at DESC) FILTER (WHERE s.reply_classification IS NOT NULL))[1] AS "replyClassification",
      BOOL_OR(s.bounced) AS "bounced",
      BOOL_OR(s.unsubscribed) AS "unsubscribed",
      BOOL_OR(s.cancelled) AS "cancelled",
      COALESCE(SUM(sc.cnt), 0) AS "sentCount",
      MAX(s.last_delivered_at) AS "lastDeliveredAt",
      MIN(s.first_contacted_at) AS "firstContactedAt",
      MIN(s.first_sent_at) AS "firstSentAt",
      CASE WHEN BOOL_OR(s.sent) AND NOT BOOL_OR(s.bounced) THEN MIN(s.first_sent_at) ELSE NULL END AS "firstDeliveredAt",
      MIN(s.first_opened_at) AS "firstOpenedAt",
      MIN(s.first_clicked_at) AS "firstClickedAt",
      MIN(s.first_replied_at) AS "firstRepliedAt",
      MIN(s.first_bounced_at) AS "firstBouncedAt",
      MIN(s.first_unsubscribed_at) AS "firstUnsubscribedAt"
    FROM instantly_lead_status_current s
    LEFT JOIN (${sentCountSubquery(emails)}) sc
      ON sc.campaign_id = s.instantly_campaign_id AND sc.lead_email = s.lead_email
    WHERE s.org_id = ${orgId}
      AND s.lead_email IN (${sqlIn(emails)})
      AND ${filterClause}
    GROUP BY s.lead_email
  `);
}

/** Brand breakdown query — grouped by (email, campaign_id) for per-campaign detail */
function brandBreakdownQuery(orgId: string, brandId: string, emails: string[]) {
  return db.execute(sql`
    SELECT
      s.lead_email AS "key",
      s.campaign_id AS "campaignId",
      BOOL_OR(s.contacted) AS "contacted",
      BOOL_OR(s.sent) AS "sent",
      (BOOL_OR(s.sent) AND NOT BOOL_OR(s.bounced)) AS "delivered",
      BOOL_OR(s.opened) AS "opened",
      BOOL_OR(s.clicked) AS "clicked",
      BOOL_OR(s.replied) AS "replied",
      (array_agg(s.reply_classification ORDER BY s.updated_at DESC) FILTER (WHERE s.reply_classification IS NOT NULL))[1] AS "replyClassification",
      BOOL_OR(s.bounced) AS "bounced",
      BOOL_OR(s.unsubscribed) AS "unsubscribed",
      BOOL_OR(s.cancelled) AS "cancelled",
      COALESCE(SUM(sc.cnt), 0) AS "sentCount",
      MAX(s.last_delivered_at) AS "lastDeliveredAt",
      MIN(s.first_contacted_at) AS "firstContactedAt",
      MIN(s.first_sent_at) AS "firstSentAt",
      CASE WHEN BOOL_OR(s.sent) AND NOT BOOL_OR(s.bounced) THEN MIN(s.first_sent_at) ELSE NULL END AS "firstDeliveredAt",
      MIN(s.first_opened_at) AS "firstOpenedAt",
      MIN(s.first_clicked_at) AS "firstClickedAt",
      MIN(s.first_replied_at) AS "firstRepliedAt",
      MIN(s.first_bounced_at) AS "firstBouncedAt",
      MIN(s.first_unsubscribed_at) AS "firstUnsubscribedAt"
    FROM instantly_lead_status_current s
    LEFT JOIN (${sentCountSubquery(emails)}) sc
      ON sc.campaign_id = s.instantly_campaign_id AND sc.lead_email = s.lead_email
    WHERE s.org_id = ${orgId}
      AND s.lead_email IN (${sqlIn(emails)})
      AND ${brandId} = ANY(s.brand_ids)
    GROUP BY s.lead_email, s.campaign_id
  `);
}

/** Aggregate brand status from per-campaign breakdown rows (BOOL_OR logic) */
function aggregateBrandStatus(rows: AggRow[]) {
  if (rows.length === 0) return emptyScoped();

  // Pick the most recent non-null replyClassification
  let replyClassification: string | null = null;
  let latestReplyAt: Date | null = null;
  for (const row of rows) {
    if (row.replyClassification != null) {
      const ts = row.lastDeliveredAt ? new Date(row.lastDeliveredAt) : null;
      if (!latestReplyAt || (ts && ts > latestReplyAt)) {
        replyClassification = row.replyClassification;
        latestReplyAt = ts;
      }
    }
  }

  // Pick the latest lastDeliveredAt across all campaigns
  let maxDeliveredAt: string | null = null;
  for (const row of rows) {
    if (row.lastDeliveredAt) {
      if (!maxDeliveredAt || new Date(row.lastDeliveredAt) > new Date(maxDeliveredAt)) {
        maxDeliveredAt = row.lastDeliveredAt;
      }
    }
  }

  // Brand-level first-occurrence = MIN across the brand's campaigns (mirror of the
  // MAX lastDeliveredAt / BOOL_OR boolean aggregation). Null skipped, so each
  // firstXAt stays consistent with its boolean: non-null iff some campaign has it.
  const minAt = (pick: (r: AggRow) => string | null) => {
    let min: string | null = null;
    for (const row of rows) {
      const v = pick(row);
      if (v && (!min || new Date(v) < new Date(min))) min = v;
    }
    return formatTimestamp(min);
  };

  return {
    contacted: rows.some((r) => r.contacted === true),
    sent: rows.some((r) => r.sent === true),
    delivered: rows.some((r) => r.delivered === true),
    opened: rows.some((r) => r.opened === true),
    clicked: rows.some((r) => r.clicked === true),
    replied: rows.some((r) => r.replied === true),
    replyClassification,
    bounced: rows.some((r) => r.bounced === true),
    unsubscribed: rows.some((r) => r.unsubscribed === true),
    cancelled: rows.some((r) => r.cancelled === true),
    // Brand scope = total emails sent to this recipient across the brand's
    // campaigns (SUM across the per-campaign breakdown rows), per contract v1.2.0.
    sentCount: rows.reduce((acc, r) => acc + Number(r.sentCount ?? 0), 0),
    lastDeliveredAt: formatTimestamp(maxDeliveredAt),
    firstContactedAt: minAt((r) => r.firstContactedAt),
    firstSentAt: minAt((r) => r.firstSentAt),
    firstDeliveredAt: minAt((r) => r.firstDeliveredAt),
    firstOpenedAt: minAt((r) => r.firstOpenedAt),
    firstClickedAt: minAt((r) => r.firstClickedAt),
    firstRepliedAt: minAt((r) => r.firstRepliedAt),
    firstBouncedAt: minAt((r) => r.firstBouncedAt),
    firstUnsubscribedAt: minAt((r) => r.firstUnsubscribedAt),
  };
}

/**
 * POST /status
 * Batch delivery status check.
 * - Brand mode (brandId, no campaignId): byCampaign breakdown + aggregated brand + global
 * - Campaign mode (campaignId present): campaign-scoped + global
 * - Global only: just global
 */
router.post("/", async (req: Request, res: Response) => {
  const parsed = StatusRequestSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({
      error: "Invalid request",
      details: parsed.error.flatten(),
    });
  }
  const { brandId, campaignId, items } = parsed.data;

  const emails = items.map((i) => i.email);
  const orgId = String(res.locals.orgId ?? "");
  const isBrandMode = !!brandId && !campaignId;
  const isCampaignMode = !!campaignId;

  try {
    // Global: bounced + unsubscribed across the entire org, read from Gold.
    const globalEmailPromise = db.execute(sql`
      SELECT
        s.lead_email AS "key",
        CAST(NULL AS text) AS "campaignId",
        CAST(NULL AS boolean) AS "contacted",
        CAST(NULL AS boolean) AS "sent",
        CAST(NULL AS boolean) AS "delivered",
        CAST(NULL AS boolean) AS "replied",
        BOOL_OR(s.bounced) AS "bounced",
        BOOL_OR(s.unsubscribed) AS "unsubscribed",
        CAST(NULL AS timestamp) AS "lastDeliveredAt"
      FROM instantly_lead_status_current s
      WHERE s.org_id = ${orgId}
        AND s.lead_email IN (${sqlIn(emails)})
      GROUP BY s.lead_email
    `);

    let brandBreakdownPromise: Promise<unknown> | null = null;
    if (isBrandMode) {
      brandBreakdownPromise = brandBreakdownQuery(orgId, brandId, emails);
    }

    let campPromise: Promise<unknown> | null = null;
    if (isCampaignMode) {
      const campFilter = sql`s.campaign_id = ${campaignId}`;
      campPromise = scopedQueryByEmail(orgId, campFilter, emails);
    }

    const [globalEmailResult, brandBreakdownResult, campResult] =
      await Promise.all([
        globalEmailPromise,
        brandBreakdownPromise ?? Promise.resolve(null),
        campPromise ?? Promise.resolve(null),
      ]);

    const globalEmailMap = new Map(extractRows(globalEmailResult).map((r) => [r.key, r]));
    const campMap = campResult ? new Map(extractRows(campResult).map((r) => [r.key, r])) : null;

    // For brand mode, index breakdown rows by email
    let brandBreakdownMap: Map<string, AggRow[]> | null = null;
    if (brandBreakdownResult) {
      brandBreakdownMap = new Map();
      for (const row of extractRows(brandBreakdownResult)) {
        const existing = brandBreakdownMap.get(row.key) ?? [];
        existing.push(row);
        brandBreakdownMap.set(row.key, existing);
      }
    }

    const results = items.map((item) => {
      const ge = globalEmailMap.get(item.email);

      const result: Record<string, unknown> = {
        email: item.email,
        byCampaign: null,
        brand: null,
        campaign: null,
        global: {
          email: {
            bounced: ge?.bounced === true,
            unsubscribed: ge?.unsubscribed === true,
          },
        },
      };

      if (isBrandMode && brandBreakdownMap) {
        const rows = brandBreakdownMap.get(item.email) ?? [];
        const byCampaign: Record<string, ReturnType<typeof buildScopedStatus>> = {};
        for (const row of rows) {
          if (row.campaignId) {
            byCampaign[row.campaignId] = buildScopedStatus(row);
          }
        }
        result.byCampaign = byCampaign;
        result.brand = aggregateBrandStatus(rows);
      }

      if (isCampaignMode && campMap) {
        const campRow = campMap.get(item.email);
        result.campaign = buildScopedStatus(campRow);
      }

      return result;
    });

    res.json({ results });
  } catch (error: any) {
    console.error(`[instantly-service] Failed to get status: ${error.message}`);
    res.status(500).json({ error: "Failed to get delivery status" });
  }
});

export default router;
