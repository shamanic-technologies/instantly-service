import { Router, Request, Response } from "express";
import { db } from "../db";
import { sql } from "drizzle-orm";
import { StatusRequestSchema } from "../schemas";

const router = Router();

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
  lastDeliveredAt: string | null;
}

function extractRows(result: unknown): AggRow[] {
  const raw = Array.isArray(result) ? result : (result as any).rows ?? [];
  return raw as AggRow[];
}

function emptyScoped() {
  return { contacted: false, sent: false, delivered: false, opened: false, clicked: false, replied: false, replyClassification: null, bounced: false, unsubscribed: false, lastDeliveredAt: null };
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
        lastDeliveredAt: formatTimestamp(row.lastDeliveredAt),
      }
    : emptyScoped();
}

function sqlIn(values: string[]) {
  return sql.join(values.map((v) => sql`${v}`), sql`, `);
}

/** Scoped query grouped by email only — used for campaign mode */
function scopedQueryByEmail(filterClause: ReturnType<typeof sql>, emails: string[]) {
  return db.execute(sql`
    SELECT
      c.recipient_email AS "key",
      CAST(NULL AS text) AS "campaignId",
      TRUE AS "contacted",
      COALESCE(BOOL_OR(e.event_type IS NOT NULL), false) AS "sent",
      COALESCE(BOOL_OR(e.event_type IS NOT NULL), false) AND NOT COALESCE(BOOL_OR(e.event_type = 'email_bounced'), false) AS "delivered",
      COALESCE(BOOL_OR(e.event_type IN ('email_opened', 'email_link_clicked', 'reply_received', 'lead_interested', 'lead_meeting_booked', 'lead_closed', 'lead_not_interested', 'lead_wrong_person', 'lead_neutral')), false) AS "opened",
      COALESCE(BOOL_OR(e.event_type = 'email_link_clicked'), false) AS "clicked",
      COALESCE(BOOL_OR(e.event_type IN ('reply_received', 'lead_interested', 'lead_meeting_booked', 'lead_closed', 'lead_not_interested', 'lead_wrong_person', 'lead_neutral', 'auto_reply_received', 'lead_out_of_office')), false) AS "replied",
      COALESCE(
        (array_agg(
          CASE e.event_type
            WHEN 'lead_interested' THEN 'positive'
            WHEN 'lead_meeting_booked' THEN 'positive'
            WHEN 'lead_closed' THEN 'positive'
            WHEN 'lead_not_interested' THEN 'negative'
            WHEN 'lead_wrong_person' THEN 'negative'
            WHEN 'lead_unsubscribed' THEN 'negative'
            WHEN 'lead_neutral' THEN 'neutral'
          END
          ORDER BY e.timestamp DESC
        ) FILTER (WHERE e.event_type IN ('lead_interested', 'lead_meeting_booked', 'lead_closed', 'lead_not_interested', 'lead_wrong_person', 'lead_unsubscribed', 'lead_neutral')))[1],
        (array_agg(
          'auto_reply'
          ORDER BY e.timestamp DESC
        ) FILTER (WHERE e.event_type IN ('auto_reply_received', 'lead_out_of_office')))[1]
      ) AS "replyClassification",
      COALESCE(BOOL_OR(e.event_type = 'email_bounced'), false) AS "bounced",
      COALESCE(BOOL_OR(e.event_type = 'lead_unsubscribed'), false) AS "unsubscribed",
      MAX(e.timestamp) FILTER (WHERE e.event_type = 'email_sent') AS "lastDeliveredAt"
    FROM instantly_campaigns c
    LEFT JOIN instantly_events e
      ON e.campaign_id = c.instantly_campaign_id
      AND e.recipient_email = c.recipient_email
    WHERE c.recipient_email IN (${sqlIn(emails)}) AND ${filterClause}
    GROUP BY c.recipient_email
  `);
}

/** Brand breakdown query — grouped by (email, campaign_id) for per-campaign detail */
function brandBreakdownQuery(brandId: string, emails: string[]) {
  return db.execute(sql`
    SELECT
      c.recipient_email AS "key",
      c.campaign_id AS "campaignId",
      TRUE AS "contacted",
      COALESCE(BOOL_OR(e.event_type IS NOT NULL), false) AS "sent",
      COALESCE(BOOL_OR(e.event_type IS NOT NULL), false) AND NOT COALESCE(BOOL_OR(e.event_type = 'email_bounced'), false) AS "delivered",
      COALESCE(BOOL_OR(e.event_type IN ('email_opened', 'email_link_clicked', 'reply_received', 'lead_interested', 'lead_meeting_booked', 'lead_closed', 'lead_not_interested', 'lead_wrong_person', 'lead_neutral')), false) AS "opened",
      COALESCE(BOOL_OR(e.event_type = 'email_link_clicked'), false) AS "clicked",
      COALESCE(BOOL_OR(e.event_type IN ('reply_received', 'lead_interested', 'lead_meeting_booked', 'lead_closed', 'lead_not_interested', 'lead_wrong_person', 'lead_neutral', 'auto_reply_received', 'lead_out_of_office')), false) AS "replied",
      COALESCE(
        (array_agg(
          CASE e.event_type
            WHEN 'lead_interested' THEN 'positive'
            WHEN 'lead_meeting_booked' THEN 'positive'
            WHEN 'lead_closed' THEN 'positive'
            WHEN 'lead_not_interested' THEN 'negative'
            WHEN 'lead_wrong_person' THEN 'negative'
            WHEN 'lead_unsubscribed' THEN 'negative'
            WHEN 'lead_neutral' THEN 'neutral'
          END
          ORDER BY e.timestamp DESC
        ) FILTER (WHERE e.event_type IN ('lead_interested', 'lead_meeting_booked', 'lead_closed', 'lead_not_interested', 'lead_wrong_person', 'lead_unsubscribed', 'lead_neutral')))[1],
        (array_agg(
          'auto_reply'
          ORDER BY e.timestamp DESC
        ) FILTER (WHERE e.event_type IN ('auto_reply_received', 'lead_out_of_office')))[1]
      ) AS "replyClassification",
      COALESCE(BOOL_OR(e.event_type = 'email_bounced'), false) AS "bounced",
      COALESCE(BOOL_OR(e.event_type = 'lead_unsubscribed'), false) AS "unsubscribed",
      MAX(e.timestamp) FILTER (WHERE e.event_type = 'email_sent') AS "lastDeliveredAt"
    FROM instantly_campaigns c
    LEFT JOIN instantly_events e
      ON e.campaign_id = c.instantly_campaign_id
      AND e.recipient_email = c.recipient_email
    WHERE c.recipient_email IN (${sqlIn(emails)}) AND ${brandId} = ANY(c.brand_ids)
    GROUP BY c.recipient_email, c.campaign_id
  `);
}

/** Aggregate brand status from per-campaign breakdown rows */
function aggregateBrandStatus(rows: AggRow[]) {
  if (rows.length === 0) return emptyScoped();

  // Reply classification: most positive HUMAN classification across campaigns
  // Positivity order: positive > neutral > negative > auto_reply > null
  const POSITIVITY_ORDER: Record<string, number> = { positive: 4, neutral: 3, negative: 2, auto_reply: 1 };
  let bestClassification: string | null = null;
  let bestScore = 0;
  for (const row of rows) {
    if (row.replyClassification != null) {
      const score = POSITIVITY_ORDER[row.replyClassification] ?? 0;
      if (score > bestScore) {
        bestScore = score;
        bestClassification = row.replyClassification;
      }
    }
  }

  let maxDeliveredAt: string | null = null;
  for (const row of rows) {
    if (row.lastDeliveredAt) {
      if (!maxDeliveredAt || new Date(row.lastDeliveredAt) > new Date(maxDeliveredAt)) {
        maxDeliveredAt = row.lastDeliveredAt;
      }
    }
  }

  return {
    contacted: rows.some((r) => r.contacted === true),
    sent: rows.some((r) => r.sent === true),
    delivered: rows.some((r) => r.delivered === true),
    opened: rows.some((r) => r.opened === true),
    clicked: rows.some((r) => r.clicked === true),
    replied: rows.some((r) => r.replied === true),
    replyClassification: bestClassification,
    bounced: rows.some((r) => r.bounced === true),
    unsubscribed: rows.some((r) => r.unsubscribed === true),
    lastDeliveredAt: formatTimestamp(maxDeliveredAt),
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
  const isBrandMode = !!brandId && !campaignId;
  const isCampaignMode = !!campaignId;

  try {
    // Global: only bounced + unsubscribed on email (derived from events)
    const globalEmailPromise = db.execute(sql`
      SELECT
        e.recipient_email AS "key",
        COALESCE(BOOL_OR(e.event_type = 'email_bounced'), false) AS "bounced",
        COALESCE(BOOL_OR(e.event_type = 'lead_unsubscribed'), false) AS "unsubscribed"
      FROM instantly_events e
      WHERE e.recipient_email IN (${sqlIn(emails)})
      GROUP BY e.recipient_email
    `);

    let brandBreakdownPromise: Promise<unknown> | null = null;
    if (isBrandMode) {
      brandBreakdownPromise = brandBreakdownQuery(brandId, emails);
    }

    let campPromise: Promise<unknown> | null = null;
    if (isCampaignMode) {
      const campFilter = sql`c.campaign_id = ${campaignId}`;
      campPromise = scopedQueryByEmail(campFilter, emails);
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
