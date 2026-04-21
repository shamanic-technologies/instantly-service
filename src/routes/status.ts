import { Router, Request, Response } from "express";
import { db } from "../db";
import { sql } from "drizzle-orm";
import { StatusRequestSchema } from "../schemas";

const router = Router();

interface AggRow {
  key: string;
  campaignId: string | null;
  contacted: boolean | null;
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
  return { contacted: false, delivered: false, opened: false, clicked: false, replied: false, replyClassification: null, bounced: false, unsubscribed: false, lastDeliveredAt: null };
}

function formatTimestamp(val: string | null | undefined): string | null {
  return val ? new Date(val).toISOString() : null;
}

function buildScopedStatus(row: AggRow | undefined) {
  return row
    ? {
        contacted: row.contacted === true,
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
      c.lead_email AS "key",
      CAST(NULL AS text) AS "campaignId",
      TRUE AS "contacted",
      BOOL_OR(c.delivery_status IN ('sent', 'delivered', 'replied')) AS "delivered",
      BOOL_OR(oe.campaign_id IS NOT NULL) AS "opened",
      BOOL_OR(ce.campaign_id IS NOT NULL) AS "clicked",
      BOOL_OR(c.delivery_status = 'replied') AS "replied",
      (array_agg(c.reply_classification ORDER BY c.updated_at DESC) FILTER (WHERE c.reply_classification IS NOT NULL))[1] AS "replyClassification",
      BOOL_OR(c.delivery_status = 'bounced') AS "bounced",
      BOOL_OR(c.delivery_status = 'unsubscribed') AS "unsubscribed",
      MAX(CASE WHEN c.delivery_status IN ('sent', 'delivered', 'replied') THEN c.updated_at END) AS "lastDeliveredAt"
    FROM instantly_campaigns c
    LEFT JOIN instantly_events oe
      ON oe.campaign_id = c.instantly_campaign_id
      AND oe.lead_email = c.lead_email
      AND oe.event_type = 'email_opened'
    LEFT JOIN instantly_events ce
      ON ce.campaign_id = c.instantly_campaign_id
      AND ce.lead_email = c.lead_email
      AND ce.event_type = 'email_link_clicked'
    WHERE c.lead_email IN (${sqlIn(emails)}) AND ${filterClause}
    GROUP BY c.lead_email
  `);
}

/** Brand breakdown query — grouped by (email, campaign_id) for per-campaign detail */
function brandBreakdownQuery(brandIds: string[], emails: string[]) {
  return db.execute(sql`
    SELECT
      c.lead_email AS "key",
      c.campaign_id AS "campaignId",
      TRUE AS "contacted",
      BOOL_OR(c.delivery_status IN ('sent', 'delivered', 'replied')) AS "delivered",
      BOOL_OR(oe.campaign_id IS NOT NULL) AS "opened",
      BOOL_OR(ce.campaign_id IS NOT NULL) AS "clicked",
      BOOL_OR(c.delivery_status = 'replied') AS "replied",
      (array_agg(c.reply_classification ORDER BY c.updated_at DESC) FILTER (WHERE c.reply_classification IS NOT NULL))[1] AS "replyClassification",
      BOOL_OR(c.delivery_status = 'bounced') AS "bounced",
      BOOL_OR(c.delivery_status = 'unsubscribed') AS "unsubscribed",
      MAX(CASE WHEN c.delivery_status IN ('sent', 'delivered', 'replied') THEN c.updated_at END) AS "lastDeliveredAt"
    FROM instantly_campaigns c
    LEFT JOIN instantly_events oe
      ON oe.campaign_id = c.instantly_campaign_id
      AND oe.lead_email = c.lead_email
      AND oe.event_type = 'email_opened'
    LEFT JOIN instantly_events ce
      ON ce.campaign_id = c.instantly_campaign_id
      AND ce.lead_email = c.lead_email
      AND ce.event_type = 'email_link_clicked'
    WHERE c.lead_email IN (${sqlIn(emails)}) AND c.brand_ids && ARRAY[${sql.join(brandIds.map((id) => sql`${id}`), sql`, `)}]::text[]
    GROUP BY c.lead_email, c.campaign_id
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

  return {
    contacted: rows.some((r) => r.contacted === true),
    delivered: rows.some((r) => r.delivered === true),
    opened: rows.some((r) => r.opened === true),
    clicked: rows.some((r) => r.clicked === true),
    replied: rows.some((r) => r.replied === true),
    replyClassification,
    bounced: rows.some((r) => r.bounced === true),
    unsubscribed: rows.some((r) => r.unsubscribed === true),
    lastDeliveredAt: formatTimestamp(maxDeliveredAt),
  };
}

/**
 * POST /status
 * Batch delivery status check.
 * - Brand mode (brandIds, no campaignId): byCampaign breakdown + aggregated brand + global
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
  const { brandIds: brandIdsRaw, campaignId, items } = parsed.data;
  const brandIds = brandIdsRaw ? brandIdsRaw.split(",").filter(Boolean) : undefined;

  const emails = items.map((i) => i.email);
  const isBrandMode = !!brandIds?.length && !campaignId;
  const isCampaignMode = !!campaignId;

  try {
    // Global: only bounced + unsubscribed on email
    const globalEmailPromise = db.execute(sql`
      SELECT
        lead_email AS "key",
        CAST(NULL AS text) AS "campaignId",
        CAST(NULL AS boolean) AS "contacted",
        CAST(NULL AS boolean) AS "delivered",
        CAST(NULL AS boolean) AS "replied",
        BOOL_OR(delivery_status = 'bounced') AS "bounced",
        BOOL_OR(delivery_status = 'unsubscribed') AS "unsubscribed",
        CAST(NULL AS timestamp) AS "lastDeliveredAt"
      FROM instantly_campaigns
      WHERE lead_email IN (${sqlIn(emails)})
      GROUP BY lead_email
    `);

    let brandBreakdownPromise: Promise<unknown> | null = null;
    if (isBrandMode) {
      brandBreakdownPromise = brandBreakdownQuery(brandIds!, emails);
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
