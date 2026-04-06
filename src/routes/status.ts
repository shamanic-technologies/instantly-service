import { Router, Request, Response } from "express";
import { db } from "../db";
import { sql } from "drizzle-orm";
import { StatusRequestSchema } from "../schemas";

const router = Router();

interface AggRow {
  key: string;
  leadIds: string[] | null;
  contacted: boolean | null;
  delivered: boolean | null;
  opened: boolean | null;
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
  return { contacted: false, delivered: false, opened: false, replied: false, replyClassification: null, bounced: false, unsubscribed: false, lastDeliveredAt: null };
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

/** Unified scoped query — groups by lead_email, returns all status fields + aggregated leadIds */
function scopedQuery(filterClause: ReturnType<typeof sql>, emails: string[]) {
  return db.execute(sql`
    SELECT
      c.lead_email AS "key",
      array_agg(DISTINCT c.lead_id) FILTER (WHERE c.lead_id IS NOT NULL) AS "leadIds",
      TRUE AS "contacted",
      BOOL_OR(c.delivery_status IN ('sent', 'delivered', 'replied')) AS "delivered",
      BOOL_OR(oe.campaign_id IS NOT NULL) AS "opened",
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
    WHERE c.lead_email IN (${sqlIn(emails)}) AND ${filterClause}
    GROUP BY c.lead_email
  `);
}

/**
 * POST /status
 * Batch delivery status check.
 * Returns campaign-scoped (if campaignId provided), brand-scoped, and global results.
 */
router.post("/", async (req: Request, res: Response) => {
  const brandId = req.headers["x-brand-id"] as string | undefined;

  const parsed = StatusRequestSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({
      error: "Invalid request",
      details: parsed.error.flatten(),
    });
  }
  const { campaignId, items } = parsed.data;

  const emails = items.map((i) => i.email);

  try {
    let brandPromise: Promise<unknown> | null = null;
    if (brandId) {
      const brandFilter = sql`${brandId} = ANY(c.brand_ids)`;
      brandPromise = scopedQuery(brandFilter, emails);
    }

    // Global: only bounced + unsubscribed on email
    const globalEmailPromise = db.execute(sql`
      SELECT
        lead_email AS "key",
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

    let campPromise: Promise<unknown> | null = null;
    if (campaignId) {
      const campFilter = sql`c.campaign_id = ${campaignId}`;
      campPromise = scopedQuery(campFilter, emails);
    }

    // Execute all in parallel
    const [brandResult, globalEmailResult, campResult] =
      await Promise.all([
        brandPromise ?? Promise.resolve(null),
        globalEmailPromise,
        campPromise ?? Promise.resolve(null),
      ]);

    // Index rows by key (email)
    const brandMap = brandResult ? new Map(extractRows(brandResult).map((r) => [r.key, r])) : null;
    const globalEmailMap = new Map(extractRows(globalEmailResult).map((r) => [r.key, r]));
    const campMap = campResult ? new Map(extractRows(campResult).map((r) => [r.key, r])) : null;

    const results = items.map((item) => {
      const ge = globalEmailMap.get(item.email);
      const brandRow = brandMap?.get(item.email);
      const campRow = campMap?.get(item.email);

      // Collect leadIds from all scopes + input
      const leadIdSet = new Set<string>();
      if (item.leadId) leadIdSet.add(item.leadId);
      for (const id of brandRow?.leadIds ?? []) leadIdSet.add(id);
      for (const id of campRow?.leadIds ?? []) leadIdSet.add(id);

      return {
        email: item.email,
        leadIds: [...leadIdSet],
        campaign: campMap ? buildScopedStatus(campRow) : null,
        brand: brandMap ? buildScopedStatus(brandRow) : null,
        global: {
          email: {
            bounced: ge?.bounced === true,
            unsubscribed: ge?.unsubscribed === true,
          },
        },
      };
    });

    res.json({ results });
  } catch (error: any) {
    console.error(`[status] Failed to get status: ${error.message}`);
    res.status(500).json({ error: "Failed to get delivery status" });
  }
});

export default router;
