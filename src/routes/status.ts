import { Router, Request, Response } from "express";
import { db } from "../db";
import { sql } from "drizzle-orm";
import { StatusRequestSchema } from "../schemas";

const router = Router();

interface AggRow {
  key: string;
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

function emptyLead() {
  return { contacted: false, delivered: false, opened: false, replied: false, replyClassification: null, lastDeliveredAt: null };
}

function emptyScopedEmail() {
  return { contacted: false, delivered: false, opened: false, bounced: false, unsubscribed: false, lastDeliveredAt: null };
}

function emptyGlobalEmail() {
  return { bounced: false, unsubscribed: false };
}

function formatTimestamp(val: string | null | undefined): string | null {
  return val ? new Date(val).toISOString() : null;
}

function buildLeadStatus(row: AggRow | undefined) {
  return row
    ? { contacted: row.contacted === true, delivered: row.delivered === true, opened: row.opened === true, replied: row.replied === true, replyClassification: row.replyClassification ?? null, lastDeliveredAt: formatTimestamp(row.lastDeliveredAt) }
    : emptyLead();
}

function buildScopedEmailStatus(row: AggRow | undefined) {
  return row
    ? { contacted: row.contacted === true, delivered: row.delivered === true, opened: row.opened === true, bounced: row.bounced === true, unsubscribed: row.unsubscribed === true, lastDeliveredAt: formatTimestamp(row.lastDeliveredAt) }
    : emptyScopedEmail();
}

function sqlIn(values: string[]) {
  return sql.join(values.map((v) => sql`${v}`), sql`, `);
}

/** Lead-level aggregation query */
function leadQuery(filterClause: ReturnType<typeof sql>, leadIds: string[]) {
  return db.execute(sql`
    SELECT
      c.lead_id AS "key",
      TRUE AS "contacted",
      BOOL_OR(c.delivery_status IN ('sent', 'delivered', 'replied')) AS "delivered",
      BOOL_OR(oe.campaign_id IS NOT NULL) AS "opened",
      BOOL_OR(c.delivery_status = 'replied') AS "replied",
      (array_agg(c.reply_classification ORDER BY c.updated_at DESC) FILTER (WHERE c.reply_classification IS NOT NULL))[1] AS "replyClassification",
      CAST(NULL AS boolean) AS "bounced",
      CAST(NULL AS boolean) AS "unsubscribed",
      MAX(CASE WHEN c.delivery_status IN ('sent', 'delivered', 'replied') THEN c.updated_at END) AS "lastDeliveredAt"
    FROM instantly_campaigns c
    LEFT JOIN instantly_events oe
      ON oe.campaign_id = c.instantly_campaign_id
      AND oe.lead_email = c.lead_email
      AND oe.event_type = 'email_opened'
    WHERE c.lead_id IN (${sqlIn(leadIds)}) AND ${filterClause}
    GROUP BY c.lead_id
  `);
}

/** Email-level aggregation query (scoped — includes all fields) */
function scopedEmailQuery(filterClause: ReturnType<typeof sql>, emails: string[]) {
  return db.execute(sql`
    SELECT
      c.lead_email AS "key",
      TRUE AS "contacted",
      BOOL_OR(c.delivery_status IN ('sent', 'delivered', 'replied')) AS "delivered",
      BOOL_OR(oe.campaign_id IS NOT NULL) AS "opened",
      CAST(NULL AS boolean) AS "replied",
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

  const leadIds = items.map((i) => i.leadId);
  const emails = items.map((i) => i.email);

  try {
    // Build queries: brand (2, optional) + global (1) + optional campaign (2)
    let brandLeadPromise: Promise<unknown> | null = null;
    let brandEmailPromise: Promise<unknown> | null = null;
    if (brandId) {
      const brandFilter = sql`${brandId} = ANY(c.brand_ids)`;
      brandLeadPromise = leadQuery(brandFilter, leadIds);
      brandEmailPromise = scopedEmailQuery(brandFilter, emails);
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

    let campLeadPromise: Promise<unknown> | null = null;
    let campEmailPromise: Promise<unknown> | null = null;
    if (campaignId) {
      const campFilter = sql`c.campaign_id = ${campaignId}`;
      campLeadPromise = leadQuery(campFilter, leadIds);
      campEmailPromise = scopedEmailQuery(campFilter, emails);
    }

    // Execute all in parallel
    const [brandLeadResult, brandEmailResult, globalEmailResult, campLeadResult, campEmailResult] =
      await Promise.all([
        brandLeadPromise ?? Promise.resolve(null),
        brandEmailPromise ?? Promise.resolve(null),
        globalEmailPromise,
        campLeadPromise ?? Promise.resolve(null),
        campEmailPromise ?? Promise.resolve(null),
      ]);

    // Index rows by key
    const brandLeadMap = brandLeadResult ? new Map(extractRows(brandLeadResult).map((r) => [r.key, r])) : null;
    const brandEmailMap = brandEmailResult ? new Map(extractRows(brandEmailResult).map((r) => [r.key, r])) : null;
    const globalEmailMap = new Map(extractRows(globalEmailResult).map((r) => [r.key, r]));
    const campLeadMap = campLeadResult ? new Map(extractRows(campLeadResult).map((r) => [r.key, r])) : null;
    const campEmailMap = campEmailResult ? new Map(extractRows(campEmailResult).map((r) => [r.key, r])) : null;

    const results = items.map((item) => {
      const ge = globalEmailMap.get(item.email);

      return {
        leadId: item.leadId,
        email: item.email,
        campaign: campLeadMap && campEmailMap
          ? {
              lead: buildLeadStatus(campLeadMap.get(item.leadId)),
              email: buildScopedEmailStatus(campEmailMap.get(item.email)),
            }
          : null,
        brand: brandLeadMap && brandEmailMap
          ? {
              lead: buildLeadStatus(brandLeadMap.get(item.leadId)),
              email: buildScopedEmailStatus(brandEmailMap.get(item.email)),
            }
          : null,
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
