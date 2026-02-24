import { Router, Request, Response } from "express";
import { db } from "../db";
import { sql } from "drizzle-orm";
import { StatusRequestSchema } from "../schemas";

const router = Router();

interface AggRow {
  key: string;
  contacted: boolean | null;
  delivered: boolean | null;
  replied: boolean | null;
  bounced: boolean | null;
  unsubscribed: boolean | null;
  lastDeliveredAt: string | null;
}

function extractRows(result: unknown): AggRow[] {
  const raw = Array.isArray(result) ? result : (result as any).rows ?? [];
  return raw as AggRow[];
}

function emptyLead() {
  return { contacted: false, delivered: false, replied: false, lastDeliveredAt: null };
}

function emptyScopedEmail() {
  return { contacted: false, delivered: false, bounced: false, unsubscribed: false, lastDeliveredAt: null };
}

function emptyGlobalEmail() {
  return { bounced: false, unsubscribed: false };
}

function formatTimestamp(val: string | null | undefined): string | null {
  return val ? new Date(val).toISOString() : null;
}

function buildLeadStatus(row: AggRow | undefined) {
  return row
    ? { contacted: row.contacted === true, delivered: row.delivered === true, replied: row.replied === true, lastDeliveredAt: formatTimestamp(row.lastDeliveredAt) }
    : emptyLead();
}

function buildScopedEmailStatus(row: AggRow | undefined) {
  return row
    ? { contacted: row.contacted === true, delivered: row.delivered === true, bounced: row.bounced === true, unsubscribed: row.unsubscribed === true, lastDeliveredAt: formatTimestamp(row.lastDeliveredAt) }
    : emptyScopedEmail();
}

function sqlIn(values: string[]) {
  return sql.join(values.map((v) => sql`${v}`), sql`, `);
}

/** Lead-level aggregation query */
function leadQuery(filterClause: ReturnType<typeof sql>, leadIds: string[]) {
  return db.execute(sql`
    SELECT
      lead_id AS "key",
      BOOL_OR(delivery_status != 'pending') AS "contacted",
      BOOL_OR(delivery_status IN ('sent', 'delivered', 'replied')) AS "delivered",
      BOOL_OR(delivery_status = 'replied') AS "replied",
      CAST(NULL AS boolean) AS "bounced",
      CAST(NULL AS boolean) AS "unsubscribed",
      MAX(CASE WHEN delivery_status IN ('sent', 'delivered', 'replied') THEN updated_at END) AS "lastDeliveredAt"
    FROM instantly_campaigns
    WHERE lead_id IN (${sqlIn(leadIds)}) AND ${filterClause}
    GROUP BY lead_id
  `);
}

/** Email-level aggregation query (scoped — includes all fields) */
function scopedEmailQuery(filterClause: ReturnType<typeof sql>, emails: string[]) {
  return db.execute(sql`
    SELECT
      lead_email AS "key",
      BOOL_OR(delivery_status != 'pending') AS "contacted",
      BOOL_OR(delivery_status IN ('sent', 'delivered', 'replied')) AS "delivered",
      CAST(NULL AS boolean) AS "replied",
      BOOL_OR(delivery_status = 'bounced') AS "bounced",
      BOOL_OR(delivery_status = 'unsubscribed') AS "unsubscribed",
      MAX(CASE WHEN delivery_status IN ('sent', 'delivered', 'replied') THEN updated_at END) AS "lastDeliveredAt"
    FROM instantly_campaigns
    WHERE lead_email IN (${sqlIn(emails)}) AND ${filterClause}
    GROUP BY lead_email
  `);
}

/**
 * POST /status
 * Batch delivery status check.
 * Returns campaign-scoped (if campaignId provided), brand-scoped, and global results.
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

  const leadIds = items.map((i) => i.leadId);
  const emails = items.map((i) => i.email);

  try {
    // Build queries: brand (2) + global (1) + optional campaign (2)
    const brandFilter = sql`brand_id = ${brandId}`;
    const brandLeadPromise = leadQuery(brandFilter, leadIds);
    const brandEmailPromise = scopedEmailQuery(brandFilter, emails);

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
      const campFilter = sql`campaign_id = ${campaignId}`;
      campLeadPromise = leadQuery(campFilter, leadIds);
      campEmailPromise = scopedEmailQuery(campFilter, emails);
    }

    // Execute all in parallel
    const [brandLeadResult, brandEmailResult, globalEmailResult, campLeadResult, campEmailResult] =
      await Promise.all([
        brandLeadPromise,
        brandEmailPromise,
        globalEmailPromise,
        campLeadPromise ?? Promise.resolve(null),
        campEmailPromise ?? Promise.resolve(null),
      ]);

    // Index rows by key
    const brandLeadMap = new Map(extractRows(brandLeadResult).map((r) => [r.key, r]));
    const brandEmailMap = new Map(extractRows(brandEmailResult).map((r) => [r.key, r]));
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
        brand: {
          lead: buildLeadStatus(brandLeadMap.get(item.leadId)),
          email: buildScopedEmailStatus(brandEmailMap.get(item.email)),
        },
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
