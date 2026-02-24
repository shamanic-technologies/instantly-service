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

function emptyEmail() {
  return { contacted: false, delivered: false, bounced: false, unsubscribed: false, lastDeliveredAt: null };
}

function formatTimestamp(val: string | null | undefined): string | null {
  return val ? new Date(val).toISOString() : null;
}

/**
 * POST /status
 * Batch delivery status check.
 * Returns campaign-scoped and global results for each lead/email pair.
 */
router.post("/", async (req: Request, res: Response) => {
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
    // 4 queries: (campaign lead, campaign email, global lead, global email)
    const [campLeadResult, campEmailResult, globalLeadResult, globalEmailResult] =
      await Promise.all([
        // Campaign-scoped lead-level
        db.execute(sql`
          SELECT
            lead_id AS "key",
            BOOL_OR(delivery_status != 'pending') AS "contacted",
            BOOL_OR(delivery_status IN ('sent', 'delivered', 'replied')) AS "delivered",
            BOOL_OR(delivery_status = 'replied') AS "replied",
            CAST(NULL AS boolean) AS "bounced",
            CAST(NULL AS boolean) AS "unsubscribed",
            MAX(CASE WHEN delivery_status IN ('sent', 'delivered', 'replied') THEN updated_at END) AS "lastDeliveredAt"
          FROM instantly_campaigns
          WHERE lead_id = ANY(${leadIds}) AND campaign_id = ${campaignId}
          GROUP BY lead_id
        `),
        // Campaign-scoped email-level
        db.execute(sql`
          SELECT
            lead_email AS "key",
            BOOL_OR(delivery_status != 'pending') AS "contacted",
            BOOL_OR(delivery_status IN ('sent', 'delivered', 'replied')) AS "delivered",
            CAST(NULL AS boolean) AS "replied",
            BOOL_OR(delivery_status = 'bounced') AS "bounced",
            BOOL_OR(delivery_status = 'unsubscribed') AS "unsubscribed",
            MAX(CASE WHEN delivery_status IN ('sent', 'delivered', 'replied') THEN updated_at END) AS "lastDeliveredAt"
          FROM instantly_campaigns
          WHERE lead_email = ANY(${emails}) AND campaign_id = ${campaignId}
          GROUP BY lead_email
        `),
        // Global lead-level
        db.execute(sql`
          SELECT
            lead_id AS "key",
            BOOL_OR(delivery_status != 'pending') AS "contacted",
            BOOL_OR(delivery_status IN ('sent', 'delivered', 'replied')) AS "delivered",
            BOOL_OR(delivery_status = 'replied') AS "replied",
            CAST(NULL AS boolean) AS "bounced",
            CAST(NULL AS boolean) AS "unsubscribed",
            MAX(CASE WHEN delivery_status IN ('sent', 'delivered', 'replied') THEN updated_at END) AS "lastDeliveredAt"
          FROM instantly_campaigns
          WHERE lead_id = ANY(${leadIds})
          GROUP BY lead_id
        `),
        // Global email-level
        db.execute(sql`
          SELECT
            lead_email AS "key",
            BOOL_OR(delivery_status != 'pending') AS "contacted",
            BOOL_OR(delivery_status IN ('sent', 'delivered', 'replied')) AS "delivered",
            CAST(NULL AS boolean) AS "replied",
            BOOL_OR(delivery_status = 'bounced') AS "bounced",
            BOOL_OR(delivery_status = 'unsubscribed') AS "unsubscribed",
            MAX(CASE WHEN delivery_status IN ('sent', 'delivered', 'replied') THEN updated_at END) AS "lastDeliveredAt"
          FROM instantly_campaigns
          WHERE lead_email = ANY(${emails})
          GROUP BY lead_email
        `),
      ]);

    // Index rows by key for O(1) lookup
    const campLeadMap = new Map(extractRows(campLeadResult).map((r) => [r.key, r]));
    const campEmailMap = new Map(extractRows(campEmailResult).map((r) => [r.key, r]));
    const globalLeadMap = new Map(extractRows(globalLeadResult).map((r) => [r.key, r]));
    const globalEmailMap = new Map(extractRows(globalEmailResult).map((r) => [r.key, r]));

    const results = items.map((item) => {
      const cl = campLeadMap.get(item.leadId);
      const ce = campEmailMap.get(item.email);
      const gl = globalLeadMap.get(item.leadId);
      const ge = globalEmailMap.get(item.email);

      return {
        leadId: item.leadId,
        email: item.email,
        campaign: {
          lead: cl
            ? { contacted: cl.contacted === true, delivered: cl.delivered === true, replied: cl.replied === true, lastDeliveredAt: formatTimestamp(cl.lastDeliveredAt) }
            : emptyLead(),
          email: ce
            ? { contacted: ce.contacted === true, delivered: ce.delivered === true, bounced: ce.bounced === true, unsubscribed: ce.unsubscribed === true, lastDeliveredAt: formatTimestamp(ce.lastDeliveredAt) }
            : emptyEmail(),
        },
        global: {
          lead: gl
            ? { contacted: gl.contacted === true, delivered: gl.delivered === true, replied: gl.replied === true, lastDeliveredAt: formatTimestamp(gl.lastDeliveredAt) }
            : emptyLead(),
          email: ge
            ? { contacted: ge.contacted === true, delivered: ge.delivered === true, bounced: ge.bounced === true, unsubscribed: ge.unsubscribed === true, lastDeliveredAt: formatTimestamp(ge.lastDeliveredAt) }
            : emptyEmail(),
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
