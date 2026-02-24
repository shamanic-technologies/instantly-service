import { Router, Request, Response } from "express";
import { db } from "../db";
import { sql } from "drizzle-orm";
import { StatusRequestSchema } from "../schemas";

const router = Router();

/**
 * POST /status
 * Get delivery status for a lead (by leadId) and email across ALL campaigns.
 * Does NOT filter by campaignId — aggregates across all campaigns.
 */
router.post("/", async (req: Request, res: Response) => {
  const parsed = StatusRequestSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({
      error: "Invalid request",
      details: parsed.error.flatten(),
    });
  }
  const { leadId, email } = parsed.data;

  try {
    // Lead-level: all rows matching this leadId (any email)
    const leadResult = await db.execute(sql`
      SELECT
        BOOL_OR(delivery_status != 'pending') AS "contacted",
        BOOL_OR(delivery_status IN ('sent', 'delivered', 'replied')) AS "delivered",
        BOOL_OR(delivery_status = 'replied') AS "replied",
        MAX(CASE WHEN delivery_status IN ('sent', 'delivered', 'replied') THEN updated_at END) AS "lastDeliveredAt"
      FROM instantly_campaigns
      WHERE lead_id = ${leadId}
    `);

    // Email-level: all rows matching this exact email
    const emailResult = await db.execute(sql`
      SELECT
        BOOL_OR(delivery_status != 'pending') AS "contacted",
        BOOL_OR(delivery_status IN ('sent', 'delivered', 'replied')) AS "delivered",
        BOOL_OR(delivery_status = 'bounced') AS "bounced",
        BOOL_OR(delivery_status = 'unsubscribed') AS "unsubscribed",
        MAX(CASE WHEN delivery_status IN ('sent', 'delivered', 'replied') THEN updated_at END) AS "lastDeliveredAt"
      FROM instantly_campaigns
      WHERE lead_email = ${email}
    `);

    const leadRows = Array.isArray(leadResult) ? leadResult : (leadResult as any).rows ?? [];
    const emailRows = Array.isArray(emailResult) ? emailResult : (emailResult as any).rows ?? [];

    const leadRow = leadRows[0] as Record<string, unknown> | undefined;
    const emailRow = emailRows[0] as Record<string, unknown> | undefined;

    res.json({
      lead: {
        contacted: leadRow?.contacted === true,
        delivered: leadRow?.delivered === true,
        replied: leadRow?.replied === true,
        lastDeliveredAt: leadRow?.lastDeliveredAt
          ? new Date(leadRow.lastDeliveredAt as string).toISOString()
          : null,
      },
      email: {
        contacted: emailRow?.contacted === true,
        delivered: emailRow?.delivered === true,
        bounced: emailRow?.bounced === true,
        unsubscribed: emailRow?.unsubscribed === true,
        lastDeliveredAt: emailRow?.lastDeliveredAt
          ? new Date(emailRow.lastDeliveredAt as string).toISOString()
          : null,
      },
    });
  } catch (error: any) {
    console.error(`[status] Failed to get status: ${error.message}`);
    res.status(500).json({ error: "Failed to get delivery status" });
  }
});

export default router;
