import { Router, Request, Response } from "express";
import { db } from "../db";
import { sql, type SQL } from "drizzle-orm";
import { StatsRequestSchema } from "../schemas";

const router = Router();

// Internal emails/domains excluded from all stats.
// Events where lead_email matches these (or equals the sender) are never counted.
const EXCLUDED_EMAILS = ["kevin.lourd@gmail.com", "kevin@distribute.you"];
const EXCLUDED_DOMAINS = [
  "distribute.you",
  "mcpfactory.org",
  "growthagency.dev",
  "growthservice.org",
  "pressbeat.io",
  "kevinlourd.com",
  "polaritycourse.com",
];

/** SQL fragment that filters out internal/sender events */
function internalExclusionClause(): SQL {
  const domainConditions = EXCLUDED_DOMAINS.map(
    (d) => sql`e.lead_email LIKE ${"%" + d}`,
  );
  return sql`
    e.lead_email != e.account_email
    AND e.lead_email NOT IN (${sql.join(
      EXCLUDED_EMAILS.map((e) => sql`${e}`),
      sql`, `,
    )})
    AND NOT (${sql.join(domainConditions, sql` OR `)})
  `;
}

/**
 * POST /stats
 * Aggregated stats from webhook events (mirrors postmark /stats pattern)
 */
router.post("/stats", async (req: Request, res: Response) => {
  const parsed = StatsRequestSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({
      error: "Invalid request",
      details: parsed.error.flatten(),
    });
  }
  const { runIds, orgId, brandId, appId, campaignId } = parsed.data;

  // Build WHERE clauses for campaign filters
  const conditions: SQL[] = [];
  if (runIds?.length) conditions.push(sql`c.run_id = ANY(${runIds})`);
  if (orgId) conditions.push(sql`c.org_id = ${orgId}`);
  if (brandId) conditions.push(sql`c.brand_id = ${brandId}`);
  if (appId) conditions.push(sql`c.app_id = ${appId}`);
  if (campaignId) conditions.push(sql`(c.id = ${campaignId} OR c.campaign_id = ${campaignId})`);

  if (conditions.length === 0) {
    return res.status(400).json({ error: "At least one filter required: runIds, orgId, brandId, appId, campaignId" });
  }

  const whereClause = sql.join(conditions, sql` AND `);

  const zeroStats = {
    stats: {
      emailsSent: 0,
      emailsDelivered: 0,
      emailsOpened: 0,
      emailsClicked: 0,
      emailsReplied: 0,
      emailsBounced: 0,
      repliesAutoReply: 0,
      repliesNotInterested: 0,
      repliesOutOfOffice: 0,
      repliesUnsubscribe: 0,
    },
    recipients: 0,
  };

  try {
    const result = await db.execute(sql`
      SELECT
        COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'email_sent'), 0)::int AS "emailsSent",
        COALESCE(
          COUNT(*) FILTER (WHERE e.event_type = 'email_sent')
          - COUNT(*) FILTER (WHERE e.event_type = 'email_bounced'),
        0)::int AS "emailsDelivered",
        COALESCE(COUNT(DISTINCT e.lead_email) FILTER (WHERE e.event_type = 'email_opened'), 0)::int AS "emailsOpened",
        COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'email_link_clicked'), 0)::int AS "emailsClicked",
        COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'reply_received'), 0)::int AS "emailsReplied",
        COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'email_bounced'), 0)::int AS "emailsBounced",
        COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'auto_reply_received'), 0)::int AS "repliesAutoReply",
        COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_not_interested'), 0)::int AS "repliesNotInterested",
        COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_out_of_office'), 0)::int AS "repliesOutOfOffice",
        COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'lead_unsubscribed'), 0)::int AS "repliesUnsubscribe",
        COALESCE(COUNT(DISTINCT e.lead_email) FILTER (WHERE e.event_type = 'email_sent'), 0)::int AS "recipients"
      FROM instantly_events e
      JOIN instantly_campaigns c ON c.instantly_campaign_id = e.campaign_id
      WHERE ${whereClause}
        AND ${internalExclusionClause()}
    `);
    const rows = Array.isArray(result) ? result : (result as any).rows ?? [];

    if (!rows.length) {
      return res.json(zeroStats);
    }

    const row = rows[0] as Record<string, number>;

    // Per-step breakdown (secondary stats) — non-fatal; overall stats still return on failure
    let stepStats: { step: number; emailsSent: number; emailsOpened: number; emailsReplied: number; emailsBounced: number }[] = [];
    try {
      const stepResult = await db.execute(sql`
        SELECT
          e.step,
          COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'email_sent'), 0)::int AS "emailsSent",
          COALESCE(COUNT(DISTINCT e.lead_email) FILTER (WHERE e.event_type = 'email_opened'), 0)::int AS "emailsOpened",
          COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'reply_received'), 0)::int AS "emailsReplied",
          COALESCE(COUNT(*) FILTER (WHERE e.event_type = 'email_bounced'), 0)::int AS "emailsBounced"
        FROM instantly_events e
        JOIN instantly_campaigns c ON c.instantly_campaign_id = e.campaign_id
        WHERE ${whereClause}
          AND ${internalExclusionClause()}
          AND e.step IS NOT NULL
        GROUP BY e.step
        ORDER BY e.step
      `);
      const stepRows = Array.isArray(stepResult) ? stepResult : (stepResult as any).rows ?? [];
      stepStats = stepRows.map((sr: any) => ({
        step: sr.step,
        emailsSent: sr.emailsSent ?? 0,
        emailsOpened: sr.emailsOpened ?? 0,
        emailsReplied: sr.emailsReplied ?? 0,
        emailsBounced: sr.emailsBounced ?? 0,
      }));
    } catch (stepError: any) {
      console.error(`[stats] Step query failed (overall stats still returned): ${stepError.cause?.message ?? stepError.message}`);
    }

    res.json({
      stats: {
        emailsSent: row.emailsSent ?? 0,
        emailsDelivered: row.emailsDelivered ?? 0,
        emailsOpened: row.emailsOpened ?? 0,
        emailsClicked: row.emailsClicked ?? 0,
        emailsReplied: row.emailsReplied ?? 0,
        emailsBounced: row.emailsBounced ?? 0,
        repliesAutoReply: row.repliesAutoReply ?? 0,
        repliesNotInterested: row.repliesNotInterested ?? 0,
        repliesOutOfOffice: row.repliesOutOfOffice ?? 0,
        repliesUnsubscribe: row.repliesUnsubscribe ?? 0,
      },
      recipients: row.recipients ?? 0,
      ...(stepStats.length > 0 && { stepStats }),
    });
  } catch (error: any) {
    console.error(`[stats] Failed to aggregate stats: ${error.cause?.message ?? error.message}`);
    res.status(500).json({ error: "Failed to aggregate stats" });
  }
});

export default router;
