import { sql } from "drizzle-orm";
import { db } from "../db";

const STATUS_EVENT_TYPES = [
  "email_sent",
  "email_bounced",
  "email_opened",
  "email_link_clicked",
  "reply_received",
  "lead_unsubscribed",
] as const;

function eventTypesSql() {
  return sql.join(STATUS_EVENT_TYPES.map((eventType) => sql`${eventType}`), sql`, `);
}

/**
 * Rebuild one current-status Gold row from Silver.
 *
 * This deliberately derives from `instantly_campaigns` + `instantly_events`
 * instead of incrementally toggling columns, so re-running it is idempotent and
 * manual/synthetic event corrections converge to the same result as a full
 * backfill.
 */
export async function refreshLeadStatusCurrent(
  instantlyCampaignId: string,
  leadEmail?: string | null,
): Promise<void> {
  const leadFilter = leadEmail ? sql`AND c.lead_email = ${leadEmail}` : sql``;

  await db.execute(sql`
    INSERT INTO instantly_lead_status_current (
      org_id,
      campaign_id,
      instantly_campaign_id,
      lead_email,
      brand_ids,
      contacted,
      sent,
      delivered,
      opened,
      clicked,
      replied,
      reply_classification,
      bounced,
      unsubscribed,
      cancelled,
      last_delivered_at,
      first_contacted_at,
      first_sent_at,
      first_delivered_at,
      first_opened_at,
      first_clicked_at,
      first_replied_at,
      first_bounced_at,
      first_unsubscribed_at,
      created_at,
      updated_at
    )
    SELECT
      c.org_id,
      c.campaign_id,
      c.instantly_campaign_id,
      c.lead_email,
      c.brand_ids,
      TRUE AS contacted,
      COALESCE(BOOL_OR(e.event_type = 'email_sent'), FALSE) AS sent,
      (
        COALESCE(BOOL_OR(e.event_type = 'email_sent'), FALSE)
        AND NOT COALESCE(BOOL_OR(e.event_type = 'email_bounced'), FALSE)
      ) AS delivered,
      COALESCE(BOOL_OR(e.event_type = 'email_opened'), FALSE) AS opened,
      COALESCE(BOOL_OR(e.event_type = 'email_link_clicked'), FALSE) AS clicked,
      COALESCE(BOOL_OR(e.event_type = 'reply_received'), FALSE) AS replied,
      c.reply_classification,
      COALESCE(BOOL_OR(e.event_type = 'email_bounced'), FALSE) AS bounced,
      COALESCE(BOOL_OR(e.event_type = 'lead_unsubscribed'), FALSE) AS unsubscribed,
      c.delivery_status = 'cancelled' AS cancelled,
      MAX(e.timestamp) FILTER (WHERE e.event_type = 'email_sent') AS last_delivered_at,
      c.created_at AS first_contacted_at,
      MIN(e.timestamp) FILTER (WHERE e.event_type = 'email_sent') AS first_sent_at,
      CASE
        WHEN COALESCE(BOOL_OR(e.event_type = 'email_sent'), FALSE)
          AND NOT COALESCE(BOOL_OR(e.event_type = 'email_bounced'), FALSE)
        THEN MIN(e.timestamp) FILTER (WHERE e.event_type = 'email_sent')
        ELSE NULL
      END AS first_delivered_at,
      MIN(e.timestamp) FILTER (WHERE e.event_type = 'email_opened') AS first_opened_at,
      MIN(e.timestamp) FILTER (WHERE e.event_type = 'email_link_clicked') AS first_clicked_at,
      MIN(e.timestamp) FILTER (WHERE e.event_type = 'reply_received') AS first_replied_at,
      MIN(e.timestamp) FILTER (WHERE e.event_type = 'email_bounced') AS first_bounced_at,
      MIN(e.timestamp) FILTER (WHERE e.event_type = 'lead_unsubscribed') AS first_unsubscribed_at,
      now() AS created_at,
      now() AS updated_at
    FROM instantly_campaigns c
    LEFT JOIN instantly_events e
      ON e.campaign_id = c.instantly_campaign_id
      AND e.lead_email = c.lead_email
      AND e.event_type IN (${eventTypesSql()})
    WHERE c.instantly_campaign_id = ${instantlyCampaignId}
      AND c.org_id IS NOT NULL
      AND c.lead_email IS NOT NULL
      AND c.instantly_campaign_id NOT LIKE 'reserving:%'
      ${leadFilter}
    GROUP BY
      c.org_id,
      c.campaign_id,
      c.instantly_campaign_id,
      c.lead_email,
      c.brand_ids,
      c.reply_classification,
      c.delivery_status,
      c.created_at
    ON CONFLICT (instantly_campaign_id, lead_email)
    DO UPDATE SET
      org_id = EXCLUDED.org_id,
      campaign_id = EXCLUDED.campaign_id,
      brand_ids = EXCLUDED.brand_ids,
      contacted = EXCLUDED.contacted,
      sent = EXCLUDED.sent,
      delivered = EXCLUDED.delivered,
      opened = EXCLUDED.opened,
      clicked = EXCLUDED.clicked,
      replied = EXCLUDED.replied,
      reply_classification = EXCLUDED.reply_classification,
      bounced = EXCLUDED.bounced,
      unsubscribed = EXCLUDED.unsubscribed,
      cancelled = EXCLUDED.cancelled,
      last_delivered_at = EXCLUDED.last_delivered_at,
      first_contacted_at = EXCLUDED.first_contacted_at,
      first_sent_at = EXCLUDED.first_sent_at,
      first_delivered_at = EXCLUDED.first_delivered_at,
      first_opened_at = EXCLUDED.first_opened_at,
      first_clicked_at = EXCLUDED.first_clicked_at,
      first_replied_at = EXCLUDED.first_replied_at,
      first_bounced_at = EXCLUDED.first_bounced_at,
      first_unsubscribed_at = EXCLUDED.first_unsubscribed_at,
      updated_at = now()
  `);
}

export async function deleteLeadStatusCurrent(
  instantlyCampaignId: string,
  leadEmail?: string | null,
): Promise<void> {
  const leadFilter = leadEmail ? sql`AND lead_email = ${leadEmail}` : sql``;

  await db.execute(sql`
    DELETE FROM instantly_lead_status_current
    WHERE instantly_campaign_id = ${instantlyCampaignId}
      ${leadFilter}
  `);
}
