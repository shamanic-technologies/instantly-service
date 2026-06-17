-- Gold projection for /orgs/status.
--
-- /orgs/status is a hot fanout endpoint called by email-gateway/features-service.
-- The old read path joined instantly_campaigns to instantly_events once per
-- status facet on every request. This table materializes the current
-- campaign/lead status so status reads become indexed lookups. It remains
-- rebuildable from silver (`instantly_campaigns` + `instantly_events`).

CREATE TABLE IF NOT EXISTS "instantly_lead_status_current" (
  "org_id" text NOT NULL,
  "campaign_id" text,
  "instantly_campaign_id" text NOT NULL,
  "lead_email" text NOT NULL,
  "brand_ids" text[] NOT NULL,
  "contacted" boolean DEFAULT true NOT NULL,
  "sent" boolean DEFAULT false NOT NULL,
  "delivered" boolean DEFAULT false NOT NULL,
  "opened" boolean DEFAULT false NOT NULL,
  "clicked" boolean DEFAULT false NOT NULL,
  "replied" boolean DEFAULT false NOT NULL,
  "reply_classification" text,
  "bounced" boolean DEFAULT false NOT NULL,
  "unsubscribed" boolean DEFAULT false NOT NULL,
  "cancelled" boolean DEFAULT false NOT NULL,
  "last_delivered_at" timestamp,
  "first_contacted_at" timestamp,
  "first_sent_at" timestamp,
  "first_delivered_at" timestamp,
  "first_opened_at" timestamp,
  "first_clicked_at" timestamp,
  "first_replied_at" timestamp,
  "first_bounced_at" timestamp,
  "first_unsubscribed_at" timestamp,
  "created_at" timestamp DEFAULT now() NOT NULL,
  "updated_at" timestamp DEFAULT now() NOT NULL,
  CONSTRAINT "instantly_lead_status_current_pk"
    PRIMARY KEY ("instantly_campaign_id", "lead_email")
);

CREATE INDEX IF NOT EXISTS "instantly_lead_status_current_org_email_idx"
  ON "instantly_lead_status_current" ("org_id", "lead_email");

CREATE INDEX IF NOT EXISTS "instantly_lead_status_current_org_campaign_email_idx"
  ON "instantly_lead_status_current" ("org_id", "campaign_id", "lead_email");

CREATE INDEX IF NOT EXISTS "instantly_lead_status_current_brand_ids_idx"
  ON "instantly_lead_status_current" USING gin ("brand_ids");

INSERT INTO "instantly_lead_status_current" (
  "org_id",
  "campaign_id",
  "instantly_campaign_id",
  "lead_email",
  "brand_ids",
  "contacted",
  "sent",
  "delivered",
  "opened",
  "clicked",
  "replied",
  "reply_classification",
  "bounced",
  "unsubscribed",
  "cancelled",
  "last_delivered_at",
  "first_contacted_at",
  "first_sent_at",
  "first_delivered_at",
  "first_opened_at",
  "first_clicked_at",
  "first_replied_at",
  "first_bounced_at",
  "first_unsubscribed_at",
  "created_at",
  "updated_at"
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
  AND e.event_type IN (
    'email_sent',
    'email_bounced',
    'email_opened',
    'email_link_clicked',
    'reply_received',
    'lead_unsubscribed'
  )
WHERE c.org_id IS NOT NULL
  AND c.lead_email IS NOT NULL
  AND c.instantly_campaign_id NOT LIKE 'reserving:%'
GROUP BY
  c.org_id,
  c.campaign_id,
  c.instantly_campaign_id,
  c.lead_email,
  c.brand_ids,
  c.reply_classification,
  c.delivery_status,
  c.created_at
ON CONFLICT ("instantly_campaign_id", "lead_email")
DO UPDATE SET
  "org_id" = EXCLUDED."org_id",
  "campaign_id" = EXCLUDED."campaign_id",
  "brand_ids" = EXCLUDED."brand_ids",
  "contacted" = EXCLUDED."contacted",
  "sent" = EXCLUDED."sent",
  "delivered" = EXCLUDED."delivered",
  "opened" = EXCLUDED."opened",
  "clicked" = EXCLUDED."clicked",
  "replied" = EXCLUDED."replied",
  "reply_classification" = EXCLUDED."reply_classification",
  "bounced" = EXCLUDED."bounced",
  "unsubscribed" = EXCLUDED."unsubscribed",
  "cancelled" = EXCLUDED."cancelled",
  "last_delivered_at" = EXCLUDED."last_delivered_at",
  "first_contacted_at" = EXCLUDED."first_contacted_at",
  "first_sent_at" = EXCLUDED."first_sent_at",
  "first_delivered_at" = EXCLUDED."first_delivered_at",
  "first_opened_at" = EXCLUDED."first_opened_at",
  "first_clicked_at" = EXCLUDED."first_clicked_at",
  "first_replied_at" = EXCLUDED."first_replied_at",
  "first_bounced_at" = EXCLUDED."first_bounced_at",
  "first_unsubscribed_at" = EXCLUDED."first_unsubscribed_at",
  "updated_at" = now();
