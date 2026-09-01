import { sql } from "drizzle-orm";
import { db } from "../db";
import { REPLY_KINDS } from "./reply-kind";

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

function replyKindsSql() {
  return sql.join(REPLY_KINDS.map((kind) => sql`${kind}`), sql`, `);
}

/**
 * The reply kind currently on record for this lead — the FINER reading of the
 * reply that `reply_classification` reports coarsely.
 *
 * Precedence mirrors the `reply_classification_source='manual'` pin exactly: a
 * standing human statement wins OUTRIGHT, whatever the timestamps, and only
 * when there is none does the latest automatic reply-kind event answer. Doing
 * it any other way (latest-wins with manual as a tie-break) would let a webhook
 * arriving after a human statement quietly outrank the human — and the coarse
 * column beside it would still be pinned to the human's value, so the two
 * fields on the same row would disagree about the same reply.
 *
 * A withdrawn statement is skipped: `withdrawn_at` marks a statement nobody
 * stands behind any more, and the lead must read as it did before anybody spoke.
 */
function replyKindLateral() {
  return sql`
    LEFT JOIN LATERAL (
      SELECT k.event_type
      FROM instantly_events k
      WHERE k.campaign_id = c.instantly_campaign_id
        AND k.lead_email = c.lead_email
        AND k.event_type IN (${replyKindsSql()})
        AND k.withdrawn_at IS NULL
      ORDER BY (k.source = 'manual') DESC, k.timestamp DESC, k.created_at DESC, k.id DESC
      LIMIT 1
    ) rk ON TRUE
  `;
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
      reply_kind,
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
      rk.event_type AS reply_kind,
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
      -- A WITHDRAWN opt-out is not an opt-out. withdrawn_at is set when a human
      -- takes a recorded opt-out back, and this read is exactly what has to
      -- stop reporting them as opted out. Scoped to lead_unsubscribed on
      -- purpose: withdrawing a reply QUALIFICATION deliberately does NOT
      -- retract the fact that a reply arrived (see
      -- applyManualQualificationWithdrawalSideEffects), so the same filter
      -- applied to every event type would silently flip replied back to false,
      -- which no withdrawal ever claimed.
      AND (e.event_type <> 'lead_unsubscribed' OR e.withdrawn_at IS NULL)
    ${replyKindLateral()}
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
      rk.event_type,
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
      reply_kind = EXCLUDED.reply_kind,
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
