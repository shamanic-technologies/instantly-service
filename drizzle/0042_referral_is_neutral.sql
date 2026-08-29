-- A referral reply stops counting as the customer's SALES INTEREST.
--
-- `lead_referral` used to project to the coarse classification `positive`, and
-- that projection is what reaches email-gateway → lead-service →
-- features-service, where it becomes the brand's positive-reply count, its
-- cost-per-positive-reply, and the learning-threshold gate. So a referral was
-- priced and reported as a buying signal. "Not me, but talk to X" is valuable
-- and it is NOT this person's buying interest — which is exactly the
-- distinction the metric exists to make. It is now `neutral`.
--
-- The classification is FROZEN at write time, so already-stored rows would keep
-- reading `positive` forever. This backfill re-derives it from the effective
-- (latest, non-withdrawn) reply-kind event, scoped to referral statements ONLY:
-- a row whose effective kind is anything else is not touched.
--
-- Forwarding is unchanged (a referral is still forwarded to the agency inbox)
-- and the sequence still stops on a referral — only the reported metric moves.
--
-- Idempotent: re-running matches nothing once every referral row reads neutral.

WITH effective AS (
  SELECT DISTINCT ON (e.campaign_id, e.lead_email)
         e.campaign_id,
         e.lead_email,
         e.event_type
  FROM instantly_events e
  WHERE e.event_type IN (
          'lead_interested', 'lead_referral', 'lead_info_requested',
          'lead_meeting_requested', 'lead_not_interested', 'lead_wrong_person',
          'lead_changed_job', 'lead_neutral', 'lead_out_of_office',
          'auto_reply_received'
        )
    AND e.withdrawn_at IS NULL
  ORDER BY e.campaign_id, e.lead_email,
           e.timestamp DESC, (e.source = 'manual') DESC, e.created_at DESC, e.id DESC
)
UPDATE instantly_campaigns c
SET reply_classification = 'neutral',
    updated_at = now()
FROM effective ef
WHERE ef.campaign_id = c.instantly_campaign_id
  AND ef.lead_email = c.lead_email
  AND ef.event_type = 'lead_referral'
  AND c.reply_classification IS DISTINCT FROM 'neutral';

-- The gold per-lead status row mirrors the campaign row, so it moves with it.
UPDATE instantly_lead_status_current g
SET reply_classification = c.reply_classification
FROM instantly_campaigns c
WHERE c.instantly_campaign_id = g.instantly_campaign_id
  AND c.lead_email = g.lead_email
  AND g.reply_classification IS DISTINCT FROM c.reply_classification
  AND c.reply_classification = 'neutral'
  AND EXISTS (
    SELECT 1 FROM instantly_events e
    WHERE e.campaign_id = c.instantly_campaign_id
      AND e.lead_email = c.lead_email
      AND e.event_type = 'lead_referral'
      AND e.withdrawn_at IS NULL
  );
