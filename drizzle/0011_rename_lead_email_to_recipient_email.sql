-- Rename lead_email → recipient_email across all tables
ALTER TABLE instantly_campaigns RENAME COLUMN lead_email TO recipient_email;
ALTER TABLE instantly_events RENAME COLUMN lead_email TO recipient_email;
ALTER TABLE sequence_costs RENAME COLUMN lead_email TO recipient_email;

-- Recreate indexes with new column name
DROP INDEX IF EXISTS instantly_campaigns_campaign_lead_idx;
CREATE UNIQUE INDEX instantly_campaigns_campaign_recipient_idx ON instantly_campaigns (campaign_id, recipient_email);

DROP INDEX IF EXISTS instantly_campaigns_lead_email_idx;
CREATE INDEX instantly_campaigns_recipient_email_idx ON instantly_campaigns (recipient_email);

DROP INDEX IF EXISTS instantly_events_lead_email_idx;
CREATE INDEX instantly_events_recipient_email_idx ON instantly_events (recipient_email);

DROP INDEX IF EXISTS sequence_costs_campaign_lead_idx;
CREATE INDEX sequence_costs_campaign_recipient_idx ON sequence_costs (campaign_id, recipient_email);
