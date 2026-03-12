CREATE INDEX IF NOT EXISTS "instantly_events_campaign_id_idx" ON "instantly_events" USING btree ("campaign_id");
CREATE INDEX IF NOT EXISTS "instantly_events_event_type_idx" ON "instantly_events" USING btree ("event_type");
CREATE INDEX IF NOT EXISTS "instantly_events_lead_email_idx" ON "instantly_events" USING btree ("lead_email");
CREATE INDEX IF NOT EXISTS "instantly_campaigns_brand_id_idx" ON "instantly_campaigns" USING btree ("brand_id");
CREATE INDEX IF NOT EXISTS "instantly_campaigns_org_id_idx" ON "instantly_campaigns" USING btree ("org_id");
CREATE INDEX IF NOT EXISTS "instantly_campaigns_run_id_idx" ON "instantly_campaigns" USING btree ("run_id");
