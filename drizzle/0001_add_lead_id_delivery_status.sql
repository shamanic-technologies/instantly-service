ALTER TABLE "instantly_campaigns" ADD COLUMN "lead_id" text;--> statement-breakpoint
ALTER TABLE "instantly_campaigns" ADD COLUMN "delivery_status" text DEFAULT 'pending' NOT NULL;--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "instantly_campaigns_lead_id_idx" ON "instantly_campaigns" USING btree ("lead_id");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "instantly_campaigns_lead_email_idx" ON "instantly_campaigns" USING btree ("lead_email");