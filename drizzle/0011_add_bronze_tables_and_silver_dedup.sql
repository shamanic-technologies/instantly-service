-- Bronze/Silver/Gold refactor for webhook reconciliation.
-- Bronze tables hold raw external payloads (webhook + 3 reconcile poll sources).
-- Silver `instantly_events` gains source attribution + unique index for idempotent
-- promotion from any bronze source.

-- ─── Bronze 1: webhook payloads (raw) ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS "instantly_webhook_payloads_raw" (
  "id" text PRIMARY KEY NOT NULL,
  "org_id" text,
  "instantly_campaign_id" text NOT NULL,
  "payload" jsonb NOT NULL,
  "received_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "instantly_webhook_payloads_raw_campaign_id_idx"
  ON "instantly_webhook_payloads_raw" USING btree ("instantly_campaign_id");
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "instantly_webhook_payloads_raw_received_at_idx"
  ON "instantly_webhook_payloads_raw" USING btree ("received_at");
--> statement-breakpoint

-- ─── Bronze 2: /campaigns/analytics responses (raw) ──────────────────────────
CREATE TABLE IF NOT EXISTS "instantly_analytics_raw" (
  "id" text PRIMARY KEY NOT NULL,
  "org_id" text,
  "instantly_campaign_id" text NOT NULL,
  "payload" jsonb NOT NULL,
  "fetched_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "instantly_analytics_raw_campaign_id_idx"
  ON "instantly_analytics_raw" USING btree ("instantly_campaign_id");
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "instantly_analytics_raw_fetched_at_idx"
  ON "instantly_analytics_raw" USING btree ("fetched_at");
--> statement-breakpoint

-- ─── Bronze 3: /emails records (raw, individual emails with step) ────────────
CREATE TABLE IF NOT EXISTS "instantly_emails_raw" (
  "id" text PRIMARY KEY NOT NULL,
  "org_id" text,
  "instantly_campaign_id" text NOT NULL,
  "instantly_email_id" text NOT NULL,
  "payload" jsonb NOT NULL,
  "fetched_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "instantly_emails_raw_email_id_idx"
  ON "instantly_emails_raw" USING btree ("instantly_email_id");
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "instantly_emails_raw_campaign_id_idx"
  ON "instantly_emails_raw" USING btree ("instantly_campaign_id");
--> statement-breakpoint

-- ─── Bronze 4: /leads/list per-lead snapshots ────────────────────────────────
CREATE TABLE IF NOT EXISTS "instantly_leads_raw" (
  "id" text PRIMARY KEY NOT NULL,
  "org_id" text,
  "instantly_campaign_id" text NOT NULL,
  "lead_email" text NOT NULL,
  "payload" jsonb NOT NULL,
  "fetched_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "instantly_leads_raw_campaign_email_idx"
  ON "instantly_leads_raw" USING btree ("instantly_campaign_id", "lead_email");
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "instantly_leads_raw_fetched_at_idx"
  ON "instantly_leads_raw" USING btree ("fetched_at");
--> statement-breakpoint

-- ─── Silver alter: instantly_events ──────────────────────────────────────────
-- raw_payload becomes nullable (bronze tables hold raw now).
ALTER TABLE "instantly_events" ALTER COLUMN "raw_payload" DROP NOT NULL;
--> statement-breakpoint
ALTER TABLE "instantly_events" ADD COLUMN IF NOT EXISTS "source" text DEFAULT 'webhook' NOT NULL;
--> statement-breakpoint
ALTER TABLE "instantly_events" ADD COLUMN IF NOT EXISTS "source_row_id" text;
--> statement-breakpoint

-- Unique index for idempotent silver promotion from any bronze source.
-- COALESCE(step, -1) lets NULL step values participate in dedup (Postgres treats
-- NULL as distinct otherwise, allowing duplicate inserts for step-less events).
CREATE UNIQUE INDEX IF NOT EXISTS "instantly_events_dedupe_idx"
  ON "instantly_events" ("campaign_id", "lead_email", "event_type", "timestamp", (COALESCE("step", -1)));
