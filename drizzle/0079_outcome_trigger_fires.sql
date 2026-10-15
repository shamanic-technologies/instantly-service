-- CLICK-FIRES-WEBSITE-VISIT-1015 (owner 2026-10-15: "Every outcome reached fires
-- its trigger, no exception"). A click on a link in one of our emails reaches the
-- `website_visit` outcome; it is recorded here, so it is fired from here: one row
-- per person (org, caller campaign, address) and outcome, delivered to
-- campaign-service's internal trigger door and retried until it is answered
-- (lib/website-visit-trigger.ts). Hand-written; do NOT drop on a db:generate diff.
CREATE TABLE IF NOT EXISTS "outcome_trigger_fires" (
  "id" text PRIMARY KEY NOT NULL,
  "outcome_key" text NOT NULL,
  "org_id" text NOT NULL,
  "campaign_id" text NOT NULL,
  "instantly_campaign_id" text NOT NULL,
  "lead_email" text NOT NULL,
  "lead_id" text,
  "source_event_id" text,
  "occurred_at" timestamp with time zone NOT NULL,
  "status" text DEFAULT 'pending' NOT NULL,
  "skip_reason" text,
  "trigger_id" text,
  "brand_id" text,
  "offer_id" text,
  "idempotency_key" text,
  "campaign_event_id" text,
  "replayed" boolean,
  "attempts" integer DEFAULT 0 NOT NULL,
  "next_attempt_at" timestamp with time zone DEFAULT now() NOT NULL,
  "last_error" text,
  "delivered_at" timestamp with time zone,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "outcome_trigger_fires_once_idx"
  ON "outcome_trigger_fires" ("outcome_key", "org_id", "campaign_id", "lead_email");
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "outcome_trigger_fires_due_idx"
  ON "outcome_trigger_fires" ("next_attempt_at")
  WHERE status = 'pending';
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "outcome_trigger_fires_instantly_campaign_idx"
  ON "outcome_trigger_fires" ("instantly_campaign_id");
