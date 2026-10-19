-- MAILBOX-MONITORING-1019. Hand-written; idempotent.
-- One row per FALL of inbox placement at one inbox provider below the market
-- bar (among the mailboxes still allowed to mail that provider's prospects).
-- Opened on the first day below the bar, closed on the first measured day back
-- at or above it. The staff alert fires once per row (event id = provider +
-- started_on), never again while it stays open.
CREATE TABLE IF NOT EXISTS "placement_alert_episodes" (
  "provider" text NOT NULL,
  "started_on" date NOT NULL,
  "last_below_on" date NOT NULL,
  "closed_on" date,
  "inbox_pct" numeric(5,1) NOT NULL,
  "found_seeds" integer NOT NULL,
  "threshold_pct" numeric(5,1) NOT NULL,
  "mailboxes_cut" integer NOT NULL,
  "alert_status" text,
  "alert_error" text,
  "alerted_at" timestamp with time zone,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL,
  CONSTRAINT "placement_alert_episodes_pk" PRIMARY KEY ("provider", "started_on")
);
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "placement_alert_episodes_open_idx"
  ON "placement_alert_episodes" ("provider") WHERE "closed_on" IS NULL;
