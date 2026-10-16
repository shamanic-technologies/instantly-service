-- DOMAIN-BLOCKLIST-1016. Every sending domain is checked daily against the
-- public domain blocklists that answer our resolver honestly (lib/domain-blocklist.ts).
-- `domain_blocklist_status` = the CURRENT answer per (domain, list);
-- `domain_blocklist_events` = append-only history: listed, delisted, removal
-- link issued / removal requested, each new listing carrying its one staff alert.
-- Hand-written; do NOT drop on a db:generate diff.
CREATE TABLE IF NOT EXISTS "domain_blocklist_status" (
  "domain" text NOT NULL,
  "list" text NOT NULL,
  "listed" boolean NOT NULL,
  "answer" text,
  "labels" text[] DEFAULT '{}'::text[] NOT NULL,
  "listed_since" timestamp with time zone,
  "last_checked_at" timestamp with time zone NOT NULL,
  "last_answered_at" timestamp with time zone,
  "first_answered_at" timestamp with time zone,
  "last_outcome" text NOT NULL,
  "last_error" text,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL,
  CONSTRAINT "domain_blocklist_status_pk" PRIMARY KEY ("domain", "list")
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS "domain_blocklist_events" (
  "id" text PRIMARY KEY NOT NULL,
  "domain" text NOT NULL,
  "list" text NOT NULL,
  "event_type" text NOT NULL,
  "answer" text,
  "labels" text[] DEFAULT '{}'::text[] NOT NULL,
  "mailboxes" text[] DEFAULT '{}'::text[] NOT NULL,
  "detail" jsonb,
  "occurred_at" timestamp with time zone DEFAULT now() NOT NULL,
  "alert_status" text,
  "alert_attempts" integer DEFAULT 0 NOT NULL,
  "alert_last_error" text,
  "alerted_at" timestamp with time zone
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "domain_blocklist_events_occurred_idx"
  ON "domain_blocklist_events" ("occurred_at");
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "domain_blocklist_events_domain_idx"
  ON "domain_blocklist_events" ("domain", "list", "occurred_at");
--> statement-breakpoint
-- The staff read's bounce series (DSNs citing a blocklist, by day).
CREATE INDEX IF NOT EXISTS "imap_messages_raw_bounce_received_idx"
  ON "imap_messages_raw" ("polled_at")
  WHERE kind = 'bounce';
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "domain_blocklist_events_alert_pending_idx"
  ON "domain_blocklist_events" ("occurred_at")
  WHERE alert_status = 'pending';
