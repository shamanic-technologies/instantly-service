-- DOMAIN-BLOCKLIST-1016 follow-up: every automated removal request to a
-- blocklist (lib/blocklist-removal.ts), sent or not, with what we posted and
-- what the list answered. Bronze, append-only; the silver event is
-- `domain_blocklist_events` (removal_requested / removal_failed).
-- Hand-written; do NOT drop on a db:generate diff.
CREATE TABLE IF NOT EXISTS "domain_blocklist_removal_attempts" (
  "id" text PRIMARY KEY NOT NULL,
  "domain" text NOT NULL,
  "list" text NOT NULL,
  "attempted_at" timestamp with time zone DEFAULT now() NOT NULL,
  "outcome" text NOT NULL,
  "http_status" integer,
  "request" jsonb,
  "response_excerpt" text,
  "error" text
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "domain_blocklist_removal_attempts_pair_idx"
  ON "domain_blocklist_removal_attempts" ("domain", "list", "attempted_at");
