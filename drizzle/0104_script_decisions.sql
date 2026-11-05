-- MIGRATE-1105: every decision of this service a stored factory Script can take (message_queued: the step rows a
-- written sequence becomes; reply_received: what a message that landed in one of our mailboxes IS), either path,
-- with its exact material, our answer, the script's when asked, compared. `path` = service (ours, nothing asked),
-- shadow (ours used, the script asked beside it) or script (the script's used). Kept 14 days. Hand-written,
-- idempotent. Read by GET /internal/script-decisions/<outcome>/recorded.
CREATE TABLE IF NOT EXISTS "script_decisions" (
  "id" text PRIMARY KEY DEFAULT gen_random_uuid()::text NOT NULL,
  "seq" bigserial NOT NULL,
  "outcome" text NOT NULL,
  "path" text NOT NULL,
  "run_id" text,
  "script_run_id" text,
  "script_slug" text,
  "material" jsonb NOT NULL,
  "ours" jsonb NOT NULL,
  "script" jsonb,
  "same" boolean,
  "error" text,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  CONSTRAINT "script_decisions_path_check" CHECK ("path" IN ('service', 'shadow', 'script'))
);
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "script_decisions_seq_idx" ON "script_decisions" ("seq");
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "script_decisions_outcome_seq_idx" ON "script_decisions" ("outcome", "seq");
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "script_decisions_created_idx" ON "script_decisions" ("created_at");
