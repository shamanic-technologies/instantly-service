-- FACTORY-P3 next outcomes (message_sent): each dispatch SELECTION the stored send-selection script was asked, with
-- its inputs (the queue, the capacities, the skipped mailboxes, the bounce history: the script's material), our own
-- selection and the script's, compared. `path` = shadow (ours sent, the script asked beside it) or script (the
-- script's sent). Kept 14 days (the material is the fleet's queue). Read by GET /internal/dispatch-selections/recorded.
CREATE TABLE IF NOT EXISTS "dispatch_selections" (
  "id" text PRIMARY KEY DEFAULT gen_random_uuid()::text NOT NULL,
  "seq" bigserial NOT NULL,
  "path" text NOT NULL,
  "run_id" text,
  "script_run_id" text,
  "script_slug" text,
  "material" jsonb NOT NULL,
  "ours" jsonb NOT NULL,
  "script" jsonb,
  "same" boolean,
  "sends" integer NOT NULL,
  "error" text,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  CONSTRAINT "dispatch_selections_path_check" CHECK ("path" IN ('shadow', 'script'))
);
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "dispatch_selections_seq_idx" ON "dispatch_selections" ("seq");
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "dispatch_selections_created_idx" ON "dispatch_selections" ("created_at");
