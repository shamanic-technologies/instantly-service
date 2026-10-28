-- FACTORY-RECORD-SEND-1028: the exact message each send put on the wire, and the
-- inputs that decided when it left. Bronze, append-only, pruned after 180 days.
-- Hand-written (the journal carries no snapshots); additive, no reader changes.
CREATE TABLE IF NOT EXISTS "smtp_sent_messages" (
  "id" text PRIMARY KEY NOT NULL,
  "dispatch_id" text NOT NULL,
  "instantly_campaign_id" text NOT NULL,
  "lead_email" text NOT NULL,
  "account_email" text NOT NULL,
  "step" integer NOT NULL,
  "kind" text NOT NULL,
  "message_id" text NOT NULL,
  "raw_message" bytea NOT NULL,
  "raw_sha256" text NOT NULL,
  "raw_bytes" integer NOT NULL,
  "message_inputs" jsonb NOT NULL,
  "decision" jsonb NOT NULL,
  "sent_at" timestamp NOT NULL,
  "created_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "smtp_sent_messages_dispatch_idx" ON "smtp_sent_messages" ("dispatch_id");
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "smtp_sent_messages_campaign_idx" ON "smtp_sent_messages" ("instantly_campaign_id");
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "smtp_sent_messages_sent_at_idx" ON "smtp_sent_messages" ("sent_at");
