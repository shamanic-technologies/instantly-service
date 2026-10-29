-- FACTORY-P3 next outcomes (lead_interested): every reply judgment keeps the exact chat-service request it sent, the
-- whole answer and the label, on either path (our own code, or the stored script while switched). Bronze, append-only.
-- Read only by GET /internal/reply-qualifications/recorded (workflow-service's case recorder). Personal data.
CREATE TABLE IF NOT EXISTS "reply_qualifications" (
  "id" text PRIMARY KEY DEFAULT gen_random_uuid()::text NOT NULL,
  "org_id" text,
  "instantly_campaign_id" text NOT NULL,
  "lead_email" text,
  "source" text,
  "path" text NOT NULL,
  "run_id" text,
  "script_slug" text,
  "inputs" jsonb NOT NULL,
  "request" jsonb,
  "response" jsonb,
  "kind" text,
  "error" text,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  CONSTRAINT "reply_qualifications_path_check" CHECK ("path" IN ('service', 'script'))
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "reply_qualifications_created_idx" ON "reply_qualifications" ("created_at", "id");
