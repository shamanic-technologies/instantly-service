-- Manual reply qualifications: human users override Instantly's automatic reply
-- classification when Instantly fails to detect a reply (e.g. reply sent from
-- a non-leurre email account). Bronze table is append-only and stores raw user
-- intent; silver-promote applies side effects (reply_classification update,
-- silver event row with source='manual').

-- ─── instantly_campaigns: add reply_classification_source ────────────────────
-- Tracks whether the current reply_classification value was set by an automatic
-- Instantly webhook event ('auto') or by a manual human qualification ('manual').
-- Manual wins: silver-promote.updateReplyClassification skips webhook-driven
-- updates when this column is 'manual'.
ALTER TABLE "instantly_campaigns"
  ADD COLUMN IF NOT EXISTS "reply_classification_source" text DEFAULT 'auto' NOT NULL;
--> statement-breakpoint

-- ─── Bronze 6: manual reply qualifications (raw) ─────────────────────────────
-- Append-only mirror of human qualification actions. Each POST inserts a new
-- row regardless of prior history (idempotence is enforced in the application
-- layer: same status as latest → no-op + 200 with existing row).
CREATE TABLE IF NOT EXISTS "instantly_manual_qualifications_raw" (
  "id" text PRIMARY KEY NOT NULL,
  "org_id" text NOT NULL,
  "campaign_id" text NOT NULL,
  "instantly_campaign_id" text NOT NULL,
  "lead_email" text NOT NULL,
  "status" text NOT NULL,
  "qualified_by" text NOT NULL,
  "notes" text,
  "payload" jsonb NOT NULL,
  "qualified_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "instantly_manual_qualifications_raw_org_campaign_email_idx"
  ON "instantly_manual_qualifications_raw" USING btree ("org_id", "campaign_id", "lead_email");
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "instantly_manual_qualifications_raw_instantly_campaign_email_idx"
  ON "instantly_manual_qualifications_raw" USING btree ("instantly_campaign_id", "lead_email");
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "instantly_manual_qualifications_raw_qualified_at_idx"
  ON "instantly_manual_qualifications_raw" USING btree ("qualified_at");
