-- A campaign cuts the queued follow-ups of NAMED leads it emailed in error
-- (lib/followup-cuts.ts, `POST /orgs/followup-cuts`). One row per cut sequence:
-- who cut it, when, why, and how many queued steps were refunded. NOT an
-- opt-out: the person never asked to stop, so nothing here is read as consent,
-- suppression or unsubscribe. Unique per sequence: cutting twice is a no-op.
CREATE TABLE IF NOT EXISTS "instantly_followup_cuts" (
  "id" text PRIMARY KEY NOT NULL,
  "org_id" text NOT NULL,
  "campaign_id" text NOT NULL,
  "instantly_campaign_id" text NOT NULL,
  "lead_email" text NOT NULL,
  "reason" text NOT NULL,
  "cut_by" text NOT NULL,
  "holds_cancelled" integer NOT NULL,
  "cut_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "instantly_followup_cuts_sequence_idx" ON "instantly_followup_cuts" ("instantly_campaign_id");
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "instantly_followup_cuts_org_campaign_idx" ON "instantly_followup_cuts" ("org_id", "campaign_id");
