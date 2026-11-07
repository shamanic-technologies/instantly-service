-- FOLLOWUP-JIT-1107: follow-ups are written and paid on their due day (lead-service), no longer held with the
-- first email. The one-time cut of the follow-up steps already queued (lib/followup-jit-cut.ts) leaves ONE audit
-- row per sequence: which steps it cancelled, how many holds, how much money went back to the org.
CREATE TABLE IF NOT EXISTS "followup_jit_cuts" (
  "id" text PRIMARY KEY NOT NULL,
  "org_id" text,
  "campaign_id" text,
  "brand_ids" text[],
  "instantly_campaign_id" text NOT NULL,
  "lead_email" text NOT NULL,
  "steps" integer[] NOT NULL,
  "holds_cancelled" integer NOT NULL,
  "released_usd_cents" numeric NOT NULL,
  "cut_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "followup_jit_cuts_sequence_idx" ON "followup_jit_cuts" ("instantly_campaign_id");
