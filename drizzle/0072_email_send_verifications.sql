-- The send door's verification ledger (lib/email-verification-gate.ts). One row
-- per verdict the gate asked apollo-service for before mail could leave for an
-- address: at POST /orgs/send (context 'send') or for a queued sequence before
-- dispatch (context 'queued'). `accepted` + `reason` = the decision and why.
CREATE TABLE IF NOT EXISTS "email_send_verifications" (
  "id" text PRIMARY KEY NOT NULL,
  "org_id" text NOT NULL,
  "email" text NOT NULL,
  "verdict" text,
  "verification_id" text,
  "verified_at" timestamp with time zone,
  "reused" boolean,
  "accepted" boolean NOT NULL,
  "reason" text NOT NULL,
  "context" text NOT NULL,
  "run_id" text,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "email_send_verifications_org_email_idx" ON "email_send_verifications" ("org_id", "email", "created_at");
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "email_send_verifications_verification_idx" ON "email_send_verifications" ("verification_id");
