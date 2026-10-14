-- Mirror of apollo-service's verification history (every BounceVerify call, every
-- org, every source), copied through its service-auth feed by
-- lib/verification-history-mirror.ts. Read by lib/delivery-by-verdict.ts to
-- know which verdict an address had when we first emailed it. `verdict` NULL =
-- the call returned no verdict (never invented).
CREATE TABLE IF NOT EXISTS "email_verification_history" (
  "verification_id" text PRIMARY KEY NOT NULL,
  "email" text NOT NULL,
  "verdict" text,
  "verified_at" timestamp with time zone NOT NULL,
  "mirrored_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "email_verification_history_email_at_idx" ON "email_verification_history" ("email", "verified_at");
--> statement-breakpoint
-- One row per mirrored feed: where the copy resumes, and when it last reached
-- the end of the feed (the instant up to which the copy is complete).
CREATE TABLE IF NOT EXISTS "email_verification_history_sync" (
  "feed" text PRIMARY KEY NOT NULL,
  "cursor" text,
  "caught_up_at" timestamp with time zone,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
