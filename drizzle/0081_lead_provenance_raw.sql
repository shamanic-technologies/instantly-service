-- SOURCE-YIELD-1016. Bronze copy of lead-service's lead provenance (every source
-- tag of a person we emailed), so the source-yield read never re-asks
-- lead-service for history. One row per distinct answer per (org, brand,
-- address); a re-fetch reading the same answer moves `confirmed_at` only.
-- Hand-written; do NOT drop on a db:generate diff.
CREATE TABLE IF NOT EXISTS "lead_provenance_raw" (
  "id" text PRIMARY KEY NOT NULL,
  "org_id" text NOT NULL,
  "brand_id" text NOT NULL,
  "email" text NOT NULL,
  "tags" text[] NOT NULL,
  "payload" jsonb NOT NULL,
  "fetched_at" timestamp with time zone DEFAULT now() NOT NULL,
  "confirmed_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "lead_provenance_raw_key_idx"
  ON "lead_provenance_raw" ("org_id", "brand_id", "email", "fetched_at");
