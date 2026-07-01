-- Inbox-placement (deliverability) history — Bronze / Silver.
--
-- The Instantly V2 API exposes no standing per-account inbox-placement field;
-- placement data exists ONLY as the output of inbox-placement TESTS (point-in-
-- time, test-scoped). The recurring placement sync captures each test + its raw
-- analytics rows here (bronze, append-only) and promotes them to a per-(test,
-- account, ESP) silver result. Gold (GET /internal/audit/account-health) reads
-- the latest test per account. See lib/placement-promote.ts.

-- Bronze A: inbox-placement test objects (one row per test).
CREATE TABLE IF NOT EXISTS "instantly_placement_tests_raw" (
  "id" text PRIMARY KEY NOT NULL,
  "test_id" text NOT NULL,
  "test_code" text,
  "payload" jsonb NOT NULL,
  "fetched_at" timestamp DEFAULT now() NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS "instantly_placement_tests_raw_test_id_idx"
  ON "instantly_placement_tests_raw" ("test_id");
CREATE INDEX IF NOT EXISTS "instantly_placement_tests_raw_fetched_at_idx"
  ON "instantly_placement_tests_raw" ("fetched_at");

-- Bronze B: raw inbox-placement-analytics rows (one per (test, sender, recipient)).
-- Deduped on Instantly's analytics row id so re-polls are idempotent.
CREATE TABLE IF NOT EXISTS "instantly_placement_analytics_raw" (
  "id" text PRIMARY KEY NOT NULL,
  "analytics_id" text NOT NULL,
  "test_id" text NOT NULL,
  "payload" jsonb NOT NULL,
  "fetched_at" timestamp DEFAULT now() NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS "instantly_placement_analytics_raw_analytics_id_idx"
  ON "instantly_placement_analytics_raw" ("analytics_id");
CREATE INDEX IF NOT EXISTS "instantly_placement_analytics_raw_test_id_idx"
  ON "instantly_placement_analytics_raw" ("test_id");

-- Silver: canonical placement result per (test, sending account, recipient ESP).
CREATE TABLE IF NOT EXISTS "instantly_placement_results" (
  "test_id" text NOT NULL,
  "account_email" text NOT NULL,
  "recipient_esp" integer NOT NULL,
  "tested_at" timestamp NOT NULL,
  "seed_total" integer NOT NULL,
  "inbox_count" integer NOT NULL,
  "spam_count" integer NOT NULL,
  "missing_count" integer NOT NULL,
  "inbox_pct" integer NOT NULL,
  "spam_pct" integer NOT NULL,
  "missing_pct" integer NOT NULL,
  "spf_pass" boolean,
  "dkim_pass" boolean,
  "dmarc_pass" boolean,
  "created_at" timestamp DEFAULT now() NOT NULL,
  CONSTRAINT "instantly_placement_results_pk"
    PRIMARY KEY ("test_id", "account_email", "recipient_esp")
);
CREATE INDEX IF NOT EXISTS "instantly_placement_results_account_tested_idx"
  ON "instantly_placement_results" ("account_email", "tested_at");
