-- Per-account LIFECYCLE (auto-driven) — replaces the manual "rest an account"
-- blacklist with a locally-stored, health-derived state machine.
--
-- Four states, derived automatically (see lib/account-lifecycle.ts deriveLifecycle):
--   deactivated_by_user       — domain in instantly_domain_policy (brand/product
--                               domains). Warmup 50/day, NO new sends, NEVER auto-
--                               promoted.
--   deactivated_by_instantly  — Instantly status <= 0 (Instantly disabled it).
--                               NO new sends, warmup left untouched.
--   in_recovery               — health score < 100 OR placement delivery < 100
--                               (or never tested). Warmup 50/day, NO new sends;
--                               Instantly daily_limit kept so the queue drains.
--   in_production             — health == 100 AND delivery == 100. Warmup 10/day,
--                               new sends ALLOWED.
--
-- Bronze (append-only) reconstructs capacity-over-time; Silver projects the
-- current state onto instantly_accounts; Gold derives-on-read.

-- ── Bronze A: periodic full snapshot of Instantly GET /accounts ──────────────
-- One row per (account, fetch). Gives health / daily_limit HISTORY. Fed by the
-- accounts-sync (POST /internal/audit/accounts-sync + the placement cron).
CREATE TABLE IF NOT EXISTS "instantly_accounts_raw" (
  "id" text PRIMARY KEY NOT NULL,
  "account_email" text NOT NULL,
  "status" integer,
  "warmup_score" integer,
  "daily_limit" integer,
  "provider_code" integer,
  "payload" jsonb NOT NULL,
  "fetched_at" timestamp DEFAULT now() NOT NULL
);
CREATE INDEX IF NOT EXISTS "instantly_accounts_raw_email_idx"
  ON "instantly_accounts_raw" ("account_email");
CREATE INDEX IF NOT EXISTS "instantly_accounts_raw_fetched_at_idx"
  ON "instantly_accounts_raw" ("fetched_at");

-- ── Bronze B: one row per lifecycle TRANSITION ───────────────────────────────
-- The audit trail AND the raw material for the capacity-over-time chart:
-- (status as-of-a-day) × (daily_limit as-of-a-day) reconstructs in_production
-- capacity for any past day. from_status is NULL on an account's first classify.
CREATE TABLE IF NOT EXISTS "instantly_account_lifecycle_events" (
  "id" text PRIMARY KEY NOT NULL,
  "account_email" text NOT NULL,
  "from_status" text,
  "to_status" text NOT NULL,
  "reason" text NOT NULL,
  "health_score" integer,
  "delivery_pct" integer,
  "daily_limit" integer,
  "created_at" timestamp DEFAULT now() NOT NULL
);
CREATE INDEX IF NOT EXISTS "instantly_account_lifecycle_events_email_created_idx"
  ON "instantly_account_lifecycle_events" ("account_email", "created_at");

-- ── Silver/config: domain policy (brand/product domains) ─────────────────────
-- Any account whose email domain is in this table → deactivated_by_user (never
-- auto-promoted). Lives in the DB (NOT a code constant) so ops can add a brand
-- domain without a deploy. Seed = ONLY the 3 brand/product domains; the legacy
-- shared-IP fleet is NOT listed here (it is handled automatically by
-- delivery < 100 → in_recovery).
CREATE TABLE IF NOT EXISTS "instantly_domain_policy" (
  "domain" text PRIMARY KEY NOT NULL,
  "reason" text DEFAULT 'brand' NOT NULL,
  "note" text,
  "created_at" timestamp DEFAULT now() NOT NULL
);
INSERT INTO "instantly_domain_policy" ("domain", "reason", "note") VALUES
  ('distribute.you', 'brand', 'Primary brand domain — never send cold from it'),
  ('growthagency.dev', 'brand', 'Live Vercel product domain'),
  ('arcadiaquest.org', 'brand', 'Legacy brand domain (cancelled DFY order leftover)')
ON CONFLICT ("domain") DO NOTHING;

-- ── Silver: rework instantly_accounts ────────────────────────────────────────
-- Drop the manual-blacklist mechanism.
ALTER TABLE "instantly_accounts" DROP COLUMN IF EXISTS "manually_blacklisted";
ALTER TABLE "instantly_accounts" DROP COLUMN IF EXISTS "manually_blacklisted_at";

-- Add the lifecycle projection + the health snapshot fields the send hot-path
-- and reconcile read from silver (no live listAccounts on the send gate).
ALTER TABLE "instantly_accounts"
  ADD COLUMN IF NOT EXISTS "lifecycle_status" text;
ALTER TABLE "instantly_accounts"
  ADD COLUMN IF NOT EXISTS "lifecycle_reason" text;
ALTER TABLE "instantly_accounts"
  ADD COLUMN IF NOT EXISTS "lifecycle_updated_at" timestamp with time zone;
ALTER TABLE "instantly_accounts"
  ADD COLUMN IF NOT EXISTS "instantly_status" integer;
ALTER TABLE "instantly_accounts"
  ADD COLUMN IF NOT EXISTS "warmup_score" integer;
ALTER TABLE "instantly_accounts"
  ADD COLUMN IF NOT EXISTS "provider_code" integer;
ALTER TABLE "instantly_accounts"
  ADD COLUMN IF NOT EXISTS "first_name" text;
ALTER TABLE "instantly_accounts"
  ADD COLUMN IF NOT EXISTS "last_name" text;
ALTER TABLE "instantly_accounts"
  ADD COLUMN IF NOT EXISTS "daily_limit" integer;
CREATE INDEX IF NOT EXISTS "instantly_accounts_lifecycle_status_idx"
  ON "instantly_accounts" ("lifecycle_status");
