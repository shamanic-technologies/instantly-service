-- Provider infrastructure inventory (issue #555).
--
-- The fleet buys domains and mailboxes from FOUR vendors — Gandi, Mailforge,
-- Primeforge and Instantly DFY — and until now only Instantly existed in code,
-- and only at the ACCOUNT grain. There was no domain entity at all: a domain
-- was `split_part(email,'@',2)`, and `instantly_domain_policy` is a 3-row config
-- table, not a registry. These tables add the layer underneath the account.
--
-- All tables are NEW and empty, so the index builds are instant and carry no
-- boot-window risk (unlike migration 0026, which had to be pre-built
-- CONCURRENTLY against a 1.38 GB table).

-- ── Bronze: append-only vendor mirrors, one row per entity per poll ───────────

CREATE TABLE IF NOT EXISTS "provider_domains_raw" (
  "id" text PRIMARY KEY NOT NULL,
  "provider" text NOT NULL,
  "provider_account" text,
  "domain" text NOT NULL,
  "payload" jsonb NOT NULL,
  "fetched_at" timestamp DEFAULT now() NOT NULL
);

CREATE INDEX IF NOT EXISTS "provider_domains_raw_domain_idx" ON "provider_domains_raw" ("domain");
CREATE INDEX IF NOT EXISTS "provider_domains_raw_provider_fetched_idx" ON "provider_domains_raw" ("provider","fetched_at");

CREATE TABLE IF NOT EXISTS "provider_mailboxes_raw" (
  "id" text PRIMARY KEY NOT NULL,
  "provider" text NOT NULL,
  "provider_account" text,
  "email" text NOT NULL,
  "domain" text NOT NULL,
  "payload" jsonb NOT NULL,
  "fetched_at" timestamp DEFAULT now() NOT NULL
);

CREATE INDEX IF NOT EXISTS "provider_mailboxes_raw_email_idx" ON "provider_mailboxes_raw" ("email");
CREATE INDEX IF NOT EXISTS "provider_mailboxes_raw_provider_fetched_idx" ON "provider_mailboxes_raw" ("provider","fetched_at");

CREATE TABLE IF NOT EXISTS "provider_account_raw" (
  "id" text PRIMARY KEY NOT NULL,
  "provider" text NOT NULL,
  "scope" text NOT NULL,
  "payload" jsonb NOT NULL,
  "fetched_at" timestamp DEFAULT now() NOT NULL
);

CREATE INDEX IF NOT EXISTS "provider_account_raw_provider_fetched_idx" ON "provider_account_raw" ("provider","fetched_at");

-- ── Silver: current state per (provider, domain) and per (provider, mailbox) ──
--
-- The primary key is (provider, domain), NOT domain alone: a domain can be
-- reported by two vendors at once (Gandi registers it while Mailforge hosts its
-- mailboxes), and collapsing that here would bake a precedence guess into
-- storage. The per-domain rollup is derived on read.
--
-- Rows are never deleted. A domain the vendor stops reporting gets `absent_since`
-- set — the disappearance is itself the fact worth keeping (it lapsed, or it was
-- transferred away), and deleting the row would erase the evidence.

CREATE TABLE IF NOT EXISTS "infra_domains" (
  "provider" text NOT NULL,
  "domain" text NOT NULL,
  "provider_account" text,
  "external_id" text,
  "role" text NOT NULL,
  "status" text,
  "created_at_provider" timestamp with time zone,
  "expires_at" timestamp with time zone,
  "autorenew" boolean,
  "deletion_scheduled" boolean DEFAULT false NOT NULL,
  "cancelled_at" timestamp with time zone,
  "price_cents" integer,
  "price_currency" text,
  "first_seen_at" timestamp with time zone DEFAULT now() NOT NULL,
  "last_seen_at" timestamp with time zone DEFAULT now() NOT NULL,
  "absent_since" timestamp with time zone,
  CONSTRAINT "infra_domains_provider_domain_pk" PRIMARY KEY("provider","domain")
);

CREATE INDEX IF NOT EXISTS "infra_domains_domain_idx" ON "infra_domains" ("domain");
CREATE INDEX IF NOT EXISTS "infra_domains_expires_idx" ON "infra_domains" ("expires_at");

CREATE TABLE IF NOT EXISTS "infra_mailboxes" (
  "provider" text NOT NULL,
  "email" text NOT NULL,
  "domain" text NOT NULL,
  "provider_account" text,
  "external_id" text,
  "status" text,
  "created_at_provider" timestamp with time zone,
  "first_seen_at" timestamp with time zone DEFAULT now() NOT NULL,
  "last_seen_at" timestamp with time zone DEFAULT now() NOT NULL,
  "absent_since" timestamp with time zone,
  CONSTRAINT "infra_mailboxes_provider_email_pk" PRIMARY KEY("provider","email")
);

CREATE INDEX IF NOT EXISTS "infra_mailboxes_domain_idx" ON "infra_mailboxes" ("domain");

-- ── Ghost accounts ────────────────────────────────────────────────────────────
--
-- Set by the accounts-sync when an account is no longer in Instantly's live
-- list. The row stays (its history and its sent events remain meaningful) but it
-- is excluded from inventory and capacity views — prod carried 10 such ghosts
-- (266 stored vs 250 live) silently inflating fleet capacity.

ALTER TABLE "instantly_accounts" ADD COLUMN IF NOT EXISTS "absent_since" timestamp with time zone;
