-- A MAILBOX is the real login a provider enforces its quota, reputation and
-- credential at. A sending ADDRESS is what a prospect sees. On Gandi one
-- mailbox carries several aliases (154 addresses on 44 mailboxes, measured);
-- on Primeforge and Instantly DFY the address IS the mailbox. Every surface
-- that budgets a day's sending already groups by the login, each rebuilding
-- the grouping from the credential map in memory. This table is that grouping
-- PERSISTED, so a read can join to it instead of re-deriving it.
--
-- It is a PROJECTION, refreshed by the mailbox sync, never an input to any
-- decision: the transport is still decided by the credential we actually hold
-- (self-send/capability.ts), and the dispatch grain is still the live login
-- map. A stale row here changes what the ops dashboard shows, not what is sent.
CREATE TABLE IF NOT EXISTS "mailboxes" (
  "login" text PRIMARY KEY NOT NULL,
  "domain" text NOT NULL,
  "provider" text,
  "pool_type" text,
  "subscription" text,
  "credential_source" text NOT NULL,
  "vendor_created_at" timestamp with time zone,
  "vendor_prewarmed_at" timestamp with time zone,
  "imported_at" timestamp with time zone,
  "absent_since" timestamp with time zone,
  "synced_at" timestamp with time zone DEFAULT now() NOT NULL
);
CREATE INDEX IF NOT EXISTS "mailboxes_domain_idx" ON "mailboxes" ("domain");

-- The address → mailbox link, persisted at the same sync. Null until the first
-- sync has run, and for an address the sync has not seen since.
ALTER TABLE "instantly_accounts"
  ADD COLUMN IF NOT EXISTS "mailbox_login" text;
CREATE INDEX IF NOT EXISTS "instantly_accounts_mailbox_login_idx"
  ON "instantly_accounts" ("mailbox_login");
