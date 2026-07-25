-- Per-feature account RESERVATION (carve-out list). An account listed here is
-- reserved EXCLUSIVELY to its feature_slug: fetchInProductionAccounts serves it
-- ONLY to sends carrying that feature and EXCLUDES it from every other feature's
-- pool. An account NOT listed is unreserved = the default/shared pool (every
-- non-reserved feature + null slug).
--
-- Pure carve-out, NOT a symmetric partition. Seeded with the 3
-- sales-crm-email-outreach accounts so unproven CRM sends never touch the
-- Apollo-verified cold fleet; the 5 cold-email features (sales/pr/hiring/vc/
-- accelerators) keep the whole unreserved fleet. Lives in the DB (NOT a code
-- constant) so ops can reserve/unreserve an account without a deploy.
--
-- Already-queued Instantly campaigns drain naturally on whatever account they
-- were assigned at send time; this only affects the account chosen for NEW sends.
-- Tiny table (a handful of rows) → CREATE + seed is instant, safe at boot.
CREATE TABLE IF NOT EXISTS "instantly_account_feature_policy" (
  "account_email" text PRIMARY KEY NOT NULL,
  "feature_slug" text NOT NULL,
  "note" text,
  "created_at" timestamp DEFAULT now() NOT NULL
);
INSERT INTO "instantly_account_feature_policy" ("account_email", "feature_slug", "note") VALUES
  ('k.lourd@growdistribute.com', 'sales-crm-email-outreach', 'Reserved for CRM outreach — isolate from Apollo-verified cold fleet'),
  ('kevin@boostdistribute.com', 'sales-crm-email-outreach', 'Reserved for CRM outreach — isolate from Apollo-verified cold fleet'),
  ('allie@leansignalio.com', 'sales-crm-email-outreach', 'Reserved for CRM outreach — isolate from Apollo-verified cold fleet')
ON CONFLICT ("account_email") DO NOTHING;
