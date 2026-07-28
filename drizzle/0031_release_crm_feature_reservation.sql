-- Release the sales-crm-email-outreach account reservation seeded by migration
-- 0030. The CRM outreach feature is not being launched, so its 3 carved-out
-- accounts (k.lourd@growdistribute.com, kevin@boostdistribute.com,
-- allie@leansignalio.com) go back to the UNRESERVED pool and serve every cold
-- feature again — including sales-cold-email-outreach.
--
-- Routing needs no code change: fetchInProductionAccounts derives the "reserved
-- slugs" set from this table, so removing the rows makes the slug non-reserved
-- and every feature (including a null slug) draws the whole in_production fleet.
--
-- The table itself STAYS (empty) — it is the ops lever to carve accounts out
-- again without a deploy. Idempotent: a re-run deletes nothing.
DELETE FROM "instantly_account_feature_policy"
WHERE "feature_slug" = 'sales-crm-email-outreach';
