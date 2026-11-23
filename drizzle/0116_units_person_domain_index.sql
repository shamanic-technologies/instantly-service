-- ENGINE-GET-ASK-1123: the Provider reads of our Units (`GET /orgs/units/email-sent`,
-- `GET /orgs/units/email-queued`, src/lib/provider-units.ts) find an org's
-- sequences by the COMPANY DOMAIN of the person's address ("did we already
-- write to anyone at acme.com?"). The address and lead-id branches have their
-- index (0040 `lower(lead_email)`, `instantly_campaigns_lead_id_idx`); this is
-- the domain branch's.
--
-- The expression is the query's literal (`split_part(lower(c.lead_email), '@', 2)`):
-- change one, change both (guard in tests/unit/migrations.test.ts).
--
-- Hand-written (drizzle has no expression-index form): do NOT drop it on a
-- `db:generate` diff. The boot migrator runs in a transaction, which forbids
-- CONCURRENTLY: pre-build it CONCURRENTLY on a prod-scale database before
-- deploy, this IF NOT EXISTS is then a no-op.
CREATE INDEX IF NOT EXISTS instantly_campaigns_org_lead_domain_idx
  ON instantly_campaigns (org_id, split_part(lower(lead_email), '@', 2));
