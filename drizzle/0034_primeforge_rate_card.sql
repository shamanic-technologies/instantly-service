-- Primeforge pricing (issue #555).
--
-- Primeforge exposes NO billing surface: /subscriptions, /billing, /invoices,
-- /plans, /prices, /orders and /usage all 404 (probed live 2026-08-02). Its 10
-- domains and 35 mailboxes were therefore the largest hole in the spend read,
-- reported as null rather than estimated.
--
-- The rate now comes from the vendor's own published pricing page, captured in
-- BRONZE below so the number on screen can always be traced to the page and the
-- date it was read — a rate card typed straight into a seed is unauditable the
-- moment the vendor changes it.
--
-- Published terms, https://www.primeforge.ai/pricing, read 2026-08-02:
--   - domain:  $14/year for a .com, charged once. The page states explicitly
--              that there is NO annual discount on domains.
--   - mailbox: $4.50/month per SLOT, minimum 10 slots, billed monthly.
--              Annual billing gives "2 months free" → $3.75/month effective.
--
-- ⚠️ We seed the MONTHLY list rate ($4.50), not the discounted annual one.
-- Which of the two we actually pay depends on the subscription, and no API or
-- invoice we can reach says which. Overstating by 20% is the safe direction for
-- a cost figure; understating it would flatter every cost-per-email derived
-- from it. If the subscription turns out to be annual, ADD a row with a new
-- `effective_from` — never edit this one, the table is non-retroactive.
--
-- Note the unit is a SLOT, not a live mailbox: Primeforge bills capacity, so a
-- deleted mailbox keeps costing until the slot is released. `mailbox_count` in
-- the spend read counts what the vendor reports, which is slots in practice.

-- Bronze: the evidence, mirrored verbatim like any other vendor payload.
INSERT INTO "provider_account_raw" ("id","provider","scope","payload","fetched_at")
VALUES (
  'primeforge-pricing-2026-08-02',
  'primeforge',
  'pricing-page',
  '{
     "sourceUrl": "https://www.primeforge.ai/pricing",
     "readAt": "2026-08-02",
     "method": "published pricing page (no billing API exists)",
     "prices": [
       { "unit": "domain",  "amount": 14,   "currency": "USD", "period": "yearly",  "note": ".com; page states there is no annual discount on domains" },
       { "unit": "mailbox", "amount": 4.5,  "currency": "USD", "period": "monthly", "note": "per SLOT, minimum 10 slots, billed monthly" },
       { "unit": "mailbox", "amount": 3.75, "currency": "USD", "period": "monthly", "note": "per SLOT, effective rate when billed yearly (2 months free)" }
     ],
     "notSeeded": "the $3.75 annual-billing rate — we do not know which billing period the subscription uses"
   }'::jsonb,
  '2026-08-02T00:00:00Z'
)
ON CONFLICT (id) DO NOTHING;

-- Silver/config: the rate the spend read will actually use.
INSERT INTO "infra_price_rates" ("provider","scope","item","unit_cents","currency","source","effective_from","note")
VALUES
  ('primeforge','domain-year','',   1400,'USD','rate-card','2026-08-02T00:00:00Z','$14/yr .com — primeforge.ai/pricing read 2026-08-02, no annual discount on domains'),
  ('primeforge','mailbox-month','',  450,'USD','rate-card','2026-08-02T00:00:00Z','$4.50/mo per SLOT (min 10) on monthly billing — primeforge.ai/pricing read 2026-08-02; annual billing would be $3.75')
ON CONFLICT DO NOTHING;
