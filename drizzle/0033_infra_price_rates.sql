-- What the infrastructure vendors charge US (issue #555, PR 2).
--
-- Deliberately SEPARATE from costs-service, which prices what we RE-BILL the
-- customer. Neither replaces the other; the difference between them is the real
-- margin per email, a number we have never had. costs-service is untouched.
--
-- Two price sources coexist and stay distinguishable:
--   - vendor-reported per-domain prices live on `infra_domains.price_cents`
--     (Mailforge returns one on the domain object; Gandi quotes renewal per
--     domain because the price depends on the TLD);
--   - this rate card covers vendors whose API exposes no billing surface at all.
-- `source` records which, so no figure is ever shown without its provenance.

CREATE TABLE IF NOT EXISTS "infra_price_rates" (
  "provider" text NOT NULL,
  "scope" text NOT NULL,
  "item" text DEFAULT '' NOT NULL,
  "unit_cents" integer NOT NULL,
  "currency" text NOT NULL,
  "source" text NOT NULL,
  "effective_from" timestamp with time zone DEFAULT now() NOT NULL,
  "note" text,
  CONSTRAINT "infra_price_rates_pk" PRIMARY KEY("provider","scope","item","effective_from")
);

-- Seed the rates we actually know, and ONLY those.
--
-- Instantly DFY bills off-API on published terms. The workspace subscriptions
-- are what the Instantly account costs us regardless of domain count.
--
-- ⚠️ Primeforge is deliberately ABSENT. Every billing path it exposes 404s
-- (/subscriptions, /billing, /invoices, /plans, /prices, /orders, /usage —
-- probed live 2026-08-02), so its rate can only come from a human. Until a row
-- exists, its 10 domains report a NULL cost and the spend read names it as
-- unpriced. Do NOT seed a placeholder: a made-up rate would silently become the
-- fleet's cost-per-email denominator.

INSERT INTO "infra_price_rates" ("provider","scope","item","unit_cents","currency","source","effective_from","note")
VALUES
  ('instantly-dfy','domain-year','',        1500,'USD','rate-card','2025-01-01T00:00:00Z','DFY pre-warmed domain, $15/yr'),
  ('instantly-dfy','mailbox-month','',      1000,'USD','rate-card','2025-01-01T00:00:00Z','DFY pre-warmed mailbox, $10/mo'),
  ('instantly','plan-month','hypergrowth',  9700,'USD','rate-card','2025-01-01T00:00:00Z','Instantly HyperGrowth workspace plan'),
  ('instantly','plan-month','inbox-placement',4700,'USD','rate-card','2025-01-01T00:00:00Z','Growth Inbox Placement subscription')
ON CONFLICT DO NOTHING;
