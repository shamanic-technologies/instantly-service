-- Per-domain position in the send fill order, within a vendor.
--
-- The fill order is vendor -> domain rank -> account age -> email
-- (src/lib/send-lead.ts `accountFillOrder`). A domain placed at the tail stops
-- receiving NEW sequences while any domain ahead of it still has room, so its
-- queued followups drain and "no email_sent for N days" becomes a per-domain
-- delete signal. The vendor tier alone could not do this: a vendor's mailboxes
-- are provisioned in batches that INTERLEAVE domains, so an age-ordered fleet
-- keeps every domain of a vendor mildly busy and none can ever be cancelled.
--
-- A domain with no row here sorts LAST within its vendor. Rollback is
-- `DELETE FROM instantly_domain_fill_order` -- the order then falls back to age
-- alone, byte-identical to the previous behaviour.
CREATE TABLE IF NOT EXISTS "instantly_domain_fill_order" (
  "domain" text PRIMARY KEY NOT NULL,
  "fill_rank" integer NOT NULL,
  "note" text,
  "created_at" timestamp DEFAULT now() NOT NULL
);

-- Seed: the 10 PrimeForge domains, ranked by remaining queued work DESCENDING,
-- so the domain that drains FASTEST sits at the tail and becomes cancellable
-- first. Measured against production 2026-08-29 (provisioned sequence_costs on
-- Instantly-active campaigns): boostdistribute 1267 steps ... maildistribute 181.
--
-- Gandi is deliberately left unranked: nothing is being wound down there, and
-- with no rows its accounts keep their existing age order exactly.
INSERT INTO "instantly_domain_fill_order" ("domain", "fill_rank", "note") VALUES
  ('boostdistribute.com', 0, 'primeforge - 1267 queued steps at seed time'),
  ('growdistribute.com',  1, 'primeforge - 1130 queued steps at seed time'),
  ('hellodistribute.com', 2, 'primeforge - 1065 queued steps at seed time'),
  ('startdistribute.com', 3, 'primeforge - 1052 queued steps at seed time'),
  ('leansignalio.com',    4, 'primeforge - 861 queued steps at seed time'),
  ('plainsignalco.com',   5, 'primeforge - 852 queued steps at seed time'),
  ('saviolabsco.com',     6, 'primeforge - 843 queued steps at seed time'),
  ('agileconsultco.com',  7, 'primeforge - 835 queued steps at seed time'),
  ('fuseconnectio.com',   8, 'primeforge - 631 queued steps at seed time'),
  ('maildistribute.com',  9, 'primeforge - 181 queued steps, drains first, cancel first')
ON CONFLICT ("domain") DO NOTHING;
