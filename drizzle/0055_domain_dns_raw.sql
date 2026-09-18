-- Bronze: an append-only daily photograph of what each of our domains publishes
-- in DNS — SPF, DMARC, the DKIM selectors we can find, MX. Deliverability starts
-- here and nothing in this service looked at it; the only auth signal we held
-- was a receiver's verdict on one seed message. A change in a record becomes a
-- change in the series, dated, instead of a mystery in the placement score.
--
-- `values` is empty when the name answers with no data; `error` carries the
-- resolver's code when the lookup itself failed (a different fact). A DKIM row
-- exists only for a selector that answered: absence of DKIM rows means "none
-- found among the probed selectors", never "no DKIM".
CREATE TABLE IF NOT EXISTS "domain_dns_raw" (
  "id" text PRIMARY KEY NOT NULL,
  "domain" text NOT NULL,
  "record_type" text NOT NULL,
  "selector" text,
  "name" text NOT NULL,
  "values" jsonb NOT NULL,
  "error" text,
  "fetched_at" timestamp with time zone DEFAULT now() NOT NULL
);
CREATE INDEX IF NOT EXISTS "domain_dns_raw_domain_fetched_idx" ON "domain_dns_raw" ("domain", "fetched_at");
