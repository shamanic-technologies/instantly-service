-- EUR → USD reference rates, one row per ECB reference day (append-only).
-- Gandi bills in EUR and every other vendor in USD; this is what lets the
-- infrastructure spend be stated as one figure. No row = no blend, never a guess.
CREATE TABLE IF NOT EXISTS "fx_rates" (
	"base" text NOT NULL,
	"quote" text NOT NULL,
	"rate" numeric(18, 8) NOT NULL,
	"as_of" date NOT NULL,
	"source" text NOT NULL,
	"fetched_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "fx_rates_base_quote_as_of_pk" PRIMARY KEY("base","quote","as_of")
);
