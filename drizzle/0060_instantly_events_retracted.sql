-- Silver events taken back out of `instantly_events` because the evidence that
-- produced them turned out not to support them (first use: a DSN that reported a
-- TEMPORARY delay was promoted as `email_bounced`). The whole original row is
-- kept as JSON so the retraction can be undone with one INSERT ... SELECT.
CREATE TABLE IF NOT EXISTS "instantly_events_retracted" (
	"id" text PRIMARY KEY NOT NULL,
	"event_id" text NOT NULL,
	"action" text NOT NULL,
	"reason" text NOT NULL,
	"event" jsonb NOT NULL,
	"prior_delivery_status" text,
	"replacement_source_row_id" text,
	"retracted_at" timestamp DEFAULT now() NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS "instantly_events_retracted_event_idx" ON "instantly_events_retracted" USING btree ("event_id");
