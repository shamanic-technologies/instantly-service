-- A reply's classification at the grain of EACH REPLY (not one per campaign x
-- lead, where a newer reply could hide behind an older verdict).
--
-- BRONZE `reply_verdicts_raw`: every verdict ever produced for a reply — by a
-- person, by Instantly, by our classifier — append-only, never updated. A
-- verdict mirrored from a silver event carries that event's id (unique, so the
-- projection is idempotent); a classifier-only verdict names its reply exactly.
--
-- SILVER `replies`: one row per real inbound reply, carrying its CURRENT
-- verdict (a person's statement beats the model's, then the most recent).
-- Rebuilt by the projection (lib/reply-verdicts); ids are minted in SQL here
-- because raw-SQL inserts get no ORM default.
CREATE TABLE IF NOT EXISTS "reply_verdicts_raw" (
	"id" text PRIMARY KEY DEFAULT gen_random_uuid()::text NOT NULL,
	"instantly_campaign_id" text NOT NULL,
	"lead_email" text,
	"kind" text NOT NULL,
	"producer_type" text NOT NULL,
	"producer" text NOT NULL,
	"origin" text NOT NULL,
	"source_event_id" text,
	"source_row_id" text,
	"reply_ref" text,
	"confidence" double precision,
	"raw" jsonb,
	"decided_at" timestamp with time zone NOT NULL,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS "reply_verdicts_raw_event_idx" ON "reply_verdicts_raw" USING btree ("source_event_id");
CREATE INDEX IF NOT EXISTS "reply_verdicts_raw_thread_idx" ON "reply_verdicts_raw" USING btree ("instantly_campaign_id","decided_at");
CREATE INDEX IF NOT EXISTS "reply_verdicts_raw_reply_idx" ON "reply_verdicts_raw" USING btree ("reply_ref");

CREATE TABLE IF NOT EXISTS "replies" (
	"id" text PRIMARY KEY NOT NULL,
	"source_table" text NOT NULL,
	"source_row_id" text NOT NULL,
	"provider_message_id" text,
	"instantly_campaign_id" text NOT NULL,
	"campaign_id" text,
	"org_id" text,
	"brand_ids" text[],
	"lead_email" text NOT NULL,
	"from_email" text,
	"transport" text NOT NULL,
	"subject" text,
	"received_at" timestamp with time zone NOT NULL,
	"current_verdict_id" text,
	"current_kind" text,
	"current_classification" text,
	"current_producer_type" text,
	"current_producer" text,
	"current_attribution" text,
	"current_confidence" double precision,
	"current_decided_at" timestamp with time zone,
	"verdict_count" integer DEFAULT 0 NOT NULL,
	"synced_at" timestamp with time zone DEFAULT now() NOT NULL
);
CREATE INDEX IF NOT EXISTS "replies_org_lead_idx" ON "replies" USING btree ("org_id", lower("lead_email"));
CREATE INDEX IF NOT EXISTS "replies_thread_idx" ON "replies" USING btree ("instantly_campaign_id","received_at");
