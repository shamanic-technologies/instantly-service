-- Observability for Instantly's `not_sending_status` diagnostic.
--
-- Adds:
--   1. Bronze table `instantly_campaigns_config_raw` — append-only GET /campaigns/{id}
--      payload snapshots written per reconcile cycle.
--   2. Silver columns on `instantly_campaigns`:
--        not_sending_status integer     — last observed value (NULL = sending)
--        not_sending_status_seen_at timestamp — when we last refreshed it
--
-- Production investigation: ~1,193 of 4,741 leads for one brand stuck at
-- delivery_status='contacted' with no follow-up dispatch. Each parent campaign
-- carried `not_sending_status=4` (Instantly diagnostic, likely capacity-related)
-- and we had no visibility into the field. PR adds the observation surface;
-- the future retry job (PR B) will consume it.
--
-- delivery_status doc note: `cancelled` is added as a reserved value
-- (text column, no schema change). The future retry job will mark stuck
-- leads `cancelled` when killing their campaign.

-- ─── Bronze: instantly_campaigns_config_raw ──────────────────────────────────
CREATE TABLE IF NOT EXISTS "instantly_campaigns_config_raw" (
  "id" text PRIMARY KEY NOT NULL,
  "org_id" text,
  "instantly_campaign_id" text NOT NULL,
  "payload" jsonb NOT NULL,
  "fetched_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "instantly_campaigns_config_raw_campaign_id_idx"
  ON "instantly_campaigns_config_raw" USING btree ("instantly_campaign_id");
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "instantly_campaigns_config_raw_fetched_at_idx"
  ON "instantly_campaigns_config_raw" USING btree ("fetched_at");
--> statement-breakpoint

-- ─── Silver: instantly_campaigns columns ─────────────────────────────────────
ALTER TABLE "instantly_campaigns"
  ADD COLUMN IF NOT EXISTS "not_sending_status" integer;
--> statement-breakpoint
ALTER TABLE "instantly_campaigns"
  ADD COLUMN IF NOT EXISTS "not_sending_status_seen_at" timestamp;
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "instantly_campaigns_not_sending_status_idx"
  ON "instantly_campaigns" USING btree ("not_sending_status")
  WHERE "not_sending_status" IS NOT NULL;
