-- UNSUB-3WAY-1103: every seed carries one of three opt-out headers
-- (tracker_link | mailbox_only | none); the daily series is split by it so the
-- three can be compared and the winner picked for prospect mail.
-- Hand-written; idempotent. Null / '' = a seed from before the experiment.
ALTER TABLE "seed_placement_dispatches" ADD COLUMN IF NOT EXISTS "opt_out_variant" text;
--> statement-breakpoint
ALTER TABLE "seed_placement_daily" ADD COLUMN IF NOT EXISTS "opt_out_variant" text DEFAULT '' NOT NULL;
--> statement-breakpoint
ALTER TABLE "seed_placement_daily" DROP CONSTRAINT IF EXISTS "seed_placement_daily_pk";
--> statement-breakpoint
ALTER TABLE "seed_placement_daily"
  ADD CONSTRAINT "seed_placement_daily_pk" PRIMARY KEY ("day", "recipient_esp", "sender_vendor", "opt_out_variant");
