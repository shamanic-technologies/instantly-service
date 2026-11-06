-- OUTCOME-PERFORMER-1106: who performed each declared website_visit is stated to runs-service (produced[].performer).
-- `website_visit_performer` = stated (runs-service proved it stored it) | conflict (another performer stored first) |
-- run_gone | refused; NULL = not stated yet (the completion pass in lib/website-visit-outcomes.ts states it once).
ALTER TABLE "instantly_campaigns" ADD COLUMN IF NOT EXISTS "website_visit_performer" text;
--> statement-breakpoint
ALTER TABLE "instantly_campaigns" ADD COLUMN IF NOT EXISTS "website_visit_performer_at" timestamp with time zone;
