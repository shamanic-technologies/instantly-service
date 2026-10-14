-- NEVER-NO-FOLLOWUP-1014: a send can declare it is a FOLLOW-UP of the brand's
-- own earlier outreach to the person (`POST /orgs/send` body `followUp: true`).
--
-- A follow-up campaign writes to the SAME person several times (first
-- follow-up, then every next one), each as its own workflow run. The
-- (campaign_id, lead_email) reservation would collapse the second one into a
-- `200 duplicate` and nothing would leave. So follow-up rows reserve on their
-- own key, (run_id, lead_email): a gateway retry forwards the same x-run-id
-- (duplicate, as before), the next follow-up is a new run (a new row).
--
-- The three arbiters are made DISJOINT on `follow_up`, so an insert can only
-- ever conflict on the one it names in ON CONFLICT.
ALTER TABLE "instantly_campaigns" ADD COLUMN IF NOT EXISTS "follow_up" boolean NOT NULL DEFAULT false;
--> statement-breakpoint
DROP INDEX IF EXISTS "instantly_campaigns_campaign_lead_idx";
--> statement-breakpoint
CREATE UNIQUE INDEX "instantly_campaigns_campaign_lead_idx"
  ON "instantly_campaigns" ("campaign_id", "lead_email")
  WHERE "follow_up" = false;
--> statement-breakpoint
DROP INDEX IF EXISTS "instantly_campaigns_platform_run_lead_active_idx";
--> statement-breakpoint
CREATE UNIQUE INDEX "instantly_campaigns_platform_run_lead_active_idx"
  ON "instantly_campaigns" ("run_id", "lead_email")
  WHERE "campaign_id" IS NULL AND "status" = 'active' AND "follow_up" = false;
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "instantly_campaigns_follow_up_run_lead_idx"
  ON "instantly_campaigns" ("run_id", "lead_email")
  WHERE "follow_up" = true;
