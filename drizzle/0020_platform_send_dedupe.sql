-- Platform-send (campaign_id = NULL) idempotency under retry/concurrency.
-- DIS-148 follow-up — the original reservation guard missed the platform case.
--
-- POST /orgs/send reserves the (campaign_id, lead_email) pair on
-- `instantly_campaigns_campaign_lead_idx` so an email-gateway timeout-retry
-- collides on the unique index and returns an idempotent 200 instead of
-- creating a second Instantly campaign. But PLATFORM sends carry
-- campaign_id = NULL, and Postgres treats every NULL as DISTINCT in a unique
-- index, so the reservation NEVER collided — every timeout-retry created a
-- fresh Instantly campaign (duplicate cold emails to the same person).
--
-- Incident 2026-06-27: outlets-service run 27acff0b fired 80 platform sends;
-- email-gateway's ~20s timeout retried each one → 160 active campaigns for 80
-- distinct leads (every journalist queued for the 3-step sequence TWICE).
--
-- Fix: a partial unique index keyed on (run_id, lead_email) for the platform
-- (campaign_id IS NULL) case. The retry forwards the SAME x-run-id, so
-- (run_id, lead_email) is the stable idempotency key (verified on the incident
-- data: all 160 rows shared run_id 27acff0b). send.ts targets this index in the
-- reservation upsert when campaign_id is null.
--
-- Scoped to status = 'active' so the index covers exactly the live
-- reservation / in-flight window (which is the retry window) and does NOT
-- collide with already-paused/completed historical duplicates. This lets it
-- build cleanly on prod, where each (run_id, lead_email) now has at most one
-- ACTIVE null-campaign row.
--
-- Precondition (verified on prod before ship): zero (run_id, lead_email) pairs
-- with >1 ACTIVE null-campaign row. If a non-prod env still has such pairs,
-- pause the redundant campaigns first (npm run heal:dupes) — a still-duplicated
-- ACTIVE pair would make this CREATE UNIQUE INDEX fail.

CREATE UNIQUE INDEX IF NOT EXISTS "instantly_campaigns_platform_run_lead_active_idx"
  ON "instantly_campaigns" ("run_id", "lead_email")
  WHERE "campaign_id" IS NULL AND "status" = 'active';
