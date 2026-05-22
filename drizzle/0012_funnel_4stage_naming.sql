-- 4-stage funnel naming refactor.
--
-- Aligns silver `delivery_status` writers with the 4-stage funnel
-- (contacted → sent → delivered → terminal). Drops the legacy
-- `instantly_analytics_snapshots` table (superseded by `instantly_analytics_raw`).
--
-- Stage semantics:
--   1. (pre-Instantly queue — not modeled in this service)
--   2. contacted = lead pushed to Instantly (POST /send success)
--   3. sent      = Instantly dispatched at least one email (webhook email_sent)
--   4. delivered = derived in queries (sent AND NOT bounced); NEVER stored
--
-- Previously two writers both wrote `delivery_status='sent'`:
--   (a) POST /send success  → stage 2 (collision)
--   (b) webhook email_sent  → stage 3 (collision)
-- Webhook `campaign_completed` also wrote `delivery_status='delivered'`, which
-- conflated "sequence finished" with stage 4. Both writers are now corrected
-- and existing rows are backfilled to disambiguate stage 2 from stage 3 using
-- the silver event log (`instantly_events`) as ground truth.

-- ─── Step 1: backfill 'delivered' rows (set by campaign_completed) ────────────
-- Rows with delivery_status='delivered' were set by the dropped
-- campaign_completed → 'delivered' mapping. Restore actual stage by checking
-- whether an email_sent event exists for the (campaign, lead) pair.
UPDATE "instantly_campaigns" c
SET delivery_status = CASE
  WHEN EXISTS (
    SELECT 1 FROM "instantly_events" e
    WHERE e.campaign_id = c.instantly_campaign_id
      AND e.lead_email = c.lead_email
      AND e.event_type = 'email_sent'
  ) THEN 'sent'
  ELSE 'contacted'
END,
updated_at = NOW()
WHERE delivery_status = 'delivered';
--> statement-breakpoint

-- ─── Step 2: disambiguate 'sent' rows ────────────────────────────────────────
-- Rows with delivery_status='sent' set by POST /send (pre-fix) but with no
-- email_sent event are actually stage 2 (contacted). Flip them.
UPDATE "instantly_campaigns" c
SET delivery_status = 'contacted',
    updated_at = NOW()
WHERE delivery_status = 'sent'
  AND NOT EXISTS (
    SELECT 1 FROM "instantly_events" e
    WHERE e.campaign_id = c.instantly_campaign_id
      AND e.lead_email = c.lead_email
      AND e.event_type = 'email_sent'
  );
--> statement-breakpoint

-- ─── Step 3: drop dead 'pending' default ─────────────────────────────────────
-- 'pending' was the historic default but POST /send always overwrites
-- immediately. Switch default to 'contacted' so any row that exists in
-- instantly_campaigns is stage 2 by definition.
ALTER TABLE "instantly_campaigns" ALTER COLUMN "delivery_status" SET DEFAULT 'contacted';
--> statement-breakpoint

-- Backfill any lingering 'pending' rows (none expected) to 'contacted'.
UPDATE "instantly_campaigns"
SET delivery_status = 'contacted',
    updated_at = NOW()
WHERE delivery_status = 'pending';
--> statement-breakpoint

-- ─── Step 4: drop legacy analytics snapshots table ───────────────────────────
-- Superseded by `instantly_analytics_raw` (bronze) in migration 0011.
DROP TABLE IF EXISTS "instantly_analytics_snapshots" CASCADE;
