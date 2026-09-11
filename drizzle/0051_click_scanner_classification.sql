-- A corporate link scanner is not a website visit.
--
-- Every GET on the `/c/` redirect was promoted straight into an
-- `email_link_clicked` silver event, and `stop-on-click` pauses a lead's
-- sequence on one of those whenever the campaign's funnel opens on a website
-- visit. Corporate mail security (Microsoft Safe Links and friends) fetches
-- every URL in an inbound message before the human ever sees it, so on the
-- self-send transport the "clicks" were largely machines: 131 of 337 smtp leads
-- on one brand (39%, against 95/2049 on the Instantly transport), all of whose
-- sequences were paused after a single email, and whose customer was billed for
-- website visits that never happened.
--
-- The fix classifies a hit BEFORE it reaches silver. Three columns carry that:
--
--   classification        'human' | 'scanner' | NULL (not decided yet)
--   classification_reason why it was called a scanner (never why it was human)
--   promoted_at           when the human hit reached silver
--
-- ⚠️ NULL is a REAL state, not "unknown". The decisive signal — the same
-- (campaign, lead) fetching the OPT-OUT link seconds either side of the click,
-- which a human never does and a scanner always does — can arrive AFTER the
-- click. So the route no longer promotes: it records the hit undecided and a
-- sweep promotes it once the pairing window has closed. Promote-then-retract
-- was rejected: stop-on-click would already have paused the sequence, and that
-- pause is exactly the harm.
ALTER TABLE "tracking_hits_raw" ADD COLUMN IF NOT EXISTS "classification" text;
ALTER TABLE "tracking_hits_raw" ADD COLUMN IF NOT EXISTS "classification_reason" text;
ALTER TABLE "tracking_hits_raw" ADD COLUMN IF NOT EXISTS "promoted_at" timestamp;

-- The real client IP, not Caddy's.
--
-- `req.ip` was read without trusting the proxy, so all 771 existing hits carry
-- `::ffff:172.18.0.27` — the reverse proxy's own docker address. A scanner's IP
-- ranges are stable, which makes this the strongest fingerprint available, and
-- it was being thrown away on every hit.
ALTER TABLE "tracking_hits_raw" ADD COLUMN IF NOT EXISTS "client_ip" text;

-- Existing click hits are marked `legacy`, NOT re-decided here.
--
-- They were all promoted synchronously by the old route, so leaving them NULL
-- would make the new sweep treat every one of them as pending and re-promote a
-- click that is already in silver. `legacy` is the backfill's candidate set:
-- `POST /internal/audit/click-scanner-backfill` re-classifies them with the same
-- pure rule the live path uses and removes the scanner-shaped ones from silver.
UPDATE "tracking_hits_raw"
SET "classification" = 'legacy', "promoted_at" = "received_at"
WHERE "kind" = 'click' AND "classification" IS NULL;

-- An unsubscribe hit is never promoted by the click sweep, but marking it keeps
-- `classification IS NULL` meaning exactly one thing: a click awaiting decision.
UPDATE "tracking_hits_raw"
SET "classification" = 'legacy'
WHERE "kind" <> 'click' AND "classification" IS NULL;

-- The sweep's candidate query, and the paired-unsubscribe lookup it runs per
-- hit. Hand-written because drizzle-kit does not track partial indexes (same
-- convention as `instantly_events_one_shot_dedupe_idx`) — do NOT drop either on
-- a `db:generate` diff.
CREATE INDEX IF NOT EXISTS "tracking_hits_raw_pending_click_idx"
  ON "tracking_hits_raw" ("received_at")
  WHERE "kind" = 'click' AND "classification" IS NULL;

CREATE INDEX IF NOT EXISTS "tracking_hits_raw_lead_kind_idx"
  ON "tracking_hits_raw" ("instantly_campaign_id", "kind", "received_at");
