-- GIN index on instantly_campaigns.metadata for fast lookup of
-- redispatchHistory.from entries during webhook handling.
--
-- Webhook handler (src/routes/webhooks.ts) receives Instantly events with
-- campaign_id = the CURRENT Instantly campaign UUID. The retry-stuck worker
-- overwrites `instantly_campaign_id` on the row when it re-dispatches a stuck
-- lead onto a fresh account — the OLD id then only survives in
-- `metadata.redispatchHistory[*].from`. Delayed events (e.g. open tracking
-- pixels firing days after send) from those old Instantly campaigns arrive
-- with the now-stale id and miss the equality lookup → 401.
--
-- The fix uses a JSONB containment query against metadata; this GIN index
-- backs that path. jsonb_path_ops is smaller than the default jsonb_ops and
-- targeted at `@>` and `@?` operators (which is exactly the access pattern).

CREATE INDEX IF NOT EXISTS instantly_campaigns_metadata_gin_idx
  ON instantly_campaigns
  USING gin (metadata jsonb_path_ops);
