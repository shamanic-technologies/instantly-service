-- Covering index for the gold stats aggregates (analytics.ts / analytics-public.ts).
--
-- The filtered /orgs/stats path joins instantly_events -> instantly_campaigns on
-- campaign_id, then filters and counts by event_type, lead_email and step. With
-- only the single-column instantly_events_campaign_id_idx, the per-campaign probe
-- still heap-fetches each row (the heap is bloated by the unread raw_payload
-- jsonb, ~127MB). This composite lets Postgres do an index-only scan per matched
-- campaign instead (validated on a prod-data branch: nested-loop index-only scan,
-- no heap fetch).
--
-- It deliberately does NOT cover the no-filter /public/stats path: with no
-- WHERE filter the planner seq-scans the whole table regardless, so that path is
-- handled by the in-memory TTL cache (src/lib/stats-cache.ts) instead.
--
-- 85k rows builds in well under a second; the non-CONCURRENT build's brief write
-- lock at boot is acceptable (reconcile/webhook writes are not in the hot path
-- during a deploy). IF NOT EXISTS keeps the migration idempotent on replay.

CREATE INDEX IF NOT EXISTS instantly_events_stats_covering_idx
  ON instantly_events (campaign_id, event_type, lead_email, step);
