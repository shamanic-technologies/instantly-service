-- DB-LOAD-1014: serve the not-a-prospect gate without a full scan of gold.
--
-- `queryNotAProspect` (src/lib/not-a-prospect.ts) matches on
-- `lower(lead_email)` and `reply_kind IN (<not-a-prospect kinds>)`; no index
-- served it, so EVERY `POST /orgs/status` scoped read (and every send) seq-
-- scanned `instantly_lead_status_current` (~62k rows, ~25 ms): the busiest
-- instantly statement on the shared Postgres (23 of 40 samples, 2026-10-14).
-- Partial, because a handful of rows carry those kinds: the index stays tiny.
-- Its predicate MUST equal NOT_A_PROSPECT_REPLY_KINDS (src/lib/reply-kind.ts),
-- inlined as literals in the query (guard: tests/unit/not-a-prospect-index.test.ts).
-- Hand-written expression index, do NOT drop it on a `db:generate` diff.
-- Non-concurrent is safe: sub-second build on this table.
CREATE INDEX IF NOT EXISTS instantly_lead_status_current_not_a_prospect_idx
  ON instantly_lead_status_current (lower(lead_email))
  WHERE reply_kind IN ('lead_already_customer', 'lead_is_client');
