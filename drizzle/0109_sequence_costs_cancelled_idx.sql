-- Serve the restore of cut follow-ups (`loadCutFollowupCandidates`,
-- src/lib/restore-cut-followups.ts) its "has a cancelled hold" sequences from a
-- small partial index instead of the whole 470k-row sequence_costs heap
-- (DASH-SPEED-1110). With the query rewritten to read each sequence's last sent
-- step and cut steps per sequence (LATERAL), the awaiting-verdict worker's pass
-- went from 1432 s (prod 2026-11-09, every 6 h) to 0.7-1.1 s on the loaded box;
-- this index is ~0.6-0.8 s of the remaining heap scan. Same rows (both passes
-- diffed byte-equal on prod).
--
-- The predicate is the query's literal (`status = 'cancelled'`): change one,
-- change both (guard in tests/unit/migrations.test.ts).
--
-- Pre-built CONCURRENTLY out of band on prod (2026-11-09, 0.95 s, 2.8 MB), so
-- this IF NOT EXISTS is a no-op there; the boot migrator runs in a transaction,
-- which forbids CONCURRENTLY. Any new prod-scale environment pre-builds it
-- CONCURRENTLY before deploy. Hand-written; declared in schema.ts with `.where`.
CREATE INDEX IF NOT EXISTS sequence_costs_cancelled_campaign_idx
  ON sequence_costs (instantly_campaign_id, step)
  WHERE status = 'cancelled';
