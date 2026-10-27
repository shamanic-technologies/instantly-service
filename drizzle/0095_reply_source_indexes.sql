-- Serve the per-reply projection's two source scans (`upsertReplies` in
-- src/lib/reply-verdicts.ts, every 2 minutes) from partial indexes
-- (BOX-DB-LOAD-1027).
--
-- The projection reads the inbound Unibox mail (`payload->>'ue_type' = '2'`, 433
-- of 136k rows, 318 MB of jsonb) and the IMAP replies (`kind IN ('reply',
-- 'auto_reply')`, 280 of 926k rows). With no index on either predicate it
-- read both tables whole on every tick: 12.9 s a call, 149 DB minutes a day
-- (pg_stat_statements 2026-10-26/27). With these it reads ~700 rows by index:
-- 0.14-0.37 s on the loaded box (EXPLAIN ANALYZE, 2026-10-27), same rows.
--
-- The predicates are the projection's literals: change one, change both
-- (guard in tests/unit/migrations.test.ts).
--
-- Pre-built CONCURRENTLY out of band on prod (2026-10-27, 14 s and 13 s, 48 kB
-- and 40 kB), so these IF NOT EXISTS are no-ops there; the boot migrator runs
-- in a transaction, which forbids CONCURRENTLY. Any new prod-scale environment
-- pre-builds them CONCURRENTLY before deploy. Hand-written; declared in
-- schema.ts with `.where`.
CREATE INDEX IF NOT EXISTS instantly_emails_raw_inbound_idx
  ON instantly_emails_raw (instantly_campaign_id)
  WHERE (payload->>'ue_type') = '2';
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS imap_messages_raw_replies_idx
  ON imap_messages_raw (instantly_campaign_id)
  WHERE kind IN ('reply', 'auto_reply');
