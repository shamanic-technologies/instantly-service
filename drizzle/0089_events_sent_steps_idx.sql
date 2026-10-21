-- Serve the status read's per-row `sentCount` (POST /orgs/status,
-- `sentCountLateral` in src/routes/status.ts) as an INDEX-ONLY scan.
--
-- The lateral counts `COUNT(DISTINCT step)` of a row's `email_sent` events. It
-- was served by `instantly_events_one_shot_dedupe_idx`, whose key carries
-- `COALESCE(step, -1)` and not `step`, so every probe fetched the ~1 kB-per-row
-- heap (510 MB with `raw_payload`). Cold, the biggest brand (distribute.you,
-- 10,401 addresses, 8,867 gold rows) read ~6,800 heap pages from disk: the
-- brand statement took 5.0 s and 11.1 s on the loaded box (prod 2026-10-22),
-- ~16 s end to end. With this partial index it is an Index Only Scan
-- (0.67 s cold, 0.4-0.5 s warm), same rows and same figures (md5 of the full
-- result identical before/after).
--
-- The predicate is the lateral's literal (`event_type = 'email_sent'`): change
-- one, change both (guard in tests/unit/migrations.test.ts).
--
-- Pre-built CONCURRENTLY out of band on prod (2026-10-22, 1.9 s, 16 MB), so
-- this IF NOT EXISTS is a no-op there; the boot migrator runs in a transaction,
-- which forbids CONCURRENTLY. Any new prod-scale environment pre-builds it
-- CONCURRENTLY before deploy. Hand-written; declared in schema.ts with `.where`.
CREATE INDEX IF NOT EXISTS instantly_events_sent_steps_idx
  ON instantly_events (campaign_id, lead_email, step)
  WHERE event_type = 'email_sent';
