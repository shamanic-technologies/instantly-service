-- Serve the per-mailbox reads of bronze `instantly_emails_raw` from an index.
--
-- `loadKnownSends` (src/lib/self-send/imap-poller.ts) and `loadMailboxLeads`
-- (src/lib/self-send/orphan-reply-sweep.ts) filter on
-- `payload->>'eaccount' = <mailbox>`. With no index on that key every call
-- seq-scanned the whole table (135k rows, 270 MB heap, ~0.9 s of 3 parallel
-- workers on prod 2026-10-08), and `loadKnownSends` runs once per address on
-- every IMAP poll and on every inbox-watcher arrival: 1.2M seq scans, 57B
-- tuples read lifetime, the top Postgres consumer of the service. ~550 rows per
-- mailbox, so the index turns each call into a few hundred heap fetches.
--
-- No partial predicate on `ue_type`: 99.6% of rows are `ue_type = '1'`, so it
-- would filter nothing.
--
-- Expression index, hand-written (drizzle's schema builder has no expression
-- form) — do NOT drop it on a `db:generate` diff. `IF NOT EXISTS` so a re-run is
-- a no-op. Non-concurrent (the boot migrator runs in a transaction): the build
-- reads the table once, ~2 s on prod; writers to this bronze table (Unibox
-- backfill, reconcile poll) wait that long, readers do not. ANALYZE gathers the
-- expression's statistics so the planner costs the new path right away.
CREATE INDEX IF NOT EXISTS instantly_emails_raw_eaccount_idx
  ON instantly_emails_raw ((payload->>'eaccount'));
--> statement-breakpoint
ANALYZE instantly_emails_raw;
