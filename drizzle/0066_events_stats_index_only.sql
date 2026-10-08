-- Serve the gold stats aggregates (`src/routes/analytics.ts`) from indexes
-- instead of the 308 MB `instantly_events` heap.
--
-- 1. `instantly_events_bounced_idx`: partial, bounces only. Every stats
--    statement (`queryGroupedStats` both branches, `queryStats`,
--    `computeStepStats`) LEFT JOINs two DISTINCT bounce sets (`BOUNCE_JOINS`)
--    that read EVERY fleet bounce through `instantly_events_event_type_idx` plus
--    ~2,700 heap pages each, whatever the scope: 2 x 2,685 buffers on a 54-lead
--    campaign read whose own work is ~480 buffers (prod 2026-10-08). The
--    partial index carries exactly the joined columns, so both sets become an
--    index-only scan of a ~340 kB index. The predicate is the literal the
--    fragment filters on (`event_type = 'email_bounced'`); change one, change
--    both (guard in tests/unit/migrations.test.ts).
--
-- 2. `instantly_events_stats_covering_idx` gains INCLUDE (account_email,
--    "timestamp"). Same key columns, so every lookup it served is served the
--    same way. The events side of a stats statement reads campaign_id,
--    event_type, lead_email, step, account_email (internalExclusionClause) and,
--    for groupBy=day, timestamp: with the two INCLUDEd the whole read is
--    index-only (~39 MB) instead of a parallel seq scan of the heap
--    (`raw_payload` makes rows ~1 kB). Measured on a copy of prod: the largest
--    campaign's per-campaign read 317 -> 170 ms and its `instantly_campaigns`
--    side moves from a parallel seq scan to a bitmap scan; the fleet
--    `groupBy=workflowSlug` read's events scan 421 -> 41 ms per worker.
--    Built under a temporary name, then swapped in, so the table's
--    ACCESS EXCLUSIVE lock (DROP INDEX) is held only between the swap and the
--    commit, not across the build.
--
-- 3. Index-only scans skip the heap only on all-visible pages, and the table
--    had never been vacuumed (47% of pages all-visible on 2026-10-08): the
--    default insert-triggered autovacuum waits for 20% of the table (~60k
--    inserts). 2% keeps the visibility map current (a vacuum skips the pages
--    that are already all-visible). Storage parameter only, no rewrite.
--
-- Hand-written (drizzle's builder has no INCLUDE form; partial index declared
-- in schema.ts with `.where`) — do NOT drop either on a `db:generate` diff.
-- Non-concurrent (the boot migrator runs in a transaction): each build reads
-- the heap once, a few seconds on prod; writers to instantly_events wait that
-- long, readers do not until the swap.
CREATE INDEX IF NOT EXISTS instantly_events_bounced_idx
  ON instantly_events (campaign_id, lead_email, step)
  WHERE event_type = 'email_bounced';
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS instantly_events_stats_covering_incl_idx
  ON instantly_events (campaign_id, event_type, lead_email, step)
  INCLUDE (account_email, "timestamp");
--> statement-breakpoint
ALTER TABLE instantly_events SET (autovacuum_vacuum_insert_scale_factor = 0.02);
--> statement-breakpoint
DROP INDEX IF EXISTS instantly_events_stats_covering_idx;
--> statement-breakpoint
ALTER INDEX instantly_events_stats_covering_incl_idx RENAME TO instantly_events_stats_covering_idx;
