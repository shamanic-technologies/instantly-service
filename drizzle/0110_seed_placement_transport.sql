-- POSTMARK-PLACEMENT-1117: a seed can leave through Postmark as well as through
-- one of our mailboxes, and a seed can belong to a one-off COMPARISON test
-- instead of the fleet's weekly measurement.
--
-- transport: how the seed left. 'smtp' = one of our mailboxes (every row before
--   this migration), 'postmark' = the Postmark API from a sending domain we own.
-- purpose: 'fleet' = the weekly test the lifecycle gate, the daily series and
--   the cadence read (every row before this migration); 'comparison' = a one-off
--   test (same receivers, same read) that NEVER reaches silver, the daily series,
--   the alerts or the cadence.
-- gmail_category: the Gmail tab a seed found in INBOX sits under (primary,
--   promotions, social, updates, forums), read with X-GM-RAW. Null on a
--   Microsoft receiver, on a spam verdict, and on every observation before this.
ALTER TABLE seed_placement_dispatches ADD COLUMN IF NOT EXISTS transport text NOT NULL DEFAULT 'smtp';
ALTER TABLE seed_placement_dispatches ADD COLUMN IF NOT EXISTS purpose text NOT NULL DEFAULT 'fleet';
ALTER TABLE seed_placement_observations ADD COLUMN IF NOT EXISTS gmail_category text;
