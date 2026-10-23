-- COST-ACCOUNTING-1023: every real email we send is declared to runs-service's
-- ledger as one `message_sent` occurrence of its step run (lib/send-outcomes.ts).
-- The declaration is async (a sweep), so each actualized step hold records
-- whether its run's outcomes reached the ledger: NULL = still to declare.
-- `outcomes_declaration` names how it ended: declared | run_gone | no_lead_id |
-- ambiguous_lead | refused. Nullable columns, no table rewrite.
ALTER TABLE sequence_costs ADD COLUMN IF NOT EXISTS outcomes_declared_at timestamp with time zone;
ALTER TABLE sequence_costs ADD COLUMN IF NOT EXISTS outcomes_declaration text;
-- The sweep's pending scan (newest send first). Hand-written partial index.
CREATE INDEX IF NOT EXISTS sequence_costs_outcomes_pending_idx
  ON sequence_costs (updated_at DESC)
  WHERE status = 'actual' AND outcomes_declared_at IS NULL;
