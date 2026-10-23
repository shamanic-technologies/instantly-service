-- COST-ACCOUNTING-1023: a sequence whose reply is judged POSITIVE is declared
-- to runs-service's ledger as one `conversation` (Positive reply) of the
-- person, on a run of ours (lib/positive-reply-outcomes.ts). The run id is
-- persisted BEFORE the declaration so a retry reuses the same run; the
-- declaration mark ends the row: declared | run_gone | no_lead_id | refused.
-- Nullable columns, no table rewrite.
ALTER TABLE instantly_campaigns ADD COLUMN IF NOT EXISTS conversation_run_id text;
ALTER TABLE instantly_campaigns ADD COLUMN IF NOT EXISTS conversation_declared_at timestamp with time zone;
ALTER TABLE instantly_campaigns ADD COLUMN IF NOT EXISTS conversation_declaration text;
-- The sweep's pending scan, every minute (hand-written partial index; ~80 rows).
CREATE INDEX IF NOT EXISTS instantly_campaigns_conversation_pending_idx
  ON instantly_campaigns (updated_at DESC)
  WHERE reply_classification = 'positive' AND conversation_declared_at IS NULL;
