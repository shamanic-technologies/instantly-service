-- OUTREACH-3WAYS-1121: a step `POST /orgs/send` queues is declared to runs-service's
-- ledger as the person's produced `email_queued`, on the step's run (the one that
-- later carries its `message_sent`), at the queue decision, so a reader of the
-- ledger (billing's "at most N new people a day") sees the person contacted hours
-- before the throttled mailbox sends. `queued_declaration`: 'pending' = the send
-- path asked for it (only rows it writes; NULL = never asked, e.g. restores and
-- rows before this migration: no backfill), then how it ended ('declared',
-- 'run_gone', 'refused', 'no_lead_id'). The sweep in lib/queued-outcomes retries
-- 'pending' rows the send path could not declare at once.
ALTER TABLE sequence_costs ADD COLUMN IF NOT EXISTS queued_declaration text;
--> statement-breakpoint
ALTER TABLE sequence_costs ADD COLUMN IF NOT EXISTS queued_declared_at timestamp with time zone;
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS sequence_costs_queued_pending_idx ON sequence_costs (created_at) WHERE queued_declaration = 'pending';
