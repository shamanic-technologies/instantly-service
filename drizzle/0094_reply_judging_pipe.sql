-- LEDGER-RESIDUAL-1025: the reply-judging run is one per (org, UTC day, PIPE)
-- = the judged sequence's campaign x workflow, so a day whose judgments kept no
-- positive reply is normal loss of that pipe (runs-service pools a pipe's
-- produced-none on its outputs), never spend with no pipe ("undeclared").
-- '' = no pipe recorded (rows from before this migration, sequences sent with
-- no campaign or workflow). Hand-written: do NOT drop on a db:generate diff.
ALTER TABLE reply_judging_runs ADD COLUMN IF NOT EXISTS campaign_id text NOT NULL DEFAULT '';
ALTER TABLE reply_judging_runs ADD COLUMN IF NOT EXISTS workflow_slug text NOT NULL DEFAULT '';
ALTER TABLE reply_judging_runs DROP CONSTRAINT IF EXISTS reply_judging_runs_pkey;
ALTER TABLE reply_judging_runs ADD PRIMARY KEY (org_id, day, campaign_id, workflow_slug);
