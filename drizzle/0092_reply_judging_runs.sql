-- COST-ACCOUNTING-1023: one `reply-judging` run of ours per (org, UTC day)
-- (lib/reply-judging-run.ts). Every reply classifier call of that org that day
-- is made ON BEHALF of it (chat-service `x-run-id`: its platform run becomes a
-- child), and that day's positive replies declare their `conversation` on it,
-- so the judging spend rolls into Positive reply (non-positive judgments ride
-- the positive ones: normal loss).
CREATE TABLE IF NOT EXISTS reply_judging_runs (
  org_id text NOT NULL,
  day date NOT NULL,
  run_id text NOT NULL,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  PRIMARY KEY (org_id, day)
);
