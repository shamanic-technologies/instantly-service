-- LEDGER-HISTORY-1024: a click on a link in one of our emails is a website
-- visit (CLICK-FIRES-WEBSITE-VISIT-1015); it is declared to runs-service's
-- ledger as one `website_visit` of the person (lib/website-visit-outcomes.ts),
-- dated when the click happened, on a spend-free run of ours per (org, UTC day
-- of the visit). Same marks as the conversation columns (0091). Nullable
-- columns, no table rewrite.
ALTER TABLE instantly_campaigns ADD COLUMN IF NOT EXISTS website_visit_run_id text;
ALTER TABLE instantly_campaigns ADD COLUMN IF NOT EXISTS website_visit_declared_at timestamp with time zone;
ALTER TABLE instantly_campaigns ADD COLUMN IF NOT EXISTS website_visit_declaration text;
-- One spend-free anchor run per (task, org, UTC day): the run a dated outcome
-- of that day is declared on. Never given a cost or a child (runs-service
-- refuses a cost on a run holding facts dated before its start).
CREATE TABLE IF NOT EXISTS outcome_anchor_runs (
  task_name text NOT NULL,
  org_id text NOT NULL,
  day date NOT NULL,
  run_id text NOT NULL,
  created_at timestamp with time zone NOT NULL DEFAULT now(),
  PRIMARY KEY (task_name, org_id, day)
);
