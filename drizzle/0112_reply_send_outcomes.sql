-- REPLY-SENT-UNIT-1118: an answer sent in a thread (lib/reply-to-lead, the door
-- `POST /orgs/replies`) is an email we sent, exactly like a sequence step, and
-- was declared nowhere: it takes no `sequence_costs` hold, so the send-outcomes
-- sweep never saw it. One row per answer that went out: the record the sweep in
-- lib/reply-send-outcomes declares to runs-service's ledger as the lead's
-- `message_sent`, on the run that asked for it (`run_id`, the caller's
-- `x-run-id`; NULL = nobody named a run, never declared under an invented one).
CREATE TABLE IF NOT EXISTS reply_send_outcomes (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  org_id text NOT NULL,
  instantly_campaign_id text NOT NULL,
  lead_email text NOT NULL,
  message_id text NOT NULL,
  transport text NOT NULL,
  sent_by text NOT NULL,
  run_id text,
  -- UTC without a zone, whatever the session's time zone (the sweep reads it as an ISO instant).
  sent_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
  outcomes_declaration text,
  outcomes_declared_at timestamp,
  outcomes_error text,
  created_at timestamp NOT NULL DEFAULT now()
);
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS reply_send_outcomes_message_idx ON reply_send_outcomes (instantly_campaign_id, message_id);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS reply_send_outcomes_pending_idx ON reply_send_outcomes (sent_at) WHERE outcomes_declaration IS NULL AND run_id IS NOT NULL;
