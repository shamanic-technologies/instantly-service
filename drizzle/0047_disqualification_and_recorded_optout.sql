-- Two additive facts a consumer of the status read could not previously see.
--
-- 1. WHICH negative reply this was. The reply-kind vocabulary already draws the
--    line the sales canon draws -- `lead_not_interested` is a "no" about the
--    MOMENT (recyclable), while `lead_wrong_person` / `lead_changed_job` are
--    facts about the PERSON (permanent) -- but only the coarse
--    positive/negative/neutral projection survived to the read side, so every
--    negative arrived downstream looking identical. `reply_kind` on the gold
--    status row carries the finer value; the coarse column is untouched, so a
--    consumer that reads only `reply_classification` sees no change at all.
--
-- 2. That a person asked us to stop through a channel that is not the
--    unsubscribe link -- an SMS, a phone call, a forwarded thread, a
--    conversation. Recorded as an explicit human statement (never inferred),
--    withdrawable, and carrying who said it, when, and how.

ALTER TABLE instantly_lead_status_current
  ADD COLUMN IF NOT EXISTS reply_kind text;

-- Backfill from silver, with the SAME precedence the refresh uses: a human
-- statement wins outright (mirroring the reply_classification_source='manual'
-- pin), otherwise the latest automatic reply-kind event. A withdrawn statement
-- is skipped -- nobody stands behind it.
--
-- Set-based on purpose: this runs at boot, before the port binds, so a
-- per-row correlated lookup over the event log is not an option. The inner
-- scan is bounded by `instantly_events_event_type_idx` (reply-kind events are a
-- small fraction of the log -- the bulk is email_sent/opened/clicked).
WITH latest_kind AS (
  SELECT DISTINCT ON (e.campaign_id, e.lead_email)
    e.campaign_id,
    e.lead_email,
    e.event_type
  FROM instantly_events e
  WHERE e.event_type IN (
      'lead_interested', 'lead_referral', 'lead_info_requested', 'lead_meeting_requested',
      'lead_not_interested', 'lead_wrong_person', 'lead_changed_job',
      'lead_neutral', 'lead_out_of_office', 'auto_reply_received'
    )
    AND e.withdrawn_at IS NULL
  ORDER BY e.campaign_id, e.lead_email, (e.source = 'manual') DESC, e.timestamp DESC, e.created_at DESC, e.id DESC
)
UPDATE instantly_lead_status_current s
SET reply_kind = k.event_type
FROM latest_kind k
WHERE k.campaign_id = s.instantly_campaign_id
  AND k.lead_email = s.lead_email;
--> statement-breakpoint

-- Bronze: a person told a human being to stop contacting them, through a
-- channel that produced no unsubscribe click.
--
-- Append-only, exactly like the manual reply qualifications beside it: this is
-- a consent record, so what was stated, by whom, when and through which channel
-- has to stay recoverable forever. Keyed on (org, lead_email) and NOT on a
-- campaign -- "stop contacting me" is a statement about the person, and honouring
-- it in one campaign while another keeps sending is the outcome the law cares
-- about.
CREATE TABLE IF NOT EXISTS "instantly_lead_optouts_raw" (
  "id" text PRIMARY KEY NOT NULL,
  "org_id" text NOT NULL,
  "lead_email" text NOT NULL,
  -- How they told us. Required: an opt-out with no channel is an assertion
  -- nobody can audit, and this vocabulary is what makes the record a consent
  -- record rather than a flag.
  "channel" text NOT NULL,
  "stated_by" text NOT NULL,
  "notes" text,
  "payload" jsonb NOT NULL,
  "stated_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint

CREATE INDEX IF NOT EXISTS "instantly_lead_optouts_raw_org_email_idx"
  ON "instantly_lead_optouts_raw" ("org_id", "lead_email");
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "instantly_lead_optouts_raw_stated_at_idx"
  ON "instantly_lead_optouts_raw" ("stated_at");
--> statement-breakpoint

-- Bronze: taking a recorded opt-out back -- a staff member who recorded it on
-- the wrong person, or a prospect who came back and asked to hear from us again.
--
-- An APPEND, never a delete: the statement row stays byte-identical and this row
-- records that it no longer stands. Keyed on the statement id (unique) so a
-- later re-statement is unaffected and a second withdrawal of the same statement
-- is a no-op at the index rather than a read-then-write race.
CREATE TABLE IF NOT EXISTS "instantly_lead_optout_withdrawals" (
  "id" text PRIMARY KEY NOT NULL,
  "optout_id" text NOT NULL,
  "org_id" text NOT NULL,
  "lead_email" text NOT NULL,
  "withdrawn_by" text NOT NULL,
  "notes" text,
  "withdrawn_at" timestamp DEFAULT now() NOT NULL,
  CONSTRAINT "instantly_lead_optout_withdrawals_optout_id_unique" UNIQUE("optout_id")
);
--> statement-breakpoint

CREATE INDEX IF NOT EXISTS "instantly_lead_optout_withdrawals_org_email_idx"
  ON "instantly_lead_optout_withdrawals" ("org_id", "lead_email");
