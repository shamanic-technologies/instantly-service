-- ONE row per email, every typology, whichever pipe carried it.
--
-- An outbound email lives in four bronze tables depending on what it was for
-- (Instantly outreach in instantly_emails_raw, our own outreach in
-- smtp_dispatch_raw, warmup in warmup_dispatches, seeds in
-- seed_placement_dispatches) and an inbound one in three. "Everything this
-- mailbox sent last week" was therefore a seven-table UNION nobody wrote.
--
-- This is a PROJECTION of those sources, keyed on the bronze row it came from
-- so a re-run is a no-op. Bronze stays the record; the body is not copied
-- (body_ref points back). State facts — sent / bounced / replied / clicked —
-- stay in instantly_events, joined through (instantly_campaign_id, step).
CREATE TABLE IF NOT EXISTS "messages" (
  "id" text PRIMARY KEY NOT NULL,
  "source_table" text NOT NULL,
  "source_row_id" text NOT NULL,
  "message_id" text,
  "direction" text NOT NULL,
  "kind" text NOT NULL,
  "transport" text NOT NULL,
  "account_email" text NOT NULL,
  "mailbox_login" text,
  "counterparty" text,
  "subject" text,
  "instantly_campaign_id" text,
  "step" integer,
  "thread_id" text NOT NULL,
  "context_ref" text,
  "org_id" text,
  "campaign_id" text,
  "outcome" text NOT NULL,
  "placement" text,
  "spf_pass" boolean,
  "dkim_pass" boolean,
  "dmarc_pass" boolean,
  "occurred_at" timestamp with time zone NOT NULL,
  "synced_at" timestamp with time zone DEFAULT now() NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS "messages_source_idx" ON "messages" ("source_table", "source_row_id");
CREATE INDEX IF NOT EXISTS "messages_account_occurred_idx" ON "messages" ("account_email", "occurred_at");
CREATE INDEX IF NOT EXISTS "messages_mailbox_occurred_idx" ON "messages" ("mailbox_login", "occurred_at");
CREATE INDEX IF NOT EXISTS "messages_thread_idx" ON "messages" ("thread_id");
CREATE INDEX IF NOT EXISTS "messages_message_id_idx" ON "messages" ("message_id");
CREATE INDEX IF NOT EXISTS "messages_kind_occurred_idx" ON "messages" ("kind", "occurred_at");
