-- OOO-RETURN-DATE-1101: an out-of-office that states a return date holds our next email until the person is back.
-- One row per stored autoresponder (`imap_messages_raw` kind 'auto_reply'), judged ONCE through chat-service:
-- `outcome` = 'return_date' (a usable date: `first_day_back` + `hold_until`), 'no_date', 'stale' (the date is
-- already past at the message), 'unreadable' (the model answered nothing we can read). The dispatch reads
-- `hold_until` per address; lead-service's next follow-up date is moved to it (`followup_*`).
CREATE TABLE IF NOT EXISTS "lead_absences" (
  "id" text PRIMARY KEY NOT NULL,
  "imap_message_id" text NOT NULL,
  "instantly_campaign_id" text NOT NULL,
  "org_id" text,
  "lead_email" text NOT NULL,
  "received_at" timestamp NOT NULL,
  "outcome" text NOT NULL,
  "stated" jsonb,
  "first_day_back" date,
  "hold_until" timestamp with time zone,
  "run_id" text,
  "judged_at" timestamp with time zone DEFAULT now() NOT NULL,
  "followup_stated_at" timestamp with time zone,
  "followup_statement" text
);
CREATE UNIQUE INDEX IF NOT EXISTS "lead_absences_imap_message_idx" ON "lead_absences" ("imap_message_id");
CREATE INDEX IF NOT EXISTS "lead_absences_lead_email_hold_idx" ON "lead_absences" ("lead_email", "hold_until") WHERE "hold_until" IS NOT NULL;
