-- Bronze mirror of what arrives in our own sending mailboxes (issue #590, PR 4).
-- Empty on creation, written only by the IMAP poller, which reads only accounts
-- on send_transport='smtp' — none today, so this is inert until a flip.
--
-- Once we dispatch ourselves, nobody tells us a prospect replied or a message
-- bounced: it lands as mail and we read it. Every message the poller looks at is
-- stored, INCLUDING the ones classified 'unrelated' — these are real mailboxes
-- that also receive ordinary mail, and keeping what we ignored is what makes
-- "why was this reply never picked up?" answerable later.
--
-- (account_email, message_id) is unique, and that IS the dedup strategy: the
-- poller re-reads an overlapping window every run rather than trusting a stored
-- cursor, and this index makes the re-read a no-op. A cursor that drifts silently
-- loses replies; an overlapping window costs nothing.
CREATE TABLE IF NOT EXISTS "imap_messages_raw" (
  "id" text PRIMARY KEY NOT NULL,
  "account_email" text NOT NULL,
  "message_id" text NOT NULL,
  "from_address" text,
  "subject" text,
  -- 'reply' | 'auto_reply' | 'bounce' | 'unrelated'
  "kind" text NOT NULL,
  -- The send this message answers, correlated through our own Message-Id.
  -- Null for 'unrelated'.
  "instantly_campaign_id" text,
  "step" integer,
  "payload" jsonb NOT NULL,
  "received_at" timestamp,
  "polled_at" timestamp DEFAULT now() NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS "imap_messages_raw_account_message_idx"
  ON "imap_messages_raw" ("account_email", "message_id");
CREATE INDEX IF NOT EXISTS "imap_messages_raw_campaign_idx"
  ON "imap_messages_raw" ("instantly_campaign_id");
CREATE INDEX IF NOT EXISTS "imap_messages_raw_polled_at_idx"
  ON "imap_messages_raw" ("polled_at");
