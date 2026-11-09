-- EVERY-PERSON-ON-THE-THREAD-1109: the cross-org feed of our mailboxes' messages (lib/mailbox-messages.ts),
-- read by crm-service as one more mail source (like Gmail), every address on every message.
--
-- `mailbox_message_versions`: APPEND-ONLY, one row each time what we serve about a message changes (its
-- source row's hash moved). The served message is rebuilt from bronze at read; this table only gives the total
-- order (`seq`, written under an advisory lock) and the thread filter.
CREATE TABLE IF NOT EXISTS "mailbox_message_versions" (
  "seq" bigserial PRIMARY KEY NOT NULL,
  "message_key" text NOT NULL,
  "source_table" text NOT NULL,
  "source_row_id" text NOT NULL,
  "source_hash" text NOT NULL,
  "thread_key" text,
  "recorded_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "mailbox_message_versions_key_idx" ON "mailbox_message_versions" ("message_key", "seq");
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "mailbox_message_versions_thread_idx" ON "mailbox_message_versions" ("thread_key", "seq");
--> statement-breakpoint
-- Bronze rows written or changed since the feed last looked. Filled by the triggers below (every writer, no
-- caller has to remember), drained by the feed worker.
CREATE TABLE IF NOT EXISTS "mailbox_message_dirty" (
  "source_table" text NOT NULL,
  "source_row_id" text NOT NULL,
  "marked_at" timestamp with time zone DEFAULT now() NOT NULL,
  PRIMARY KEY ("source_table", "source_row_id")
);
--> statement-breakpoint
CREATE OR REPLACE FUNCTION mailbox_message_mark_dirty() RETURNS trigger AS $$
BEGIN
  IF TG_TABLE_NAME = 'imap_messages_raw' THEN
    -- Mail a person wrote or that concerns a thread of ours; plain `unrelated` noise (lists, warmup, DSNs to
    -- nobody) is never served.
    IF NEW.kind = 'unrelated' AND NOT (NEW.payload ? 'orphanCandidate') THEN
      RETURN NEW;
    END IF;
  ELSIF TG_TABLE_NAME = 'smtp_dispatch_raw' THEN
    IF NEW.outcome <> 'sent' THEN
      RETURN NEW;
    END IF;
  END IF;
  INSERT INTO mailbox_message_dirty (source_table, source_row_id)
  VALUES (TG_TABLE_NAME, NEW.id)
  ON CONFLICT (source_table, source_row_id) DO UPDATE SET marked_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
--> statement-breakpoint
DROP TRIGGER IF EXISTS mailbox_message_dirty_imap ON imap_messages_raw;
--> statement-breakpoint
CREATE TRIGGER mailbox_message_dirty_imap AFTER INSERT OR UPDATE ON imap_messages_raw
  FOR EACH ROW EXECUTE FUNCTION mailbox_message_mark_dirty();
--> statement-breakpoint
DROP TRIGGER IF EXISTS mailbox_message_dirty_smtp ON smtp_dispatch_raw;
--> statement-breakpoint
CREATE TRIGGER mailbox_message_dirty_smtp AFTER INSERT OR UPDATE ON smtp_dispatch_raw
  FOR EACH ROW EXECUTE FUNCTION mailbox_message_mark_dirty();
--> statement-breakpoint
DROP TRIGGER IF EXISTS mailbox_message_dirty_iem ON instantly_emails_raw;
--> statement-breakpoint
CREATE TRIGGER mailbox_message_dirty_iem AFTER INSERT OR UPDATE ON instantly_emails_raw
  FOR EACH ROW EXECUTE FUNCTION mailbox_message_mark_dirty();
