-- BOX-DISK-1101: `raw-snapshot-retention.ts` deletes an old `unrelated` copy of a
-- message only when a newer copy of the same Message-Id stays; that probe needs
-- an index on `message_id` (the unique index leads with `account_email`). It also
-- serves gmail-threads' `WHERE message_id = ANY(...)`, a seq scan until now.
-- Plain (non-CONCURRENT) build: the boot migrator runs in a transaction. Heap is
-- ~250 MB (payload is TOAST, not read), a few seconds of blocked IMAP inserts at boot.
CREATE INDEX IF NOT EXISTS imap_messages_raw_message_id_idx
  ON imap_messages_raw (message_id);
