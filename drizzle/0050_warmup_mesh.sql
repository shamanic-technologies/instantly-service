-- The warmup mesh: our own mailboxes keeping each other warm.
--
-- Instantly's Email Outreach subscription bundles a warmup pool of tens of
-- thousands of mailboxes that exchange mail, read it, rescue it from spam and
-- reply. We are cancelling that subscription, so the fleet does it itself.
--
-- Two bronze tables, mirroring the seed-placement split for the same reason:
--   dispatches — what we PUT ON THE WIRE (one row per warmup email)
--   receipts   — what the RECEIVING side found and did with it
--
-- Keeping them apart is what makes "sent and never arrived" representable. A
-- single table could only record messages that were found, so a mailbox whose
-- warmup mail vanishes entirely would look identical to one nobody wrote to.
--
-- ⚠️ `warmup_dispatches` is ALSO a capacity input. The self-send dispatcher
-- counts a mailbox's real sends today against its daily cap; warmup mail comes
-- out of the SAME Gmail per-user quota, so it has to be counted there too or the
-- mesh silently pushes mailboxes over the limit that the ramp exists to respect
-- (`550-5.4.5 Daily user sending limit exceeded`). Hence `day_key` as a stored
-- column: the cap is a UTC-day question and this is the index that answers it.
--
-- Nothing here promotes to silver. A warmup email is not outreach: minting an
-- `email_sent` event for one would corrupt step accounting, the per-account
-- queue attribution and the per-brand re-contact window, all of which read that
-- event as evidence a PROSPECT was mailed.

CREATE TABLE IF NOT EXISTS "warmup_dispatches" (
  "id" text PRIMARY KEY NOT NULL,
  -- The sending mailbox, as the account is addressed.
  "sender_email" text NOT NULL,
  -- The mailbox that authenticates the send. Several aliases share one, and a
  -- day's quota belongs to THIS, not to the address — same grain the dispatcher
  -- budgets on.
  "sender_mailbox" text NOT NULL,
  "receiver_email" text NOT NULL,
  -- UTC calendar day, the unit both the pairing and the cap use.
  "day_key" text NOT NULL,
  "message_id" text NOT NULL,
  "subject" text,
  -- 'sent' | 'transient' | 'permanent' — the same classification the self-send
  -- dispatcher records, so a refused warmup send stays visible rather than
  -- vanishing.
  "outcome" text NOT NULL,
  "response" text,
  "dispatched_at" timestamp DEFAULT now() NOT NULL
);

-- Idempotence WITHOUT a cursor: the pairing is deterministic per (day, mailbox),
-- so a re-run inside the same day plans the same edges and this index makes the
-- second attempt a no-op.
CREATE UNIQUE INDEX IF NOT EXISTS "warmup_dispatches_edge_day_idx"
  ON "warmup_dispatches" ("sender_email", "receiver_email", "day_key");

CREATE UNIQUE INDEX IF NOT EXISTS "warmup_dispatches_message_id_idx"
  ON "warmup_dispatches" ("message_id");

-- The capacity read: how much of this mailbox's quota warmup already spent today.
CREATE INDEX IF NOT EXISTS "warmup_dispatches_mailbox_day_idx"
  ON "warmup_dispatches" ("sender_mailbox", "day_key");

CREATE TABLE IF NOT EXISTS "warmup_receipts" (
  "id" text PRIMARY KEY NOT NULL,
  "message_id" text NOT NULL,
  "receiver_email" text NOT NULL,
  -- The folder the message was found in, verbatim from the IMAP server.
  "folder" text NOT NULL,
  -- 'inbox' | 'spam' — where it LANDED, frozen before any rescue moved it.
  "placement" text NOT NULL,
  -- Did we move it out of spam / mark it read / flag it.
  "rescued" boolean DEFAULT false NOT NULL,
  "replied" boolean DEFAULT false NOT NULL,
  "observed_at" timestamp DEFAULT now() NOT NULL
);

-- First observation wins, exactly as the seed harness does it: the question is
-- where the message landed AT DELIVERY, so a later re-read must not overwrite
-- it — and this is what makes the poller idempotent over its re-read window.
CREATE UNIQUE INDEX IF NOT EXISTS "warmup_receipts_message_receiver_idx"
  ON "warmup_receipts" ("message_id", "receiver_email");

CREATE INDEX IF NOT EXISTS "warmup_receipts_receiver_idx"
  ON "warmup_receipts" ("receiver_email", "observed_at");
