-- In-house seed placement: the self-hosted replacement for Instantly's paid
-- inbox-placement test ($47/mo Growth Inbox Placement subscription).
--
-- Two bronze tables, because the measurement has two independent halves:
--   dispatches   — the DENOMINATOR (a seed we sent)
--   observations — the NUMERATOR   (a seed we found, and in which folder)
--
-- `missing` is dispatched-minus-observed. Keeping the halves apart is what lets
-- a vanished seed count AGAINST the sender rather than silently shrink the
-- sample — a single result table could not represent "sent and never arrived".
--
-- Silver is unchanged: these promote into the EXISTING
-- `instantly_placement_results` under a `seed:` test id, so the lifecycle
-- delivery gate, account-health and the history series all read them with no
-- code change.

CREATE TABLE IF NOT EXISTS "seed_placement_dispatches" (
  "id" text PRIMARY KEY NOT NULL,
  "test_id" text NOT NULL,
  "sender_email" text NOT NULL,
  "receiver_email" text NOT NULL,
  "recipient_esp" integer NOT NULL,
  "message_id" text NOT NULL,
  "outcome" text NOT NULL,
  "response" text,
  "dispatched_at" timestamp DEFAULT now() NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS "seed_placement_dispatches_message_id_idx"
  ON "seed_placement_dispatches" ("message_id");
CREATE INDEX IF NOT EXISTS "seed_placement_dispatches_test_idx"
  ON "seed_placement_dispatches" ("test_id");

CREATE TABLE IF NOT EXISTS "seed_placement_observations" (
  "id" text PRIMARY KEY NOT NULL,
  "test_id" text NOT NULL,
  "message_id" text NOT NULL,
  "receiver_email" text NOT NULL,
  "folder" text NOT NULL,
  "placement" text NOT NULL,
  "spf_pass" boolean,
  "dkim_pass" boolean,
  "dmarc_pass" boolean,
  "observed_at" timestamp DEFAULT now() NOT NULL
);

-- First observation wins: Gmail can reclassify after delivery, and where the
-- seed landed AT DELIVERY is the question being asked. The unique index makes a
-- re-read a no-op rather than an overwrite.
CREATE UNIQUE INDEX IF NOT EXISTS "seed_placement_observations_receiver_message_idx"
  ON "seed_placement_observations" ("receiver_email", "message_id");
CREATE INDEX IF NOT EXISTS "seed_placement_observations_test_idx"
  ON "seed_placement_observations" ("test_id");
