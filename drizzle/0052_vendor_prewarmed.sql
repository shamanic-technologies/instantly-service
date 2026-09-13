-- A mailbox the VENDOR warmed before handing it over.
--
-- `timestamp_created` is Instantly's own creation date, which for an imported
-- mailbox is the day WE imported it — not the month the vendor spent warming it.
-- Two of our gates read that date and both then treat a pre-warmed mailbox as
-- brand new: the placement test's age floor refuses to measure it for a week,
-- and the volume ramp reads a sustained volume of zero and pins it at the floor.
-- Neither is a fact about the mailbox; both are facts about when we first saw it.
--
-- Holds the vendor's OWN mailbox creation date, so the gates can read the age
-- that exists rather than the age we happen to have observed. Null for every
-- mailbox we did not buy pre-warmed, which is the whole existing fleet.
ALTER TABLE "instantly_accounts"
  ADD COLUMN IF NOT EXISTS "vendor_prewarmed_at" timestamp with time zone;
