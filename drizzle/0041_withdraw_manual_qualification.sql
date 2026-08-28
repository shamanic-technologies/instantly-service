-- Withdrawing a manual reply qualification.
--
-- A person who states a reply kind and gets it wrong must be able to take the
-- statement back, so the lead reads exactly as it did before anybody said
-- anything. Nothing is deleted and no "none" member is added to the reply-kind
-- vocabulary: a withdrawal is a row that SUPERSEDES a statement downward.
--
-- Two appends:
--
--  1. `instantly_manual_qualification_withdrawals` (bronze, append-only) — one
--     row per withdrawn statement, keyed on the statement's own id. UNIQUE, so
--     withdrawing the same statement twice is a no-op at the index rather than
--     at a read-then-write race, and a later re-statement is untouched by an
--     earlier withdrawal.
--
--  2. `instantly_events.withdrawn_at` (silver) — set on the manual mirror event
--     of the withdrawn statement. The row stays (silver is the audit of what was
--     asserted); the gold current-sentiment projection filters it out, which is
--     what makes the analytics counters a manual statement moved stop counting
--     it. Silver is derived and rebuildable, so marking it here is legitimate.
--
-- Both are additive: `withdrawn_at` is NULL on every existing row, which means
-- "still stands", so no read changes behaviour until someone withdraws.
--
-- Note: `id` is minted here explicitly — the drizzle column carries a
-- `$defaultFn`, i.e. an APPLICATION-side default with no DB default, so any raw
-- SQL insert into this table must supply it.

CREATE TABLE IF NOT EXISTS "instantly_manual_qualification_withdrawals" (
  "id" text PRIMARY KEY NOT NULL,
  "qualification_id" text NOT NULL,
  "org_id" text NOT NULL,
  "campaign_id" text NOT NULL,
  "instantly_campaign_id" text NOT NULL,
  "lead_email" text NOT NULL,
  "withdrawn_by" text NOT NULL,
  "notes" text,
  "withdrawn_at" timestamp DEFAULT now() NOT NULL,
  CONSTRAINT "instantly_manual_qualification_withdrawals_qualification_id_unique" UNIQUE("qualification_id")
);

CREATE INDEX IF NOT EXISTS "instantly_manual_qualification_withdrawals_org_campaign_email_idx"
  ON "instantly_manual_qualification_withdrawals" ("org_id", "campaign_id", "lead_email");

ALTER TABLE "instantly_events" ADD COLUMN IF NOT EXISTS "withdrawn_at" timestamp;
