-- Foundation for dispatching our own mail (issue #590), so the $97/mo Instantly
-- Email Outreach subscription can be dropped. Purely additive: every account
-- defaults to 'instantly', so nothing changes until an operator runs an explicit
-- UPDATE, and setting it back is the rollback.
--
-- Boot-safe. Both ALTERs add a column with a NON-VOLATILE default, which
-- Postgres 11+ records as metadata only (no table rewrite), so this stays
-- instant on instantly_campaigns despite its size. The CREATE TABLE is empty.

-- POLICY: which pipe NEW sends assigned to this mailbox should use.
ALTER TABLE "instantly_accounts"
  ADD COLUMN IF NOT EXISTS "send_transport" text DEFAULT 'instantly' NOT NULL;

-- DECISION: frozen at send time from the chosen account's policy above.
--
-- Not redundant with the account column. A sequence spans days, so reading the
-- account policy live would re-route a lead's followups mid-flight the moment an
-- operator flips that mailbox — and a lead already pushed to Instantly holds no
-- local step bodies to send from, so its followups would simply stop. Same
-- persist-at-write reasoning as account_email (migration 0025).
ALTER TABLE "instantly_campaigns"
  ADD COLUMN IF NOT EXISTS "send_transport" text DEFAULT 'instantly' NOT NULL;

-- SILVER: the steps of a sequence we dispatch ourselves.
--
-- Canonical state, not a mirror. While Instantly dispatches, the bodies live
-- there and instantly_campaigns_config_raw is our BRONZE copy of what they hold;
-- once we send, there is no upstream to mirror, so the same content moves UP a
-- layer instead of becoming a second bronze beside Instantly's.
--
-- step is 1-based, matching sequence_costs.step. delay_days is the gap from THIS
-- step to the next, so the gap k to k+1 is the delay_days of the row at step k —
-- ordered by step, these rows are exactly the array delayForGap already indexes,
-- which keeps the self-send scheduler, the fleet forecast and the per-account
-- queue breakdown on one shared cadence source. NULL falls back to
-- STEP_GAP_CALENDAR_DAYS, same as a missing bronze config delay.
CREATE TABLE IF NOT EXISTS "sequence_steps" (
  "id" text PRIMARY KEY NOT NULL,
  "instantly_campaign_id" text NOT NULL,
  "step" integer NOT NULL,
  "subject" text,
  "body_html" text NOT NULL,
  "delay_days" integer,
  "created_at" timestamp DEFAULT now() NOT NULL,
  "updated_at" timestamp DEFAULT now() NOT NULL
);

-- Makes the per-step write idempotent: a redispatch re-upserts the same step
-- rather than stacking a duplicate the scheduler would then send twice.
CREATE UNIQUE INDEX IF NOT EXISTS "sequence_steps_campaign_step_idx"
  ON "sequence_steps" ("instantly_campaign_id", "step");
