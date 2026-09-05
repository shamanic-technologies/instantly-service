-- A reply that is waiting for the prospect's own business hours.
--
-- Everything else this service sends already waits: a sequence step is held
-- until `isWithinLocalSendWindow` opens in the RECIPIENT's timezone, and on the
-- Instantly transport the campaign schedule enforces the same Mon-Fri
-- 08:00-17:00 window. The one-to-one answer to a prospect who wrote back did
-- not -- it went out at whatever moment the caller happened to run, so a
-- prospect who replied at 23:00 their time could receive our answer at 23:05,
-- which reads as a machine.
--
-- This table is the WAITING ROOM, not a second scheduler. The cadence, the
-- hours, the days, the timezone resolution and the default when we hold no
-- timezone all stay exactly where they already live (`sending-window.ts` +
-- `sending-calendar.ts`), and the drain rides the SAME hourly worker that sends
-- the sequence steps.
--
-- A reply is still NOT a sequence step: no `sequence_steps` row, no
-- `sequence_costs` hold, no step number. Only the MOMENT of dispatch changed.
CREATE TABLE IF NOT EXISTS "scheduled_replies" (
  "id" text PRIMARY KEY NOT NULL,
  -- The identity the reply is sent under. `user_id` is required because the
  -- org's Instantly key is resolved per user, and the drain runs long after the
  -- request that carried the header is gone.
  "org_id" text NOT NULL,
  "user_id" text NOT NULL,
  -- The caller's logical campaign id, plus the per-lead Instantly id resolved
  -- from it. Both are stored so the drain re-resolves nothing it already knew.
  "campaign_id" text NOT NULL,
  "instantly_campaign_id" text NOT NULL,
  "lead_email" text NOT NULL,
  -- The answer itself, unsigned. The signature is appended at dispatch by the
  -- existing body pipeline, from the persona of the mailbox that answers.
  "body_html" text NOT NULL,
  -- The prospect's IANA timezone as the campaign row carried it at enqueue.
  -- Null means we hold none; the fleet default then applies, which is the same
  -- zone the Instantly schedule degrades to.
  "timezone" text,
  -- The first instant the prospect's own window opens. A LOWER BOUND, exactly
  -- like every other date this service projects: the drain additionally holds a
  -- weekend, so the reply arrives on this instant or after it, never before.
  "scheduled_for" timestamp NOT NULL,
  -- 'pending' | 'sent' | 'failed'
  "status" text DEFAULT 'pending' NOT NULL,
  "attempts" integer DEFAULT 0 NOT NULL,
  "last_error" text,
  "sent_at" timestamp,
  "created_at" timestamp DEFAULT now() NOT NULL,
  "updated_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint

-- The drain's own query: everything still pending, oldest due first.
CREATE INDEX IF NOT EXISTS "scheduled_replies_pending_idx"
  ON "scheduled_replies" ("status", "scheduled_for");
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "scheduled_replies_campaign_lead_idx"
  ON "scheduled_replies" ("instantly_campaign_id", "lead_email");
