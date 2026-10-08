-- The outreach FACT FEED (lib/outreach-facts): every dated thing our outreach
-- did or saw for a person, in one total order lead-service copies by cursor
-- (`GET /internal/outreach-facts`). Append-only: a correction is a NEW row that
-- names the one it supersedes (`supersedes_seq`), never an edit.
--
-- `seq` is the cursor. Emission holds a global advisory lock for its
-- transaction, so seq values commit in order and a reader paging by cursor
-- never skips a fact.
CREATE TABLE IF NOT EXISTS "outreach_facts" (
  "seq" bigserial PRIMARY KEY,
  -- What the fact is about: `ievt:<instantly_events.id>` or `reply:<replies.id>`.
  "subject_key" text NOT NULL,
  -- email_sent | email_opened | link_clicked | email_bounced | unsubscribed | reply | withdrawn
  "type" text NOT NULL,
  "supersedes_seq" bigint,
  "content_hash" text,
  "occurred_at" timestamp with time zone NOT NULL,
  "lead_email" text NOT NULL,
  "org_id" text,
  "campaign_id" text,
  "instantly_campaign_id" text NOT NULL,
  "brand_ids" text[],
  "transport" text,
  "payload" jsonb NOT NULL,
  "recorded_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
-- An event is emitted ONCE (its corrections are `withdrawn` rows).
CREATE UNIQUE INDEX IF NOT EXISTS "outreach_facts_event_once_idx"
  ON "outreach_facts" ("subject_key")
  WHERE "type" IN ('email_sent', 'email_opened', 'link_clicked', 'email_bounced', 'unsubscribed');
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "outreach_facts_subject_idx" ON "outreach_facts" ("subject_key", "seq");
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "outreach_facts_supersedes_idx" ON "outreach_facts" ("supersedes_seq") WHERE "supersedes_seq" IS NOT NULL;
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "outreach_facts_lead_idx" ON "outreach_facts" (lower("lead_email"), "seq");
--> statement-breakpoint
-- The feed's incremental scan reads events by insertion time (a backdated event
-- still has a fresh created_at), never the 300 MB heap.
CREATE INDEX IF NOT EXISTS "instantly_events_created_at_idx" ON "instantly_events" ("created_at");
--> statement-breakpoint
-- BRONZE: each Jev judgment about a reply, judged ONCE per (reply, question)
-- and kept. lib/reply-judgments.
CREATE TABLE IF NOT EXISTS "reply_judgments" (
  "id" text PRIMARY KEY DEFAULT gen_random_uuid()::text,
  "reply_id" text NOT NULL,
  "question" text NOT NULL,
  "choice" text NOT NULL,
  "confidence" double precision,
  "probabilities" jsonb,
  "model" text,
  "input_tokens" integer,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "reply_judgments_reply_question_idx" ON "reply_judgments" ("reply_id", "question");
