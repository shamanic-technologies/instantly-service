-- MS-SEED-PLACEMENT-1018: measure inbox placement at Microsoft (Outlook.com and
-- Microsoft 365), and route Microsoft-hosted prospects to mailboxes measured in
-- Inbox there. Hand-written; idempotent.

-- A Microsoft mailbox we read seeds from. Microsoft refuses password IMAP, so the
-- owner authorizes it once (OAuth device code) and the refresh token keeps it
-- readable. Pending rows carry the device code being polled.
CREATE TABLE IF NOT EXISTS "microsoft_seed_mailboxes" (
  "email" text PRIMARY KEY NOT NULL,
  "status" text NOT NULL,
  "client_id" text NOT NULL,
  "tenant" text NOT NULL,
  "account_kind" text,
  "device_code" text,
  "user_code" text,
  "verification_uri" text,
  "device_code_expires_at" timestamp with time zone,
  "poll_interval_seconds" integer,
  "refresh_token" text,
  "authorized_at" timestamp with time zone,
  "last_refreshed_at" timestamp with time zone,
  "last_error" text,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
-- Microsoft says more than the folder: Focused vs Other inside the Inbox, and
-- the spam confidence level it scored the seed at.
ALTER TABLE "seed_placement_observations" ADD COLUMN IF NOT EXISTS "ms_inference" text;
--> statement-breakpoint
ALTER TABLE "seed_placement_observations" ADD COLUMN IF NOT EXISTS "ms_scl" integer;
--> statement-breakpoint
-- The stored daily series behind the admin chart: seeds sent that day per
-- receiving provider and sending vendor, and where they landed. Recomputed
-- from the two bronze tables on every seed sync.
CREATE TABLE IF NOT EXISTS "seed_placement_daily" (
  "day" date NOT NULL,
  "recipient_esp" integer NOT NULL,
  "sender_vendor" text NOT NULL,
  "seeds" integer NOT NULL,
  "inbox" integer NOT NULL,
  "spam" integer NOT NULL,
  "missing" integer NOT NULL,
  "inbox_focused" integer NOT NULL,
  "inbox_other" integer NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL,
  CONSTRAINT "seed_placement_daily_pk" PRIMARY KEY ("day", "recipient_esp", "sender_vendor")
);
--> statement-breakpoint
-- Who hosts a prospect domain's mail, read off its MX records. A domain whose
-- mail Microsoft hosts is only written to from a mailbox Microsoft puts in Inbox.
CREATE TABLE IF NOT EXISTS "recipient_mail_hosts" (
  "domain" text PRIMARY KEY NOT NULL,
  "provider" text NOT NULL,
  "mx" jsonb DEFAULT '[]'::jsonb NOT NULL,
  "error" text,
  "checked_at" timestamp with time zone DEFAULT now() NOT NULL
);
