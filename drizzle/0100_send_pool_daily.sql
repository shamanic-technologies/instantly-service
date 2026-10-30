-- SEND-POOL-VENDOR-402-1030: how many mailboxes the sender could hand a NEW email to, per UTC day and send pool
-- ('shared' = every org, else a reserved feature slug), sampled through the sender's own selection 60 s after boot
-- then every 15 min. `sendable_min` = the day's lowest sample (an outage of a few hours still shows as 0),
-- `sendable_last` + `by_vendor` = the latest sample. Read by GET /internal/ops/infra (`sendPoolHistory`).
CREATE TABLE IF NOT EXISTS "send_pool_daily" (
  "day" date NOT NULL,
  "pool" text NOT NULL,
  "sendable_min" integer NOT NULL,
  "sendable_last" integer NOT NULL,
  "by_vendor" jsonb DEFAULT '{}'::jsonb NOT NULL,
  "samples" integer DEFAULT 1 NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL,
  CONSTRAINT "send_pool_daily_pk" PRIMARY KEY ("day", "pool")
);
