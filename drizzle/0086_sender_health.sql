-- SENDER-HEALTH-1019. Hand-written; idempotent.

-- Outlook.com (consumer) and a Microsoft 365 tenant filter apart: our consumer
-- receiver now grades under ESP 4, the business tenant stays ESP 2.
UPDATE "seed_placement_dispatches" d SET "recipient_esp" = 4
WHERE d."recipient_esp" = 2
  AND d."receiver_email" IN (SELECT "email" FROM "microsoft_seed_mailboxes" WHERE "account_kind" = 'consumer');
--> statement-breakpoint
-- Re-aggregate silver for every seed test that has a Microsoft dispatch, from
-- bronze, exactly as `aggregateSeedPlacement` does (tested_at = the test's
-- first dispatch; missing = sent minus found; auth flags AND-folded).
DELETE FROM "instantly_placement_results"
WHERE "recipient_esp" IN (2, 4)
  AND "test_id" IN (SELECT DISTINCT "test_id" FROM "seed_placement_dispatches" WHERE "recipient_esp" IN (2, 4));
--> statement-breakpoint
INSERT INTO "instantly_placement_results"
  ("test_id", "account_email", "recipient_esp", "tested_at", "seed_total", "inbox_count", "spam_count",
   "missing_count", "inbox_pct", "spam_pct", "missing_pct", "spf_pass", "dkim_pass", "dmarc_pass")
SELECT g.test_id, g.sender_email, g.recipient_esp, t.tested_at, g.seeds, g.inbox, g.spam,
       g.seeds - g.inbox - g.spam,
       round(100.0 * g.inbox / g.seeds)::int, round(100.0 * g.spam / g.seeds)::int,
       round(100.0 * (g.seeds - g.inbox - g.spam) / g.seeds)::int,
       g.spf, g.dkim, g.dmarc
FROM (
  SELECT d."test_id" AS test_id, d."sender_email" AS sender_email, d."recipient_esp" AS recipient_esp,
         count(*)::int AS seeds,
         count(o."id") FILTER (WHERE o."placement" = 'inbox')::int AS inbox,
         count(o."id") FILTER (WHERE o."placement" IS NOT NULL AND o."placement" <> 'inbox')::int AS spam,
         bool_and(o."spf_pass") AS spf, bool_and(o."dkim_pass") AS dkim, bool_and(o."dmarc_pass") AS dmarc
  FROM "seed_placement_dispatches" d
  LEFT JOIN "seed_placement_observations" o
    ON o."message_id" = d."message_id" AND o."receiver_email" = d."receiver_email"
  WHERE d."outcome" = 'sent' AND d."recipient_esp" IN (2, 4)
  GROUP BY 1, 2, 3
) g
JOIN (
  SELECT "test_id", min("dispatched_at") AS tested_at
  FROM "seed_placement_dispatches" WHERE "outcome" = 'sent' GROUP BY 1
) t ON t."test_id" = g.test_id
ON CONFLICT ("test_id", "account_email", "recipient_esp") DO NOTHING;
--> statement-breakpoint
-- A consumer Outlook.com domain is routed on its own verdict.
UPDATE "recipient_mail_hosts" SET "provider" = 'microsoft_consumer'
WHERE "provider" = 'microsoft' AND "mx"::text ILIKE '%.olc.protection.outlook.com%';
--> statement-breakpoint
-- The stored daily series behind the admin sender-health chart: which mailboxes
-- the dispatcher keeps out of prospect mail (refused by the relay; mostly spam
-- at a provider) and how many emails it held for it.
CREATE TABLE IF NOT EXISTS "sender_health_daily" (
  "day" date PRIMARY KEY NOT NULL,
  "refused" jsonb DEFAULT '[]'::jsonb NOT NULL,
  "mostly_spam" jsonb DEFAULT '{}'::jsonb NOT NULL,
  "held_placement" integer DEFAULT 0 NOT NULL,
  "held_mostly_spam" integer DEFAULT 0 NOT NULL,
  "skipped_refused" integer DEFAULT 0 NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
