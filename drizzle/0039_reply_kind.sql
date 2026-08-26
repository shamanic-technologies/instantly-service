-- Reply kind — separate "what kind of reply arrived" from "how far the deal got".
--
-- The two deal-progress values (`lead_meeting_booked`, `lead_closed`) leave this
-- service's vocabulary: they are lead outcomes, channel-agnostic, and owned by
-- the service that records lead outcomes. Keeping them here made the two axes
-- overwrite each other — only the latest statement per lead survived the gold
-- projection, so a lead who replied positively and then had a meeting booked
-- read as having no reply sentiment at all.
--
-- Nothing here rewrites the append-only record of human intent: `status`,
-- `qualified_by`, `qualified_at`, `notes` and `payload` are untouched on every
-- pre-existing row. What this migration does is (a) add the resolved reply kind
-- as a NEW column so no reader ever needs a read-time translation, and (b)
-- APPEND the resolved silver event for the statements that used a deal-progress
-- value, so those leads keep a reply kind in the new vocabulary.
--
-- Prod scale at authoring time: 84 statements / 79 distinct (campaign, lead)
-- pairs, of which 4 `lead_meeting_booked` + 4 `lead_closed`. The only silver
-- deal-progress events in the whole log are the 8 mirrors of those statements.

--> statement-breakpoint
ALTER TABLE "instantly_manual_qualifications_raw" ADD COLUMN IF NOT EXISTS "reply_kind" text;

--> statement-breakpoint
-- Resolve every existing statement. Identity for the vocabulary; the documented
-- resolution for the two deal-progress values — a person whose reply was
-- qualified as a booked meeting or a closed-won deal had by definition replied
-- positively, so `lead_interested` (the plain positive) is what the domain fact
-- supports without re-encoding the deal axis.
UPDATE "instantly_manual_qualifications_raw"
SET "reply_kind" = CASE "status"
  WHEN 'lead_meeting_booked' THEN 'lead_interested'
  WHEN 'lead_closed' THEN 'lead_interested'
  WHEN 'lead_meeting_completed' THEN 'lead_interested'
  ELSE "status"
END
WHERE "reply_kind" IS NULL;

--> statement-breakpoint
-- Fail loud: a row we cannot resolve must not sit around as a NULL that every
-- reader has to guess about.
ALTER TABLE "instantly_manual_qualifications_raw" ALTER COLUMN "reply_kind" SET NOT NULL;

--> statement-breakpoint
-- APPEND (never update) the resolved silver event for each deal-progress
-- statement. The original `lead_meeting_booked` / `lead_closed` event rows stay
-- exactly as they are — they are the audit of what was mirrored at the time —
-- but they are no longer part of the reply-kind vocabulary, so without this the
-- affected leads would read as having no reply at all.
--
-- The appended row shares the statement's timestamp and its bronze
-- `source_row_id`; the gold latest-sentiment projection breaks the tie on
-- `created_at DESC`, so the resolved kind wins over the retired event.
-- Idempotent: re-running inserts nothing.
-- `instantly_events.id` has NO database default — the app supplies it through
-- drizzle's `$defaultFn(crypto.randomUUID)`, so raw SQL has to mint its own or
-- the insert dies on the NOT NULL.
INSERT INTO "instantly_events" (
  "id",
  "event_type", "campaign_id", "lead_email", "account_email", "step", "variant",
  "timestamp", "raw_payload", "source", "source_row_id", "inferred"
)
SELECT
  gen_random_uuid()::text,
  'lead_interested', q."instantly_campaign_id", q."lead_email", NULL, NULL, NULL,
  q."qualified_at", q."payload", 'manual', q."id", false
FROM "instantly_manual_qualifications_raw" q
WHERE q."status" IN ('lead_meeting_booked', 'lead_closed', 'lead_meeting_completed')
  AND NOT EXISTS (
    SELECT 1 FROM "instantly_events" e
    WHERE e."campaign_id" = q."instantly_campaign_id"
      AND e."lead_email" = q."lead_email"
      AND e."event_type" = 'lead_interested'
      AND e."source_row_id" = q."id"
  );

--> statement-breakpoint
-- The campaign row's coarse classification is already `positive` for all eight
-- (both deal values mapped to positive), so no re-pin is needed there.
