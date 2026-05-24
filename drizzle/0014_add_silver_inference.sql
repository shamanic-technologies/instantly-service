-- Silver-layer inference: synthesize missing predecessor events deterministically.
-- Rules (opened ⇒ sent, clicked ⇒ opened+sent, replied/bounced/unsub ⇒ sent,
-- sent step N ⇒ sent steps 1..N-1) are evaluated in `src/lib/silver-promote.ts`.
--
-- Synthetic rows are flagged with `inferred=true` + `inferred_from_event_id` so
-- audit + stats can distinguish derived from observed. Real webhooks/polls that
-- arrive later upgrade the row via the partial unique index below.

-- ─── Add inference columns ──────────────────────────────────────────────────
ALTER TABLE "instantly_events" ADD COLUMN IF NOT EXISTS "inferred" boolean DEFAULT false NOT NULL;
--> statement-breakpoint
ALTER TABLE "instantly_events" ADD COLUMN IF NOT EXISTS "inferred_from_event_id" text;
--> statement-breakpoint
ALTER TABLE "instantly_events" ADD COLUMN IF NOT EXISTS "inferred_rule" text;
--> statement-breakpoint

-- ─── Pre-dedupe for one-shot partial unique index ───────────────────────────
-- One-shot events (`email_sent`, `email_bounced`, `lead_unsubscribed`,
-- `reply_received`) MUST be at-most-1 per (campaign, lead, step). Historical
-- silver may contain duplicates with different timestamps (e.g. webhook +
-- poll_emails reconcile both inserted the same sent under the old dedupe key
-- which includes timestamp). Collapse to one row per natural key: keep oldest
-- created_at (i.e. first-promoted real signal), drop the rest.
DELETE FROM "instantly_events"
WHERE "ctid" IN (
  SELECT "ctid" FROM (
    SELECT "ctid",
           ROW_NUMBER() OVER (
             PARTITION BY "campaign_id", "lead_email", "event_type", COALESCE("step", -1)
             ORDER BY "created_at" ASC, "ctid" ASC
           ) AS rn
    FROM "instantly_events"
    WHERE "event_type" IN ('email_sent', 'email_bounced', 'lead_unsubscribed', 'reply_received')
  ) sub
  WHERE rn > 1
);
--> statement-breakpoint

-- ─── Partial unique index for one-shot events ───────────────────────────────
-- Allows INSERT ... ON CONFLICT DO UPDATE to upgrade synthetic rows when the
-- real webhook eventually arrives. Repeatable events (opens, clicks) keep
-- using `instantly_events_dedupe_idx` (timestamp-inclusive).
CREATE UNIQUE INDEX IF NOT EXISTS "instantly_events_one_shot_dedupe_idx"
  ON "instantly_events" ("campaign_id", "lead_email", "event_type", (COALESCE("step", -1)))
  WHERE "event_type" IN ('email_sent', 'email_bounced', 'lead_unsubscribed', 'reply_received');
--> statement-breakpoint

-- ─── Helpful audit index ─────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS "instantly_events_inferred_idx"
  ON "instantly_events" ("inferred")
  WHERE "inferred" = true;
