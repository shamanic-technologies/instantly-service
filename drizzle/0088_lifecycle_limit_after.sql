-- CAPACITY-FROZEN-LIMIT-1021. Hand-written; idempotent. Do NOT drop on a db:generate diff.
-- The limit an account carries AFTER each lifecycle flip. Capacity history read
-- each address's limit as-of a day from the Instantly snapshots alone; those
-- stopped on 2026-10-12 (plan cancelled), while reconcileLifecycle kept writing
-- the limit onto silver on every flip, so every day since replayed the 10-12
-- limit. The reader now takes the latest of (snapshot, flip) at or before each day.
ALTER TABLE "instantly_account_lifecycle_events" ADD COLUMN IF NOT EXISTS "daily_limit_after" integer;
--> statement-breakpoint
-- Backfill from MEASURED values only, and only for flips after the account's last
-- snapshot (the days the snapshots still covered keep their history): the limit
-- after a flip is the before-limit the NEXT flip recorded (`daily_limit` = silver
-- at that moment), and for the account's latest flip the limit silver holds now
-- (only flips and snapshots write it, and snapshots stopped). Accounts never
-- snapshotted are left alone: their days stay "limit unknown", never invented.
WITH last_snap AS (
  SELECT account_email, max(fetched_at) AS t FROM instantly_accounts_raw GROUP BY account_email
),
chain AS (
  SELECT e.id,
         e.created_at,
         s.t AS last_snapshot_at,
         lead(e.daily_limit) OVER (PARTITION BY e.account_email ORDER BY e.created_at, e.id) AS next_before,
         lead(e.id) OVER (PARTITION BY e.account_email ORDER BY e.created_at, e.id) AS next_id,
         ia.daily_limit AS silver_limit
    FROM instantly_account_lifecycle_events e
    JOIN last_snap s ON s.account_email = e.account_email
    LEFT JOIN instantly_accounts ia ON ia.email = e.account_email
)
UPDATE instantly_account_lifecycle_events e
   SET daily_limit_after = CASE WHEN c.next_id IS NULL THEN c.silver_limit ELSE c.next_before END
  FROM chain c
 WHERE c.id = e.id
   AND c.created_at > c.last_snapshot_at
   AND e.daily_limit_after IS NULL;
