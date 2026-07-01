-- Reconcile snapshot — pre-aggregated Instantly-side counts for the reconcile audit.
--
-- GET /internal/audit/reconcile compares OUR live local counts against INSTANTLY's
-- counts. Instantly's side needs a fleet-wide throttled API sweep (campaign
-- analytics + per-campaign sequence lengths across thousands of campaigns) that
-- takes minutes — far past the gateway/browser timeout, which left the staff
-- dashboard on an infinite loading skeleton. The Instantly side is now pre-computed
-- by a background refresh and stored in this single-row table; the GET reads it in
-- one fast query. Fail loud (503) when absent — never fabricate a count.
-- See lib/reconcile-snapshot.ts + CLAUDE.md "Reconciliation audit".
CREATE TABLE IF NOT EXISTS "instantly_reconcile_snapshot" (
  "id" text PRIMARY KEY NOT NULL,
  "active_campaigns" integer NOT NULL,
  "emails_sent" integer NOT NULL,
  "contacted_dispatched" integer NOT NULL,
  "contacts_stored" integer NOT NULL,
  "pending_sends" integer NOT NULL,
  "refreshed_at" timestamp DEFAULT now() NOT NULL
);
