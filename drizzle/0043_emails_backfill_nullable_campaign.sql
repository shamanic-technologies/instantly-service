-- An Unibox email does not necessarily belong to a campaign.
--
-- `instantly_emails_raw` was only ever written by the reconcile poll, which
-- fetches `/emails?campaign_id=<id>` — so every row it wrote had a campaign by
-- construction and the column could be NOT NULL. The workspace-wide backfill
-- reads `/emails` with NO campaign filter, which is the only way to mirror the
-- whole Unibox before Instantly deletes it, and that list legitimately contains
-- mail attached to no campaign.
--
-- Skipping those rows would drop exactly the thing the backfill exists to
-- preserve, and inventing a campaign id for them would be a fabricated fact.
-- The column becomes nullable instead. Every reader filters by a concrete
-- campaign id, so widening it changes no existing query.
--
-- Idempotent: re-running finds the column already nullable and does nothing.

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_name = 'instantly_emails_raw'
      AND column_name = 'instantly_campaign_id'
      AND is_nullable = 'NO'
  ) THEN
    ALTER TABLE "instantly_emails_raw"
      ALTER COLUMN "instantly_campaign_id" DROP NOT NULL;
  END IF;
END $$;
