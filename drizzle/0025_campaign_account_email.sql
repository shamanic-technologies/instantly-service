-- Persist the chosen Instantly sending account on each campaign row.
--
-- The account is picked in sendLeadToInstantly (send.ts phase-2, retry-stuck
-- redispatch) but was never stored locally, so per-account queue size had to be
-- reverse-engineered from the first observed `email_sent` webhook — which lags
-- by minutes and leaves a just-`contacted` lead unattributed. Storing the
-- account at write time closes that gap: queue load is known the instant the row
-- exists. Nullable — historical rows stay NULL and readers COALESCE to the
-- observed-send attribution. See account-sending-stats.ts + CLAUDE.md.
ALTER TABLE "instantly_campaigns" ADD COLUMN IF NOT EXISTS "account_email" text;
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "instantly_campaigns_account_email_idx" ON "instantly_campaigns" ("account_email");
