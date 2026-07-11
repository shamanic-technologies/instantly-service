-- Persist the per-lead Instantly campaign id on each sequence_costs hold.
--
-- The cost-lifecycle resolvers (handleEmailSent → actualize, cancelRemaining
-- Provisions → cancel, in silver-promote.ts) matched a provisioned hold by the
-- CALLER campaign id (`sequence_costs.campaign_id`). That id is NULL for platform
-- sends (migration 0017 made it nullable), and the webhook/reconcile side
-- identifies a send by its per-lead `instantly_campaign_id` (globally unique,
-- 1 Instantly campaign = 1 lead) — which was never stored on the hold. So every
-- platform-send email cost stranded `provisioned` forever: `handleEmailSent`
-- early-returned on the null caller id and its `campaign_id = NULL` predicate
-- never matched. Persisting the Instantly id at write time (send.ts phase-2 +
-- retry-stuck redispatch — the id is already in hand) lets the resolvers match
-- on it directly, for both org AND platform sends. Same persist-at-write pattern
-- as 0025 (account_email). Nullable — historical rows stay NULL; the resolvers
-- keep a caller-campaign_id fallback for historical ORG rows, and the one-time
-- reconcile-provisioned-holds sweep drains the historical PLATFORM backlog by
-- each hold's own run_id + cost_id + real send evidence.
ALTER TABLE "sequence_costs" ADD COLUMN IF NOT EXISTS "instantly_campaign_id" text;
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "sequence_costs_instantly_campaign_id_idx" ON "sequence_costs" ("instantly_campaign_id");
