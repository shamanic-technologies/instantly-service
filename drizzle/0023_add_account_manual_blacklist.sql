-- Manual per-account blacklist ("rest an account") for the staff account-health
-- toggle (POST /internal/audit/account-blacklist).
--
-- When staff blacklist an account we stop NEW sends from it (the live send gate
-- classifyAccountBlock returns "manual", highest precedence) but KEEP its Instantly
-- daily_limit (max send) intact so already-queued emails still drain, and raise its
-- warmup daily volume to 50 to recover reputation. Re-allowing drops warmup back to
-- 10 and clears the flag. This is per-ACCOUNT (keyed on email), independent of the
-- domain-level BLOCKED_DOMAINS list and the derived status/warmup gates.
--
-- Additive: existing rows default manually_blacklisted=false ⇒ zero behavior change
-- on deploy until staff toggles one. See lib/account-blacklist.ts + CLAUDE.md.
ALTER TABLE "instantly_accounts"
  ADD COLUMN IF NOT EXISTS "manually_blacklisted" boolean DEFAULT false NOT NULL;
ALTER TABLE "instantly_accounts"
  ADD COLUMN IF NOT EXISTS "manually_blacklisted_at" timestamp with time zone;
