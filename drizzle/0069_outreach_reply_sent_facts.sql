-- The outreach fact feed states every email we sent into a prospect's thread
-- outside the sequence (`reply_sent`, lib/outreach-facts), once per message.
CREATE UNIQUE INDEX IF NOT EXISTS "outreach_facts_reply_sent_once_idx"
  ON "outreach_facts" ("subject_key")
  WHERE "type" = 'reply_sent';
--> statement-breakpoint
-- Its sources, each read whole every tick through a small partial index:
-- our replies (step 0) and the answers typed into Instantly's Unibox (ue_type 3).
CREATE INDEX IF NOT EXISTS "smtp_dispatch_raw_replies_idx"
  ON "smtp_dispatch_raw" ("instantly_campaign_id")
  WHERE "step" = 0;
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "instantly_emails_raw_manual_sent_idx"
  ON "instantly_emails_raw" ("instantly_campaign_id")
  WHERE "payload"->>'ue_type' = '3';
