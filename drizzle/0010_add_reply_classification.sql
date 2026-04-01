-- Add reply_classification column to track lead interest status from Instantly webhooks
ALTER TABLE "instantly_campaigns" ADD COLUMN "reply_classification" text;
