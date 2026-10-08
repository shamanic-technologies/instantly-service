-- Exactly-once claim for the "answer this reply yourself" email to the client
-- (lib/ask-client-to-answer.ts). Sent when a celebrated positive reply lands on
-- a brand with no AI responder running, so nobody else will answer it. Set
-- BEFORE the send; released only when no member could be emailed.
ALTER TABLE "instantly_campaigns" ADD COLUMN IF NOT EXISTS "client_answer_requested_at" timestamp;
