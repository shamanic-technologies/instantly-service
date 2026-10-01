-- Exactly-once claim for the MEETING-REQUEST email to the client
-- (lib/celebrate-positive-reply.ts). A prospect who asks for a call gets its own
-- dedicated email, even when an earlier reply on the same thread (an info request)
-- already took the general claim `positive_reply_forwarded_at`. A meeting request
-- that arrives FIRST takes both claims in one statement, so a later, calmer reply
-- on that thread sends nothing. Released only when the send itself fails.
ALTER TABLE "instantly_campaigns" ADD COLUMN IF NOT EXISTS "meeting_request_celebrated_at" timestamp;
