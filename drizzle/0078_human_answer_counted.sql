-- REPLY-ANY-LEAD-1015: a message the brand's person typed by hand on this
-- thread was counted as one of OUR touches (lead-service `acted`, next follow-up
-- date from the usual ladder). The takeover gate ignores every human answer at or
-- before this instant (lib/count-human-answer.ts). NULL = nothing counted yet.
ALTER TABLE "instantly_campaigns" ADD COLUMN IF NOT EXISTS "human_answer_counted_at" timestamp with time zone;
