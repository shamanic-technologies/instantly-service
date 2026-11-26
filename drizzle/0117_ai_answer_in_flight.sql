-- ESCALATION-AND-AI-REPLY-1126: one path per reply. An automated answer marks
-- the thread BEFORE it is prepared (`ai_answer_started_at`, atomically, only
-- while no escalation is claimed) and clears the mark once it went out or
-- failed. The escalation claim reads the mark in the same row lock: an answer
-- in flight is waited on, and the brand's team is then told the AI answered
-- (its words quoted) instead of "our AI paused". Before this, the reply gate
-- was a plain SELECT: Living Vital 2026-11-09, the escalation claimed at
-- 15:30:12.5 and the AI's own answer left 1.4 s later.
ALTER TABLE "instantly_campaigns" ADD COLUMN IF NOT EXISTS "ai_answer_started_at" timestamp;
