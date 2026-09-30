-- Exactly-once claim for an ESCALATION (lib/escalate-reply.ts): the responder
-- could not answer, so the thread is handed to a person — the brand's sales rep
-- in the thread itself, or the agency inbox when the brand named no rep. Set
-- atomically BEFORE anything is sent, so a second escalation call on the same
-- thread sends nothing and stops nothing twice. `escalation_handed_to` records
-- who now owns the conversation; while it is set the automated responder never
-- writes on this thread again (the reply route refuses it).
ALTER TABLE "instantly_campaigns" ADD COLUMN IF NOT EXISTS "escalated_at" timestamp;
ALTER TABLE "instantly_campaigns" ADD COLUMN IF NOT EXISTS "escalation_handed_to" text;
