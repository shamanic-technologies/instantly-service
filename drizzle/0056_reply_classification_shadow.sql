-- Measurement: what BOTH engines said about the same inbound reply.
--
-- The reply classification is FROZEN at write time and a frozen wrong one stays
-- wrong forever, invisibly. The LLM returns a label and nothing else, so a reply
-- it hesitated over looks exactly like one it was certain about — which is why
-- we cannot decline to freeze: nothing ever tells us there was anything to
-- decline. chat-service's judgment route answers the same typed question with a
-- full probability distribution and a confidence, so this table records the pair
-- and lets the disagreement rate and the confidence distribution be measured on
-- real production replies.
--
-- ⚠️ NOTHING READS THIS TO DECIDE ANYTHING. The stored classification, the
-- forward to the agency inbox, the opt-out recording and every statistic are
-- exactly what the LLM said, unchanged. This table is the evidence for a LATER
-- decision (refuse to freeze a hesitant classification, route it to an unknown
-- state) — it is not that decision.
--
-- `llm_classification` null = the existing engine returned nothing usable.
-- `judgment_*` null WITH `error` set = the judgment engine failed, and the row
-- exists precisely so that failure is countable rather than silent.
-- `agreed` is null whenever either side is absent: an absence is not a
-- disagreement, and scoring it as one would inflate the number this table is
-- here to measure.
CREATE TABLE IF NOT EXISTS "reply_classification_shadow" (
  "id" text PRIMARY KEY NOT NULL,
  "instantly_campaign_id" text,
  "lead_email" text,
  "source" text NOT NULL,
  "llm_classification" text,
  "judgment_classification" text,
  "judgment_confidence" double precision,
  "judgment_probabilities" jsonb,
  "judgment_model" text,
  "judgment_input_tokens" integer,
  "agreed" boolean,
  "error" text,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL
);
CREATE INDEX IF NOT EXISTS "reply_classification_shadow_created_idx" ON "reply_classification_shadow" ("created_at");
CREATE INDEX IF NOT EXISTS "reply_classification_shadow_agreed_idx" ON "reply_classification_shadow" ("agreed");
