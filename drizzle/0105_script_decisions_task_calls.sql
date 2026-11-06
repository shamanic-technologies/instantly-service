-- MIGRATE-1105 S2b: one outcome can be made through two decisions of this service (reply_received: each poll batch's
-- filing, task `inbound-decision`, and the orphan-reply judgment, task `orphan-judgment`). `task` names the decision
-- (the Engine's platform run task); `calls` = the vendor calls OUR decision made, in the script's words (the orphan
-- judgment's `jev.choice`), so a recorded decision replays with its answers. NULL on rows recorded before.
ALTER TABLE "script_decisions" ADD COLUMN IF NOT EXISTS "task" text;
--> statement-breakpoint
ALTER TABLE "script_decisions" ADD COLUMN IF NOT EXISTS "calls" jsonb;
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "script_decisions_outcome_task_seq_idx" ON "script_decisions" ("outcome", "task", "seq");
