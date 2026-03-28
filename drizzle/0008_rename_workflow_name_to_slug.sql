ALTER TABLE "instantly_campaigns" RENAME COLUMN "workflow_name" TO "workflow_slug";
DROP INDEX IF EXISTS "instantly_campaigns_workflow_name_idx";
CREATE INDEX "instantly_campaigns_workflow_slug_idx" ON "instantly_campaigns" USING btree ("workflow_slug");
