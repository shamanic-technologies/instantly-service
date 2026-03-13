ALTER TABLE "instantly_campaigns" ADD COLUMN "workflow_name" text;
CREATE INDEX "instantly_campaigns_workflow_name_idx" ON "instantly_campaigns" USING btree ("workflow_name");
