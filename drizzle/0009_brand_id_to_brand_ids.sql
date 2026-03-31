-- Migrate brand_id (single text) to brand_ids (text array) for multi-brand support
ALTER TABLE "instantly_campaigns" ADD COLUMN "brand_ids" text[];
UPDATE "instantly_campaigns" SET "brand_ids" = ARRAY["brand_id"];
ALTER TABLE "instantly_campaigns" ALTER COLUMN "brand_ids" SET NOT NULL;
ALTER TABLE "instantly_campaigns" DROP COLUMN "brand_id";
DROP INDEX IF EXISTS "instantly_campaigns_brand_id_idx";
CREATE INDEX "instantly_campaigns_brand_ids_idx" ON "instantly_campaigns" USING gin ("brand_ids");
