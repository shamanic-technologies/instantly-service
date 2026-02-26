-- Migration: Remove Clerk ID columns and mapping tables
-- clerkOrgId/clerkUserId replaced by orgId/userId (client-service UUIDs)

ALTER TABLE "instantly_campaigns" DROP COLUMN IF EXISTS "clerk_org_id";

DROP TABLE IF EXISTS "organizations";
DROP TABLE IF EXISTS "users";
