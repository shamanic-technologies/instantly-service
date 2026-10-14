-- A client's own email signature per brand (lib/brand-email-signature.ts,
-- GET/PUT/DELETE /orgs/email-signature). No row = our default signature.
CREATE TABLE IF NOT EXISTS "brand_email_signatures" (
  "org_id" text NOT NULL,
  "brand_id" text NOT NULL,
  "signature" text NOT NULL,
  "updated_by_user_id" text,
  "created_at" timestamp with time zone DEFAULT now() NOT NULL,
  "updated_at" timestamp with time zone DEFAULT now() NOT NULL,
  CONSTRAINT "brand_email_signatures_org_id_brand_id_pk" PRIMARY KEY ("org_id", "brand_id")
);
--> statement-breakpoint
-- The signature a thread was sent with, pinned at send. NULL = our default.
ALTER TABLE "instantly_campaigns" ADD COLUMN IF NOT EXISTS "signature_html" text;
