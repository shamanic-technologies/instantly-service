CREATE TABLE IF NOT EXISTS "instantly_accounts" (
	"id" text PRIMARY KEY NOT NULL,
	"email" text NOT NULL,
	"warmup_enabled" boolean DEFAULT false NOT NULL,
	"status" text DEFAULT 'active' NOT NULL,
	"daily_send_limit" integer,
	"org_id" text,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL,
	CONSTRAINT "instantly_accounts_email_unique" UNIQUE("email")
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS "instantly_analytics_snapshots" (
	"id" text PRIMARY KEY NOT NULL,
	"campaign_id" text NOT NULL,
	"total_leads" integer DEFAULT 0 NOT NULL,
	"contacted" integer DEFAULT 0 NOT NULL,
	"opened" integer DEFAULT 0 NOT NULL,
	"replied" integer DEFAULT 0 NOT NULL,
	"bounced" integer DEFAULT 0 NOT NULL,
	"unsubscribed" integer DEFAULT 0 NOT NULL,
	"snapshot_at" timestamp NOT NULL,
	"raw_data" jsonb,
	"created_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS "instantly_campaigns" (
	"id" text PRIMARY KEY NOT NULL,
	"campaign_id" text,
	"lead_email" text,
	"instantly_campaign_id" text NOT NULL,
	"name" text NOT NULL,
	"status" text DEFAULT 'active' NOT NULL,
	"org_id" text,
	"clerk_org_id" text,
	"brand_id" text NOT NULL,
	"app_id" text NOT NULL,
	"run_id" text,
	"metadata" jsonb,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL,
	CONSTRAINT "instantly_campaigns_instantly_campaign_id_unique" UNIQUE("instantly_campaign_id")
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS "instantly_events" (
	"id" text PRIMARY KEY NOT NULL,
	"event_type" text NOT NULL,
	"campaign_id" text,
	"lead_email" text,
	"account_email" text,
	"step" integer,
	"variant" integer,
	"timestamp" timestamp NOT NULL,
	"raw_payload" jsonb NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS "instantly_leads" (
	"id" text PRIMARY KEY NOT NULL,
	"instantly_campaign_id" text NOT NULL,
	"email" text NOT NULL,
	"first_name" text,
	"last_name" text,
	"company_name" text,
	"custom_variables" jsonb,
	"status" text DEFAULT 'active' NOT NULL,
	"org_id" text,
	"run_id" text,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS "organizations" (
	"id" text PRIMARY KEY NOT NULL,
	"clerk_org_id" text NOT NULL,
	"name" text,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL,
	CONSTRAINT "organizations_clerk_org_id_unique" UNIQUE("clerk_org_id")
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS "sequence_costs" (
	"id" text PRIMARY KEY NOT NULL,
	"campaign_id" text NOT NULL,
	"lead_email" text NOT NULL,
	"step" integer NOT NULL,
	"run_id" text NOT NULL,
	"cost_id" text NOT NULL,
	"status" text DEFAULT 'provisioned' NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE IF NOT EXISTS "users" (
	"id" text PRIMARY KEY NOT NULL,
	"clerk_user_id" text NOT NULL,
	"organization_id" text NOT NULL,
	"email" text,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL,
	CONSTRAINT "users_clerk_user_id_unique" UNIQUE("clerk_user_id")
);
--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "instantly_campaigns_campaign_lead_idx" ON "instantly_campaigns" USING btree ("campaign_id","lead_email");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "instantly_campaigns_campaign_id_idx" ON "instantly_campaigns" USING btree ("campaign_id");--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "instantly_leads_campaign_email_idx" ON "instantly_leads" USING btree ("instantly_campaign_id","email");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "sequence_costs_campaign_lead_idx" ON "sequence_costs" USING btree ("campaign_id","lead_email");--> statement-breakpoint
CREATE UNIQUE INDEX IF NOT EXISTS "sequence_costs_cost_id_idx" ON "sequence_costs" USING btree ("cost_id");