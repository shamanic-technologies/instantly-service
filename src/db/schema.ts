import {
  pgTable,
  text,
  timestamp,
  jsonb,
  boolean,
  integer,
  uniqueIndex,
} from "drizzle-orm/pg-core";

// Campaigns table
export const instantlyCampaigns = pgTable("instantly_campaigns", {
  id: text("id").primaryKey().$defaultFn(() => crypto.randomUUID()),
  instantlyCampaignId: text("instantly_campaign_id").notNull().unique(),
  name: text("name").notNull(),
  status: text("status").notNull().default("active"),
  orgId: text("org_id").notNull(),
  runId: text("run_id"),
  metadata: jsonb("metadata"),
  createdAt: timestamp("created_at").defaultNow().notNull(),
  updatedAt: timestamp("updated_at").defaultNow().notNull(),
});

// Leads table
export const instantlyLeads = pgTable(
  "instantly_leads",
  {
    id: text("id").primaryKey().$defaultFn(() => crypto.randomUUID()),
    instantlyCampaignId: text("instantly_campaign_id").notNull(),
    email: text("email").notNull(),
    firstName: text("first_name"),
    lastName: text("last_name"),
    companyName: text("company_name"),
    customVariables: jsonb("custom_variables"),
    status: text("status").notNull().default("active"),
    orgId: text("org_id").notNull(),
    runId: text("run_id"),
    createdAt: timestamp("created_at").defaultNow().notNull(),
    updatedAt: timestamp("updated_at").defaultNow().notNull(),
  },
  (table) => [
    uniqueIndex("instantly_leads_campaign_email_idx").on(
      table.instantlyCampaignId,
      table.email
    ),
  ]
);

// Email accounts table
export const instantlyAccounts = pgTable("instantly_accounts", {
  id: text("id").primaryKey().$defaultFn(() => crypto.randomUUID()),
  email: text("email").notNull().unique(),
  warmupEnabled: boolean("warmup_enabled").notNull().default(false),
  status: text("status").notNull().default("active"),
  dailySendLimit: integer("daily_send_limit"),
  orgId: text("org_id"),
  createdAt: timestamp("created_at").defaultNow().notNull(),
  updatedAt: timestamp("updated_at").defaultNow().notNull(),
});

// Events table (webhooks)
export const instantlyEvents = pgTable("instantly_events", {
  id: text("id").primaryKey().$defaultFn(() => crypto.randomUUID()),
  eventType: text("event_type").notNull(),
  campaignId: text("campaign_id"),
  leadEmail: text("lead_email"),
  accountEmail: text("account_email"),
  timestamp: timestamp("timestamp").notNull(),
  rawPayload: jsonb("raw_payload").notNull(),
  createdAt: timestamp("created_at").defaultNow().notNull(),
});

// Analytics snapshots table
export const instantlyAnalyticsSnapshots = pgTable("instantly_analytics_snapshots", {
  id: text("id").primaryKey().$defaultFn(() => crypto.randomUUID()),
  campaignId: text("campaign_id").notNull(),
  totalLeads: integer("total_leads").notNull().default(0),
  contacted: integer("contacted").notNull().default(0),
  opened: integer("opened").notNull().default(0),
  replied: integer("replied").notNull().default(0),
  bounced: integer("bounced").notNull().default(0),
  unsubscribed: integer("unsubscribed").notNull().default(0),
  snapshotAt: timestamp("snapshot_at").notNull(),
  rawData: jsonb("raw_data"),
  createdAt: timestamp("created_at").defaultNow().notNull(),
});
