import {
  pgTable,
  text,
  timestamp,
  jsonb,
  boolean,
  integer,
  uniqueIndex,
  index,
} from "drizzle-orm/pg-core";
import { sql } from "drizzle-orm";

// Campaigns table
// Each POST /send creates its own row (one Instantly campaign per lead).
// `campaignId` groups sub-campaigns that belong to the same logical campaign.
export const instantlyCampaigns = pgTable(
  "instantly_campaigns",
  {
    id: text("id").primaryKey().$defaultFn(() => crypto.randomUUID()),
    campaignId: text("campaign_id"),
    leadEmail: text("lead_email"),
    instantlyCampaignId: text("instantly_campaign_id").notNull().unique(),
    name: text("name").notNull(),
    status: text("status").notNull().default("active"),
    orgId: text("org_id"),
    userId: text("user_id"),
    brandIds: text("brand_ids").array().notNull(),
    workflowSlug: text("workflow_slug"),
    featureSlug: text("feature_slug"),
    runId: text("run_id"),
    leadId: text("lead_id"),
    // 4-stage funnel:
    //   contacted   = lead pushed to Instantly (POST /send success — DEFAULT)
    //   sent        = Instantly dispatched at least one email (webhook email_sent)
    //   delivered   = derived in queries (sent AND NOT bounced); never stored
    //   bounced / replied / unsubscribed = terminal markers from webhooks
    //   failed      = push to Instantly errored (campaign-error-handler)
    //   cancelled   = retry-stuck job determined the row is unretriable (parent
    //                 run gone, key gone, no sequence, no local lead, etc.) and
    //                 terminally killed the campaign + cancelled costs
    deliveryStatus: text("delivery_status").notNull().default("contacted"),
    replyClassification: text("reply_classification"),
    // Instantly's per-campaign pacing diagnostic. NULL = no pacing constraint
    // observed at last reconcile. Non-NULL values 1..4 are transient pacing
    // states that resolve naturally (out of sending schedule, daily quota hit,
    // etc. — see Instantly API docs). 99 = generic error. Stored for /stats
    // observability only — never treated as an error signal in send-time
    // dispatch or retry-stuck selection.
    notSendingStatus: integer("not_sending_status"),
    notSendingStatusSeenAt: timestamp("not_sending_status_seen_at"),
    // Source of `reply_classification`. 'auto' = derived from Instantly webhook
    // event; 'manual' = set via human qualification (POST /orgs/manual-qualifications).
    // Manual wins: silver-promote skips webhook-driven updates when this is 'manual'.
    replyClassificationSource: text("reply_classification_source").notNull().default("auto"),
    metadata: jsonb("metadata"),
    createdAt: timestamp("created_at").defaultNow().notNull(),
    updatedAt: timestamp("updated_at").defaultNow().notNull(),
  },
  (table) => [
    uniqueIndex("instantly_campaigns_campaign_lead_idx").on(
      table.campaignId,
      table.leadEmail,
    ),
    index("instantly_campaigns_campaign_id_idx").on(table.campaignId),
    index("instantly_campaigns_lead_id_idx").on(table.leadId),
    index("instantly_campaigns_lead_email_idx").on(table.leadEmail),
    index("instantly_campaigns_brand_ids_idx").using("gin", table.brandIds),
    index("instantly_campaigns_org_id_idx").on(table.orgId),
    index("instantly_campaigns_run_id_idx").on(table.runId),
    index("instantly_campaigns_workflow_slug_idx").on(table.workflowSlug),
  ],
);

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
    orgId: text("org_id"),
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

// Silver: canonical event log derived from bronze sources (webhooks + reconcile polls)
// and deterministic inference. `raw_payload` is nullable for backwards compat; new
// rows store source attribution pointing to bronze instead.
//
// Indexes:
//   - `instantly_events_dedupe_idx` — primary dedupe across (campaign, lead, event_type,
//     timestamp, step). Used by repeatable events (opens, clicks) which can fire many
//     times per step at different timestamps.
//   - `instantly_events_one_shot_dedupe_idx` — partial unique index for events that are
//     at-most-1 per (campaign, lead, event_type, step), regardless of timestamp. Enables
//     UPSERT semantics so a real webhook arriving after a synthetic inference can upgrade
//     the row (`inferred=true` → `inferred=false`, real timestamp wins).
//
// Inference columns:
//   - `inferred` — true if synthesized from a strong-implication rule (opened ⇒ sent, etc.)
//   - `inferred_from_event_id` — silver id of the event that triggered the inference
//   - `inferred_rule` — rule name (e.g. `opened_implies_sent`, `sent_cascade`) for audit
export const instantlyEvents = pgTable(
  "instantly_events",
  {
    id: text("id").primaryKey().$defaultFn(() => crypto.randomUUID()),
    eventType: text("event_type").notNull(),
    campaignId: text("campaign_id"),
    leadEmail: text("lead_email"),
    accountEmail: text("account_email"),
    step: integer("step"),
    variant: integer("variant"),
    timestamp: timestamp("timestamp").notNull(),
    rawPayload: jsonb("raw_payload"),
    source: text("source").notNull().default("webhook"),
    sourceRowId: text("source_row_id"),
    inferred: boolean("inferred").notNull().default(false),
    inferredFromEventId: text("inferred_from_event_id"),
    inferredRule: text("inferred_rule"),
    createdAt: timestamp("created_at").defaultNow().notNull(),
  },
  (table) => [
    index("instantly_events_campaign_id_idx").on(table.campaignId),
    index("instantly_events_event_type_idx").on(table.eventType),
    index("instantly_events_lead_email_idx").on(table.leadEmail),
  ],
);

// Sequence costs table — tracks provisioned/actual/cancelled cost items per lead step
export const sequenceCosts = pgTable(
  "sequence_costs",
  {
    id: text("id").primaryKey().$defaultFn(() => crypto.randomUUID()),
    campaignId: text("campaign_id").notNull(),
    leadEmail: text("lead_email").notNull(),
    step: integer("step").notNull(),
    runId: text("run_id").notNull(),
    costId: text("cost_id").notNull(),
    status: text("status").notNull().default("provisioned"),
    createdAt: timestamp("created_at").defaultNow().notNull(),
    updatedAt: timestamp("updated_at").defaultNow().notNull(),
  },
  (table) => [
    index("sequence_costs_campaign_lead_idx").on(
      table.campaignId,
      table.leadEmail,
    ),
    uniqueIndex("sequence_costs_cost_id_idx").on(table.costId),
  ],
);

// ─── Bronze tables (raw external sources, append-only, never mutated) ─────────

// Bronze 1: webhook payloads received from Instantly
export const instantlyWebhookPayloadsRaw = pgTable(
  "instantly_webhook_payloads_raw",
  {
    id: text("id").primaryKey().$defaultFn(() => crypto.randomUUID()),
    orgId: text("org_id"),
    instantlyCampaignId: text("instantly_campaign_id").notNull(),
    payload: jsonb("payload").notNull(),
    receivedAt: timestamp("received_at").defaultNow().notNull(),
  },
  (table) => [
    index("instantly_webhook_payloads_raw_campaign_id_idx").on(table.instantlyCampaignId),
    index("instantly_webhook_payloads_raw_received_at_idx").on(table.receivedAt),
  ],
);

// Bronze 2: /campaigns/analytics responses (per-campaign aggregate snapshots)
export const instantlyAnalyticsRaw = pgTable(
  "instantly_analytics_raw",
  {
    id: text("id").primaryKey().$defaultFn(() => crypto.randomUUID()),
    orgId: text("org_id"),
    instantlyCampaignId: text("instantly_campaign_id").notNull(),
    payload: jsonb("payload").notNull(),
    fetchedAt: timestamp("fetched_at").defaultNow().notNull(),
  },
  (table) => [
    index("instantly_analytics_raw_campaign_id_idx").on(table.instantlyCampaignId),
    index("instantly_analytics_raw_fetched_at_idx").on(table.fetchedAt),
  ],
);

// Bronze 3: /emails records (individual email rows with step field)
export const instantlyEmailsRaw = pgTable(
  "instantly_emails_raw",
  {
    id: text("id").primaryKey().$defaultFn(() => crypto.randomUUID()),
    orgId: text("org_id"),
    instantlyCampaignId: text("instantly_campaign_id").notNull(),
    instantlyEmailId: text("instantly_email_id").notNull(),
    payload: jsonb("payload").notNull(),
    fetchedAt: timestamp("fetched_at").defaultNow().notNull(),
  },
  (table) => [
    uniqueIndex("instantly_emails_raw_email_id_idx").on(table.instantlyEmailId),
    index("instantly_emails_raw_campaign_id_idx").on(table.instantlyCampaignId),
  ],
);

// Bronze 4: /leads/list per-lead snapshots (status + engagement counts)
export const instantlyLeadsRaw = pgTable(
  "instantly_leads_raw",
  {
    id: text("id").primaryKey().$defaultFn(() => crypto.randomUUID()),
    orgId: text("org_id"),
    instantlyCampaignId: text("instantly_campaign_id").notNull(),
    leadEmail: text("lead_email").notNull(),
    payload: jsonb("payload").notNull(),
    fetchedAt: timestamp("fetched_at").defaultNow().notNull(),
  },
  (table) => [
    index("instantly_leads_raw_campaign_email_idx").on(table.instantlyCampaignId, table.leadEmail),
    index("instantly_leads_raw_fetched_at_idx").on(table.fetchedAt),
  ],
);

// Bronze 5: GET /campaigns/{id} responses — full campaign config snapshots.
// Reconciler writes one row per campaign per cycle. Used to derive
// `instantly_campaigns.not_sending_status` (Instantly diagnostic).
export const instantlyCampaignsConfigRaw = pgTable(
  "instantly_campaigns_config_raw",
  {
    id: text("id").primaryKey().$defaultFn(() => crypto.randomUUID()),
    orgId: text("org_id"),
    instantlyCampaignId: text("instantly_campaign_id").notNull(),
    payload: jsonb("payload").notNull(),
    fetchedAt: timestamp("fetched_at").defaultNow().notNull(),
  },
  (table) => [
    index("instantly_campaigns_config_raw_campaign_id_idx").on(table.instantlyCampaignId),
    index("instantly_campaigns_config_raw_fetched_at_idx").on(table.fetchedAt),
  ],
);

// Bronze 6: manual reply qualifications set by human users via POST /orgs/manual-qualifications.
// External-to-pipeline (UI action), append-only. Source-of-truth for "what the human
// said about a lead's reply". Resolved (org_id, campaign_id, lead_email) identifier;
// instantly_campaign_id stored at insertion time for direct join with silver tables.
export const instantlyManualQualificationsRaw = pgTable(
  "instantly_manual_qualifications_raw",
  {
    id: text("id").primaryKey().$defaultFn(() => crypto.randomUUID()),
    orgId: text("org_id").notNull(),
    campaignId: text("campaign_id").notNull(),
    instantlyCampaignId: text("instantly_campaign_id").notNull(),
    leadEmail: text("lead_email").notNull(),
    status: text("status").notNull(),
    qualifiedBy: text("qualified_by").notNull(),
    notes: text("notes"),
    payload: jsonb("payload").notNull(),
    qualifiedAt: timestamp("qualified_at").defaultNow().notNull(),
  },
  (table) => [
    index("instantly_manual_qualifications_raw_org_campaign_email_idx").on(
      table.orgId,
      table.campaignId,
      table.leadEmail,
    ),
    index("instantly_manual_qualifications_raw_instantly_campaign_email_idx").on(
      table.instantlyCampaignId,
      table.leadEmail,
    ),
    index("instantly_manual_qualifications_raw_qualified_at_idx").on(table.qualifiedAt),
  ],
);
