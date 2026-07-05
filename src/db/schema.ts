import {
  pgTable,
  text,
  timestamp,
  jsonb,
  boolean,
  integer,
  uniqueIndex,
  index,
  primaryKey,
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
    // A row is RESERVED (atomic claim on the (campaignId, leadEmail) unique
    // index) BEFORE the external Instantly campaign exists, carrying a unique
    // `reserving:<uuid>` sentinel here, then phase-2 updated with the real id.
    // A value matching `reserving:%` is the "reservation in flight" marker —
    // see POST /send in src/routes/send.ts. Column stays notNull+unique: the
    // sentinel is unique per reservation, so readers never see a bare NULL.
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
    // Reservation arbiter for NON-platform sends — see POST /send. campaignId is
    // non-null here, so (campaignId, leadEmail) collides on a timeout-retry.
    uniqueIndex("instantly_campaigns_campaign_lead_idx").on(
      table.campaignId,
      table.leadEmail,
    ),
    // Reservation arbiter for PLATFORM sends (campaignId IS NULL). Postgres treats
    // NULLs as DISTINCT in a unique index, so (campaignId, leadEmail) NEVER
    // collides when campaignId is null → every email-gateway timeout-retry used to
    // create a fresh duplicate campaign. This partial unique index keys on
    // (run_id, leadEmail) instead — the retry forwards the same x-run-id, so it is
    // the stable idempotency key — scoped to status='active' so it covers exactly
    // the live reservation/in-flight window and never collides with already
    // paused/completed historical duplicates. Defined in migration
    // 0020_platform_send_dedupe.sql (drizzle-kit does not track partial indexes,
    // same convention as instantly_events_one_shot_dedupe_idx); send.ts targets it
    // via onConflictDoUpdate when campaignId is null. Do NOT drop it on a
    // db:generate diff.
    //   UNIQUE (run_id, lead_email) WHERE campaign_id IS NULL AND status = 'active'
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
// Silver: current-state projection of every Instantly sending account.
//
// The `lifecycle_*` columns hold the auto-derived per-account LIFECYCLE (see
// lib/account-lifecycle.ts). The health snapshot columns (instantly_status /
// warmup_score / daily_limit / provider_code) + first/last name are refreshed by
// the accounts-sync (POST /internal/audit/accounts-sync). The live send gate
// reads `lifecycle_status = 'in_production'` from THIS table (no live listAccounts
// on the hot path); reconcileLifecycle recomputes lifecycle_status from these
// snapshot columns + the latest placement delivery + instantly_domain_policy.
export const instantlyAccounts = pgTable("instantly_accounts", {
  id: text("id").primaryKey().$defaultFn(() => crypto.randomUUID()),
  email: text("email").notNull().unique(),
  warmupEnabled: boolean("warmup_enabled").notNull().default(false),
  status: text("status").notNull().default("active"),
  dailySendLimit: integer("daily_send_limit"),
  orgId: text("org_id"),
  // ── Health snapshot (from the accounts-sync — mirrors the Instantly account) ──
  instantlyStatus: integer("instantly_status"),
  warmupScore: integer("warmup_score"),
  dailyLimit: integer("daily_limit"),
  providerCode: integer("provider_code"),
  firstName: text("first_name"),
  lastName: text("last_name"),
  // ── Lifecycle (auto-derived; projection of the latest lifecycle event) ────────
  // One of: in_production | in_recovery | deactivated_by_instantly |
  // deactivated_by_user. Null until the first reconcileLifecycle classifies it.
  lifecycleStatus: text("lifecycle_status"),
  lifecycleReason: text("lifecycle_reason"),
  lifecycleUpdatedAt: timestamp("lifecycle_updated_at", { withTimezone: true }),
  createdAt: timestamp("created_at").defaultNow().notNull(),
  updatedAt: timestamp("updated_at").defaultNow().notNull(),
});

// Bronze: periodic full snapshot of Instantly GET /accounts (append-only, never
// mutated). One row per (account, fetch) — gives health / daily_limit HISTORY,
// the raw material for the capacity-over-time reconstruction.
export const instantlyAccountsRaw = pgTable(
  "instantly_accounts_raw",
  {
    id: text("id").primaryKey().$defaultFn(() => crypto.randomUUID()),
    accountEmail: text("account_email").notNull(),
    status: integer("status"),
    warmupScore: integer("warmup_score"),
    dailyLimit: integer("daily_limit"),
    providerCode: integer("provider_code"),
    payload: jsonb("payload").notNull(),
    fetchedAt: timestamp("fetched_at").defaultNow().notNull(),
  },
  (table) => [
    index("instantly_accounts_raw_email_idx").on(table.accountEmail),
    index("instantly_accounts_raw_fetched_at_idx").on(table.fetchedAt),
  ],
);

// Bronze: one row per lifecycle TRANSITION (append-only audit trail). Joined with
// instantly_accounts_raw daily_limit history to reconstruct in_production capacity
// for any past day. `from_status` is null on an account's first classification.
export const instantlyAccountLifecycleEvents = pgTable(
  "instantly_account_lifecycle_events",
  {
    id: text("id").primaryKey().$defaultFn(() => crypto.randomUUID()),
    accountEmail: text("account_email").notNull(),
    fromStatus: text("from_status"),
    toStatus: text("to_status").notNull(),
    reason: text("reason").notNull(),
    healthScore: integer("health_score"),
    deliveryPct: integer("delivery_pct"),
    dailyLimit: integer("daily_limit"),
    createdAt: timestamp("created_at").defaultNow().notNull(),
  },
  (table) => [
    index("instantly_account_lifecycle_events_email_created_idx").on(
      table.accountEmail,
      table.createdAt,
    ),
  ],
);

// Silver/config: brand/product domains. Any account whose email domain is here →
// deactivated_by_user (never auto-promoted). Lives in the DB (NOT a code
// constant) so ops can add a brand domain without a deploy. The legacy shared-IP
// fleet is deliberately NOT listed — it is handled by delivery < 100 → in_recovery.
export const instantlyDomainPolicy = pgTable("instantly_domain_policy", {
  domain: text("domain").primaryKey(),
  reason: text("reason").notNull().default("brand"),
  note: text("note"),
  createdAt: timestamp("created_at").defaultNow().notNull(),
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
    // Covering index for the gold stats aggregates. The filtered /orgs/stats
    // path joins events->campaigns on campaign_id then filters/counts by
    // event_type, lead_email and step; this composite lets Postgres do an
    // index-only scan per matched campaign (validated: nested-loop index-only
    // scan instead of a heap fetch). It does NOT help the no-filter
    // /public/stats path (the planner seq-scans everything anyway) — that path
    // is handled by the in-memory TTL cache instead.
    index("instantly_events_stats_covering_idx").on(
      table.campaignId,
      table.eventType,
      table.leadEmail,
      table.step,
    ),
  ],
);

// Gold: current delivery/status projection for the hot /orgs/status read path.
//
// Rebuildable from silver (`instantly_campaigns` + `instantly_events`) and kept
// fresh by the promotion paths. One row = one current campaign/lead status.
export const instantlyLeadStatusCurrent = pgTable(
  "instantly_lead_status_current",
  {
    orgId: text("org_id").notNull(),
    campaignId: text("campaign_id"),
    instantlyCampaignId: text("instantly_campaign_id").notNull(),
    leadEmail: text("lead_email").notNull(),
    brandIds: text("brand_ids").array().notNull(),
    contacted: boolean("contacted").notNull().default(true),
    sent: boolean("sent").notNull().default(false),
    delivered: boolean("delivered").notNull().default(false),
    opened: boolean("opened").notNull().default(false),
    clicked: boolean("clicked").notNull().default(false),
    replied: boolean("replied").notNull().default(false),
    replyClassification: text("reply_classification"),
    bounced: boolean("bounced").notNull().default(false),
    unsubscribed: boolean("unsubscribed").notNull().default(false),
    cancelled: boolean("cancelled").notNull().default(false),
    lastDeliveredAt: timestamp("last_delivered_at"),
    firstContactedAt: timestamp("first_contacted_at"),
    firstSentAt: timestamp("first_sent_at"),
    firstDeliveredAt: timestamp("first_delivered_at"),
    firstOpenedAt: timestamp("first_opened_at"),
    firstClickedAt: timestamp("first_clicked_at"),
    firstRepliedAt: timestamp("first_replied_at"),
    firstBouncedAt: timestamp("first_bounced_at"),
    firstUnsubscribedAt: timestamp("first_unsubscribed_at"),
    createdAt: timestamp("created_at").defaultNow().notNull(),
    updatedAt: timestamp("updated_at").defaultNow().notNull(),
  },
  (table) => [
    primaryKey({
      name: "instantly_lead_status_current_pk",
      columns: [table.instantlyCampaignId, table.leadEmail],
    }),
    index("instantly_lead_status_current_org_email_idx").on(
      table.orgId,
      table.leadEmail,
    ),
    index("instantly_lead_status_current_org_campaign_email_idx").on(
      table.orgId,
      table.campaignId,
      table.leadEmail,
    ),
    index("instantly_lead_status_current_brand_ids_idx").using("gin", table.brandIds),
  ],
);

// Sequence costs table — tracks provisioned/actual/cancelled cost items per lead step
export const sequenceCosts = pgTable(
  "sequence_costs",
  {
    id: text("id").primaryKey().$defaultFn(() => crypto.randomUUID()),
    campaignId: text("campaign_id"),
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

// ─── Inbox-placement (deliverability) — Bronze / Silver ─────────────────────
// Instantly inbox-placement TESTS are the only source of real per-account inbox
// vs spam vs missing data (the V2 API exposes no standing per-account placement
// field). A test is a point-in-time event; the recurring sync captures each test
// + its analytics rows in bronze (append-only) and promotes them to a silver
// per-(test, account, ESP) result. Gold (account-health) reads the latest test
// per account. See lib/placement-promote.ts + CLAUDE.md "Inbox-placement history".

// Bronze A: inbox-placement test objects we created / observed (one row per test).
export const instantlyPlacementTestsRaw = pgTable(
  "instantly_placement_tests_raw",
  {
    id: text("id").primaryKey().$defaultFn(() => crypto.randomUUID()),
    // Instantly's inbox-placement-test UUID.
    testId: text("test_id").notNull(),
    // Our ptid_ test_code marker (identifies tests this service created).
    testCode: text("test_code"),
    payload: jsonb("payload").notNull(),
    fetchedAt: timestamp("fetched_at").defaultNow().notNull(),
  },
  (table) => [
    uniqueIndex("instantly_placement_tests_raw_test_id_idx").on(table.testId),
    index("instantly_placement_tests_raw_fetched_at_idx").on(table.fetchedAt),
  ],
);

// Bronze B: raw inbox-placement-analytics rows (one per (test, sender, recipient)).
// Dedupe on Instantly's analytics row id so re-polls of the same test are idempotent.
export const instantlyPlacementAnalyticsRaw = pgTable(
  "instantly_placement_analytics_raw",
  {
    id: text("id").primaryKey().$defaultFn(() => crypto.randomUUID()),
    analyticsId: text("analytics_id").notNull(),
    testId: text("test_id").notNull(),
    payload: jsonb("payload").notNull(),
    fetchedAt: timestamp("fetched_at").defaultNow().notNull(),
  },
  (table) => [
    uniqueIndex("instantly_placement_analytics_raw_analytics_id_idx").on(table.analyticsId),
    index("instantly_placement_analytics_raw_test_id_idx").on(table.testId),
  ],
);

// Silver: canonical placement result per (test, sending account, recipient ESP).
// Aggregated across the seed recipients of that (test, account, ESP): inbox /
// spam / missing counts + percentages, plus representative auth-pass flags.
// `tested_at` is the test's run timestamp. Gold reads the latest test per account
// (DISTINCT ON account_email ORDER BY tested_at DESC) and blends across ESP.
export const instantlyPlacementResults = pgTable(
  "instantly_placement_results",
  {
    testId: text("test_id").notNull(),
    accountEmail: text("account_email").notNull(),
    // Recipient ESP enum from Instantly (1=Google, 2=Outlook, 12/13=others).
    recipientEsp: integer("recipient_esp").notNull(),
    testedAt: timestamp("tested_at").notNull(),
    seedTotal: integer("seed_total").notNull(),
    inboxCount: integer("inbox_count").notNull(),
    spamCount: integer("spam_count").notNull(),
    missingCount: integer("missing_count").notNull(),
    inboxPct: integer("inbox_pct").notNull(),
    spamPct: integer("spam_pct").notNull(),
    missingPct: integer("missing_pct").notNull(),
    spfPass: boolean("spf_pass"),
    dkimPass: boolean("dkim_pass"),
    dmarcPass: boolean("dmarc_pass"),
    createdAt: timestamp("created_at").defaultNow().notNull(),
  },
  (table) => [
    primaryKey({ columns: [table.testId, table.accountEmail, table.recipientEsp] }),
    index("instantly_placement_results_account_tested_idx").on(
      table.accountEmail,
      table.testedAt,
    ),
  ],
);

// ─── Reconcile snapshot (Instantly-side counts cache) ───────────────────────
// GET /internal/audit/reconcile compares OUR live local counts against
// INSTANTLY's counts. The Instantly side requires a fleet-wide THROTTLED API
// sweep (`listAllCampaignAnalytics` + `listAllCampaignSequenceLengths`, the
// latter paginating `/campaigns` across thousands of campaigns at ~110ms/page)
// that takes MINUTES — far past the gateway/browser timeout, so doing it
// synchronously in the request left the dashboard on an infinite skeleton.
// The Instantly side is therefore PRE-AGGREGATED here by a background refresh
// (POST /internal/audit/reconcile/refresh + on-read stale-while-revalidate), and
// the GET reads this single row in one fast query. Single-row table keyed on a
// fixed sentinel id ('singleton'); the refresh upserts it. Fail loud (503) when
// absent — never fabricate an Instantly number. See lib/reconcile-snapshot.ts +
// CLAUDE.md "Reconciliation audit".
export const instantlyReconcileSnapshot = pgTable("instantly_reconcile_snapshot", {
  // Fixed sentinel — exactly one row. See RECONCILE_SNAPSHOT_ID.
  id: text("id").primaryKey(),
  activeCampaigns: integer("active_campaigns").notNull(),
  emailsSent: integer("emails_sent").notNull(),
  contactedDispatched: integer("contacted_dispatched").notNull(),
  contactsStored: integer("contacts_stored").notNull(),
  pendingSends: integer("pending_sends").notNull(),
  refreshedAt: timestamp("refreshed_at").defaultNow().notNull(),
});
