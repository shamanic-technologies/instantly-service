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
    // The Instantly sending account chosen for this lead at send/redispatch time
    // (send.ts phase-2, retry-stuck redispatch). Persisted here so per-account
    // load (queue size) is known the instant the row is `contacted` — the chosen
    // account is already in hand at write time, so deriving it from the first
    // observed `email_sent` webhook (which lags by minutes) is unnecessary. NULL
    // on historical rows written before this column existed; readers COALESCE to
    // the observed-send attribution for those. See account-sending-stats.ts.
    accountEmail: text("account_email"),
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
    // Exactly-once claim for the "forward positive reply to the agency inbox"
    // side effect (lib/forward-positive-reply.ts). Set atomically BEFORE the
    // forward send when Instantly qualifies a reply positive; a webhook retry /
    // reconcile re-poll / re-qualification finds it non-null and skips. Released
    // back to NULL only if the send itself fails, so a later retry re-attempts.
    // NULL = the positive reply for this lead has never been forwarded.
    positiveReplyForwardedAt: timestamp("positive_reply_forwarded_at"),
    // Which pipe dispatches THIS lead's sequence: 'instantly' (default) or 'smtp'
    // (our own sender). FROZEN at send time from the chosen account's policy
    // column, never re-read from the account afterwards — a sequence spans days,
    // so following the live account policy would re-route a lead's followups
    // mid-flight when an operator flips that mailbox, and a lead already pushed
    // to Instantly has no local step bodies to send from, so its followups would
    // simply stop. Same persist-at-write reasoning as `account_email` (0025).
    // See src/lib/self-send/transport.ts.
    sendTransport: text("send_transport").notNull().default("instantly"),
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
    // NOTE: a second, hand-written EXPRESSION index on `lower(lead_email)`
    // exists in migration 0040 — drizzle has no expression-index form, so it
    // cannot be declared here. It serves the per-brand re-contact-window
    // lookup (src/lib/recontact-window.ts), which normalizes the prospect's
    // address the same way the serve path does. Do NOT drop it on a
    // `db:generate` diff.
    index("instantly_campaigns_brand_ids_idx").using("gin", table.brandIds),
    index("instantly_campaigns_org_id_idx").on(table.orgId),
    index("instantly_campaigns_run_id_idx").on(table.runId),
    index("instantly_campaigns_workflow_slug_idx").on(table.workflowSlug),
    index("instantly_campaigns_account_email_idx").on(table.accountEmail),
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
  // Instantly account creation time (from the account snapshot). Drives age-based
  // send de-prioritization (fresh accounts picked last, overflow-only) + age-driven
  // slow ramp. DISTINCT from created_at below (= local row-insert time).
  timestampCreated: timestamp("timestamp_created", { withTimezone: true }),
  // ── Lifecycle (auto-derived; projection of the latest lifecycle event) ────────
  // One of: in_production | in_recovery | deactivated_by_instantly |
  // deactivated_by_user. Null until the first reconcileLifecycle classifies it.
  lifecycleStatus: text("lifecycle_status"),
  lifecycleReason: text("lifecycle_reason"),
  lifecycleUpdatedAt: timestamp("lifecycle_updated_at", { withTimezone: true }),
  // Set when an accounts-sync no longer finds the account in Instantly's live
  // list — the account was deleted upstream. The row is KEPT (its history and
  // its sent events stay meaningful) but it must be excluded from any fleet
  // inventory or capacity view, or a deleted mailbox keeps inflating capacity.
  // Cleared automatically if the account reappears. Prod at the time of writing
  // carried 10 such ghosts (266 stored vs 250 live).
  absentSince: timestamp("absent_since", { withTimezone: true }),
  // ── Send transport POLICY ────────────────────────────────────────────────────
  // 'instantly' (default) or 'smtp' (dispatch from this mailbox ourselves, over
  // smtp.gmail.com with its Primeforge app password). This is the policy for NEW
  // sends only: the decision is frozen onto instantly_campaigns.send_transport at
  // send time, so flipping this never disturbs sequences already in flight, and
  // flipping it back is the rollback. See src/lib/self-send/transport.ts.
  sendTransport: text("send_transport").notNull().default("instantly"),
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

// Silver/config: per-DOMAIN position in the send fill order, WITHIN a vendor.
// The fill order is vendor -> this rank -> account age -> email, so a domain
// placed at the tail receives no NEW sequences while any domain ahead of it has
// room. That is what lets a whole domain go quiet and become cancellable: the
// vendor tier alone cannot do it, because a vendor's mailboxes are provisioned
// in batches that INTERLEAVE domains (Primeforge creates them alphabetically by
// first name), so an age-ordered fleet drains every domain of a vendor at once.
//
// Lives in the DB (NOT a code constant) so re-ordering a domain is an UPDATE,
// never a deploy. A domain with no row sorts LAST within its vendor — same
// reasoning as an unattributed vendor: we cannot honestly place it, and the tail
// risks the least. Deleting every row restores the previous (age-only) order
// exactly, which is the rollback.
export const instantlyDomainFillOrder = pgTable("instantly_domain_fill_order", {
  domain: text("domain").primaryKey(),
  fillRank: integer("fill_rank").notNull(),
  note: text("note"),
  createdAt: timestamp("created_at").defaultNow().notNull(),
});

// Silver/config: per-feature account RESERVATION (carve-out list). An account
// listed here is reserved EXCLUSIVELY to its `feature_slug` — the live-send pool
// (`fetchInProductionAccounts`) serves it ONLY to sends carrying that feature and
// EXCLUDES it from every other feature's pool. An account NOT listed is
// unreserved = the default/shared pool (every non-reserved feature + null slug).
// This is a pure carve-out, NOT a symmetric partition: seeded with the 3
// `sales-crm-email-outreach` accounts so unproven CRM sends never touch the
// Apollo-verified cold fleet; the 5 cold-email features (sales/pr/hiring/vc/
// accelerators) keep the whole unreserved fleet. Lives in the DB (NOT a code
// constant) so ops can reserve/unreserve an account without a deploy.
export const instantlyAccountFeaturePolicy = pgTable(
  "instantly_account_feature_policy",
  {
    accountEmail: text("account_email").primaryKey(),
    featureSlug: text("feature_slug").notNull(),
    note: text("note"),
    createdAt: timestamp("created_at").defaultNow().notNull(),
  },
);

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
    // Set when the human statement this event mirrors has been WITHDRAWN
    // (source='manual' rows only). The row is kept — silver is the audit of
    // what was asserted — but the gold current-sentiment projection skips it,
    // so the lead reads as if nobody had stated a kind. Silver is derived and
    // rebuildable, so marking a row here is legitimate; the bronze statement it
    // mirrors is never touched. NULL = the statement still stands (and on every
    // non-manual row, which can never be withdrawn).
    withdrawnAt: timestamp("withdrawn_at"),
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
    // Per-lead Instantly campaign id (globally unique, 1 campaign = 1 lead).
    // Persisted at send time so the cost-lifecycle resolvers (handleEmailSent /
    // cancelRemainingProvisions) can match a hold by it — the ONLY stable key
    // that works for platform sends (campaign_id NULL). Nullable: historical
    // rows stay NULL (resolvers fall back to campaign_id for org rows; the
    // reconcile-provisioned-holds sweep drains historical platform rows). See
    // migration 0027 + CLAUDE.md "Send cost lifecycle".
    instantlyCampaignId: text("instantly_campaign_id"),
    leadEmail: text("lead_email").notNull(),
    step: integer("step").notNull(),
    runId: text("run_id").notNull(),
    // Runs-service cost id — NULL on every row written from 2026-08 onwards.
    //
    // This table is TWO things at once, and only one of them still involves
    // money. It is the billing hold ledger (a `provisioned` row is a reserved
    // charge that later actualizes or cancels), AND it is the send QUEUE: every
    // ops surface — the self-send dispatch worker, the fleet sending-forecast,
    // the per-account queue breakdown, capacity-aware account selection,
    // account-health `queueSize`, reconcile's `pendingSends` — reads
    // `status='provisioned'` as "steps scheduled but not yet sent".
    //
    // The Instantly subscriptions became a FIXED cost we absorb rather than
    // rebill, so instantly-service stopped declaring `instantly-*-email-sent` /
    // `instantly-contact-uploaded` to runs-service entirely. The queue still has
    // to exist, so the row is still written — it simply no longer carries a cost
    // id. Do NOT re-add `.notNull()`: that would make the queue undeclarable
    // without a billing row and silently empty the send pipeline.
    //
    // Historical rows keep their cost id and keep resolving against
    // runs-service; `settleHoldCost` branches on NULL. The unique index below
    // stays non-partial — Postgres treats every NULL as distinct, so unbilled
    // rows never collide.
    costId: text("cost_id"),
    status: text("status").notNull().default("provisioned"),
    createdAt: timestamp("created_at").defaultNow().notNull(),
    updatedAt: timestamp("updated_at").defaultNow().notNull(),
  },
  (table) => [
    index("sequence_costs_campaign_lead_idx").on(
      table.campaignId,
      table.leadEmail,
    ),
    index("sequence_costs_instantly_campaign_id_idx").on(
      table.instantlyCampaignId,
    ),
    uniqueIndex("sequence_costs_cost_id_idx").on(table.costId),
  ],
);

// Silver: the steps of a sequence we dispatch OURSELVES (send_transport='smtp').
//
// This is canonical state, not a mirror. While Instantly dispatches, the step
// bodies live there and `instantly_campaigns_config_raw` is our BRONZE copy of
// what they hold; once we send, there is no upstream to mirror, so the steps
// become ours — the same content moving UP a layer rather than a second bronze
// competing with Instantly's.
//
// `step` is 1-based, matching `sequence_costs.step`. `delay_days` is the gap
// from THIS step to the next, so the gap k → k+1 is `row(step=k).delay_days` and
// the rows ordered by step drop straight into the existing `delayForGap`
// resolver — the self-send scheduler, the fleet forecast and the per-account
// queue breakdown keep one shared cadence source. NULL = fall back to
// `STEP_GAP_CALENDAR_DAYS`, same as a missing bronze config delay.
export const sequenceSteps = pgTable(
  "sequence_steps",
  {
    id: text("id").primaryKey().$defaultFn(() => crypto.randomUUID()),
    // Per-lead campaign id — the same globally-unique key `sequence_costs` and
    // the silver event log already join on (1 campaign = 1 lead = 1 sequence).
    instantlyCampaignId: text("instantly_campaign_id").notNull(),
    step: integer("step").notNull(),
    subject: text("subject"),
    bodyHtml: text("body_html").notNull(),
    delayDays: integer("delay_days"),
    createdAt: timestamp("created_at").defaultNow().notNull(),
    updatedAt: timestamp("updated_at").defaultNow().notNull(),
  },
  (table) => [
    // Makes the per-step write idempotent: a redispatch re-upserts the same step
    // instead of stacking a duplicate the scheduler would send twice.
    uniqueIndex("sequence_steps_campaign_step_idx").on(
      table.instantlyCampaignId,
      table.step,
    ),
  ],
);

// ─── Bronze tables (raw external sources, append-only, never mutated) ─────────

// Bronze: the raw verdict of an SMTP server on a message we dispatched ourselves.
//
// A genuinely PARALLEL bronze source to Instantly's webhooks, not a replacement:
// both promote into the same silver (`instantly_events`), exactly as the webhook
// path and the reconcile-poll path already converge on `promoteEvent`. Named for
// the TRANSPORT rather than the vendor — these are not Instantly payloads.
//
// One row per dispatch ATTEMPT, success or failure, so a step that was refused
// leaves the same evidence trail as one that went out. `outcome` is 'sent' |
// 'permanent' | 'transient': the SMTP reply class is the discriminator this
// codebase already uses for account health (5xx = real rejection, 4xx = retry).
export const smtpDispatchRaw = pgTable(
  "smtp_dispatch_raw",
  {
    id: text("id").primaryKey().$defaultFn(() => crypto.randomUUID()),
    instantlyCampaignId: text("instantly_campaign_id").notNull(),
    leadEmail: text("lead_email").notNull(),
    accountEmail: text("account_email").notNull(),
    step: integer("step").notNull(),
    outcome: text("outcome").notNull(),
    // RFC 5322 Message-Id the server accepted. Threads the NEXT step of this
    // sequence (In-Reply-To / References) and correlates an async DSN bounce
    // back to the dispatch that caused it. Null on a failed attempt.
    messageId: text("message_id"),
    responseCode: integer("response_code"),
    // The server's reply line, verbatim. Bronze keeps what it said, not our
    // reading of it — the classification lives in `outcome` beside it.
    response: text("response"),
    payload: jsonb("payload").notNull(),
    dispatchedAt: timestamp("dispatched_at").defaultNow().notNull(),
  },
  (table) => [
    index("smtp_dispatch_raw_campaign_idx").on(table.instantlyCampaignId),
    index("smtp_dispatch_raw_account_idx").on(table.accountEmail),
    index("smtp_dispatch_raw_dispatched_at_idx").on(table.dispatchedAt),
    // Correlates an inbound DSN back to the dispatch it bounced from.
    index("smtp_dispatch_raw_message_id_idx").on(table.messageId),
  ],
);

// Bronze: a message that arrived in one of our own sending mailboxes.
//
// Once we dispatch ourselves, nobody tells us a prospect replied or a message
// bounced — it simply lands as mail, and we read it. Every message the poller
// looks at is stored, INCLUDING the ones classified `unrelated`: these are real
// mailboxes that also receive ordinary mail, and keeping the ones we ignored is
// what makes "why was this reply never picked up?" answerable later.
//
// `(account_email, message_id)` is unique, which is the whole dedup strategy: the
// poller re-reads an overlapping window every run rather than trusting a stored
// cursor, and the index makes a re-read a no-op. A cursor that drifts silently
// loses replies; a window that overlaps costs nothing.
export const imapMessagesRaw = pgTable(
  "imap_messages_raw",
  {
    id: text("id").primaryKey().$defaultFn(() => crypto.randomUUID()),
    accountEmail: text("account_email").notNull(),
    messageId: text("message_id").notNull(),
    fromAddress: text("from_address"),
    subject: text("subject"),
    // 'reply' | 'auto_reply' | 'bounce' | 'unrelated'
    kind: text("kind").notNull(),
    // The send this message answers, once correlated through our own Message-Id.
    // Null for `unrelated`.
    instantlyCampaignId: text("instantly_campaign_id"),
    step: integer("step"),
    payload: jsonb("payload").notNull(),
    receivedAt: timestamp("received_at"),
    polledAt: timestamp("polled_at").defaultNow().notNull(),
  },
  (table) => [
    uniqueIndex("imap_messages_raw_account_message_idx").on(
      table.accountEmail,
      table.messageId,
    ),
    index("imap_messages_raw_campaign_idx").on(table.instantlyCampaignId),
    index("imap_messages_raw_polled_at_idx").on(table.polledAt),
  ],
);

// Bronze: an HTTP hit from a recipient on a link we minted (opt-out click now;
// open pixel and click redirect once tracking lands).
//
// Bronze because it is an external request, and because `promoteEvent` requires
// real provenance — a silver event has to point at the bronze row that caused
// it, so an unsubscribe cannot be promoted without recording the hit first.
// One `kind` column rather than a table per link type: they are the same kind of
// fact (a recipient hit a URL we signed) and differ only in which one.
export const trackingHitsRaw = pgTable(
  "tracking_hits_raw",
  {
    id: text("id").primaryKey().$defaultFn(() => crypto.randomUUID()),
    // 'unsubscribe' | 'open' | 'click'
    kind: text("kind").notNull(),
    instantlyCampaignId: text("instantly_campaign_id").notNull(),
    leadEmail: text("lead_email").notNull(),
    step: integer("step"),
    // GET or POST. An opt-out is only ACTED on for POST: corporate link scanners
    // fetch every URL in an inbound email, so acting on GET would unsubscribe
    // prospects who never clicked. The GET is still recorded — it is a real hit,
    // and it is the evidence that the scanner (not the human) came first.
    method: text("method"),
    userAgent: text("user_agent"),
    payload: jsonb("payload").notNull(),
    receivedAt: timestamp("received_at").defaultNow().notNull(),
  },
  (table) => [
    index("tracking_hits_raw_campaign_idx").on(table.instantlyCampaignId),
    index("tracking_hits_raw_received_at_idx").on(table.receivedAt),
  ],
);

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
    // Nullable since migration 0043: the workspace-wide Unibox backfill reads
    // `/emails` with no campaign filter, and that list legitimately carries mail
    // attached to no campaign. The reconcile poll still always writes a concrete
    // id, and every reader filters by one.
    instantlyCampaignId: text("instantly_campaign_id"),
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
    /** The RAW human statement — append-only, never rewritten. May still be one
     *  of the two legacy deal-progress values while the consoles migrate. */
    status: text("status").notNull(),
    /** The reply kind `status` RESOLVES to, frozen at write (`resolveReplyKind`).
     *  Persisted rather than derived on read so no consumer ever needs a
     *  read-time translation of a legacy value. */
    replyKind: text("reply_kind").notNull(),
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

// Bronze: withdrawals of manual reply qualifications — a human retracting a
// statement they got wrong.
//
// A withdrawal is an APPEND, never an edit or a delete: the statement row above
// stays byte-identical (what was stated is part of the audit) and this row
// records that it no longer stands, by whom and when. Nothing is ever removed,
// so "the vocabulary has no 'nothing stated' member" stays true — absence of a
// standing statement is expressed by a superseding row, not by a sentinel kind.
//
// The STANDING statement for a (org, instantly_campaign, lead) pair is the
// latest `instantly_manual_qualifications_raw` row that has NO withdrawal row
// pointing at it. Withdrawing keys on the statement id rather than on a
// timestamp watermark so a later re-statement is unaffected by an earlier
// withdrawal, and so re-withdrawing the same statement is a no-op by the unique
// index rather than by a read-then-write race.
export const instantlyManualQualificationWithdrawals = pgTable(
  "instantly_manual_qualification_withdrawals",
  {
    id: text("id").primaryKey().$defaultFn(() => crypto.randomUUID()),
    /** The withdrawn statement's bronze row id. Unique — one withdrawal per statement. */
    qualificationId: text("qualification_id").notNull().unique(),
    orgId: text("org_id").notNull(),
    campaignId: text("campaign_id").notNull(),
    instantlyCampaignId: text("instantly_campaign_id").notNull(),
    leadEmail: text("lead_email").notNull(),
    withdrawnBy: text("withdrawn_by").notNull(),
    notes: text("notes"),
    withdrawnAt: timestamp("withdrawn_at").defaultNow().notNull(),
  },
  (table) => [
    index("instantly_manual_qualification_withdrawals_org_campaign_email_idx").on(
      table.orgId,
      table.campaignId,
      table.leadEmail,
    ),
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

// ─── Provider infrastructure inventory (Gandi / Mailforge / Primeforge / DFY) ──
//
// The fleet buys domains and mailboxes from FOUR vendors; until issue #555 only
// Instantly existed in code, and only at the ACCOUNT grain. These tables add the
// missing layer UNDERNEATH the account: what we own, from whom, and what it
// costs. Filled by `POST /internal/infra/sync` (see lib/infra-sync.ts).

// Bronze: one row per domain per poll, append-only. The vendor payload verbatim.
export const providerDomainsRaw = pgTable(
  "provider_domains_raw",
  {
    id: text("id").primaryKey().$defaultFn(() => crypto.randomUUID()),
    provider: text("provider").notNull(),
    providerAccount: text("provider_account"),
    domain: text("domain").notNull(),
    payload: jsonb("payload").notNull(),
    fetchedAt: timestamp("fetched_at").defaultNow().notNull(),
  },
  (table) => [
    index("provider_domains_raw_domain_idx").on(table.domain),
    index("provider_domains_raw_provider_fetched_idx").on(table.provider, table.fetchedAt),
  ],
);

// Bronze: one row per mailbox per poll, append-only.
export const providerMailboxesRaw = pgTable(
  "provider_mailboxes_raw",
  {
    id: text("id").primaryKey().$defaultFn(() => crypto.randomUUID()),
    provider: text("provider").notNull(),
    providerAccount: text("provider_account"),
    email: text("email").notNull(),
    domain: text("domain").notNull(),
    payload: jsonb("payload").notNull(),
    fetchedAt: timestamp("fetched_at").defaultNow().notNull(),
  },
  (table) => [
    index("provider_mailboxes_raw_email_idx").on(table.email),
    index("provider_mailboxes_raw_provider_fetched_idx").on(table.provider, table.fetchedAt),
  ],
);

// Bronze: vendor-level facts (Instantly plan ids, Primeforge workspaces, a
// prepaid balance). One row per (provider, scope) per poll.
export const providerAccountRaw = pgTable(
  "provider_account_raw",
  {
    id: text("id").primaryKey().$defaultFn(() => crypto.randomUUID()),
    provider: text("provider").notNull(),
    scope: text("scope").notNull(),
    payload: jsonb("payload").notNull(),
    fetchedAt: timestamp("fetched_at").defaultNow().notNull(),
  },
  (table) => [index("provider_account_raw_provider_fetched_idx").on(table.provider, table.fetchedAt)],
);

// Silver: current state of one domain AS SEEN BY ONE PROVIDER. The PK is
// (provider, domain), NOT domain alone — a domain can legitimately be reported
// by two vendors (Gandi registers it while Mailforge hosts its mailboxes), and
// collapsing them here would force a precedence guess into storage. The
// per-domain rollup is derived on read in gold.
//
// `absentSince` marks a row the provider STOPPED reporting. Rows are never
// deleted: a domain that disappears is itself a fact (it lapsed, or it was
// transferred), and deleting it would erase the evidence.
export const infraDomains = pgTable(
  "infra_domains",
  {
    provider: text("provider").notNull(),
    domain: text("domain").notNull(),
    providerAccount: text("provider_account"),
    externalId: text("external_id"),
    // registrar | mailbox | prewarm — what the vendor does for this domain.
    role: text("role").notNull(),
    // The vendor's own status string, verbatim. Never mapped to a local enum:
    // each vendor's vocabulary is its own, and a lossy mapping hides states.
    status: text("status"),
    createdAtProvider: timestamp("created_at_provider", { withTimezone: true }),
    expiresAt: timestamp("expires_at", { withTimezone: true }),
    // Null = the vendor exposes no such flag. Distinct from false.
    autorenew: boolean("autorenew"),
    deletionScheduled: boolean("deletion_scheduled").default(false).notNull(),
    cancelledAt: timestamp("cancelled_at", { withTimezone: true }),
    // Price the VENDOR itself reports (Mailforge `priceCents`). Null when the
    // vendor exposes none — the rate card fills that in gold, never here.
    priceCents: integer("price_cents"),
    priceCurrency: text("price_currency"),
    firstSeenAt: timestamp("first_seen_at", { withTimezone: true }).defaultNow().notNull(),
    lastSeenAt: timestamp("last_seen_at", { withTimezone: true }).defaultNow().notNull(),
    absentSince: timestamp("absent_since", { withTimezone: true }),
  },
  (table) => [
    primaryKey({ columns: [table.provider, table.domain] }),
    index("infra_domains_domain_idx").on(table.domain),
    index("infra_domains_expires_idx").on(table.expiresAt),
  ],
);

// Silver: current state of one mailbox as seen by one provider. Same
// never-delete / mark-absent discipline as infra_domains.
export const infraMailboxes = pgTable(
  "infra_mailboxes",
  {
    provider: text("provider").notNull(),
    email: text("email").notNull(),
    domain: text("domain").notNull(),
    providerAccount: text("provider_account"),
    externalId: text("external_id"),
    status: text("status"),
    createdAtProvider: timestamp("created_at_provider", { withTimezone: true }),
    firstSeenAt: timestamp("first_seen_at", { withTimezone: true }).defaultNow().notNull(),
    lastSeenAt: timestamp("last_seen_at", { withTimezone: true }).defaultNow().notNull(),
    absentSince: timestamp("absent_since", { withTimezone: true }),
  },
  (table) => [
    primaryKey({ columns: [table.provider, table.email] }),
    index("infra_mailboxes_domain_idx").on(table.domain),
  ],
);

// Silver/config: what a vendor charges US. Deliberately SEPARATE from
// costs-service, which prices what we RE-BILL the customer — neither replaces
// the other, and the difference between them is the real margin per email.
//
// Two price sources coexist and must stay distinguishable:
//   - a VENDOR-REPORTED per-domain price lives on `infra_domains.price_cents`
//     (Mailforge's `priceCents`, Gandi's renewal quote). Per-domain because the
//     price depends on the TLD.
//   - a RATE CARD lives here, for vendors whose API exposes no billing surface
//     at all (Primeforge returns 404 on every billing path; Instantly DFY bills
//     off-API). `source` records which it is, so a figure on screen can always
//     say where it came from.
//
// A vendor with no row and no per-domain price reports NULL, never a guess.
export const infraPriceRates = pgTable(
  "infra_price_rates",
  {
    provider: text("provider").notNull(),
    // domain-year | mailbox-month | plan-month
    scope: text("scope").notNull(),
    // Discriminates several rates in one scope (a plan name). '' when there is
    // only one — part of the PK, so it cannot be null.
    item: text("item").notNull().default(""),
    unitCents: integer("unit_cents").notNull(),
    currency: text("currency").notNull(),
    // api | rate-card — provenance, shown next to every derived figure.
    source: text("source").notNull(),
    // Non-retroactive by construction: a rate change adds a row, never edits one.
    effectiveFrom: timestamp("effective_from", { withTimezone: true }).defaultNow().notNull(),
    note: text("note"),
  },
  (table) => [
    primaryKey({ columns: [table.provider, table.scope, table.item, table.effectiveFrom] }),
  ],
);

// ─── In-house seed placement (Bronze) ───────────────────────────────────────
//
// The self-hosted replacement for Instantly's paid inbox-placement test. We
// send a seed from every testable mailbox to a small set of receiver mailboxes
// we own, then read each receiver over IMAP and record which folder the seed
// landed in. Two bronze tables because the measurement has two independent
// halves and BOTH are needed to be honest:
//
//   dispatches   — the DENOMINATOR. A seed we sent. Written at send time.
//   observations — the NUMERATOR. A seed we found. Written at read time.
//
// `missing` is derived as dispatched-minus-observed, so a seed that vanished is
// counted against the sender instead of silently shrinking the sample. Keeping
// the halves apart is what makes that possible: a single "result" table would
// have no way to represent a seed that was sent and never arrived.
//
// Silver is the EXISTING `instantly_placement_results` — nothing in that table
// is Instantly-specific (`test_id` is plain text), so these rows promote into it
// under a `seed:` test id and every downstream reader (the lifecycle delivery
// gate, account-health, the history series) works unchanged.

// Bronze A: one row per seed email dispatched. `message_id` is the correlation
// key against the observation side, and is globally unique per sent message, so
// it doubles as the idempotency key for a re-run.
export const seedPlacementDispatches = pgTable(
  "seed_placement_dispatches",
  {
    id: text("id").primaryKey().$defaultFn(() => crypto.randomUUID()),
    testId: text("test_id").notNull(),
    senderEmail: text("sender_email").notNull(),
    receiverEmail: text("receiver_email").notNull(),
    // Recipient ESP, same enum as the Instantly silver rows (1=Google, 2=Outlook).
    recipientEsp: integer("recipient_esp").notNull(),
    messageId: text("message_id").notNull(),
    // `sent` counts toward the denominator. A `permanent` / `transient` failure
    // is recorded but NOT counted — we never learned anything about placement,
    // and scoring an SMTP refusal as a missing inbox would blame the receiver
    // for a send that never left.
    outcome: text("outcome").notNull(),
    response: text("response"),
    dispatchedAt: timestamp("dispatched_at").defaultNow().notNull(),
  },
  (table) => [
    uniqueIndex("seed_placement_dispatches_message_id_idx").on(table.messageId),
    index("seed_placement_dispatches_test_idx").on(table.testId),
  ],
);

// Bronze B: one row per seed found in a receiver mailbox.
//
// FIRST OBSERVATION WINS (the unique index makes a re-read a no-op). Gmail can
// reclassify a message after delivery; where it landed AT DELIVERY is the answer
// the deliverability question is actually asking, so a later move must not
// overwrite it.
export const seedPlacementObservations = pgTable(
  "seed_placement_observations",
  {
    id: text("id").primaryKey().$defaultFn(() => crypto.randomUUID()),
    testId: text("test_id").notNull(),
    messageId: text("message_id").notNull(),
    receiverEmail: text("receiver_email").notNull(),
    // The IMAP path the seed was read from, kept verbatim for auditability.
    folder: text("folder").notNull(),
    // `inbox` | `spam` — the classified verdict for that folder.
    placement: text("placement").notNull(),
    spfPass: boolean("spf_pass"),
    dkimPass: boolean("dkim_pass"),
    dmarcPass: boolean("dmarc_pass"),
    observedAt: timestamp("observed_at").defaultNow().notNull(),
  },
  (table) => [
    uniqueIndex("seed_placement_observations_receiver_message_idx").on(
      table.receiverEmail,
      table.messageId,
    ),
    index("seed_placement_observations_test_idx").on(table.testId),
  ],
);
