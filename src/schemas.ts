// Side-effect import — extends Zod with `.openapi()` so subsequent local schema
// declarations (`z.object({...}).openapi("Name")`) work. Imported contract
// schemas are re-exported as-is without `.openapi(name)`: zod-to-openapi v8's
// `.openapi(name)` requires the schema instance to be created AFTER the
// extension (Zod 4 attaches prototype methods at construction time). The
// OpenAPI generator inlines contract shapes where they're referenced; trade-off
// accepted to keep a single source of truth in the contract package.
import "./zod-setup";

import { z } from "zod";
import { OpenAPIRegistry } from "@asteasolutions/zod-to-openapi";
import {
  ReplyClassificationSchema as RawReplyClassification,
  RepliesDetailSchema as RawRepliesDetail,
  RecipientStatsSchema as RawRecipientStats,
  StepStatsSchema as RawStepStats,
  EmailStatsSchema as RawEmailStats,
  ChannelStatsSchema as RawChannelStats,
  StatusScopeSchema as RawStatusScope,
  GlobalStatusSchema as RawGlobalStatus,
  ProviderStatusSchema as RawProviderStatus,
} from "@shamanic-technologies/email-domain-contract";

export const registry = new OpenAPIRegistry();

// ─── Shared cross-provider schemas (imported from email-domain-contract) ────
// Re-exported as-is. The OpenAPI generator inlines them where they're referenced
// (no $ref name) because zod-to-openapi v8's `.openapi(name)` cannot be applied
// to pre-existing Zod 4 schema instances without the consumer creating them
// fresh. Trade-off accepted for v1: slightly more verbose OpenAPI output, but
// the schemas remain a single source of truth in the contract package.

export const ReplyClassificationSchema = RawReplyClassification;
export type ReplyClassification = z.infer<typeof ReplyClassificationSchema>;

export const RepliesDetailSchema = RawRepliesDetail;
export type RepliesDetail = z.infer<typeof RepliesDetailSchema>;

export const RecipientStatsSchema = RawRecipientStats;
export type RecipientStats = z.infer<typeof RecipientStatsSchema>;

export const StepStatsSchema = RawStepStats;
export type StepStats = z.infer<typeof StepStatsSchema>;

export const EmailStatsSchema = RawEmailStats;
export type EmailStats = z.infer<typeof EmailStatsSchema>;

export const ChannelStatsSchema = RawChannelStats;
export type ChannelStats = z.infer<typeof ChannelStatsSchema>;

export const StatusScopeSchema = RawStatusScope;
export type StatusScope = z.infer<typeof StatusScopeSchema>;

export const GlobalStatusSchema = RawGlobalStatus;
export type GlobalStatus = z.infer<typeof GlobalStatusSchema>;

export const ProviderStatusSchema = RawProviderStatus;
export type ProviderStatus = z.infer<typeof ProviderStatusSchema>;

// ─── Tracking Headers (optional, injected by workflow-service) ─────────────

export const TrackingHeadersSchema = z.object({
  "x-campaign-id": z.string().optional().describe("Campaign ID — automatically injected by workflow-service on all DAG calls"),
  "x-brand-id": z.string().optional().describe("Brand ID(s) — comma-separated UUIDs, automatically injected by workflow-service on all DAG calls. Example: uuid1,uuid2,uuid3"),
  "x-workflow-slug": z.string().optional().describe("Workflow slug — automatically injected by workflow-service on all DAG calls"),
  "x-feature-slug": z.string().optional().describe("Feature slug — propagated through the full call chain for tracking"),
  "x-goal": z.string().optional().describe("Explicit active goal attribution. Stored only when supplied; never inferred."),
  "x-brand-profile-id": z.string().optional().describe("Explicit brand-profile attribution. Stored only when supplied; never inferred."),
  "x-audience-id": z.string().optional().describe("Explicit audience attribution (human-service audience.id). Stored only when supplied; never inferred."),
});

// ─── Error ──────────────────────────────────────────────────────────────────

export const ErrorSchema = z
  .object({
    error: z.string(),
  })
  .openapi("Error");

// ─── Health ─────────────────────────────────────────────────────────────────

const RootResponseSchema = z
  .object({
    service: z.string(),
    version: z.string(),
  })
  .openapi("RootResponse");

const HealthResponseSchema = z
  .object({
    status: z.string(),
    service: z.string(),
  })
  .openapi("HealthResponse");

registry.registerPath({
  method: "get",
  path: "/",
  summary: "Service info",
  responses: {
    200: {
      description: "Service info",
      content: { "application/json": { schema: RootResponseSchema } },
    },
  },
});

registry.registerPath({
  method: "get",
  path: "/health",
  summary: "Health check",
  responses: {
    200: {
      description: "Service is healthy",
      content: { "application/json": { schema: HealthResponseSchema } },
    },
  },
});

// ─── Webhooks ───────────────────────────────────────────────────────────────

export const WebhookPayloadSchema = z
  .object({
    event_type: z.string(),
    campaign_id: z.string().optional(),
    lead_email: z.string().optional(),
    email_account: z.string().optional(),
    timestamp: z.string().optional(),
    step: z.number().int().optional(),
    variant: z.number().int().optional(),
  })
  .openapi("WebhookPayload");

const WebhookResponseSchema = z
  .object({
    success: z.boolean(),
    eventType: z.string(),
    bronzeRowId: z.string().nullable(),
    promoted: z.boolean(),
    degraded: z.boolean().describe("True when bronze or silver write failed but webhook was acknowledged with 200 to avoid Instantly auto-pause"),
    degradedReason: z.string().nullable().describe("Failure message when degraded=true, null otherwise"),
  })
  .openapi("WebhookResponse");

const WebhookConfigResponseSchema = z
  .object({
    webhookUrl: z.string().url(),
  })
  .openapi("WebhookConfigResponse");

registry.registerPath({
  method: "get",
  path: "/webhooks/instantly/config",
  summary: "Get webhook URL for BYOK configuration",
  description:
    "Returns the webhook URL that BYOK customers should paste into their Instantly dashboard webhook settings.",
  responses: {
    200: {
      description: "Webhook configuration",
      content: { "application/json": { schema: WebhookConfigResponseSchema } },
    },
    500: {
      description: "INSTANTLY_SERVICE_URL not configured",
      content: { "application/json": { schema: ErrorSchema } },
    },
  },
});

registry.registerPath({
  method: "post",
  path: "/webhooks/instantly",
  summary: "Receive Instantly webhook events",
  description:
    "Verification: the campaign_id in the payload must exist in the database. " +
    "Each campaign UUID is unguessable and stored with its org on creation.",
  request: {
    body: {
      content: { "application/json": { schema: WebhookPayloadSchema } },
    },
  },
  responses: {
    200: {
      description: "Webhook processed",
      content: { "application/json": { schema: WebhookResponseSchema } },
    },
    400: {
      description: "Missing event_type or campaign_id",
      content: { "application/json": { schema: ErrorSchema } },
    },
    401: {
      description: "Unknown campaign_id",
      content: { "application/json": { schema: ErrorSchema } },
    },
  },
});

// ─── Send ───────────────────────────────────────────────────────────────────

export const SequenceStepSchema = z.object({
  step: z.number().int().min(1).describe("1-based ordinal step number"),
  bodyHtml: z.string().describe("HTML body for this step"),
  daysSinceLastStep: z
    .number()
    .int()
    .min(0)
    .describe("Delay in days since the previous step (0 = immediate)"),
});

export const SendRequestSchema = z
  .object({
    leadId: z.string().optional().describe("External lead ID from lead-service"),
    to: z.string(),
    firstName: z.string().optional(),
    lastName: z.string().optional(),
    company: z.string().optional(),
    variables: z.record(z.string(), z.string()).optional(),
    timezone: z
      .string()
      .trim()
      .refine(isValidIanaTimezone, "Invalid IANA timezone")
      .optional()
      .describe(
        "Recipient's IANA timezone (e.g. America/New_York). Sets the Instantly campaign sending-schedule timezone so business-hours sends land in the lead's local time. Absent/invalid → America/Chicago default.",
      ),
    subject: z.string().describe("Shared subject for all steps in the sequence"),
    bcc: z
      .array(z.string())
      .optional()
      .describe(
        "Optional BCC recipients — set as the created campaign's bcc_list so every step of the sequence BCCs these addresses. Absent/empty = no BCC.",
      ),
    sequence: z
      .array(SequenceStepSchema)
      .min(1)
      .describe("Ordered email steps — at least one required"),
  })
  .openapi("SendRequest");

export type SendRequest = z.infer<typeof SendRequestSchema>;

const StepRunSchema = z.object({
  step: z.number().int().min(1),
  runId: z.string(),
});

const SendResponseSchema = z
  .object({
    success: z.boolean(),
    campaignId: z.string().nullable().optional(),
    leadId: z.string().nullable().optional(),
    added: z.number(),
    stepRuns: z.array(StepRunSchema).optional(),
  })
  .openapi("SendResponse");

registry.registerPath({
  method: "post",
  path: "/orgs/send",
  summary: "Send email via Instantly campaign",
  description:
    "Brand IDs, campaign ID, and workflow slug are read from headers (x-brand-id, x-campaign-id, x-workflow-slug) — do NOT pass them in the body.",
  request: {
    headers: TrackingHeadersSchema,
    body: {
      content: { "application/json": { schema: SendRequestSchema } },
    },
  },
  responses: {
    200: {
      description: "Email sent",
      content: { "application/json": { schema: SendResponseSchema } },
    },
    400: {
      description: "Missing required fields",
      content: { "application/json": { schema: ErrorSchema } },
    },
    401: { description: "Unauthorized" },
    500: {
      description: "Failed to send",
      content: { "application/json": { schema: ErrorSchema } },
    },
  },
});

// ─── Campaigns ──────────────────────────────────────────────────────────────

registry.registerPath({
  method: "get",
  path: "/orgs/campaigns/{campaignId}",
  summary: "Get a campaign",
  request: {
    headers: TrackingHeadersSchema,
    params: z.object({ campaignId: z.string() }),
  },
  responses: {
    200: { description: "Campaign found" },
    404: {
      description: "Campaign not found",
      content: { "application/json": { schema: ErrorSchema } },
    },
    401: { description: "Unauthorized" },
  },
});

registry.registerPath({
  method: "get",
  path: "/orgs/campaigns",
  summary: "List campaigns for the authenticated org",
  request: {
    headers: TrackingHeadersSchema,
  },
  responses: {
    200: { description: "Campaigns list" },
    401: { description: "Unauthorized" },
  },
});

export const UpdateStatusRequestSchema = z
  .object({
    status: z.enum(["active", "paused", "completed"]),
  })
  .openapi("UpdateStatusRequest");

export type UpdateStatusRequest = z.infer<typeof UpdateStatusRequestSchema>;

registry.registerPath({
  method: "patch",
  path: "/orgs/campaigns/{campaignId}/status",
  summary: "Update campaign status",
  request: {
    headers: TrackingHeadersSchema,
    params: z.object({ campaignId: z.string() }),
    body: {
      content: {
        "application/json": { schema: UpdateStatusRequestSchema },
      },
    },
  },
  responses: {
    200: { description: "Status updated" },
    400: {
      description: "Invalid status",
      content: { "application/json": { schema: ErrorSchema } },
    },
    404: {
      description: "Campaign not found",
      content: { "application/json": { schema: ErrorSchema } },
    },
    401: { description: "Unauthorized" },
  },
});

// ─── Reconcile ──────────────────────────────────────────────────────────────

const ReconcileAcceptedSchema = z
  .object({
    runId: z.string().uuid().describe("Opaque identifier for log correlation"),
    startedAt: z.string().describe("ISO timestamp when the job was dispatched"),
  })
  .openapi("ReconcileAccepted");

registry.registerPath({
  method: "post",
  path: "/internal/campaigns/reconcile",
  summary: "Dispatch reconcile webhook state against Instantly API",
  request: {},
  description:
    "Daily catch-up job that pulls Instantly's per-campaign state (aggregate, " +
    "per-lead status, per-email records) and promotes any events missed by " +
    "the webhook into the silver event log. Idempotent — safe to re-run.\n\n" +
    "Returns 202 immediately and runs the job in the background. Verify " +
    "completion via Railway logs (`reconcile: done`) or by polling " +
    "`instantly_*_raw` bronze tables.",
  responses: {
    202: {
      description: "Reconcile job dispatched (running in background)",
      content: { "application/json": { schema: ReconcileAcceptedSchema } },
    },
    401: { description: "Unauthorized" },
  },
});

// ─── Retry-stuck (cron) ─────────────────────────────────────────────────────

registry.registerPath({
  method: "post",
  path: "/internal/campaigns/retry-stuck",
  summary: "Dispatch daily retry-stuck sweep",
  request: {},
  description:
    "Continuous worker that scans campaigns with `delivery_status='contacted'` " +
    "stuck for >72h with no silver event proving Instantly ever sent. For each " +
    "row: re-sends the lead onto a fresh healthy Instantly account, refunds the " +
    "old cost rows, and provisions fresh costs against the new campaign. The " +
    "row's local `delivery_status` stays `contacted` until a real `email_sent` " +
    "webhook lands or it is terminally cancelled. The worker now runs " +
    "continuously (not a daily cron) — this endpoint exists for legacy callers " +
    "and is a no-op trigger.",
  responses: {
    202: {
      description: "Retry-stuck sweep dispatched (running in background)",
      content: { "application/json": { schema: ReconcileAcceptedSchema } },
    },
    401: { description: "Unauthorized" },
  },
});

// ─── Leads ──────────────────────────────────────────────────────────────────

registry.registerPath({
  method: "get",
  path: "/orgs/campaigns/{campaignId}/leads",
  summary: "List campaign leads",
  request: {
    headers: TrackingHeadersSchema,
    params: z.object({ campaignId: z.string() }),
    query: z.object({
      limit: z.string().optional(),
      skip: z.string().optional(),
    }),
  },
  responses: {
    200: { description: "Leads list" },
    404: {
      description: "Campaign not found",
      content: { "application/json": { schema: ErrorSchema } },
    },
    401: { description: "Unauthorized" },
  },
});

// ─── Stats ──────────────────────────────────────────────────────────────────

export function isValidIanaTimezone(value: string): boolean {
  try {
    new Intl.DateTimeFormat("en-US", { timeZone: value }).format(new Date(0));
    return true;
  } catch {
    return false;
  }
}

export const StatsQuerySchema = z
  .object({
    runIds: z.string().optional().describe("Comma-separated list of run IDs"),
    brandId: z.string().optional().describe("Filter by brand ID (matches campaigns containing this brand)"),
    campaignId: z.string().optional(),
    goal: z.string().optional().describe("Filter by explicit goal attribution stored in campaign metadata"),
    brandProfileId: z.string().optional().describe("Filter by explicit brand-profile attribution stored in campaign metadata"),
    audienceId: z.string().optional().describe("Filter by explicit audience attribution stored in campaign metadata"),
    workflowSlugs: z.string().optional().describe("Comma-separated list of workflow slugs to filter by"),
    featureSlugs: z.string().optional().describe("Comma-separated list of feature slugs to filter by"),
    groupBy: z.enum(["brandId", "campaignId", "workflowSlug", "featureSlug", "leadEmail", "audienceId", "day"]).optional().describe("Group results by dimension. groupBy=day keys buckets as YYYY-MM-DD in the requested timezone. Audience grouping uses only explicit campaign metadata."),
    timezone: z.string().trim().refine(isValidIanaTimezone, "Invalid IANA timezone").optional().describe("IANA timezone for groupBy=day buckets. Defaults to UTC."),
  })
  .openapi("StatsQuery");

export type StatsQuery = z.infer<typeof StatsQuerySchema>;

const StatsResponseSchema = z
  .object({
    recipientStats: RecipientStatsSchema,
    emailStats: EmailStatsSchema,
  })
  .openapi("StatsResponse");

const StatsGroupedEntrySchema = z.object({
  key: z.string().describe("Group key. For groupBy=day this is YYYY-MM-DD in the requested timezone."),
  recipientStats: RecipientStatsSchema,
  emailStats: EmailStatsSchema,
});

const StatsGroupedResponseSchema = z
  .object({
    groups: z.array(StatsGroupedEntrySchema),
  })
  .openapi("StatsGroupedResponse");

const StatsOrGroupedResponseSchema = z.union([StatsResponseSchema, StatsGroupedResponseSchema]);

const EngagementLatencyMetricSchema = z
  .object({
    averageMs: z.number().nullable().describe("Average elapsed time in milliseconds. Null when sampleSize is 0."),
    medianMs: z.number().nullable().describe("Median elapsed time in milliseconds. Null when sampleSize is 0."),
    sampleSize: z.number().int().describe("Number of recipients included in the aggregate."),
  })
  .openapi("EngagementLatencyMetric");

const EngagementLatencyResponseSchema = z
  .object({
    workflowSlugs: z.array(z.string()).describe("Workflow slugs included in this aggregate."),
    timeToFirstLinkClick: EngagementLatencyMetricSchema,
    timeToFirstPositiveReply: EngagementLatencyMetricSchema,
  })
  .openapi("EngagementLatencyResponse");

export const EngagementLatencyGroupedRequestSchema = z
  .object({
    groups: z.record(
      z.string(),
      z.object({
        workflowSlugs: z.array(z.string().trim().min(1)).min(1).describe("Workflow slugs included in this public-safe group."),
      }),
    ),
  })
  .openapi("EngagementLatencyGroupedRequest");

export type EngagementLatencyGroupedRequest = z.infer<typeof EngagementLatencyGroupedRequestSchema>;

const EngagementLatencyGroupedEntrySchema = z
  .object({
    key: z.string().describe("Caller-owned public group key, for example a workflow dynasty slug."),
    workflowSlugs: z.array(z.string()),
    timeToFirstLinkClick: EngagementLatencyMetricSchema,
    timeToFirstPositiveReply: EngagementLatencyMetricSchema,
  })
  .openapi("EngagementLatencyGroupedEntry");

const EngagementLatencyGroupedResponseSchema = z
  .object({
    groups: z.array(EngagementLatencyGroupedEntrySchema),
  })
  .openapi("EngagementLatencyGroupedResponse");

registry.registerPath({
  method: "get",
  path: "/orgs/stats",
  summary: "Get aggregated stats by filters",
  description:
    "Aggregates stats from webhook events across campaigns matching the provided filters. Filters passed as query params; runIds is comma-separated.",
  request: {
    headers: TrackingHeadersSchema,
    query: StatsQuerySchema,
  },
  responses: {
    200: {
      description: "Aggregated campaign stats",
      content: { "application/json": { schema: StatsOrGroupedResponseSchema } },
    },
    400: {
      description: "No filter provided",
      content: { "application/json": { schema: ErrorSchema } },
    },
    401: { description: "Unauthorized" },
    500: {
      description: "Server error",
      content: { "application/json": { schema: ErrorSchema } },
    },
  },
});

registry.registerPath({
  method: "get",
  path: "/public/stats",
  summary: "Get aggregated stats (no identity headers required)",
  description:
    "Same as GET /orgs/stats but without x-org-id requirement. " +
    "Requires only X-API-Key. Used by leaderboard and landing pages with no user context.",
  request: {
    query: StatsQuerySchema,
  },
  responses: {
    200: {
      description: "Aggregated campaign stats",
      content: { "application/json": { schema: StatsOrGroupedResponseSchema } },
    },
    400: {
      description: "Invalid request",
      content: { "application/json": { schema: ErrorSchema } },
    },
    401: { description: "Unauthorized" },
    500: {
      description: "Server error",
      content: { "application/json": { schema: ErrorSchema } },
    },
  },
});

registry.registerPath({
  method: "get",
  path: "/public/stats/engagement-latency",
  summary: "Get public-safe engagement latency for workflow slugs",
  description:
    "Computes aggregate elapsed time from each recipient's first real send to first link click and first positive reply across the supplied workflow slugs. " +
    "Returns only aggregate average, median, and sample size; no recipient, lead, campaign, or message data is exposed.",
  request: {
    query: z.object({
      workflowSlugs: z.string().describe("Comma-separated workflow slugs to aggregate together."),
    }),
  },
  responses: {
    200: {
      description: "Engagement latency aggregate",
      content: { "application/json": { schema: EngagementLatencyResponseSchema } },
    },
    400: {
      description: "Invalid request",
      content: { "application/json": { schema: ErrorSchema } },
    },
    401: { description: "Unauthorized" },
    500: {
      description: "Server error",
      content: { "application/json": { schema: ErrorSchema } },
    },
  },
});

registry.registerPath({
  method: "post",
  path: "/public/stats/engagement-latency/grouped",
  summary: "Get public-safe engagement latency for workflow slug groups",
  description:
    "Computes aggregate elapsed time from each recipient's first real send to first link click and first positive reply for caller-owned workflow slug groups. " +
    "Use group keys such as workflow dynasty slugs when the consumer owns dynasty metadata. Returns no per-recipient rows or campaign internals.",
  request: {
    body: {
      content: { "application/json": { schema: EngagementLatencyGroupedRequestSchema } },
    },
  },
  responses: {
    200: {
      description: "Engagement latency aggregates by caller-owned group",
      content: { "application/json": { schema: EngagementLatencyGroupedResponseSchema } },
    },
    400: {
      description: "Invalid request",
      content: { "application/json": { schema: ErrorSchema } },
    },
    401: { description: "Unauthorized" },
    500: {
      description: "Server error",
      content: { "application/json": { schema: ErrorSchema } },
    },
  },
});

// ─── Grouped Stats ──────────────────────────────────────────────────────────

export const GroupedStatsRequestSchema = z
  .object({
    groups: z.record(
      z.string(),
      z.object({ runIds: z.array(z.string()).min(1) }),
    ),
  })
  .openapi("GroupedStatsRequest");

export type GroupedStatsRequest = z.infer<typeof GroupedStatsRequestSchema>;

const GroupedStatsEntrySchema = z.object({
  key: z.string().describe("Group key from the request"),
  recipientStats: RecipientStatsSchema,
  emailStats: EmailStatsSchema,
});

const GroupedStatsResponseSchema = z
  .object({
    groups: z.array(GroupedStatsEntrySchema),
  })
  .openapi("GroupedStatsResponse");

registry.registerPath({
  method: "post",
  path: "/orgs/stats/grouped",
  summary: "Get stats grouped by sets of run IDs",
  description:
    "Accepts named groups of run IDs and returns aggregated stats per group in a single call. Used by the leaderboard to fetch per-workflow stats.",
  request: {
    headers: TrackingHeadersSchema,
    body: {
      content: { "application/json": { schema: GroupedStatsRequestSchema } },
    },
  },
  responses: {
    200: {
      description: "Stats per group",
      content: { "application/json": { schema: GroupedStatsResponseSchema } },
    },
    400: {
      description: "Invalid request",
      content: { "application/json": { schema: ErrorSchema } },
    },
    401: { description: "Unauthorized" },
    500: {
      description: "Server error",
      content: { "application/json": { schema: ErrorSchema } },
    },
  },
});

// ─── Accounts ───────────────────────────────────────────────────────────────

registry.registerPath({
  method: "get",
  path: "/internal/accounts",
  summary: "List all email accounts",
  request: {},
  responses: {
    200: { description: "Accounts list" },
    401: { description: "Unauthorized" },
  },
});

registry.registerPath({
  method: "post",
  path: "/orgs/accounts/sync",
  summary: "Sync accounts from Instantly",
  request: { headers: TrackingHeadersSchema },
  responses: {
    200: {
      description: "Sync complete",
      content: {
        "application/json": {
          schema: z
            .object({ success: z.boolean(), synced: z.number() })
            .openapi("SyncResponse"),
        },
      },
    },
    401: { description: "Unauthorized" },
  },
});

export const WarmupRequestSchema = z
  .object({
    enabled: z.boolean(),
  })
  .openapi("WarmupRequest");

export type WarmupRequest = z.infer<typeof WarmupRequestSchema>;

// ─── Transfer Brand ─────────────────────────────────────────────────────────

export const TransferBrandRequestSchema = z
  .object({
    sourceBrandId: z.string().describe("Brand UUID to transfer from the source org"),
    sourceOrgId: z.string().describe("Current org UUID that owns the brand"),
    targetOrgId: z.string().describe("Destination org UUID"),
    targetBrandId: z.string().optional().describe("Brand UUID in the target org — when present, rewrites brand_id references to this value"),
  })
  .openapi("TransferBrandRequest");

export type TransferBrandRequest = z.infer<typeof TransferBrandRequestSchema>;

const TransferBrandUpdatedTableSchema = z.object({
  tableName: z.string(),
  count: z.number(),
});

export const TransferBrandResponseSchema = z
  .object({
    updatedTables: z.array(TransferBrandUpdatedTableSchema),
  })
  .openapi("TransferBrandResponse");

registry.registerPath({
  method: "post",
  path: "/internal/transfer-brand",
  summary: "Transfer solo-brand rows from one org to another",
  description:
    "Re-assigns org_id (and optionally brand_id) on all rows that reference exactly one brand matching sourceBrandId. " +
    "When targetBrandId is present, also rewrites brand references to the target brand. " +
    "Skips co-branding rows (multiple brand IDs). Idempotent.",
  request: {
    body: {
      content: { "application/json": { schema: TransferBrandRequestSchema } },
    },
  },
  responses: {
    200: {
      description: "Transfer complete",
      content: { "application/json": { schema: TransferBrandResponseSchema } },
    },
    400: {
      description: "Invalid request",
      content: { "application/json": { schema: ErrorSchema } },
    },
    401: { description: "Unauthorized" },
    500: {
      description: "Server error",
      content: { "application/json": { schema: ErrorSchema } },
    },
  },
});

const WarmupResponseSchema = z
  .object({
    success: z.boolean(),
    email: z.string(),
    warmupEnabled: z.boolean(),
  })
  .openapi("WarmupResponse");

registry.registerPath({
  method: "post",
  path: "/orgs/accounts/{email}/warmup",
  summary: "Enable or disable warmup for an account",
  request: {
    headers: TrackingHeadersSchema,
    params: z.object({ email: z.string() }),
    body: {
      content: { "application/json": { schema: WarmupRequestSchema } },
    },
  },
  responses: {
    200: {
      description: "Warmup setting updated",
      content: { "application/json": { schema: WarmupResponseSchema } },
    },
    400: {
      description: "Invalid request",
      content: { "application/json": { schema: ErrorSchema } },
    },
    401: { description: "Unauthorized" },
  },
});

registry.registerPath({
  method: "get",
  path: "/orgs/accounts/warmup-analytics",
  summary: "Get warmup analytics",
  request: { headers: TrackingHeadersSchema },
  responses: {
    200: { description: "Warmup analytics" },
    401: { description: "Unauthorized" },
  },
});

// ─── Status ──────────────────────────────────────────────────────────────────

const StatusItemSchema = z.object({
  email: z.string().describe("Email address"),
});

export const StatusRequestSchema = z
  .object({
    brandId: z
      .string()
      .optional()
      .describe("Brand ID — when provided without campaignId, returns per-campaign breakdown + aggregated brand status"),
    campaignId: z
      .string()
      .optional()
      .describe("Campaign ID — when provided, returns campaign-scoped status (brandId is ignored)"),
    items: z
      .array(StatusItemSchema)
      .min(1)
      .describe("Emails to check"),
  })
  .openapi("StatusRequest", {
    example: {
      brandId: "b8f0e2a1-1234-4abc-9def-000000000001",
      items: [{ email: "alice@media.com" }, { email: "bob@test.com" }],
    },
  });

export type StatusRequest = z.infer<typeof StatusRequestSchema>;

const StatusResultSchema = z.object({
  email: z.string(),
  byCampaign: z.record(z.string(), StatusScopeSchema).nullable().describe("Per-campaign breakdown — present only when brandId is provided without campaignId"),
  brand: StatusScopeSchema.nullable().describe("Aggregated brand status (most advanced across campaigns) — present only when brandId is provided without campaignId"),
  campaign: StatusScopeSchema.nullable().describe("Campaign-scoped status — present only when campaignId is provided"),
  global: GlobalStatusSchema,
});

const StatusResponseSchema = z
  .object({
    results: z.array(StatusResultSchema),
  })
  .openapi("StatusResponse", {
    example: {
      results: [
        {
          email: "alice@media.com",
          byCampaign: {
            "c1a2b3c4-0000-0000-0000-000000000001": {
              contacted: true, sent: true, delivered: true, opened: true, clicked: false,
              replied: false, replyClassification: null, bounced: false, unsubscribed: false,
              cancelled: false, lastDeliveredAt: "2026-03-01T10:00:00.000Z",
            },
            "c1a2b3c4-0000-0000-0000-000000000002": {
              contacted: true, sent: true, delivered: true, opened: false, clicked: true,
              replied: true, replyClassification: "positive", bounced: false, unsubscribed: false,
              cancelled: false, lastDeliveredAt: "2026-03-02T12:00:00.000Z",
            },
          },
          brand: {
            contacted: true, sent: true, delivered: true, opened: true, clicked: true,
            replied: true, replyClassification: "positive", bounced: false, unsubscribed: false,
            cancelled: false, lastDeliveredAt: "2026-03-02T12:00:00.000Z",
          },
          campaign: null,
          global: { email: { bounced: false, unsubscribed: false } },
        },
      ],
    },
  });

// ─── Manual Qualifications ──────────────────────────────────────────────────

const MANUAL_QUALIFICATION_STATUS_VALUES = [
  "lead_interested",
  "lead_meeting_booked",
  "lead_closed",
  "lead_not_interested",
  "lead_wrong_person",
  "lead_neutral",
  "lead_out_of_office",
  "auto_reply_received",
] as const;

export const ManualQualificationStatusSchema = z
  .enum(MANUAL_QUALIFICATION_STATUS_VALUES)
  .describe(
    "Manual reply qualification status — mirrors Instantly webhook reply event_type values exactly. Set by a human via the dashboard when Instantly fails to detect a reply (e.g. reply received on a non-leurre email account).",
  );

export type ManualQualificationStatus = z.infer<typeof ManualQualificationStatusSchema>;

export const ManualQualificationCreateBodySchema = z
  .object({
    campaign_id: z.string().min(1).describe("Logical campaign id (groups sub-campaigns for the same workflow run)"),
    email: z.string().email().describe("Lead email address"),
    status: ManualQualificationStatusSchema,
    notes: z.string().max(2000).optional().describe("Optional free-text human note for audit"),
  })
  .openapi("ManualQualificationCreateBody", {
    example: {
      campaign_id: "c1a2b3c4-0000-0000-0000-000000000001",
      email: "alice@media.com",
      status: "lead_interested",
      notes: "Reply received on Gmail — Instantly missed it",
    },
  });

export const ManualQualificationListQuerySchema = z.object({
  campaign_id: z.string().min(1).optional().describe("Filter by logical campaign id"),
  email: z.string().email().optional().describe("Filter by lead email"),
  limit: z
    .coerce.number()
    .int()
    .min(1)
    .max(500)
    .optional()
    .describe("Max rows to return (default 200, max 500)"),
});

const ManualQualificationRowSchema = z.object({
  id: z.string(),
  orgId: z.string(),
  campaignId: z.string(),
  instantlyCampaignId: z.string(),
  email: z.string(),
  status: ManualQualificationStatusSchema,
  qualifiedBy: z.string(),
  notes: z.string().nullable(),
  qualifiedAt: z.string().describe("ISO 8601 timestamp"),
});

const ManualQualificationCreateResponseSchema = z
  .object({
    idempotent: z
      .boolean()
      .describe("True if the latest existing row already matched the requested status — no new bronze row was inserted, no side effects fired"),
    qualification: ManualQualificationRowSchema,
  })
  .openapi("ManualQualificationCreateResponse");

const ManualQualificationListResponseSchema = z
  .object({ qualifications: z.array(ManualQualificationRowSchema) })
  .openapi("ManualQualificationListResponse");

registry.registerPath({
  method: "post",
  path: "/orgs/manual-qualifications",
  summary: "Set a manual reply qualification for a (campaign, lead) pair",
  description:
    "Record a human-set reply classification for a lead in a campaign. Used when Instantly's automatic webhook reply classification fails to detect a reply (e.g. the reply was sent to a non-leurre account that Instantly does not monitor).\n\n" +
    "**Bronze:** an `instantly_manual_qualifications_raw` row is appended for audit (append-only).\n\n" +
    "**Silver / Gold:** a corresponding row is inserted into `instantly_events` with `source='manual'`, so analytics counters (RepliesDetail) include the manual qualification alongside webhook events. `instantly_campaigns.reply_classification` is updated to the derived positive/negative/neutral value and `reply_classification_source` is set to `manual` so subsequent webhook events do not overwrite the human choice.\n\n" +
    "**Idempotence:** if the latest row for (org, campaign, lead) already has `status`, the call is a no-op — no new bronze row, no side effects. The response includes `idempotent: true` and the existing row.",
  request: {
    headers: TrackingHeadersSchema,
    body: {
      content: { "application/json": { schema: ManualQualificationCreateBodySchema } },
    },
  },
  responses: {
    200: {
      description: "Manual qualification recorded (or idempotent no-op)",
      content: { "application/json": { schema: ManualQualificationCreateResponseSchema } },
    },
    400: {
      description: "Invalid body or missing identity header",
      content: { "application/json": { schema: ErrorSchema } },
    },
    401: { description: "Unauthorized" },
    404: {
      description: "Campaign not found in this org for the given email",
      content: { "application/json": { schema: ErrorSchema } },
    },
  },
});

registry.registerPath({
  method: "get",
  path: "/orgs/manual-qualifications",
  summary: "List manual reply qualifications (org-scoped audit history)",
  description:
    "Returns the org's manual qualification history, sorted by `qualifiedAt` DESC. Optionally filter by `campaign_id` and/or `email`. Cross-org reads are blocked — only rows where `org_id` matches the request header are returned.",
  request: {
    headers: TrackingHeadersSchema,
    query: ManualQualificationListQuerySchema,
  },
  responses: {
    200: {
      description: "List of manual qualifications",
      content: { "application/json": { schema: ManualQualificationListResponseSchema } },
    },
    400: {
      description: "Invalid query parameters",
      content: { "application/json": { schema: ErrorSchema } },
    },
    401: { description: "Unauthorized" },
  },
});

registry.registerPath({
  method: "post",
  path: "/orgs/status",
  summary: "Batch delivery status check for emails",
  description:
    "Batch delivery status check. Filters are in the body — headers are tracing/logging only.\n\n" +
    "**Modes:**\n" +
    "- **Brand mode** (`brandId` set, no `campaignId`): returns `byCampaign` (per-campaign breakdown keyed by campaign UUID), `brand` (aggregated), and `global`.\n" +
    "- **Campaign mode** (`campaignId` set, with or without `brandId`): returns `campaign` (single campaign status) and `global`. When both are provided, `brandId` is ignored.\n" +
    "- **Global only** (neither): returns only `global`.\n\n" +
    "**Aggregation rules for `brand`:**\n" +
    "- Boolean fields (`contacted`, `sent`, `delivered`, `opened`, `clicked`, `replied`, `bounced`, `unsubscribed`): `true` if true in at least one campaign (BOOL_OR).\n" +
    "- `replyClassification`: from the campaign with the most recent `lastDeliveredAt` that has a non-null classification.\n" +
    "- `lastDeliveredAt`: MAX across all campaigns.\n" +
    "- `firstContactedAt` / `firstSentAt` / `firstDeliveredAt` / `firstOpenedAt` / `firstClickedAt` / `firstRepliedAt` / `firstBouncedAt` / `firstUnsubscribedAt`: first-occurrence (MIN) timestamp of each event type in the scope, null if it never happened; brand = MIN across campaigns. Each agrees with its boolean (non-null iff the boolean is true; `firstDeliveredAt` consistent with `delivered = sent AND NOT bounced`).\n\n" +
    "**`global.email`** aggregates `bounced`/`unsubscribed` across ALL campaigns in the org, regardless of brand or campaign filters.\n\n" +
    "Fields not applicable to the active mode are always present but set to `null`.",
  request: {
    headers: TrackingHeadersSchema,
    body: {
      content: { "application/json": { schema: StatusRequestSchema } },
    },
  },
  responses: {
    200: {
      description: "Delivery status results",
      content: { "application/json": { schema: StatusResponseSchema } },
    },
    400: {
      description: "Invalid request",
      content: { "application/json": { schema: ErrorSchema } },
    },
    401: { description: "Unauthorized" },
    500: {
      description: "Server error",
      content: { "application/json": { schema: ErrorSchema } },
    },
  },
});

// ─── Audit — sending forecast (staff ops) ───────────────────────────────────

const ForecastDaySchema = z
  .object({
    date: z.string().describe("Calendar day, YYYY-MM-DD (UTC)"),
    scheduledCount: z
      .number()
      .int()
      .describe("Emails scheduled to send that day across the whole fleet"),
  })
  .openapi("ForecastDay");

const SendingForecastResponseSchema = z
  .object({
    asOf: z.string().describe("ISO8601 timestamp of computation"),
    dailyCapacity: z
      .number()
      .int()
      .describe(
        "Emails/day the healthy fleet can send — Σ daily send limit over accounts passing filterHealthyAccounts (Instantly-active + warmup ≥ 100 + domain not blocked)",
      ),
    healthyAccountCount: z
      .number()
      .int()
      .describe("Accounts passing filterHealthyAccounts"),
    totalAccountCount: z
      .number()
      .int()
      .describe("All accounts in the shared workspace before filtering"),
    blockedDomainCount: z
      .number()
      .int()
      .describe("Accounts excluded because their domain is in BLOCKED_DOMAINS"),
    days: z
      .array(ForecastDaySchema)
      .describe(
        "Per-day scheduled send volume from today forward, chronological. Bounded: stops when the active-campaign backlog drains. May be [] when nothing is scheduled.",
      ),
  })
  .openapi("SendingForecastResponse");

registry.registerPath({
  method: "get",
  path: "/internal/audit/sending-forecast",
  summary: "Fleet sending forecast — daily capacity vs upcoming scheduled volume",
  description:
    "Platform-scoped (no org). Returns the cold-email fleet's available daily sending CAPACITY (sum of the daily send limit over only healthy accounts) alongside a TRUE per-day projection of upcoming scheduled send VOLUME (active campaigns' remaining un-sent sequence steps projected across the business-hours weekday send schedule). The volume projection is capacity-INDEPENDENT and bounded by the sequence structure — not a backlog÷capacity approximation. Fails loud (500) on any missing source; no silent zero fallbacks.",
  responses: {
    200: {
      description: "Sending forecast",
      content: {
        "application/json": { schema: SendingForecastResponseSchema },
      },
    },
    401: { description: "Unauthorized" },
    500: {
      description: "Server error (e.g. shared workspace key unavailable)",
      content: { "application/json": { schema: ErrorSchema } },
    },
  },
});

const InboxPlacementSchema = z
  .object({
    inboxPct: z.number().describe("Percentage of test emails landing in inbox"),
    spamPct: z.number().describe("Percentage landing in spam"),
    missingPct: z.number().describe("Percentage not delivered / missing"),
    testedAt: z.string().describe("ISO8601 timestamp of the placement test"),
  })
  .openapi("InboxPlacement");

const AccountHealthSchema = z
  .object({
    email: z.string().describe("Sending account email"),
    domain: z
      .string()
      .nullable()
      .describe("Sending domain (part after @); null if the email is malformed"),
    status: z
      .string()
      .describe("Existing status representation — 'active' when Instantly status > 0, else 'inactive'"),
    warmupScore: z
      .number()
      .int()
      .nullable()
      .describe("Instantly Health Score stat_warmup_score (0-100); null if unknown"),
    dailyLimit: z
      .number()
      .int()
      .nullable()
      .describe("Per-account daily send limit; null if unknown"),
    blocked: z
      .boolean()
      .describe(
        "True when the account is NOT send-eligible per the live send gate (filterHealthyAccounts/classifyAccountBlock)",
      ),
    blockReason: z
      .enum(["inactive", "under-warmed", "blacklisted-domain"])
      .nullable()
      .describe("Short reason when blocked (first failing gate); null when send-eligible"),
    inboxPlacement: InboxPlacementSchema.nullable().describe(
      "Inbox-placement breakdown — ALWAYS null in v1: the Instantly V2 API exposes no per-account placement property (only test-scoped, subscription-gated inbox-placement-test results). Never fabricated.",
    ),
    sentToday: z
      .number()
      .int()
      .describe(
        "Real (non-inferred) email_sent events observed today (UTC) from this account, from our silver log — the N in an N/dailyLimit read. 0 when none today, never fabricated.",
      ),
    queueSize: z
      .number()
      .int()
      .describe(
        "Emails queued to Instantly for this account but not yet sent — still-provisioned sequence-cost holds on active campaigns attributed to this account (1 campaign = 1 account). 0 when nothing queued. Campaigns not yet sending have an unknown account and are unattributed.",
      ),
    accountType: z
      .string()
      .nullable()
      .describe(
        "Connection provider from Instantly's provider_code — 'google' / 'microsoft' / 'imap'; null when unreported. This is the sending type, NOT the provisioning class (DFY-prewarmed vs legacy), which Instantly does not expose.",
      ),
  })
  .openapi("AccountHealth");

const AccountHealthResponseSchema = z
  .object({
    asOf: z.string().describe("ISO8601 timestamp of computation"),
    accounts: z
      .array(AccountHealthSchema)
      .describe(
        "Per-account deliverability health across the shared workspace. Always present; may be [].",
      ),
  })
  .openapi("AccountHealthResponse");

registry.registerPath({
  method: "get",
  path: "/internal/audit/account-health",
  summary: "Per-account deliverability health — identity, sending config, blocked state",
  description:
    "Platform-scoped (no org). Returns every sending account with its identity (email/domain), sending config (status, warmup Health Score, daily send limit), and blocked state (blocked + short blockReason, from the SAME gate the live send path uses — filterHealthyAccounts/classifyAccountBlock). `inboxPlacement` is the latest inbox/spam/missing breakdown from our own Bronze/Silver/Gold placement history (recurring inbox-placement tests promoted to silver, latest test per account blended across ESP); null when the account has never been in a test. The Instantly V2 API exposes no standing per-account placement property — this figure is derived from real test results, never fabricated. Fails loud (500) on any missing REQUIRED source (account list); no silent fallbacks.",
  responses: {
    200: {
      description: "Per-account deliverability health",
      content: {
        "application/json": { schema: AccountHealthResponseSchema },
      },
    },
    401: { description: "Unauthorized" },
    500: {
      description: "Server error (e.g. shared workspace key unavailable)",
      content: { "application/json": { schema: ErrorSchema } },
    },
  },
});

const PlacementHistoryEntrySchema = z
  .object({
    testId: z.string().describe("Instantly inbox-placement test ID"),
    inboxPct: z.number().describe("Percentage of seed emails landing in inbox"),
    spamPct: z.number().describe("Percentage landing in spam"),
    missingPct: z.number().describe("Percentage not delivered / missing"),
    testedAt: z.string().describe("ISO8601 timestamp of the test"),
  })
  .openapi("PlacementHistoryEntry");

const AccountHealthHistoryResponseSchema = z
  .object({
    email: z.string().describe("The queried sending account email"),
    history: z
      .array(PlacementHistoryEntrySchema)
      .describe("Blended placement per test, newest first. [] when never tested."),
  })
  .openapi("AccountHealthHistoryResponse");

registry.registerPath({
  method: "get",
  path: "/internal/audit/account-health/history",
  summary: "Per-account inbox-placement history (blended per test, newest first)",
  description:
    "Platform-scoped (no org). Returns the inbox-placement history for one sending account (`email` query param, required) — one blended inbox/spam/missing entry per inbox-placement test, newest first, from our silver placement results. Empty history when the account has never been in a test.",
  request: {
    query: z.object({
      email: z.string().describe("Sending account email (required)"),
    }),
  },
  responses: {
    200: {
      description: "Per-account placement history",
      content: {
        "application/json": { schema: AccountHealthHistoryResponseSchema },
      },
    },
    400: {
      description: "Missing email query param",
      content: { "application/json": { schema: ErrorSchema } },
    },
    401: { description: "Unauthorized" },
    500: {
      description: "Server error",
      content: { "application/json": { schema: ErrorSchema } },
    },
  },
});

const PlacementSyncAcceptedSchema = z
  .object({
    accepted: z.boolean(),
    runId: z.string().describe("Background run identifier (watch logs)"),
  })
  .openapi("PlacementSyncAccepted");

registry.registerPath({
  method: "post",
  path: "/internal/audit/placement-test/sync",
  summary: "Poll Instantly placement tests + analytics → promote to silver",
  description:
    "Platform-scoped (no org). Polls every Instantly inbox-placement test and its analytics rows, mirrors them to bronze, and promotes to silver so account-health + history reflect the latest results. Read-only against Instantly (spends no test quota). 202 + background; watch logs for `placement-sync: done`.",
  responses: {
    202: {
      description: "Accepted — sync runs in the background",
      content: { "application/json": { schema: PlacementSyncAcceptedSchema } },
    },
    401: { description: "Unauthorized" },
  },
});

const PlacementRunResponseSchema = z
  .object({
    created: z.number().int().describe("One-time placement tests created this call (1)"),
    testCode: z.string().describe("test_code assigned to the created one-time test"),
    recipientEsps: z
      .array(z.string())
      .describe("Recipient ESPs the test seeds (Google/Outlook)"),
  })
  .openapi("PlacementRunResponse");

registry.registerPath({
  method: "post",
  path: "/internal/audit/placement-test/run",
  summary: "Run one one-time inbox-placement test now (plan-compatible)",
  description:
    "Platform-scoped (no org). Creates ONE one-time (type 1) fleet inbox-placement test that runs immediately — the plan-compatible recurring path (the cron calls this every 6h). Automated (type 2) tests are HyperGrowth-gated (see /ensure); one-time tests run on the Growth Inbox Placement sub. SPENDS Growth-sub test quota → gated behind PLACEMENT_TESTS_ENABLED=true (returns 409 when disabled). Fails loud (500) on a create rejection (402 quota / 400).",
  responses: {
    200: {
      description: "One-time test created",
      content: { "application/json": { schema: PlacementRunResponseSchema } },
    },
    401: { description: "Unauthorized" },
    409: {
      description: "Placement testing disabled (PLACEMENT_TESTS_ENABLED != true)",
      content: { "application/json": { schema: ErrorSchema } },
    },
    500: {
      description: "Server error (e.g. Instantly 402 quota / 400)",
      content: { "application/json": { schema: ErrorSchema } },
    },
  },
});

const PlacementEnsureResponseSchema = z
  .object({
    existing: z.number().int().describe("Automated placement tests already present"),
    created: z.number().int().describe("Automated placement tests created this call"),
    perDay: z.number().int().describe("Target automated tests per day"),
  })
  .openapi("PlacementEnsureResponse");

registry.registerPath({
  method: "post",
  path: "/internal/audit/placement-test/ensure",
  summary: "Ensure the recurring automated inbox-placement tests exist",
  description:
    "Platform-scoped (no org). Ensures PLACEMENT_TESTS_PER_DAY automated (type 2) inbox-placement tests exist, staggered across the day, so Instantly runs the fleet placement test on a schedule server-side. Idempotent (creates only the missing ones). SPENDS Growth-sub test quota → gated behind PLACEMENT_TESTS_ENABLED=true (returns 409 when disabled). Fails loud (500) on a create rejection (402 quota / 400).",
  responses: {
    200: {
      description: "Schedule ensured",
      content: { "application/json": { schema: PlacementEnsureResponseSchema } },
    },
    401: { description: "Unauthorized" },
    409: {
      description: "Placement scheduling disabled (PLACEMENT_TESTS_ENABLED != true)",
      content: { "application/json": { schema: ErrorSchema } },
    },
    500: {
      description: "Server error (e.g. Instantly 402 quota / 400)",
      content: { "application/json": { schema: ErrorSchema } },
    },
  },
});
