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
    subject: z.string().describe("Shared subject for all steps in the sequence"),
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
    campaignId: z.string(),
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

const CheckStatusErrorSchema = z.object({
  instantlyCampaignId: z.string(),
  campaignId: z.string().nullable(),
  leadEmail: z.string().nullable(),
  reason: z.string(),
});

const CheckStatusResponseSchema = z
  .object({
    checked: z.number().describe("Number of active campaigns checked"),
    errors: z.array(CheckStatusErrorSchema).describe("Campaigns that entered error state"),
  })
  .openapi("CheckStatusResponse");

registry.registerPath({
  method: "post",
  path: "/internal/campaigns/check-status",
  summary: "Poll active campaigns for errors",
  request: {},
  description:
    "Checks all active campaigns against the Instantly API to detect error states. " +
    "For each errored campaign: updates DB status, cancels provisioned costs, fails the run, " +
    "and sends an admin notification.",
  responses: {
    200: {
      description: "Status check complete",
      content: { "application/json": { schema: CheckStatusResponseSchema } },
    },
    401: { description: "Unauthorized" },
    500: {
      description: "Server error",
      content: { "application/json": { schema: ErrorSchema } },
    },
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
    "Daily sweep (02:00 UTC) that scans campaigns with " +
    "`delivery_status='contacted'` stuck for >24h, fetches " +
    "`not_sending_status` live from Instantly, and for each row Instantly is " +
    "refusing to dispatch: pauses the Instantly campaign, writes the observed " +
    "`not_sending_status` onto the row, cancels actual+provisioned costs, " +
    "marks `delivery_status='cancelled'`. Idempotent — already-cancelled rows " +
    "fall outside the SELECT.\n\n" +
    "Per-run cap: 500 oldest rows. Serialized via a Postgres advisory lock — " +
    "overlapping calls short-circuit with no-op summary. Returns 202 " +
    "immediately and runs in the background. Verify completion via Railway " +
    "logs (`retry-stuck: done`).",
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

export const StatsQuerySchema = z
  .object({
    runIds: z.string().optional().describe("Comma-separated list of run IDs"),
    brandId: z.string().optional().describe("Filter by brand ID (matches campaigns containing this brand)"),
    campaignId: z.string().optional(),
    workflowSlugs: z.string().optional().describe("Comma-separated list of workflow slugs to filter by"),
    featureSlugs: z.string().optional().describe("Comma-separated list of feature slugs to filter by"),
    groupBy: z.enum(["brandId", "campaignId", "workflowSlug", "featureSlug", "leadEmail"]).optional().describe("Group results by dimension"),
  })
  .openapi("StatsQuery");

export type StatsQuery = z.infer<typeof StatsQuerySchema>;

const StatsResponseSchema = z
  .object({
    recipientStats: RecipientStatsSchema,
    emailStats: EmailStatsSchema,
  })
  .openapi("StatsResponse");

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
      content: { "application/json": { schema: StatsResponseSchema } },
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
      content: { "application/json": { schema: StatsResponseSchema } },
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
    "- `lastDeliveredAt`: MAX across all campaigns.\n\n" +
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
