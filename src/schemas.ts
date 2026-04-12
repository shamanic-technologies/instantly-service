import { z } from "zod";
import {
  OpenAPIRegistry,
  extendZodWithOpenApi,
} from "@asteasolutions/zod-to-openapi";

extendZodWithOpenApi(z);

export const registry = new OpenAPIRegistry();

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

export const CreateCampaignRequestSchema = z
  .object({
    name: z.string(),
    accountIds: z.array(z.string()).optional(),
    metadata: z.record(z.string(), z.unknown()).optional(),
  })
  .openapi("CreateCampaignRequest");

export type CreateCampaignRequest = z.infer<typeof CreateCampaignRequestSchema>;

const CampaignSummarySchema = z
  .object({
    id: z.string(),
    instantlyCampaignId: z.string(),
    name: z.string(),
    status: z.string(),
  })
  .openapi("CampaignSummary");

const CreateCampaignResponseSchema = z
  .object({
    success: z.boolean(),
    campaign: CampaignSummarySchema,
  })
  .openapi("CreateCampaignResponse");

registry.registerPath({
  method: "post",
  path: "/orgs/campaigns",
  summary: "Create a campaign",
  description:
    "Brand IDs and workflow slug are read from headers (x-brand-id, x-workflow-slug) — do NOT pass them in the body.",
  request: {
    headers: TrackingHeadersSchema,
    body: {
      content: {
        "application/json": { schema: CreateCampaignRequestSchema },
      },
    },
  },
  responses: {
    201: {
      description: "Campaign created",
      content: {
        "application/json": { schema: CreateCampaignResponseSchema },
      },
    },
    400: {
      description: "Missing required fields",
      content: { "application/json": { schema: ErrorSchema } },
    },
    401: { description: "Unauthorized" },
    500: {
      description: "Failed to create campaign",
      content: { "application/json": { schema: ErrorSchema } },
    },
  },
});

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

// ─── Leads ──────────────────────────────────────────────────────────────────

const LeadInputSchema = z.object({
  email: z.string(),
  firstName: z.string().optional(),
  lastName: z.string().optional(),
  companyName: z.string().optional(),
  customVariables: z.record(z.string(), z.string()).optional(),
});

export const AddLeadsRequestSchema = z
  .object({
    leads: z.array(LeadInputSchema),
  })
  .openapi("AddLeadsRequest");

export type AddLeadsRequest = z.infer<typeof AddLeadsRequestSchema>;

const AddLeadsResponseSchema = z
  .object({
    success: z.boolean(),
    added: z.number(),
    total: z.number(),
  })
  .openapi("AddLeadsResponse");

registry.registerPath({
  method: "post",
  path: "/orgs/campaigns/{campaignId}/leads",
  summary: "Add leads to a campaign",
  request: {
    headers: TrackingHeadersSchema,
    params: z.object({ campaignId: z.string() }),
    body: {
      content: { "application/json": { schema: AddLeadsRequestSchema } },
    },
  },
  responses: {
    201: {
      description: "Leads added",
      content: { "application/json": { schema: AddLeadsResponseSchema } },
    },
    400: {
      description: "Missing required fields",
      content: { "application/json": { schema: ErrorSchema } },
    },
    404: {
      description: "Campaign not found",
      content: { "application/json": { schema: ErrorSchema } },
    },
    401: { description: "Unauthorized" },
    500: {
      description: "Failed to add leads",
      content: { "application/json": { schema: ErrorSchema } },
    },
  },
});

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

export const DeleteLeadsRequestSchema = z
  .object({
    emails: z.array(z.string()),
  })
  .openapi("DeleteLeadsRequest");

export type DeleteLeadsRequest = z.infer<typeof DeleteLeadsRequestSchema>;

const DeleteLeadsResponseSchema = z
  .object({
    success: z.boolean(),
    deleted: z.number(),
  })
  .openapi("DeleteLeadsResponse");

registry.registerPath({
  method: "delete",
  path: "/orgs/campaigns/{campaignId}/leads",
  summary: "Delete leads from a campaign",
  request: {
    headers: TrackingHeadersSchema,
    params: z.object({ campaignId: z.string() }),
    body: {
      content: {
        "application/json": { schema: DeleteLeadsRequestSchema },
      },
    },
  },
  responses: {
    200: {
      description: "Leads deleted",
      content: {
        "application/json": { schema: DeleteLeadsResponseSchema },
      },
    },
    400: {
      description: "Missing emails",
      content: { "application/json": { schema: ErrorSchema } },
    },
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
    workflowDynastySlug: z.string().optional().describe("Filter by workflow dynasty slug (resolved to all versioned slugs)"),
    featureDynastySlug: z.string().optional().describe("Filter by feature dynasty slug (resolved to all versioned slugs)"),
    groupBy: z.enum(["brandId", "campaignId", "workflowSlug", "featureSlug", "leadEmail", "workflowDynastySlug", "featureDynastySlug"]).optional().describe("Group results by dimension"),
  })
  .openapi("StatsQuery");

export type StatsQuery = z.infer<typeof StatsQuerySchema>;

const RepliesDetailSchema = z.object({
  interested: z.number().describe("lead_interested events"),
  meetingBooked: z.number().describe("lead_meeting_booked events"),
  closed: z.number().describe("lead_closed events"),
  notInterested: z.number().describe("lead_not_interested events"),
  wrongPerson: z.number().describe("lead_wrong_person events"),
  unsubscribe: z.number().describe("lead_unsubscribed events"),
  neutral: z.number().describe("lead_neutral events"),
  autoReply: z.number().describe("auto_reply_received events"),
  outOfOffice: z.number().describe("lead_out_of_office events"),
});

const RepliesAggregatesSchema = z.object({
  repliesPositive: z.number().describe("interested + meetingBooked + closed"),
  repliesNegative: z.number().describe("notInterested + wrongPerson + unsubscribe"),
  repliesNeutral: z.number().describe("neutral (lead_neutral events only)"),
  repliesAutoReply: z.number().describe("autoReply + outOfOffice"),
  repliesDetail: RepliesDetailSchema,
});

const StepStatsSchema = z.object({
  step: z.number().describe("Step number (1-based)"),
  emailsSent: z.number(),
  emailsOpened: z.number(),
  emailsBounced: z.number(),
}).merge(RepliesAggregatesSchema);

const StatsResponseSchema = z
  .object({
    stats: z.object({
      emailsContacted: z
        .number()
        .describe("Leads added to a campaign (row exists in instantly_campaigns, immediate)"),
      emailsSent: z.number().describe("Total email_sent events (confirmed by Instantly webhook)"),
      emailsDelivered: z
        .number()
        .describe("emailsSent minus emailsBounced"),
      emailsOpened: z
        .number()
        .describe(
          "Unique recipients who opened (COUNT DISTINCT lead_email with email_opened events)",
        ),
      emailsClicked: z.number().describe("Total link click events"),
      emailsBounced: z.number().describe("Total email_bounced events"),
    }).merge(RepliesAggregatesSchema),
    recipients: z
      .number()
      .describe("Unique recipients (COUNT DISTINCT lead_email with email_sent events)"),
    stepStats: z
      .array(StepStatsSchema)
      .optional()
      .describe("Per-step breakdown (only present when step data exists)"),
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
  stats: StatsResponseSchema.shape.stats,
  recipients: z.number(),
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

const ReplyClassificationSchema = z.enum([
  "interested", "meetingBooked", "closed",
  "notInterested", "wrongPerson",
  "neutral", "outOfOffice", "autoReply",
]);

const ScopedStatusFieldsSchema = z.object({
  contacted: z.boolean().describe("true if the lead was added to this campaign/brand"),
  delivered: z.boolean().describe("true if at least one email was delivered (sent/delivered/replied status)"),
  opened: z.boolean().describe("true if at least one email_opened event exists"),
  replied: z.boolean().describe("true if at least one reply was received"),
  replyClassification: ReplyClassificationSchema.nullable().describe("Reply classification based on Instantly interest status. null = no reply"),
  bounced: z.boolean().describe("true if at least one email bounced"),
  unsubscribed: z.boolean().describe("true if lead unsubscribed"),
  lastDeliveredAt: z.string().nullable().describe("ISO 8601 timestamp of the most recent delivery. null if never delivered"),
});

const GlobalEmailStatusSchema = z.object({
  bounced: z.boolean().describe("true if this email bounced in ANY campaign across the entire org (not scoped to brand)"),
  unsubscribed: z.boolean().describe("true if this email unsubscribed in ANY campaign across the entire org (not scoped to brand)"),
});

const GlobalStatusSchema = z.object({
  email: GlobalEmailStatusSchema,
});

const StatusResultSchema = z.object({
  email: z.string(),
  byCampaign: z.record(z.string(), ScopedStatusFieldsSchema).nullable().describe("Per-campaign breakdown — present only when brandId is provided without campaignId"),
  brand: ScopedStatusFieldsSchema.nullable().describe("Aggregated brand status (most advanced across campaigns) — present only when brandId is provided without campaignId"),
  campaign: ScopedStatusFieldsSchema.nullable().describe("Campaign-scoped status — present only when campaignId is provided"),
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
              contacted: true, delivered: true, opened: true, replied: false,
              replyClassification: null, bounced: false, unsubscribed: false,
              lastDeliveredAt: "2026-03-01T10:00:00.000Z",
            },
            "c1a2b3c4-0000-0000-0000-000000000002": {
              contacted: true, delivered: true, opened: false, replied: true,
              replyClassification: "interested", bounced: false, unsubscribed: false,
              lastDeliveredAt: "2026-03-02T12:00:00.000Z",
            },
          },
          brand: {
            contacted: true, delivered: true, opened: true, replied: true,
            replyClassification: "interested", bounced: false, unsubscribed: false,
            lastDeliveredAt: "2026-03-02T12:00:00.000Z",
          },
          campaign: null,
          global: { email: { bounced: false, unsubscribed: false } },
        },
      ],
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
    "- Boolean fields (`contacted`, `delivered`, `opened`, `replied`, `bounced`, `unsubscribed`): `true` if true in at least one campaign (BOOL_OR).\n" +
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
