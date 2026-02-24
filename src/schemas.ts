import { z } from "zod";
import {
  OpenAPIRegistry,
  extendZodWithOpenApi,
} from "@asteasolutions/zod-to-openapi";

extendZodWithOpenApi(z);

export const registry = new OpenAPIRegistry();

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

registry.registerPath({
  method: "post",
  path: "/webhooks/instantly",
  summary: "Receive Instantly webhook events",
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
      description: "Missing event_type",
      content: { "application/json": { schema: ErrorSchema } },
    },
    401: {
      description: "Invalid webhook secret",
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
    orgId: z.string().optional(),
    brandId: z.string(),
    appId: z.string(),
    runId: z.string(),
    campaignId: z.string(),
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
  path: "/send",
  summary: "Send email via Instantly campaign",
  request: {
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
    orgId: z.string(),
    clerkOrgId: z.string(),
    brandId: z.string(),
    appId: z.string(),
    name: z.string(),
    runId: z.string().optional(),
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
  path: "/campaigns",
  summary: "Create a campaign",
  request: {
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
  path: "/campaigns/{campaignId}",
  summary: "Get a campaign",
  request: {
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
  path: "/campaigns/by-org/{orgId}",
  summary: "List campaigns by organization",
  request: {
    params: z.object({ orgId: z.string() }),
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
  path: "/campaigns/{campaignId}/status",
  summary: "Update campaign status",
  request: {
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
  path: "/campaigns/check-status",
  summary: "Poll active campaigns for errors",
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
    orgId: z.string(),
    runId: z.string().optional(),
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
  path: "/campaigns/{campaignId}/leads",
  summary: "Add leads to a campaign",
  request: {
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
  path: "/campaigns/{campaignId}/leads",
  summary: "List campaign leads",
  request: {
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
  path: "/campaigns/{campaignId}/leads",
  summary: "Delete leads from a campaign",
  request: {
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

export const StatsRequestSchema = z
  .object({
    runIds: z.array(z.string()).optional(),
    clerkOrgId: z.string().optional(),
    brandId: z.string().optional(),
    appId: z.string().optional(),
    campaignId: z.string().optional(),
  })
  .openapi("StatsRequest");

export type StatsRequest = z.infer<typeof StatsRequestSchema>;

const StepStatsSchema = z.object({
  step: z.number().describe("Step number (1-based)"),
  emailsSent: z.number(),
  emailsOpened: z.number(),
  emailsReplied: z.number(),
  emailsBounced: z.number(),
});

const StatsResponseSchema = z
  .object({
    stats: z.object({
      emailsSent: z.number().describe("Total email_sent events"),
      emailsDelivered: z
        .number()
        .describe("emailsSent minus emailsBounced"),
      emailsOpened: z
        .number()
        .describe(
          "Unique recipients who opened (COUNT DISTINCT lead_email with email_opened events)",
        ),
      emailsClicked: z.number().describe("Total link click events"),
      emailsReplied: z.number().describe("Total reply_received events"),
      emailsBounced: z.number().describe("Total email_bounced events"),
      repliesAutoReply: z.number().describe("Total auto_reply_received events"),
      repliesNotInterested: z
        .number()
        .describe("Total lead_not_interested events"),
      repliesOutOfOffice: z
        .number()
        .describe("Total lead_out_of_office events"),
      repliesUnsubscribe: z
        .number()
        .describe("Total lead_unsubscribed events"),
    }),
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
  method: "post",
  path: "/stats",
  summary: "Get aggregated stats by filters",
  description:
    "Aggregates stats from webhook events across campaigns matching the provided filters. At least one filter is required. Response shape aligned with Postmark stats contract.",
  request: {
    body: {
      content: { "application/json": { schema: StatsRequestSchema } },
    },
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

// ─── Accounts ───────────────────────────────────────────────────────────────

registry.registerPath({
  method: "get",
  path: "/accounts",
  summary: "List email accounts",
  responses: {
    200: { description: "Accounts list" },
    401: { description: "Unauthorized" },
  },
});

registry.registerPath({
  method: "post",
  path: "/accounts/sync",
  summary: "Sync accounts from Instantly",
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
  path: "/accounts/{email}/warmup",
  summary: "Enable or disable warmup for an account",
  request: {
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
  path: "/accounts/warmup-analytics",
  summary: "Get warmup analytics",
  responses: {
    200: { description: "Warmup analytics" },
    401: { description: "Unauthorized" },
  },
});

// ─── Status ──────────────────────────────────────────────────────────────────

const StatusItemSchema = z.object({
  leadId: z.string().describe("Lead-service lead ID (human)"),
  email: z.string().describe("Email address"),
});

export const StatusRequestSchema = z
  .object({
    campaignId: z.string().describe("Campaign ID to scope the campaign-level results"),
    items: z
      .array(StatusItemSchema)
      .min(1)
      .describe("Lead/email pairs to check"),
  })
  .openapi("StatusRequest");

export type StatusRequest = z.infer<typeof StatusRequestSchema>;

const LeadStatusSchema = z.object({
  contacted: z.boolean(),
  delivered: z.boolean(),
  replied: z.boolean(),
  lastDeliveredAt: z.string().nullable(),
});

const EmailStatusSchema = z.object({
  contacted: z.boolean(),
  delivered: z.boolean(),
  bounced: z.boolean(),
  unsubscribed: z.boolean(),
  lastDeliveredAt: z.string().nullable(),
});

const ScopedStatusSchema = z.object({
  lead: LeadStatusSchema,
  email: EmailStatusSchema,
});

const StatusResultSchema = z.object({
  leadId: z.string(),
  email: z.string(),
  campaign: ScopedStatusSchema,
  global: ScopedStatusSchema,
});

const StatusResponseSchema = z
  .object({
    results: z.array(StatusResultSchema),
  })
  .openapi("StatusResponse");

registry.registerPath({
  method: "post",
  path: "/status",
  summary: "Batch delivery status check for leads/emails",
  description:
    "Returns campaign-scoped and global (cross-campaign) delivery status " +
    "for each lead/email pair. Campaign-scoped filters by the given campaignId; " +
    "global aggregates across all campaigns.",
  request: {
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
