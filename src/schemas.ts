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
    eventType: z.string().nullable().describe("Null when the payload carried no event_type"),
    bronzeRowId: z.string().nullable(),
    promoted: z.boolean(),
    degraded: z.boolean().describe("True when the event could not be fully ingested. The endpoint still answers 200 — Instantly disables a webhook on repeated non-2xx"),
    degradedReason: z.string().nullable().describe("invalid_payload | missing_campaign_id | unknown_campaign_id | campaign lookup failed: … | bronze failed: … | silver failed: …"),
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
  },
});

// ─── Unsubscribe (self-send opt-out) ────────────────────────────────────────

const UnsubscribePathParams = z.object({
  payload: z
    .string()
    .describe("base64url of `<instantlyCampaignId>:<leadEmail>`"),
  signature: z.string().describe("base64url HMAC-SHA256 of the payload"),
});

registry.registerPath({
  method: "get",
  path: "/u/{payload}/{signature}",
  summary: "Render the opt-out confirmation for a self-sent email",
  description:
    "Public and unauthenticated — a prospect clicks it from their inbox — so the " +
    "HMAC in the URL is the entire gate. This endpoint does NOT unsubscribe: " +
    "corporate link scanners fetch every URL in an inbound email before the human " +
    "sees it, so acting on GET would opt out prospects who never clicked. It " +
    "returns an HTML page carrying a POST form. An invalid signature and an " +
    "unknown campaign both return 404, so the route cannot enumerate campaigns.",
  request: { params: UnsubscribePathParams },
  responses: {
    200: { description: "HTML confirmation page", content: { "text/html": { schema: z.string() } } },
    404: { description: "Invalid signature or malformed payload", content: { "text/html": { schema: z.string() } } },
  },
});

registry.registerPath({
  method: "post",
  path: "/u/{payload}/{signature}",
  summary: "Opt a recipient out of a self-sent sequence",
  description:
    "Performs the opt-out, and is also the RFC 8058 one-click target advertised " +
    "by the List-Unsubscribe-Post header. Promotes a real `lead_unsubscribed` " +
    "silver event through the shared promoteEvent path, so the existing machinery " +
    "stops the sequence, cancels the remaining provisioned holds and marks the " +
    "campaign terminal.",
  request: { params: UnsubscribePathParams },
  responses: {
    200: { description: "HTML confirmation that the recipient was removed", content: { "text/html": { schema: z.string() } } },
    404: { description: "Invalid signature or malformed payload", content: { "text/html": { schema: z.string() } } },
  },
});

registry.registerPath({
  method: "get",
  path: "/c/{payload}/{signature}",
  summary: "Record a click and redirect to its real destination",
  description:
    "Public and unauthenticated — a prospect follows it from their inbox — so the " +
    "HMAC in the URL is the entire gate. The destination is INSIDE the signed " +
    "payload, never a query parameter: a redirector that forwards to whatever a " +
    "caller supplies is an open redirect, which would let anyone borrow the " +
    "domain to bounce victims at a phishing page and get it blacklisted. Records " +
    "the hit in bronze, promotes `email_link_clicked` (which is what stop-on-click " +
    "fires on), then 302s. An invalid signature, a malformed payload and a " +
    "non-http destination all return 404 identically.",
  request: { params: z.object({ payload: z.string(), signature: z.string() }) },
  responses: {
    302: { description: "Redirect to the signed destination" },
    404: { description: "Invalid signature, malformed payload, or unusable destination", content: { "text/html": { schema: z.string() } } },
  },
});

// ─── Self-send dispatch (ops trigger) ───────────────────────────────────────

const SelfSendDispatchRequestSchema = z
  .object({
    limit: z
      .number()
      .int()
      .optional()
      .describe("Cap the batch. Omit to send everything currently due."),
  })
  .openapi("SelfSendDispatchRequest");

const AcceptedResponseSchema = z
  .object({ accepted: z.boolean() })
  .openapi("SelfSendDispatchAccepted");

registry.registerPath({
  method: "post",
  path: "/internal/self-send/dispatch",
  summary: "Send the steps now due on the self-hosted SMTP transport",
  description:
    "Platform-scoped ops sweep. Reads the still-provisioned cost ledger for " +
    "campaigns frozen to send_transport='smtp', picks each lead's next due step " +
    "from the shared cadence, clips to what each mailbox can still send today " +
    "(min(daily_limit, rampCapForVolume) minus real sends so far), dispatches, and " +
    "promotes a real `email_sent`. Idempotent: a step leaves the due set as soon " +
    "as its hold is actualized. Returns 202 and runs in the background — watch " +
    "for `self-send-dispatch: done`. 409 when SELF_SEND_DISPATCH_ENABLED is not " +
    "'true'.",
  request: {
    body: {
      content: { "application/json": { schema: SelfSendDispatchRequestSchema } },
    },
  },
  responses: {
    202: {
      description: "Sweep accepted; runs in the background",
      content: { "application/json": { schema: AcceptedResponseSchema } },
    },
    409: {
      description: "Disabled (SELF_SEND_DISPATCH_ENABLED is not 'true')",
      content: { "application/json": { schema: ErrorSchema } },
    },
  },
});

registry.registerPath({
  method: "post",
  path: "/internal/self-send/poll",
  summary: "Read the self-send mailboxes and ingest replies and bounces",
  description:
    "Platform-scoped ops sweep. Connects to each mailbox on send_transport='smtp' " +
    "over IMAP, correlates what it finds against the Message-Ids we know we sent, " +
    "and promotes `reply_received`, `auto_reply_received` or `email_bounced` into " +
    "silver. An autoresponder is deliberately NOT a reply: `reply_received` stops " +
    "the sequence and cancels the lead's remaining holds, so filing an " +
    "out-of-office as one would end the outreach for a prospect who never " +
    "engaged. Only a PERMANENT delivery failure becomes `email_bounced`: a " +
    "delivery-status notice reporting a temporary delay (RFC 3464 " +
    "`Action: delayed`, 4.x.x, Gmail 'Delivery incomplete') is stored as " +
    "`delay` and promotes nothing. Idempotent without a cursor — each run re-reads an overlapping " +
    "window and the unique (account, message_id) bronze index absorbs the " +
    "overlap. Returns 202 and runs in the background; watch for " +
    "`self-send-poll: done`. 409 when SELF_SEND_DISPATCH_ENABLED is not 'true'.",
  responses: {
    202: {
      description: "Poll accepted; runs in the background",
      content: { "application/json": { schema: AcceptedResponseSchema } },
    },
    409: {
      description: "Disabled (SELF_SEND_DISPATCH_ENABLED is not 'true')",
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
    duplicate: z
      .boolean()
      .optional()
      .describe("True when this lead is already claimed for this campaign; nothing was sent by this call."),
    held: z
      .object({
        state: z
          .enum(["in_flight", "queued", "finished"])
          .describe(
            "`in_flight`: a concurrent send is creating the sequence right now. `queued`: we hold the sequence and will send its remaining steps — the lead is NOT lost, do not re-serve it. `finished`: nothing left to send (every step sent, or the sequence stopped).",
          ),
        awaitingFirstEmail: z.boolean().describe("True while the sequence's FIRST email has not gone out yet."),
        queuedSince: z.string().nullable().describe("When the held sequence was handed to us (ISO 8601)."),
        remainingSteps: z.number().int().describe("Steps still scheduled and not yet sent."),
      })
      .optional()
      .describe("Present on a duplicate: what we hold for this lead, so a caller can tell queued from lost."),
  })
  .openapi("SendResponse");

/**
 * A 409 on /orgs/send is a REFUSAL, distinguishable from a success (200) and
 * from a transport failure (500). `code` separates the two refusal reasons.
 */
const SendRefusalSchema = z
  .object({
    error: z.string(),
    code: z.enum(["recent_brand_contact", "lead_opted_out"]),
    details: z.string(),
    brandId: z.string().optional().describe("recent_brand_contact only — the brand already contacted"),
    lastEmailedAt: z
      .string()
      .optional()
      .describe("recent_brand_contact only — when the previous email to this person for that brand went out"),
    windowInterval: z
      .string()
      .optional()
      .describe("recent_brand_contact only — the re-contact window, e.g. \"3 months\""),
  })
  .openapi("SendRefusal");

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
    409: {
      description:
        "Refused, not a transport failure — no email was sent and nothing was billed. " +
        "`code: \"recent_brand_contact\"` = the recipient was already emailed for one of " +
        "this send's brands inside the 3-month re-contact window. " +
        "`code: \"lead_opted_out\"` = the recipient asked this org to stop. " +
        "A different lead_id for an address already on file is NOT refused: the email is the " +
        "identity and the stored rows are re-keyed onto the caller's lead_id.",
      content: { "application/json": { schema: SendRefusalSchema } },
    },
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

const REPLY_KIND_VALUES = [
  "lead_interested",
  "lead_referral",
  "lead_info_requested",
  "lead_meeting_requested",
  "lead_not_interested",
  "lead_wrong_person",
  "lead_changed_job",
  "lead_neutral",
  "lead_out_of_office",
  "auto_reply_received",
] as const;

export const ReplyKindSchema = z
  .enum(REPLY_KIND_VALUES)
  .describe(
    "What KIND of reply arrived, and nothing about how far the deal got. Positive splits four ways: lead_interested (personally interested), lead_referral (not personally interested but relevant — points at the right person), lead_info_requested (wants to know more), lead_meeting_requested (wants to book). Negative splits by whether the no is about the moment or the person: lead_not_interested (declines today — recyclable), lead_wrong_person (not the right contact, hands nothing back) and lead_changed_job (has left the role we were selling to). The last two are objective facts about the person and permanent for the lead; the first is not. Deal outcomes (a booked meeting, a closed deal) are lead outcomes owned by the lead-outcomes service, not reply kinds.",
  );

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

/**
 * The contract's `StatusScope`, plus the two ADDITIVE fields this service
 * serves on top of it.
 *
 * `replyClassification` keeps its exact meaning — every existing consumer is
 * unaffected — but it cannot answer the question the triage board asks: a "no"
 * about the MOMENT and a "no" about the PERSON are both `negative`, so a
 * recyclable lead and a permanently disqualified one arrive downstream looking
 * identical. `replyKind` carries the finer statement and `disqualified` is
 * strictly derived from it, so the two can never contradict each other.
 *
 * Declared here rather than in the contract package: the vocabulary is this
 * service's, and widening the shared contract is a separate cross-repo change.
 */
const ScopedStatusSchema = StatusScopeSchema.extend({
  replyKind: ReplyKindSchema.nullable().describe(
    "WHICH reply this is — the finer reading of `replyClassification`, from the human statement when one exists and from the automatic classification otherwise. Null when no reply kind is on record.",
  ),
  disqualified: z
    .boolean()
    .describe(
      "True iff this person is PERMANENTLY out: they are not the right contact (`lead_wrong_person`) or they have left the role (`lead_changed_job`). A prospect who simply declines today (`lead_not_interested`) is NOT disqualified — the lead stays recyclable. Strictly a function of `replyKind`, so the two fields never disagree; false whenever no kind is on record, because an absence is not a disqualification.",
    ),
  queued: z
    .boolean()
    .describe(
      "True iff we still HOLD this lead in our own send queue: an active sequence with at least one step scheduled and not yet sent (not stopped, not cancelled). A queued lead is NOT lost — it will be sent, and a repeat `POST /orgs/send` for it returns `duplicate: true` without sending. `contacted: true, sent: false` alone cannot tell queued from lost; this can. False when nothing is held.",
    ),
  queuedSince: z
    .string()
    .nullable()
    .describe(
      "When the earliest still-held sequence in this scope was handed to us (ISO 8601). Null when `queued` is false.",
    ),
  awaitingFirstEmail: z
    .boolean()
    .describe(
      "True iff a held sequence in this scope has not sent its FIRST email yet (`queued` and nothing sent on it). The first email of such a sequence goes out in the prospect's next business-hours window on any production mailbox with room.",
    ),
  finished: z
    .boolean()
    .describe(
      "True iff we hold this lead's claim in this scope and are DONE with it: the sequence ended, was stopped or was cancelled, and nothing is left to send. A repeat `POST /orgs/send` for the same (campaign, email) returns `duplicate: true` with `held.state: \"finished\"` and sends nothing, so re-serving the lead can never produce an email here — whatever its age. Never true while `queued` is true. Brand scope: true only when every campaign of the brand is finished with the lead. False when nothing is held.",
    ),
});

const StatusResultSchema = z.object({
  email: z.string(),
  byCampaign: z.record(z.string(), ScopedStatusSchema).nullable().describe("Per-campaign breakdown — present only when brandId is provided without campaignId"),
  brand: ScopedStatusSchema.nullable().describe("Aggregated brand status (most advanced across campaigns) — present only when brandId is provided without campaignId"),
  campaign: ScopedStatusSchema.nullable().describe("Campaign-scoped status — present only when campaignId is provided"),
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
              replied: false, replyClassification: null, replyKind: null, disqualified: false,
              bounced: false, unsubscribed: false,
              cancelled: false, lastDeliveredAt: "2026-03-01T10:00:00.000Z",
            },
            "c1a2b3c4-0000-0000-0000-000000000002": {
              contacted: true, sent: true, delivered: true, opened: false, clicked: true,
              replied: true, replyClassification: "positive", replyKind: "lead_interested",
              disqualified: false, bounced: false, unsubscribed: false,
              cancelled: false, lastDeliveredAt: "2026-03-02T12:00:00.000Z",
            },
          },
          brand: {
            contacted: true, sent: true, delivered: true, opened: true, clicked: true,
            replied: true, replyClassification: "positive", replyKind: "lead_interested",
            disqualified: false, bounced: false, unsubscribed: false,
            cancelled: false, lastDeliveredAt: "2026-03-02T12:00:00.000Z",
          },
          campaign: null,
          global: { email: { bounced: false, unsubscribed: false } },
        },
      ],
    },
  });

// ─── Manual Qualifications ──────────────────────────────────────────────────

/**
 * The reply-kind vocabulary, plus the two legacy deal-progress values the staff
 * console is still writing today. Every value resolves to a reply kind at WRITE
 * (see lib/reply-kind) — bronze keeps the raw statement, readers see the new
 * vocabulary only. Dropping the legacy pair is a separate, later change.
 */
const MANUAL_QUALIFICATION_STATUS_VALUES = [
  "lead_interested",
  "lead_referral",
  "lead_info_requested",
  "lead_meeting_requested",
  "lead_not_interested",
  "lead_wrong_person",
  "lead_changed_job",
  "lead_neutral",
  "lead_out_of_office",
  "auto_reply_received",
  // Legacy deal-progress values — accepted, resolved to `lead_interested`.
  "lead_meeting_booked",
  "lead_closed",
] as const;

export const ManualQualificationStatusSchema = z
  .enum(MANUAL_QUALIFICATION_STATUS_VALUES)
  .describe(
    "The raw statement a human made about a reply. Normally a reply kind; the two deal-progress values (lead_meeting_booked, lead_closed) are still ACCEPTED while the consoles migrate their pickers and resolve to lead_interested at write.",
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
  replyKind: ReplyKindSchema.describe(
    "The reply kind this statement resolved to at write time. Always a value of the current vocabulary, including for a statement made with a legacy deal-progress value.",
  ),
  qualifiedBy: z.string(),
  notes: z.string().nullable(),
  qualifiedAt: z.string().describe("ISO 8601 timestamp"),
  withdrawnAt: z
    .string()
    .nullable()
    .describe(
      "ISO 8601 timestamp of the withdrawal, or null while the statement still STANDS. Non-null means a human took this statement back: it is kept for audit and must never be rendered as the lead's current reply kind.",
    ),
  withdrawnBy: z
    .string()
    .nullable()
    .describe("Who withdrew the statement, or null while it still stands."),
});

export const ManualQualificationWithdrawBodySchema = z
  .object({
    campaign_id: z.string().min(1).describe("Logical campaign id (groups sub-campaigns for the same workflow run)"),
    email: z.string().email().describe("Lead email address"),
    notes: z.string().max(2000).optional().describe("Optional free-text human note for audit"),
  })
  .openapi("ManualQualificationWithdrawBody", {
    example: {
      campaign_id: "c1a2b3c4-0000-0000-0000-000000000001",
      email: "alice@media.com",
      notes: "Picked the wrong kind by mistake",
    },
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

/** The 404 body of the withdrawal route — `code` is what makes the refusal
 *  distinguishable from a transport failure and from its sibling 404. */
const ManualQualificationWithdrawErrorSchema = z
  .object({
    error: z.string(),
    code: z
      .enum(["no_standing_qualification", "campaign_not_found"])
      .describe(
        "no_standing_qualification: nobody currently stands behind a reply kind for this pair (nothing was ever stated, or the statement is already withdrawn). campaign_not_found: no campaign in this org for the given email.",
      ),
  })
  .openapi("ManualQualificationWithdrawError");

const ManualQualificationWithdrawResponseSchema = z
  .object({
    qualification: ManualQualificationRowSchema.describe(
      "The statement that was withdrawn, now carrying withdrawnAt / withdrawnBy.",
    ),
  })
  .openapi("ManualQualificationWithdrawResponse");

export const ReplyToLeadBodySchema = z
  .object({
    campaign_id: z
      .string()
      .min(1)
      .describe("Logical campaign id — the same key manual qualifications and opt-outs use"),
    email: z.string().email().describe("The lead who replied"),
    body_html: z
      .string()
      .min(1)
      .describe(
        "The answer, HTML. Signed by this service with the sending account's persona — do NOT include a signature.",
      ),
    sent_by: z
      .enum(["human", "automation"])
      .optional()
      .describe(
        "Who asked for this answer to be sent. It decides ONE thing: whether the human-takeover gate applies. An `automation` reply is refused (409 `human_took_over`) once a person has answered the thread since the prospect last wrote; a `human` reply is never gated. Absent resolves to `automation` — the automated responder is the only caller today, so the gate is live without waiting on it, and a human surface declares `human` explicitly.",
      ),
  })
  .openapi("ReplyToLeadBody", {
    example: {
      campaign_id: "c1a2b3c4-0000-0000-0000-000000000001",
      email: "alice@media.com",
      body_html: "<p>Great — how does Thursday 3pm look?</p>",
      sent_by: "automation",
    },
  });

const ReplyToLeadResultSchema = z
  .object({
    transport: z
      .enum(["instantly", "smtp"])
      .describe("Which pipe carried the reply — the one that carried the outreach"),
    instantlyCampaignId: z.string(),
    leadEmail: z.string(),
    accountEmail: z
      .string()
      .describe("The mailbox that answered. Resolved by this service, never supplied by the caller."),
    from: z.string().describe("The From header as the prospect sees it, persona included"),
    subject: z.string().describe("The conversation's own subject under `Re:`"),
    cc: z
      .string()
      .describe(
        "The agency inbox this reply was CC'd to — VISIBLE to the prospect, so a reply-all reaches a human. Never a BCC.",
      ),
    messageId: z.string().describe("Instantly's email id, or the RFC 5322 Message-Id we sent"),
    inReplyTo: z.string().describe("The prospect message this reply threads onto"),
  })
  .openapi("ReplyToLeadResult");

const ReplyToLeadResponseSchema = z
  .object({
    success: z.literal(true),
    status: z.literal("sent"),
    reply: ReplyToLeadResultSchema,
  })
  .openapi("ReplyToLeadResponse");

const ReplyToLeadErrorSchema = z
  .object({
    error: z.string(),
    code: z
      .enum([
        "campaign_not_found",
        "no_reply_to_thread",
        "sending_account_unresolved",
        "mailbox_credential_unavailable",
        "human_took_over",
        "reply_dispatch_failed",
      ])
      .describe(
        "campaign_not_found: no campaign in this org for the given email. no_reply_to_thread: the lead never wrote back, so there is nothing to thread onto. sending_account_unresolved: we cannot tell which mailbox contacted them. mailbox_credential_unavailable: the mailbox is on our own sender and we hold no credential for it. human_took_over: a person already answered this thread since the prospect last wrote, so an automated reply is refused. reply_dispatch_failed: the transport refused the send.",
      ),
  })
  .openapi("ReplyToLeadError");

registry.registerPath({
  method: "post",
  path: "/orgs/replies",
  summary: "Reply to a lead who replied, in their existing thread",
  description:
    "Answer a prospect who wrote back, in the SAME email thread, from the SAME mailbox that originally contacted them, under the SAME persona they have been corresponding with.\n\n" +
    "**The sending identity is resolved here, never supplied.** Which mailbox answers comes from `instantly_campaigns.account_email` (persisted at send time), with the mailbox recorded on the inbound message as the fallback for a historical row. The display name and signature follow that account. A caller-supplied from-address would let a reply arrive from a mailbox this prospect has never heard from — the exact failure this endpoint exists to prevent.\n\n" +
    "**Threaded or nothing.** On the Instantly transport the reply goes out through `POST /emails/reply`, threaded onto the prospect's latest inbound message; on the self-send transport we dispatch it ourselves with `In-Reply-To` / `References` over the conversation we already hold in bronze. There is NO fallback to a fresh email: if the thread cannot be found the call fails with `code: \"no_reply_to_thread\"`.\n\n" +
    "**The agency inbox is CC'd, visibly.** Every reply carries it as a CC (never a BCC) so a human can read the exchange and be pulled into it by a reply-all. Cold sequence sends carry no CC — this applies only to the one-to-one answer.\n\n" +
    "**Signature:** send the prospect-facing words only. This service appends the account's persona signature (idempotently — a body re-sent never stacks signatures). Deliberately NO unsubscribe footer: a one-to-one answer is not bulk mail.\n\n" +
    "**The answer goes out immediately, at any hour, on any day.** A prospect who just wrote to us is at their inbox now, so a one-to-one answer is never held for their business hours (cold sequence steps still are). It is still NOT a sequence step: no hold, no step number, no capacity consumed.\n\n" +
    "**A human takeover stops the automated responder.** When `sent_by` is `automation` (the default), the reply is refused with `409 human_took_over` if a PERSON has answered this thread since the prospect last wrote — read from what actually went out, both from this service and from Instantly's own inbox. A `human` reply is never gated. Nothing is prepared or sent on a refusal.\n\n" +
    "**Cost:** none declared. The mailbox estate is a fixed cost we absorb rather than rebill, so a reply is priced exactly like the sequence sends themselves.",
  request: {
    headers: TrackingHeadersSchema,
    body: { content: { "application/json": { schema: ReplyToLeadBodySchema } } },
  },
  responses: {
    200: {
      description: "The reply was sent into the lead's existing thread",
      content: { "application/json": { schema: ReplyToLeadResponseSchema } },
    },
    400: {
      description: "Invalid body or missing x-user-id",
      content: { "application/json": { schema: ErrorSchema } },
    },
    401: { description: "Unauthorized" },
    404: {
      description: "No campaign in this org for the given email",
      content: { "application/json": { schema: ReplyToLeadErrorSchema } },
    },
    409: {
      description:
        "Refused, nothing sent: `no_reply_to_thread`, `sending_account_unresolved`, `mailbox_credential_unavailable`, or `human_took_over` (a person already answered this thread).",
      content: { "application/json": { schema: ReplyToLeadErrorSchema } },
    },
    502: {
      description: "The transport refused the send (`reply_dispatch_failed`)",
      content: { "application/json": { schema: ReplyToLeadErrorSchema } },
    },
  },
});

export const EscalateReplyBodySchema = z
  .object({
    campaign_id: z
      .string()
      .min(1)
      .describe("Logical campaign id — the same key POST /orgs/replies takes"),
    email: z.string().email().describe("The lead who asked something we cannot answer"),
    question: z
      .string()
      .min(1)
      .describe(
        "What they asked, in their own words. Required — it is the whole content of the notification, and lead-service refuses a stop with no reason.",
      ),
  })
  .openapi("EscalateReplyBody", {
    example: {
      campaign_id: "c1a2b3c4-0000-0000-0000-000000000001",
      email: "alice@media.com",
      question: "What does it cost for 5 seats, and can you send two references?",
    },
  });

const EscalateReplyResultSchema = z
  .object({
    instantlyCampaignId: z.string(),
    leadEmail: z.string(),
    threadMessages: z
      .number()
      .int()
      .describe("How many messages of the exchange went to the agency inbox"),
    followupsStopped: z
      .boolean()
      .describe(
        "Whether the follow-up ladder was emptied. False when lead-service holds no row for this person on this campaign — a real state, not a failure; the human was still told.",
      ),
  })
  .openapi("EscalateReplyResult");

const EscalateReplyResponseSchema = z
  .object({ success: z.literal(true), escalation: EscalateReplyResultSchema })
  .openapi("EscalateReplyResponse");

const EscalateReplyErrorSchema = z
  .object({
    error: z.string(),
    code: z
      .enum(["campaign_not_found", "question_required"])
      .describe(
        "campaign_not_found: no campaign in this org for the given email. question_required: the question was empty, and an escalation with nothing to answer is not actionable.",
      ),
  })
  .openapi("EscalateReplyError");

registry.registerPath({
  method: "post",
  path: "/orgs/replies/escalate",
  summary: "Hand a thread to a human and stop the automated follow-ups",
  description:
    "The automated responder cannot answer what this prospect asked. Sends the prospect NOTHING, forwards the exchange to the agency inbox naming the unanswered question, and empties the lead's follow-up schedule so the ladder stops insisting.\n\n" +
    "**The decision is the caller's, not this service's.** Whether a question is answerable is a judgement about the draft and can only be made where the draft is made. This endpoint owns the two halves the drafter does not have: the mailbox and thread to forward, and the campaign identity that names the lead row whose schedule stops. There is no content heuristic here.\n\n" +
    "**Not permanent.** A stop is not a tombstone: if the prospect writes again they are re-enqueued and qualification re-decides. A new message is a new decision.\n\n" +
    "**Order is forward-then-stop.** A failure between the two leaves a human informed on a thread still scheduled, which is visible and undoable; the reverse would stop the ladder silently and the prospect would never hear from anyone.\n\n" +
    "**Cost:** none declared. Nothing is sent to the prospect, and the notification is transactional-email-service's own send.",
  request: {
    headers: TrackingHeadersSchema,
    body: { content: { "application/json": { schema: EscalateReplyBodySchema } } },
  },
  responses: {
    200: {
      description: "A human was told and the follow-up ladder was stopped",
      content: { "application/json": { schema: EscalateReplyResponseSchema } },
    },
    400: {
      description: "Invalid body, empty question, or missing x-user-id",
      content: { "application/json": { schema: EscalateReplyErrorSchema } },
    },
    401: { description: "Unauthorized" },
    404: {
      description: "No campaign in this org for the given email",
      content: { "application/json": { schema: EscalateReplyErrorSchema } },
    },
    500: {
      description:
        "The notification could not be sent. Nothing was stopped — an escalation that told nobody is worse than none, so it fails loud.",
      content: { "application/json": { schema: ErrorSchema } },
    },
  },
});

export const LeadConversationQuerySchema = z
  .object({
    campaign_id: z
      .string()
      .min(1)
      .describe("Logical campaign id — the same key POST /orgs/replies takes"),
    email: z.string().email().describe("The lead whose conversation to read"),
  })
  .openapi("LeadConversationQuery");

const ConversationMessageSchema = z
  .object({
    direction: z
      .enum(["inbound", "outbound"])
      .describe("inbound: the prospect wrote it. outbound: we did."),
    from: z.string(),
    to: z.string(),
    at: z.string().describe("ISO 8601 UTC. Empty only when the source carried no timestamp."),
    subject: z.string(),
    text: z
      .string()
      .describe("The message as readable TEXT — markup stripped, never HTML"),
    campaignId: z
      .string()
      .describe("The stored campaign row this message was exchanged under"),
    instantlyCampaignId: z.string().describe("That row's sequence id"),
  })
  .openapi("ConversationMessage");

const ConversationSourceSchema = z
  .enum(["mirror", "self_send", "provider"])
  .describe(
    "Where the messages were read from. mirror: our own bronze copy of the Instantly Unibox — the normal case, and the one that survives the Instantly plan being cancelled. self_send: the sequence we dispatched ourselves. provider: read live from Instantly because our mirror held nothing for a sequence that did exchange mail.",
  );

const ConversationSequenceSchema = z
  .object({
    campaignId: z.string(),
    instantlyCampaignId: z.string(),
    accountEmail: z.string().nullable(),
    transport: z.enum(["instantly", "smtp"]),
    source: ConversationSourceSchema,
    messageCount: z.number().int(),
  })
  .openapi("ConversationSequence");

const LeadConversationSchema = z
  .object({
    campaignId: z.string().describe("The campaign id asked for"),
    campaignIds: z
      .array(z.string())
      .describe(
        "Every stored campaign row of this campaign holding this lead, oldest first. campaign-service keeps an ancestor row per workflow change, so one campaign is routinely many rows and a prospect emailed over months sits in several — the exchange spans all of them.",
      ),
    instantlyCampaignId: z
      .string()
      .describe("The asked row's sequence id; each message carries the row it came from"),
    leadEmail: z.string().describe("The stored casing, which may differ from the one asked for"),
    accountEmail: z
      .string()
      .nullable()
      .describe("The asked row's mailbox; null on a row predating the account persist"),
    transport: z
      .enum(["instantly", "smtp"])
      .describe("The asked row's pipe — the caller does not need to know this to ask"),
    source: ConversationSourceSchema.describe(
      "Where the ASKED row's messages were read from; `sequences` says it per row.",
    ),
    messageCount: z.number().int(),
    messages: z
      .array(ConversationMessageSchema)
      .describe("Oldest first, across every contributing row. Empty when nothing has been exchanged."),
    sequences: z
      .array(ConversationSequenceSchema)
      .describe("What each contributing row carried, oldest first"),
  })
  .openapi("LeadConversation");

const LeadConversationResponseSchema = z
  .object({ success: z.literal(true), conversation: LeadConversationSchema })
  .openapi("LeadConversationResponse");

const LeadConversationErrorSchema = z
  .object({
    error: z.string(),
    code: z
      .enum([
        "campaign_not_found",
        "thread_unavailable",
        "campaign_identity_unavailable",
        "too_many_sequences",
      ])
      .describe(
        "campaign_not_found: this org holds no such exchange for that email — distinct from an exchange that exists and is empty, which is a 200 with messages: []. thread_unavailable: it exists but could not be read; returning it as empty would claim the prospect said nothing. campaign_identity_unavailable: campaign-service could not say which rows this campaign is made of, so only part of the exchange could be found. too_many_sequences: the lead sits in more rows of this campaign than the read will fan out to.",
      ),
  })
  .openapi("LeadConversationError");

registry.registerPath({
  method: "get",
  path: "/orgs/conversations",
  summary: "Read the messages exchanged with a lead on a campaign",
  description:
    "Return what a prospect wrote and what we sent, oldest first, for one (campaign, lead) pair — the exact identity `POST /orgs/replies` takes. Intended for a worker about to answer a reply: it reads the conversation so its answer can address what that person actually said.\n\n" +
    "**The whole campaign, not one stored row.** campaign-service mints a fresh campaign row every time the campaign's workflow changes and keeps the ancestors, so a campaign as the customer knows it is routinely dozens of rows and a prospect emailed over months sits in several. The answer spans every row of the campaign that holds this lead, merged into one ordered thread — so its earliest outbound message is the send the delivery evidence reports as first. `campaignIds` names the rows it drew from and each message carries its own; a single-row campaign answers as it always did.\n\n" +
    "**Both transports.** The consumer cannot know which pipe carried a given prospect, exactly as the reply endpoint cannot. On the Instantly transport the messages come from Instantly's Unibox; on the self-send transport they are interleaved from what we dispatched and what we read back over IMAP. One response shape either way.\n\n" +
    "**Text, not HTML.** Every `text` is markup-stripped so it drops straight into a prompt. No truncation is applied here; note that a self-send inbound body is stored as the first 4000 characters of the message at ingestion time.\n\n" +
    "**Absent is not empty.** A conversation this org has no record of is a 404 (`campaign_not_found`); an exchange that exists with nothing said is a 200 with an empty `messages`; one we hold but cannot read is a 502 (`thread_unavailable`, or `campaign_identity_unavailable` when campaign-service could not say which rows the campaign is made of). No path returns an empty list to stand in for a failure.\n\n" +
    "**Cost:** none. It sends nothing and declares nothing — a read of what already happened.",
  request: {
    headers: TrackingHeadersSchema,
    query: LeadConversationQuerySchema,
  },
  responses: {
    200: {
      description: "The conversation, oldest first (possibly empty)",
      content: { "application/json": { schema: LeadConversationResponseSchema } },
    },
    400: {
      description: "Invalid query or missing x-user-id",
      content: { "application/json": { schema: ErrorSchema } },
    },
    401: { description: "Unauthorized" },
    404: {
      description: "No campaign in this org for the given email (`campaign_not_found`)",
      content: { "application/json": { schema: LeadConversationErrorSchema } },
    },
    502: {
      description: "The sequence exists but its thread could not be read (`thread_unavailable`)",
      content: { "application/json": { schema: LeadConversationErrorSchema } },
    },
  },
});

export const EngagedLeadsQuerySchema = z
  .object({
    brand_id: z
      .string()
      .uuid()
      .optional()
      .describe("Only leads whose sequence carries this brand"),
    campaign_id: z
      .string()
      .min(1)
      .optional()
      .describe("Only leads on this logical campaign"),
    since: z
      .string()
      .datetime()
      .optional()
      .describe("Only leads whose engagement STARTED at or after this instant"),
    limit: z.coerce
      .number()
      .int()
      .positive()
      .optional()
      .describe(
        "Cap the number of rows. Omitted, every engaged lead is returned — the population is a few hundred per org at the largest, so a silent default would hide the tail.",
      ),
  })
  .openapi("EngagedLeadsQuery");

const EngagedLeadSchema = z
  .object({
    campaignId: z
      .string()
      .nullable()
      .describe("The caller's own campaign id. Null on a platform send."),
    instantlyCampaignId: z
      .string()
      .describe("This service's per-lead sequence id — present even when campaignId is null"),
    leadEmail: z.string(),
    brandIds: z.array(z.string()),
    engagedAt: z
      .string()
      .describe(
        "When this lead FIRST showed interest — the earlier of their first reply and their first click",
      ),
    replied: z.boolean(),
    clicked: z.boolean(),
    firstRepliedAt: z.string().nullable(),
    firstClickedAt: z.string().nullable(),
    replyClassification: z
      .string()
      .nullable()
      .describe("positive | negative | neutral. Null when no reply is qualified."),
    replyKind: z
      .string()
      .nullable()
      .describe("The finer reading of the same statement. Null when none is on record."),
    disqualified: z
      .boolean()
      .describe(
        "True only for a kind that is permanent about the PERSON (wrong person, changed job) — never for a 'not right now', which stays recyclable",
      ),
  })
  .openapi("EngagedLead");

export const EngagedLeadsResponseSchema = z
  .object({
    success: z.literal(true),
    count: z.number().int(),
    leads: z.array(EngagedLeadSchema).describe("Most recently engaged first"),
  })
  .openapi("EngagedLeadsResponse");

registry.registerPath({
  method: "get",
  path: "/orgs/engaged-leads",
  summary: "The org's leads that have shown interest",
  description:
    "List the leads worth opening a conversation on: everyone who replied without asking to stop, plus everyone who clicked a link we sent. Each row carries the exact identity `GET /orgs/conversations` takes, so a dashboard can list the panel and then read each thread.\n\n" +
    "**Why this exists.** `GET /orgs/conversations` answers about ONE (campaign, lead) pair the caller must already know. A consumer holding a lead has no way to discover which of an org's tens of thousands of contacted leads ever engaged, nor which campaign id to ask about. This is the discovery half.\n\n" +
    "**The gate.** `(replied AND NOT unsubscribed) OR clicked`. Any human reply counts whatever its sentiment — a negative reply is still a conversation worth reading, and a 'not right now' is recyclable pipeline. An unsubscribe request is the one answer that is explicitly a request to stop, so it is excluded. Autoresponders never appear: `auto_reply_received` and `lead_out_of_office` are distinct event types that never set `replied`.\n\n" +
    "**⚠️ `clicked` is a click on a link WE sent**, on either transport — it is the only visit signal this service has. An anonymous website visit that never went through one of our links is invisible here.\n\n" +
    "**No `engagementKind` scalar, deliberately.** A lead who clicked on Monday and replied on Friday has one engagement start and two signals; one enum would force a label that disagrees with its own timestamp. `engagedAt` is when interest started, the two booleans say which signals exist, and the two timestamps say when each happened.\n\n" +
    "**Read from gold**, the same projection `POST /orgs/status` answers from — so `replyKind` and `disqualified` cannot disagree between the two surfaces.\n\n" +
    "**Cost:** none. It sends nothing and declares nothing.",
  request: {
    headers: TrackingHeadersSchema,
    query: EngagedLeadsQuerySchema,
  },
  responses: {
    200: {
      description: "The engaged leads, most recently engaged first (possibly empty)",
      content: { "application/json": { schema: EngagedLeadsResponseSchema } },
    },
    400: {
      description: "Invalid query",
      content: { "application/json": { schema: ErrorSchema } },
    },
    401: { description: "Unauthorized" },
    500: {
      description: "The engaged leads could not be read",
      content: { "application/json": { schema: ErrorSchema } },
    },
  },
});

registry.registerPath({
  method: "post",
  path: "/orgs/manual-qualifications",
  summary: "Set a manual reply qualification for a (campaign, lead) pair",
  description:
    "Record a human-set reply classification for a lead in a campaign. Used when Instantly's automatic webhook reply classification fails to detect a reply (e.g. the reply was sent to a non-leurre account that Instantly does not monitor).\n\n" +
    "**Bronze:** an `instantly_manual_qualifications_raw` row is appended for audit (append-only).\n\n" +
    "**Silver / Gold:** a corresponding row is inserted into `instantly_events` with `source='manual'`, so analytics counters (RepliesDetail) include the manual qualification alongside webhook events. `instantly_campaigns.reply_classification` is updated to the derived positive/negative/neutral value and `reply_classification_source` is set to `manual` so subsequent webhook events do not overwrite the human choice.\n\n" +
    "**Idempotence:** if the STANDING statement for (org, campaign, lead) already has `status`, the call is a no-op — no new bronze row, no side effects. The response includes `idempotent: true` and the existing row. A withdrawn statement does not stand, so re-stating the same kind after a withdrawal records it again.",
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
  method: "post",
  path: "/orgs/manual-qualifications/withdrawals",
  summary: "Withdraw the standing manual reply qualification for a (campaign, lead) pair",
  description:
    "Take back a human statement about a reply — for a person who picked the wrong kind by mistake. After the withdrawal the lead reads as it did before anybody said anything: no standing human statement, and the AUTOMATIC classification takes over again.\n\n" +
    "**A correction, not an erasure.** Nothing is deleted and no \"none\" value is added to the reply-kind vocabulary. The statement row stays byte-identical in `instantly_manual_qualifications_raw` and a row is APPENDED to `instantly_manual_qualification_withdrawals` recording that it no longer stands, by whom and when. `GET /orgs/manual-qualifications` returns both, and a withdrawn one carries a non-null `withdrawnAt`.\n\n" +
    "**Silver / Gold:** the statement's silver mirror event is marked withdrawn (the row is kept — it is the audit of what was asserted) so the current-sentiment projection stops counting it; `instantly_campaigns.reply_classification` is recomputed from whatever automatic classification is left (NULL when there is none) and `reply_classification_source` returns to `auto`, which releases the pin so a subsequent webhook event is free to classify the reply as it normally would.\n\n" +
    "**Scope:** this withdraws the reply KIND and its pin. It does not retract the separate fact that a reply arrived, nor undo the sequence stop, hold cancellation and Instantly pause a sequence-stopping statement already caused — those are irreversible actions already taken. The resulting state (a reply on record with no kind attached) is exactly the state of an auto-detected reply nobody has qualified.\n\n" +
    "**Idempotence:** withdrawing when nothing is standing writes nothing and returns 404 with `code: \"no_standing_qualification\"` — including a second withdrawal of the same statement.",
  request: {
    headers: TrackingHeadersSchema,
    body: {
      content: { "application/json": { schema: ManualQualificationWithdrawBodySchema } },
    },
  },
  responses: {
    200: {
      description: "The standing statement was withdrawn",
      content: { "application/json": { schema: ManualQualificationWithdrawResponseSchema } },
    },
    400: {
      description: "Invalid body or missing identity header",
      content: { "application/json": { schema: ErrorSchema } },
    },
    401: { description: "Unauthorized" },
    404: {
      description:
        "Nothing to withdraw. `code: \"no_standing_qualification\"` — nobody has stated a reply kind for this pair, or the statement was already withdrawn. `code: \"campaign_not_found\"` — no campaign in this org for the given email.",
      content: { "application/json": { schema: ManualQualificationWithdrawErrorSchema } },
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

// ─── Recorded opt-outs (a person asked a human to stop) ─────────────────────

/**
 * How the person told us. Closed on purpose: an opt-out with no channel is an
 * assertion nobody can audit later, which is the one thing a consent record must
 * never be.
 */
const OPT_OUT_CHANNEL_VALUES = [
  "sms",
  "phone_call",
  "email_reply",
  "forwarded_thread",
  "in_person",
  "web_form",
  "other",
] as const;

export const OptOutChannelSchema = z
  .enum(OPT_OUT_CHANNEL_VALUES)
  .describe(
    "The channel the person used to ask us to stop. Required — this is what makes the record auditable as a consent record rather than a flag.",
  );

export const LeadOptOutCreateBodySchema = z
  .object({
    email: z.string().email().describe("The person who asked us to stop"),
    channel: OptOutChannelSchema,
    notes: z.string().max(2000).optional().describe("Optional free-text human note for audit"),
  })
  .openapi("LeadOptOutCreateBody", {
    example: {
      email: "alice@media.com",
      channel: "sms",
      notes: "Texted my mobile asking not to be contacted again",
    },
  });

export const LeadOptOutWithdrawBodySchema = z
  .object({
    email: z.string().email().describe("The person whose opt-out is being taken back"),
    notes: z.string().max(2000).optional().describe("Optional free-text human note for audit"),
  })
  .openapi("LeadOptOutWithdrawBody", {
    example: { email: "alice@media.com", notes: "Recorded on the wrong lead" },
  });

export const LeadOptOutListQuerySchema = z.object({
  email: z.string().email().optional().describe("Filter by lead email"),
  standing_only: z
    .coerce.boolean()
    .optional()
    .describe("Return only records that still STAND (default false — withdrawn records are part of the audit)"),
  limit: z
    .coerce.number()
    .int()
    .min(1)
    .optional()
    .describe(
      "Max rows to return. OMIT IT TO GET EVERY MATCHING ROW — there is no default and no ceiling. " +
        "This list is read by a consent GATE, and a truncated page is indistinguishable from a " +
        "complete shorter one, so a caller that cannot tell them apart can only refuse to serve.",
    ),
});

const LeadOptOutRowSchema = z.object({
  id: z.string(),
  orgId: z.string(),
  email: z.string(),
  channel: OptOutChannelSchema,
  statedBy: z.string().describe("The staff member who recorded it (x-user-id)"),
  notes: z.string().nullable(),
  statedAt: z.string().describe("ISO 8601 timestamp"),
  withdrawnAt: z
    .string()
    .nullable()
    .describe(
      "ISO 8601 timestamp of the withdrawal, or null while the record still STANDS. Non-null means it was taken back: it is kept for audit and must never be rendered as a current opt-out.",
    ),
  withdrawnBy: z.string().nullable(),
});

const LeadOptOutCreateResponseSchema = z
  .object({
    idempotent: z
      .boolean()
      .describe("True if a standing opt-out already existed — no new record, no repeated side effects"),
    campaignsAffected: z.number().int().describe("Campaigns of this org holding that address"),
    campaignsStopped: z
      .number()
      .int()
      .describe("How many of them could be stopped at the SENDER. Below campaignsAffected means a pause failed and is logged — the local stop and the opt-out record still hold."),
    optOut: LeadOptOutRowSchema,
  })
  .openapi("LeadOptOutCreateResponse");

const LeadOptOutWithdrawResponseSchema = z
  .object({
    campaignsAffected: z.number().int(),
    optOut: LeadOptOutRowSchema.describe("The record that was withdrawn, now carrying withdrawnAt / withdrawnBy."),
  })
  .openapi("LeadOptOutWithdrawResponse");

const LeadOptOutWithdrawErrorSchema = z
  .object({
    error: z.string(),
    code: z
      .enum(["no_standing_optout"])
      .describe("Nothing currently stands for this lead — nothing was ever recorded, or it is already withdrawn."),
  })
  .openapi("LeadOptOutWithdrawError");

const LeadOptOutListResponseSchema = z
  .object({ optOuts: z.array(LeadOptOutRowSchema) })
  .openapi("LeadOptOutListResponse");

registry.registerPath({
  method: "post",
  path: "/orgs/opt-outs",
  summary: "Record that a person asked a human to stop contacting them",
  description:
    "A prospect rarely clicks the unsubscribe link: they send an SMS, they call, they reply to a thread somebody forwarded them, they say it in person. This records that statement — never infers it — and then honours it.\n\n" +
    "**It stops the sending.** For every campaign this org holds for that address, a real `lead_unsubscribed` silver event is promoted through the SAME path a clicked unsubscribe uses (the sequence stops, the remaining provisioned holds are cancelled) AND the campaign is paused at the sender. The pause is the half the click path gets for free from Instantly and this path cannot: Instantly never saw the SMS.\n\n" +
    "**It surfaces where a clicked unsubscribe surfaces.** `POST /orgs/status` reports `unsubscribed: true` for that person, scoped and global, with no second field for a consumer to learn.\n\n" +
    "**Scope is the PERSON, not a campaign.** Honouring \"stop contacting me\" in one campaign while another keeps sending is the outcome the law cares about.\n\n" +
    "**Bronze:** an `instantly_lead_optouts_raw` row is appended (append-only) carrying who stated it, when, and through which channel — it is a consent record.\n\n" +
    "**Idempotence:** a standing record for the same person returns `idempotent: true` with no second row and no repeated side effects. The record is written even when the org holds no campaign for the address; the response then reports zero campaigns.",
  request: {
    headers: TrackingHeadersSchema,
    body: { content: { "application/json": { schema: LeadOptOutCreateBodySchema } } },
  },
  responses: {
    200: {
      description: "Opt-out recorded (or idempotent no-op)",
      content: { "application/json": { schema: LeadOptOutCreateResponseSchema } },
    },
    400: {
      description: "Invalid body or missing identity header",
      content: { "application/json": { schema: ErrorSchema } },
    },
    401: { description: "Unauthorized" },
  },
});

registry.registerPath({
  method: "post",
  path: "/orgs/opt-outs/withdrawals",
  summary: "Withdraw the standing recorded opt-out for a person",
  description:
    "Take a recorded opt-out back — recorded on the wrong lead, or a prospect who came back and asked to hear from us again. After it, `POST /orgs/status` stops reporting that person as unsubscribed.\n\n" +
    "**A correction, not an erasure.** The record stays byte-identical in `instantly_lead_optouts_raw` and a row is APPENDED to `instantly_lead_optout_withdrawals`. `GET /orgs/opt-outs` returns both; a withdrawn one carries a non-null `withdrawnAt`.\n\n" +
    "**Silver / Gold:** only the `lead_unsubscribed` events THIS record promoted are marked withdrawn (the rows are kept — silver is the audit of what was asserted), and the gold status row is refreshed. A `lead_unsubscribed` the prospect produced by clicking the link is never touched: nobody withdrew that.\n\n" +
    "**Scope:** this releases the OPT-OUT. It does not resume the sequences it stopped — the holds were cancelled and the campaigns paused, and silently restarting outreach at somebody who asked us to stop is the one mistake worth being unable to make by accident.\n\n" +
    "**Idempotence:** withdrawing when nothing stands writes nothing and returns 404 `no_standing_optout`, including a second withdrawal of the same record.",
  request: {
    headers: TrackingHeadersSchema,
    body: { content: { "application/json": { schema: LeadOptOutWithdrawBodySchema } } },
  },
  responses: {
    200: {
      description: "The standing opt-out was withdrawn",
      content: { "application/json": { schema: LeadOptOutWithdrawResponseSchema } },
    },
    400: {
      description: "Invalid body or missing identity header",
      content: { "application/json": { schema: ErrorSchema } },
    },
    401: { description: "Unauthorized" },
    404: {
      description: "Nothing stands for this lead",
      content: { "application/json": { schema: LeadOptOutWithdrawErrorSchema } },
    },
  },
});

registry.registerPath({
  method: "get",
  path: "/orgs/opt-outs",
  summary: "The org's recorded opt-out log",
  description:
    "Every opt-out recorded by a human for this org, newest first. Withdrawn records are returned too and carry `withdrawnAt` / `withdrawnBy` — hiding them would destroy the audit. Pass `standing_only=true` for only the records that still stand.",
  request: { query: LeadOptOutListQuerySchema, headers: TrackingHeadersSchema },
  responses: {
    200: {
      description: "Recorded opt-outs",
      content: { "application/json": { schema: LeadOptOutListResponseSchema } },
    },
    400: { description: "Invalid query", content: { "application/json": { schema: ErrorSchema } } },
    401: { description: "Unauthorized" },
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
        "Emails/day the fleet can send — Σ daily send limit over accounts whose lifecycle_status == 'in_production' (the live send gate)",
      ),
    healthyAccountCount: z
      .number()
      .int()
      .describe("Accounts currently in_production (send-eligible)"),
    totalAccountCount: z
      .number()
      .int()
      .describe("All accounts in the shared workspace before filtering"),
    blockedDomainCount: z
      .number()
      .int()
      .describe("Accounts blocked by domain policy (lifecycle deactivated_by_user)"),
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
    "Platform-scoped (no org). Returns the cold-email fleet's available daily sending CAPACITY (sum of the daily send limit over only healthy accounts) alongside a TRUE per-day projection of upcoming scheduled send VOLUME (active campaigns' remaining un-sent sequence steps bucketed on their raw nominal UTC send day — identical bucketing to the per-account queue breakdown, so the two ops surfaces agree for the same pending steps). The volume projection is capacity-INDEPENDENT and bounded by the sequence structure — not a backlog÷capacity approximation. Fails loud (500) on any missing source; no silent zero fallbacks.",
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
    inboxPct: z
      .number()
      .describe(
        "Percentage of test emails landing in inbox, pooled across every ESP of the latest test (sum inbox / sum seeds) — the same number the lifecycle delivery gate reads",
      ),
    spamPct: z.number().describe("Percentage landing in spam, pooled across every ESP"),
    missingPct: z.number().describe("Percentage not delivered / missing, pooled across every ESP"),
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
      .describe("Per-account daily MAX-SEND limit (cold-send cap); null if unknown"),
    effectiveDailyCap: z
      .number()
      .int()
      .nullable()
      .describe(
        "The daily cap send SELECTION actually compares this account's load against: min(dailyLimit, age ramp) at asOf. Equal to dailyLimit for a mature mailbox, strictly lower for one under 4 weeks old (whose real per-user quota is far below the fleet cap). Rendering dailyLimit alone reads 50 for an account the selector holds at 23. Null when the account carries no daily limit at all.",
      ),
    fillRank: z
      .number()
      .int()
      .nullable()
      .describe(
        "1-based position of this account in the send-selection fill order over the in-production pool — rank 1 is the mailbox a NEW sequence is offered first, and the fleet saturates it before touching rank 2. Null when the account is not in that pool (blocked / not in_production, or reserved to a feature slug). Never fabricated: an account the selector would not consider has no position in its order. Followups stay pinned to their originating account, so a high rank still DISPATCHES for a full sequence length after it stops being assigned.",
      ),
    warmupLimit: z
      .number()
      .int()
      .nullable()
      .describe(
        "Per-account daily WARMUP send volume — Instantly warmup.limit, the warm-up emails/day target. DISTINCT from dailyLimit (the max-send cap): a live account often runs warmup 10/day while its send cap is 50/day. Null when Instantly reports no warmup config.",
      ),
    blocked: z
      .boolean()
      .describe(
        "True when the account is NOT send-eligible (lifecycle_status != 'in_production')",
      ),
    blockReason: z
      .string()
      .nullable()
      .describe(
        "Short reason when blocked — the account's lifecycle_status (in_recovery / deactivated_by_instantly / deactivated_by_user), or 'unclassified' when the lifecycle has not yet run; null when in_production.",
      ),
    lifecycleStatus: z
      .enum([
        "in_production",
        "in_recovery",
        "deactivated_by_instantly",
        "deactivated_by_user",
      ])
      .nullable()
      .describe("Auto-derived lifecycle state; null until reconcileLifecycle first runs"),
    lifecycleReason: z
      .string()
      .nullable()
      .describe("Snapshot reason on the latest lifecycle transition; null until classified"),
    lifecycleUpdatedAt: z
      .string()
      .nullable()
      .describe("ISO8601 timestamp of the latest lifecycle transition; null until classified"),
    inboxPlacement: InboxPlacementSchema.nullable().describe(
      "Inbox-placement breakdown from the account's latest placement test, promoted through our own Bronze/Silver/Gold pipeline. Headline percentages are the WORST gated ESP leg (the leg the lifecycle delivery gate reads), with the full per-ESP breakdown in `perEsp`. Null when the account has never been in a test — the Instantly V2 API exposes no standing per-account placement property, so this is derived from real test results and never fabricated.",
    ),
    sentToday: z
      .number()
      .int()
      .describe(
        "Real (non-inferred) email_sent events observed today (UTC) from this account, from our silver log — the N in an N/dailyLimit read. 0 when none today, never fabricated.",
      ),
    sentYesterday: z
      .number()
      .int()
      .describe(
        "Real (non-inferred) email_sent events observed YESTERDAY — the full previous UTC calendar day — from this account, from our silver log. Same provenance as sentToday. 0 when none, never fabricated.",
      ),
    queueSize: z
      .number()
      .int()
      .describe(
        "Total queued STEPS for this account — every remaining un-sent email across all its queued sequences (still-provisioned sequence-cost holds on active campaigns attributed to this account, 1 campaign = 1 account). PARTITIONED by the four queued* date buckets: queueSize === queuedFirstUnsent + queuedNextToday + queuedNextTomorrow + queuedNextLater. 0 when nothing queued; unattributable steps excluded, never fabricated.",
      ),
    queuedSequences: z
      .number()
      .int()
      .describe(
        "Distinct queued SEQUENCES (1 Instantly campaign = 1 lead = 1 sequence) attributed to this account — a DIFFERENT granularity from queueSize (which counts pending STEPS) and from the four date buckets (which partition STEPS). Both kept, both intentional. 0 when nothing queued; unattributable sequences excluded, never fabricated.",
      ),
    queuedFirstUnsent: z
      .number()
      .int()
      .describe(
        "Q0-first — queued STEPS belonging to sequences whose first email has not been sent yet (no lastSentAt anchor → not date-projected, counted as 'not started').",
      ),
    queuedFirstUnsentSequences: z
      .number()
      .int()
      .describe(
        "Q0-first as SEQUENCES — never-contacted leads on this account, i.e. how many FIRST emails are actually due. This is the quantity SEND SELECTION counts toward today's load; queuedFirstUnsent counts every remaining step of those same sequences and therefore over-states 'today' by the whole future sequence. An ops 'queued today' figure should be queuedFirstUnsentSequences + queuedNextToday, compared against dailyLimit — that is the number the selector decides on.",
      ),
    queuedNextToday: z
      .number()
      .int()
      .describe(
        "Q0-next — queued STEPS projected today (UTC) or overdue. Each step's date = lastSentAt + the CHAINED configured delays across every hop up to it (nominal-cadence LOWER BOUND that COMPOUNDS across steps; real Instantly dispatch slips later under throttling), so this reads as 'step DUE today-or-overdue', not a guaranteed send today.",
      ),
    queuedOverdue: z
      .number()
      .int()
      .describe(
        "BACKLOG subset of queuedNextToday — queued STEPS whose nominal due date is STRICTLY BEFORE today (UTC), i.e. owed on an earlier day and never dispatched. Always <= queuedNextToday, and NOT part of the four-bucket partition of queueSize (it re-counts steps already in queuedNextToday — do not add it to the sum). Separates 'due today' from 'behind'; a rising value means Instantly is dispatching slower than we assign.",
      ),
    queuedNextTomorrow: z
      .number()
      .int()
      .describe("Q1-next — queued STEPS projected tomorrow (UTC) via the chained-delay projection."),
    queuedNextLater: z
      .number()
      .int()
      .describe("Q-next — queued STEPS projected after tomorrow (UTC) via the chained-delay projection."),
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
    "Platform-scoped (no org). Returns every sending account with its identity (email/domain), sending config (status, warmup Health Score, daily send limit), and lifecycle state (lifecycleStatus/lifecycleReason/lifecycleUpdatedAt + blocked + blockReason, from the SAME auto-derived lifecycle the live send path reads: send-eligible ⇔ lifecycle_status == 'in_production'). `inboxPlacement` is the latest inbox/spam/missing breakdown from our own Bronze/Silver/Gold placement history (recurring inbox-placement tests promoted to silver, latest test per account blended across ESP); null when the account has never been in a test. The Instantly V2 API exposes no standing per-account placement property — this figure is derived from real test results, never fabricated. Fails loud (500) on any missing REQUIRED source (account list); no silent fallbacks.",
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

const EmailsBackfillRequestSchema = z
  .object({
    maxPages: z
      .number()
      .int()
      .positive()
      .optional()
      .describe("Bound the walk to N pages (100 emails each). Omit to sweep the whole Unibox."),
    startingAfter: z
      .string()
      .optional()
      .describe(
        "Resume from the cursor a previous run reported (its summary log line, or a progress line). Omit to start from the newest email.",
      ),
  })
  .openapi("EmailsBackfillRequest");

const EmailsBackfillAcceptedSchema = z
  .object({
    accepted: z.boolean(),
    runId: z.string().describe("Background run identifier (watch logs)"),
  })
  .openapi("EmailsBackfillAccepted");

registry.registerPath({
  method: "post",
  path: "/internal/audit/emails-backfill",
  summary: "Mirror the whole Instantly Unibox into bronze",
  description:
    "Platform-scoped (no org). Walks `GET /emails` with no campaign filter and mirrors every email — sent and received — into `instantly_emails_raw`. Cancelling an Instantly plan or a single inbox permanently deletes those conversations, replies included, and silver records only THAT a lead replied, never what they wrote. Read-only against Instantly (spends no quota, declares no cost). Idempotent: a re-run re-reads pages but writes only what is new. A deploy recreates the container and kills the sweep, so the summary and each progress line report the cursor to resume from — pass it back as `startingAfter`. 202 + background; watch logs for `emails-backfill: done`.",
  request: {
    body: {
      required: false,
      content: { "application/json": { schema: EmailsBackfillRequestSchema } },
    },
  },
  responses: {
    202: {
      description: "Accepted — backfill runs in the background",
      content: { "application/json": { schema: EmailsBackfillAcceptedSchema } },
    },
    401: { description: "Unauthorized" },
  },
});

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
    created: z.number().int().describe("One-time placement tests created this call (1, or 0 when no healthy senders)"),
    testCode: z
      .string()
      .nullable()
      .describe("test_code of the created one-time test; null when nothing was created"),
    recipientEsps: z
      .array(z.string())
      .describe("Recipient ESPs the test seeds (Google/Outlook)"),
    senderCount: z
      .number()
      .int()
      .describe("Number of live-send-eligible accounts the test seeds from"),
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

// ─── In-house seed placement (replaces the paid Instantly placement test) ────

const SeedPlacementRunRequestSchema = z
  .object({
    limit: z
      .number()
      .int()
      .positive()
      .optional()
      .describe("Cap the number of seed emails dispatched this run (batching / smoke test)"),
    force: z
      .boolean()
      .optional()
      .describe("Bypass the due check and run now. The cron never sets this."),
  })
  .openapi("SeedPlacementRunRequest");

registry.registerPath({
  method: "post",
  path: "/internal/audit/seed-placement/run",
  summary: "Dispatch one in-house seed inbox-placement test",
  description:
    "Platform-scoped (no org). Sends one seed email from every testable mailbox to every receiver mailbox we own and can read over IMAP — the self-hosted replacement for Instantly's paid Growth Inbox Placement subscription, which it spends nothing to do. SENDS REAL MAIL from the fleet, so it is gated behind SEED_PLACEMENT_ENABLED=true (409 when disabled). Results are read by the SEPARATE /seed-placement/sync run on its own schedule — never chained here, or the read would run before the seeds land. 202 + background; watch logs for `seed-placement-run: done`.",
  request: {
    body: {
      required: false,
      content: { "application/json": { schema: SeedPlacementRunRequestSchema } },
    },
  },
  responses: {
    202: {
      description: "Accepted — the seed dispatch runs in the background",
      content: { "application/json": { schema: PlacementSyncAcceptedSchema } },
    },
    401: { description: "Unauthorized" },
    409: {
      description: "Seed placement disabled (SEED_PLACEMENT_ENABLED != true)",
      content: { "application/json": { schema: ErrorSchema } },
    },
  },
});

registry.registerPath({
  method: "post",
  path: "/internal/audit/seed-placement/sync",
  summary: "Read the seeds back and promote them to placement silver",
  description:
    "Platform-scoped (no org). Opens every receiver mailbox that still owes an observation, records which folder each seed landed in (inbox vs spam), and promotes the affected tests into the SAME `instantly_placement_results` silver the Instantly path writes — so the lifecycle delivery gate and account-health read them with no code change. Read-only against the mailboxes and idempotent, so it is NOT behind the kill-switch. 202 + background; watch logs for `seed-placement-sync: done`.",
  responses: {
    202: {
      description: "Accepted — the read + promotion runs in the background",
      content: { "application/json": { schema: PlacementSyncAcceptedSchema } },
    },
    401: { description: "Unauthorized" },
  },
});

// ─── Provider infrastructure inventory (issue #555) ──────────────────────────

const InfraSyncAcceptedSchema = z
  .object({
    accepted: z.literal(true),
    runId: z.string().describe("Background run identifier (watch logs)"),
  })
  .openapi("InfraSyncAccepted");

registry.registerPath({
  method: "post",
  path: "/internal/infra/fx-sync",
  summary: "Fetch and record today's ECB EUR → USD reference rate",
  description:
    "Platform-scoped. One request to the European Central Bank's daily reference rates; the rate is stored per reference day (append-only, idempotent — a second run the same day inserts nothing). Also runs at the end of every `/internal/infra/sync`. Synchronous, so a hand-run right after a deploy answers with the rate stored. 502 when the ECB cannot be read — never a fallback rate.",
  responses: {
    200: {
      description: "Rate recorded (or already on record)",
      content: {
        "application/json": {
          schema: z
            .object({ asOf: z.string(), eurUsd: z.number(), inserted: z.number().int() })
            .openapi("FxSyncResponse"),
        },
      },
    },
    401: { description: "Unauthorized" },
    502: { description: "ECB unreadable", content: { "application/json": { schema: ErrorSchema } } },
  },
});

registry.registerPath({
  method: "post",
  path: "/internal/infra/sync",
  summary: "Poll every infrastructure provider → bronze + silver inventory",
  description:
    "Platform-scoped (no org). Polls Gandi (three organisations), Mailforge, Primeforge and Instantly DFY for the domains and mailboxes we own, mirrors each vendor payload to bronze, upserts the canonical silver inventory, and flags rows a vendor stopped reporting (never deletes them). Read-only against every vendor and free of metered spend — these are flat-subscription inventory reads, so no run or cost is declared. A single vendor failing is counted and logged without stopping the others; the run throws only when every vendor failed. 202 + background; watch logs for `infra-sync: done`.",
  responses: {
    202: {
      description: "Accepted — sync runs in the background",
      content: { "application/json": { schema: InfraSyncAcceptedSchema } },
    },
    401: { description: "Unauthorized" },
  },
});

const MailboxSyncSummarySchema = z
  .object({
    mailboxes: z.number().int().describe("Real mailboxes (SASL logins) upserted"),
    addresses: z.number().int().describe("Instantly addresses linked to a mailbox"),
    addressesWithoutCredential: z
      .number()
      .int()
      .describe("Addresses the credential map does not know — each is its own mailbox"),
    vendorOnlyMailboxes: z
      .number()
      .int()
      .describe("Vendor mailboxes carrying no Instantly address (paid, unused)"),
  })
  .openapi("MailboxSyncSummary");

registry.registerPath({
  method: "post",
  path: "/internal/ops/mailboxes-sync",
  summary: "Re-derive and persist the address → real-mailbox grouping",
  description:
    "Platform-scoped (no org). A MAILBOX is the login a provider enforces quota, reputation and credential at; a sending ADDRESS is what a prospect sees (on Gandi one mailbox carries several aliases). Reads the credential map (manual key + Primeforge), `infra_mailboxes`, `infra_domains` and `instantly_accounts`, derives one row per real mailbox (provider, pool type, subscription, credential source, vendor/import/prewarm dates, absence) and upserts `mailboxes` + `instantly_accounts.mailbox_login`. A PROJECTION for reads — the transport decision and the dispatch grain still come from the live credential map. Also runs at the end of every `POST /internal/infra/sync`. Synchronous; spends nothing metered.",
  responses: {
    200: {
      description: "Sync summary",
      content: { "application/json": { schema: MailboxSyncSummarySchema } },
    },
    401: { description: "Unauthorized" },
  },
});

const MessagesSyncRequestSchema = z
  .object({
    sinceDays: z
      .number()
      .int()
      .min(1)
      .optional()
      .describe("Re-read window in days on each source's own write timestamp (default 3). Large = backfill."),
  })
  .openapi("MessagesSyncRequest");

const MessagesSyncSummarySchema = z
  .object({
    windowDays: z.number().int(),
    read: z.record(z.string(), z.number().int()).describe("Rows read per bronze source"),
    upserted: z.number().int(),
    skipped: z.number().int().describe("Source rows with no usable account or timestamp"),
  })
  .openapi("MessagesSyncSummary");

registry.registerPath({
  method: "post",
  path: "/internal/ops/messages-sync",
  summary: "Project every email (outreach, reply, warmup, seed) into one `messages` row each",
  description:
    "Platform-scoped (no org). Re-reads a window of the seven bronze message sources — Instantly's Unibox mirror, our own SMTP dispatches, IMAP inbound, warmup dispatches (+ receipts), seed placement dispatches (+ observations) — maps each row to one `messages` row (direction, kind, transport, account, real mailbox, counterparty, subject, sequence + step, thread, outcome, placement, auth verdict, timestamp) and upserts keyed on the bronze row. Bronze keeps the body; state facts stay in `instantly_events`. Also runs on a 10-minute in-process interval. Idempotent: the unique (source_table, source_row_id) index makes any overlap a no-op, so there is no cursor to drift. Synchronous; spends nothing metered.",
  request: { body: { content: { "application/json": { schema: MessagesSyncRequestSchema } } } },
  responses: {
    200: {
      description: "Sync summary",
      content: { "application/json": { schema: MessagesSyncSummarySchema } },
    },
    400: { description: "sinceDays invalid" },
    401: { description: "Unauthorized" },
  },
});

const DnsSyncSummarySchema = z
  .object({
    domains: z.number().int(),
    records: z.number().int(),
    domainsWithErrors: z.number().int().describe("Domains where a lookup errored — recorded with the error, never skipped"),
  })
  .openapi("DnsSyncSummary");

registry.registerPath({
  method: "post",
  path: "/internal/ops/dns-sync",
  summary: "Photograph SPF / DMARC / DKIM / MX for every domain we own or send from",
  description:
    "Platform-scoped (no org). Resolves the SPF TXT, the `_dmarc` TXT, a probed list of DKIM selectors (Google Workspace, Gandi, common defaults — a selector is a name only the signer knows, so an empty DKIM result means none found among the probed selectors, never no DKIM) and MX for every live `infra_domains` domain plus every domain an Instantly account sends from, and appends the answers to `domain_dns_raw`. A lookup that errors is recorded with its code, not skipped. Also runs at the end of every `POST /internal/infra/sync`. Synchronous; spends nothing metered.",
  responses: {
    200: { description: "Sync summary", content: { "application/json": { schema: DnsSyncSummarySchema } } },
    401: { description: "Unauthorized" },
  },
});

// ─── Unified ops model — gold reads (/internal/ops/*) ────────────────────────
//
// These are staff reads over the unified model (mailboxes / messages / DNS +
// the existing silver). Deep shapes are described at the top level and left
// open below it: the rows are assembled from silver and pure projections, and
// a closed schema here would be a second declaration of what those already say.

const OpsObject = z.object({}).passthrough();

registry.registerPath({
  method: "get",
  path: "/internal/ops/lifecycle-rules",
  summary: "The lifecycle rules as data — bars, limits per state, ramp, placement cadence, warmup",
  description:
    "Platform-scoped. Serves the constants `deriveLifecycle` and its siblings decide on (health entry bar, delivery bar, evidence max age, per-state campaign/warmup limits, ramp floor/growth/window, placement cadence, warmup budget) plus the rule order, so a dashboard renders what in_production / in_recovery MEAN from the decision's own inputs and cannot drift from them.",
  responses: { 200: { description: "Rules", content: { "application/json": { schema: OpsObject.openapi("LifecycleRules") } } }, 401: { description: "Unauthorized" } },
});

registry.registerPath({
  method: "get",
  path: "/internal/ops/domains",
  summary: "One row per (provider, domain): purchase/renewal, DNS, delivery, addresses, mailboxes, volume, cost",
  description:
    "Platform-scoped, cached 60s. The `/internal/infra/domains` row plus: `dns` (latest SPF / DMARC / DKIM selectors found / MX photograph, with per-record errors), `delivery` (Σinbox/Σseeds pooled across the domain's addresses' latest placement tests), `addresses` (total + by lifecycle), `mailboxes` (real logins), `volume30d` (outreach / warmup / seed / replies / bounces + bounce rate per mille from `messages`), and `cost.paidToDate` — an ESTIMATE (`source: 'estimate'`, rate × months since the registration started) because no vendor exposes an invoice API Plus `fx` (the ECB EUR → USD rate on record, null when none) and `cost.usd` (monthly / per-email / recurring / renewal / paid-to-date in USD at that rate, null wherever it cannot be stated), and `vendorReportsMailboxes` (false for a vendor whose inventory never reports mailboxes, so its `vendorMailboxes: 0` means 'not reported').",
  responses: { 200: { description: "Domains", content: { "application/json": { schema: z.object({ asOf: z.string(), domains: z.array(OpsObject) }).openapi("OpsDomainsResponse") } } }, 401: { description: "Unauthorized" } },
});

registry.registerPath({
  method: "get",
  path: "/internal/ops/mailboxes",
  summary: "One row per REAL mailbox: vendor, pool, subscription, dates, aliases, cap + ramp projection, volume, delivery, cost",
  description:
    "Platform-scoped, cached 60s. From `mailboxes` (migration 0053): provider, pool type (google-workspace / gandi-relay / mailforge-relay / dfy-google), subscription (standard / prewarmed / dfy), credential source, vendor creation / prewarm / import dates, absence; its addresses with lifecycle and transport; `sustainedDaily` + `effectiveDailyCap` (the selector's own `capForAccount`, minimum across aliases) + `rampProjection` (the cap trajectory to the ceiling if the room is used) + `warmupBudgetToday`; `delivery` pooled across aliases with `evidenceExpiresAt`; `volume7d` / `volume30d` by typology; `cost` from the mailbox-month rate with a labelled paid-to-date estimate.",
  responses: { 200: { description: "Mailboxes", content: { "application/json": { schema: z.object({ asOf: z.string(), mailboxes: z.array(OpsObject) }).openapi("OpsMailboxesResponse") } } }, 401: { description: "Unauthorized" } },
});

registry.registerPath({
  method: "get",
  path: "/internal/ops/addresses",
  summary: "Every account-health row, plus mailbox, transport, evidence expiry, next seed test, ramp projection, 7-day volume, lifecycle history",
  description:
    "Platform-scoped, cached 60s. A SUPERSET of `GET /internal/audit/account-health` — the same rows from the same assembly (`loadAccountHealth`), so the two cannot disagree — with `mailboxLogin`, `sendTransport`, `evidenceExpiresAt` (testedAt + 16 days), `nextSeedTest` (the seed cadence re-applied: due / reason / expected day), `rampProjection`, `volume7d` by typology, and the last 10 lifecycle transitions.",
  responses: { 200: { description: "Addresses", content: { "application/json": { schema: z.object({ asOf: z.string(), accounts: z.array(OpsObject) }).openapi("OpsAddressesResponse") } } }, 401: { description: "Unauthorized" } },
});

registry.registerPath({
  method: "get",
  path: "/internal/ops/infra",
  summary: "The sending infrastructure: fleet totals and one rollup per pool (capacity, lifecycle counts, queue, volume, placement) plus the manual exclusions",
  description:
    "Platform-scoped, cached 60s. `fleet` carries the SAME `dailyCapacity` / `healthyAccountCount` / `totalAccountCount` / `blockedDomainCount` as `/internal/audit/sending-forecast` (same `computeCapacitySummary` over the same inputs) plus queued steps and accounts by lifecycle. `pools` splits the fleet by `mailboxes.pool_type` — google-workspace, gandi-relay, mailforge-relay, dfy-google, unknown — each with mailboxes, addresses, by-lifecycle, ramped daily capacity, in-production count, queued steps, 7-day volume by typology and the mean of latest inbox percentages. `exclusions` lists `instantly_domain_policy` (brand domains pulled from cold) and `instantly_account_feature_policy` (per-feature reservations).",
  responses: { 200: { description: "Infra", content: { "application/json": { schema: OpsObject.openapi("OpsInfraResponse") } } }, 401: { description: "Unauthorized" } },
});

const OpsListQuery = z.object({
  limit: z.number().int().min(1).max(500).describe("REQUIRED. Page size — there is no default; a caller states how much it wants."),
  cursor: z.string().optional().describe("Opaque, from the previous page's `nextCursor`"),
  kind: z.string().optional().describe("outreach | manual_reply | warmup | warmup_reply | seed | reply | auto_reply | bounce"),
  direction: z.enum(["in", "out"]).optional(),
  account: z.string().optional().describe("Sending address"),
  mailbox: z.string().optional().describe("Real mailbox login"),
  domain: z.string().optional().describe("Sending domain"),
  counterparty: z.string().optional().describe("Substring match on the other party"),
  orgId: z.string().optional(),
  campaignId: z.string().optional().describe("The caller campaign id"),
  since: z.string().optional().describe("ISO timestamp, inclusive"),
  until: z.string().optional().describe("ISO timestamp, exclusive"),
  placement: z.string().optional().describe("inbox | spam | missing (warmup + seed)"),
});

registry.registerPath({
  method: "get",
  path: "/internal/ops/threads",
  summary: "Threads over `messages`, newest activity first — the inbox list",
  description:
    "Platform-scoped. One row per `thread_id` (a sequence for outreach and its replies; the message itself for warmup and seeds): kind and subject of the root, account, real mailbox, counterparty, transport, message / inbound / outbound counts, first and last activity, and — for a sequence — the lead's delivery status, reply classification and reply kind from silver/gold. `limit` is REQUIRED (no silent default); `hasInbound=true|false` filters threads with / without an answer. Cursor pagination on (lastAt, threadId).",
  request: { query: OpsListQuery.extend({ hasInbound: z.enum(["true", "false"]).optional() }) },
  responses: { 200: { description: "Threads page", content: { "application/json": { schema: z.object({ threads: z.array(OpsObject), nextCursor: z.string().nullable() }).openapi("OpsThreadsResponse") } } }, 400: { description: "Bad filter" }, 401: { description: "Unauthorized" } },
});

registry.registerPath({
  method: "get",
  path: "/internal/ops/messages",
  summary: "Messages, newest first — every email of every typology, with the same filters as threads plus `threadId`",
  description:
    "Platform-scoped. One row per `messages` row. `limit` is REQUIRED. The body is not inline — read it from `GET /internal/ops/messages/{id}/body`, which goes back to the bronze row the message came from.",
  request: { query: OpsListQuery.extend({ threadId: z.string().optional() }) },
  responses: { 200: { description: "Messages page", content: { "application/json": { schema: z.object({ messages: z.array(OpsObject), nextCursor: z.string().nullable() }).openapi("OpsMessagesResponse") } } }, 400: { description: "Bad filter" }, 401: { description: "Unauthorized" } },
});

registry.registerPath({
  method: "get",
  path: "/internal/ops/messages/{id}/body",
  summary: "The body of one message, read from its bronze source",
  description:
    "Platform-scoped. Instantly mail: `body.text` / `body.html` from the Unibox mirror; our own dispatch: the sequence step's HTML (or the reply's); IMAP inbound: the stored text snippet (first 4000 characters). Warmup and seed bodies are generated per send and not stored beside the dispatch, so both read null with `source` naming the table.",
  request: { params: z.object({ id: z.string() }) },
  responses: { 200: { description: "Body", content: { "application/json": { schema: z.object({ text: z.string().nullable(), html: z.string().nullable(), source: z.string() }).openapi("OpsMessageBody") } } }, 404: { description: "Unknown message" }, 401: { description: "Unauthorized" } },
});

const FxRateSchema = z
  .object({
    base: z.literal("EUR"),
    quote: z.literal("USD"),
    rate: z.number().describe("USD per 1 EUR"),
    asOf: z.string().describe("The ECB reference day (YYYY-MM-DD) the rate is for"),
    source: z.string().describe("ecb-eurofxref-daily"),
  })
  .nullable()
  .describe("The EUR → USD rate every `usd` figure was converted at. Null when none is on record — then every `usd` figure is null too; there is no fallback rate.")
  .openapi("FxRate");

const InfraDomainRowSchema = z
  .object({
    domain: z.string(),
    provider: z.string().describe("gandi | mailforge | primeforge | instantly-dfy"),
    role: z.string().describe("registrar | mailbox | prewarm — what the vendor does for this domain"),
    status: z.string().nullable().describe("The vendor's own status string, verbatim"),
    expiresAt: z.string().nullable(),
    autorenew: z.boolean().nullable().describe("Null when the vendor exposes no such flag — distinct from false"),
    deletionScheduled: z.boolean(),
    cancelledAt: z.string().nullable(),
    absentSince: z.string().nullable().describe("Set when the vendor stopped reporting the domain; the row is kept, never deleted"),
    vendorMailboxes: z.number().int().describe("Mailboxes the VENDOR hosts — routinely differs from the Instantly account count on relayed domains"),
    vendorReportsMailboxes: z
      .boolean()
      .describe("False when this vendor's inventory never reports mailboxes (Instantly DFY): vendorMailboxes is then structurally 0 and means 'not reported', not 'hosts none'"),
    instantlyAccounts: z.number().int().describe("Live Instantly sending accounts on this domain (ghosts excluded)"),
    inProductionAccounts: z.number().int(),
    sentLast30d: z.number().int().describe("Real (non-inferred) dispatches over the trailing 30 days"),
    monthlyCostCents: z.number().nullable().describe("Null when nothing prices this domain — never a substitute figure"),
    currency: z.string().nullable(),
    costSource: z.string().nullable().describe("api (vendor-reported) | rate-card (versioned local row)"),
    costPerEmailCents: z.number().nullable(),
    recurringMonthlyCents: z
      .number()
      .nullable()
      .describe("The mailbox subscription — stops billing the moment the domain is cancelled"),
    renewalCents: z
      .number()
      .nullable()
      .describe("The YEARLY registration, already paid until renewalAt; deleting today avoids it then, it refunds nothing now"),
    renewalAt: z.string().nullable().describe("When that renewal falls due"),
    usd: z
      .object({
        monthlyCostCents: z.number().nullable(),
        costPerEmailCents: z.number().nullable(),
        recurringMonthlyCents: z.number().nullable(),
        renewalCents: z.number().nullable(),
      })
      .describe("The same amounts in USD at the response's `fx`. Null wherever the native amount is null, its currency is unknown, or no rate is on record"),
  })
  .openapi("InfraDomainRow");

const InfraDomainsResponseSchema = z
  .object({ asOf: z.string(), fx: FxRateSchema, domains: z.array(InfraDomainRowSchema) })
  .openapi("InfraDomainsResponse");

registry.registerPath({
  method: "get",
  path: "/internal/infra/domains",
  summary: "Domain inventory across all four infrastructure vendors",
  description:
    "Platform-scoped (no org). One row per (provider, domain): who we buy it from, when it expires, how many mailboxes the vendor hosts, how many Instantly accounts send from it, and what it costs per month and per email. The vendor mailbox count and the Instantly account count are deliberately both shown and routinely differ — the legacy relayed domains run dozens of accounts against a single vendor mailbox. Every amount carries its provenance; a domain nothing prices reports null.",
  responses: {
    200: { description: "Inventory", content: { "application/json": { schema: InfraDomainsResponseSchema } } },
    401: { description: "Unauthorized" },
    500: { description: "Server error", content: { "application/json": { schema: ErrorSchema } } },
  },
});

const InfraWasteResponseSchema = z
  .object({
    asOf: z.string(),
    findingCount: z.number().int(),
    findings: z.array(
      z.object({
        domain: z.string(),
        provider: z.string(),
        reason: z
          .string()
          .describe("paid_no_sending_accounts | cancelled_by_vendor | deletion_scheduled | expiring_within_30d"),
        detail: z.string(),
        monthlyCostCents: z.number().nullable(),
        currency: z.string().nullable(),
        expiresAt: z.string().nullable(),
      }),
    ),
  })
  .openapi("InfraWasteResponse");

registry.registerPath({
  method: "get",
  path: "/internal/infra/waste",
  summary: "Domains billed for but unused, cancelled, or expiring",
  description:
    "Platform-scoped (no org). REPORT-ONLY by design: it never cancels an autorenew and never schedules a deletion. A false positive costs us a domain, and several flagged domains are brand domains held on purpose — a human decides.",
  responses: {
    200: { description: "Findings", content: { "application/json": { schema: InfraWasteResponseSchema } } },
    401: { description: "Unauthorized" },
    500: { description: "Server error", content: { "application/json": { schema: ErrorSchema } } },
  },
});

const InfraSpendResponseSchema = z
  .object({
    asOf: z.string(),
    byProvider: z.array(
      z.object({
        provider: z.string(),
        domainCount: z.number().int(),
        mailboxCount: z
          .number()
          .int()
          .describe(
            "Mailboxes billed. For every vendor that reports an inventory this is the vendor's own count; for Instantly DFY, whose mailboxes are the Instantly accounts and are reported nowhere else, it is the live account count.",
          ),
        monthlyCents: z.number(),
        currency: z.string(),
        source: z.string().describe("api | rate-card | mixed"),
      }),
    ),
    monthlyByCurrency: z.array(z.object({ currency: z.string(), cents: z.number() })),
    fx: FxRateSchema,
    monthlyTotalUsdCents: z
      .number()
      .nullable()
      .describe("monthlyByCurrency stated as one USD figure at `fx`. Null when any currency cannot be converted (no rate on record)"),
    unpricedProviders: z
      .array(z.string())
      .describe("Vendors whose domains we hold and cannot price at all — their cost is MISSING from the totals, not estimated into them"),
    unpricedDomainCount: z.number().int(),
    planSubscriptions: z.array(
      z.object({
        item: z.string(),
        monthlyCents: z.number(),
        currency: z.string(),
        note: z.string().nullable(),
      }),
    ),
    sentLast30d: z.number().int(),
    costPerEmailByCurrency: z.array(z.object({ currency: z.string(), cents: z.number() })),
  })
  .openapi("InfraSpendResponse");

registry.registerPath({
  method: "get",
  path: "/internal/infra/spend",
  summary: "Monthly infrastructure run-rate by vendor",
  description:
    "Platform-scoped (no org). What the vendors charge US — deliberately separate from costs-service, which prices what we RE-BILL the customer; the difference between the two is the real margin per email. Totals stay PER CURRENCY (Gandi bills in EUR, the rest in USD) because blending them would need an FX rate this service does not own. `unpricedProviders` is the honest hole, so cost-per-email is a FLOOR while it is non-empty.",
  responses: {
    200: { description: "Spend summary", content: { "application/json": { schema: InfraSpendResponseSchema } } },
    401: { description: "Unauthorized" },
    500: { description: "Server error", content: { "application/json": { schema: ErrorSchema } } },
  },
});
