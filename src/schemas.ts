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

export const DsnJudgmentsRequestSchema = z
  .object({
    dryRun: z.boolean().optional(),
    sinceDays: z.number().int().min(1).max(3650).nullable().optional(),
  })
  .openapi("DsnJudgmentsRequest");

registry.registerPath({
  method: "post",
  path: "/internal/self-send/dsn-judgments",
  summary: "Settle permanent delivery failures by what they say",
  description:
    "Platform-scoped ops sweep over every permanent delivery-status notice (DSN) tied " +
    "to one of our sends. Each is judged once by a chat-service judgment (`choice`, " +
    "persisted on the bronze row): `address_dead` (the address does not exist, the " +
    "mailbox is gone) is a bounce and promotes `email_bounced`; `message_refused` " +
    "(spam, content, policy, blocklist, reputation, authentication) is NOT: every " +
    "`email_bounced` event on that send is retracted (archived in " +
    "`instantly_events_retracted`) and the person stays contactable. `sinceDays` " +
    "(default null = the whole history); `dryRun` (default TRUE) judges and persists " +
    "the judgments but retracts and promotes nothing. Returns 202; watch for " +
    "`dsn-judgments: done`.",
  request: {
    body: {
      content: { "application/json": { schema: DsnJudgmentsRequestSchema } },
    },
  },
  responses: {
    202: {
      description: "Sweep accepted; runs in the background",
      content: { "application/json": { schema: AcceptedResponseSchema } },
    },
  },
});

export const OrphanRepliesRequestSchema = z
  .object({
    dryRun: z.boolean().optional(),
    backfill: z.boolean().optional(),
    sinceDays: z.number().int().min(1).max(120).optional(),
    limit: z.number().int().min(1).max(20000).optional(),
  })
  .openapi("OrphanRepliesRequest");

registry.registerPath({
  method: "post",
  path: "/internal/self-send/orphan-replies",
  summary: "Recover prospect replies sent from another address",
  description:
    "Platform-scoped ops sweep over `unrelated` inbound mail (no reference to any " +
    "send of ours). Structural noise (our own domains, lists, automated mail, " +
    "Instantly warmup) is skipped; each remaining message is offered, with the " +
    "leads its mailbox wrote to that share a name, company domain or subject with " +
    "it, to a chat-service judgment (`choice`, persisted on the bronze row, judged " +
    "once). A match at or above 0.75 probability is re-filed as that lead's reply " +
    "and promotes `reply_received` (stops the sequence) plus the usual qualification. " +
    "`backfill` (default true) reads every unjudged row of `sinceDays` (default 30) " +
    "and re-reads missing bodies over IMAP; `dryRun` (default TRUE) judges but writes " +
    "nothing. Returns 202; watch for `orphan-replies: done`.",
  request: {
    body: {
      content: { "application/json": { schema: OrphanRepliesRequestSchema } },
    },
  },
  responses: {
    202: {
      description: "Sweep accepted; runs in the background",
      content: { "application/json": { schema: AcceptedResponseSchema } },
    },
  },
});

export const ForwardedRepliesRequestSchema = z
  .object({
    dryRun: z.boolean().optional(),
    mailboxes: z.array(z.string().email()).max(500).optional(),
    domains: z.array(z.string().min(3)).max(100).optional(),
  })
  .openapi("ForwardedRepliesRequest");

registry.registerPath({
  method: "post",
  path: "/internal/self-send/forwarded-replies",
  summary: "Recover prospect replies a mailbox forwarded away into a staff Gmail",
  description:
    "Platform-scoped ops sweep for mailboxes we sent from but could not read (a forward " +
    "without a copy): reads the staff Gmail mirrors (google-service) for mail to or from " +
    "the mailbox's addresses, correlates each message on the Message-Id of one of our " +
    "sends exactly like the IMAP poll, files it in the inbound bronze and promotes it " +
    "(reply, stop, classification, opt-out). Our staff's own answers are recorded first. " +
    "A reply older than 3 days records its fact without acting as if it were fresh. " +
    "Targets: `mailboxes`/`domains`, else every mailbox currently blind. `dryRun` " +
    "(default TRUE) plans and writes nothing. Returns 202; watch for `forwarded-replies: done`.",
  request: {
    body: {
      content: { "application/json": { schema: ForwardedRepliesRequestSchema } },
    },
  },
  responses: {
    202: {
      description: "Sweep accepted; runs in the background",
      content: { "application/json": { schema: AcceptedResponseSchema } },
    },
  },
});

export const GmailThreadsRequestSchema = z
  .object({
    dryRun: z.boolean().optional(),
    since: z.string().datetime({ offset: true }).optional(),
    leads: z.array(z.string().email()).max(1000).optional(),
  })
  .openapi("GmailThreadsRequest");

registry.registerPath({
  method: "post",
  path: "/internal/self-send/gmail-threads",
  summary: "Fold the conversations our people hold with leads from their own Gmail into the leads' threads",
  description:
    "Platform-scoped ops sweep (also run by an in-process worker every 15 minutes, commit mode, " +
    "last 3 days): reads the Gmail mirrors (google-service) for mail between a lead we hold a " +
    "sequence with and one of our own addresses, both directions, files it on the lead's " +
    "sequence (our side as a staff answer, theirs as a reply) and promotes it like any reply " +
    "(stop, classification, side effects). A thread a person holds from their own mailbox is " +
    "never answered by the automated responder; the person is asked instead. A message older " +
    "than 3 days records its fact without acting as if it were fresh. `since` (default 3 days " +
    "ago), `leads` (default every lead). `dryRun` (default TRUE) plans and writes nothing. " +
    "Returns 202; watch for `gmail-threads: done`.",
  request: {
    body: {
      content: { "application/json": { schema: GmailThreadsRequestSchema } },
    },
  },
  responses: {
    202: {
      description: "Sweep accepted; runs in the background",
      content: { "application/json": { schema: AcceptedResponseSchema } },
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
    followUp: z
      .boolean()
      .optional()
      .describe(
        "`true` = this email CONTINUES the brand's own earlier outreach to this person (a follow-up), it is not a new cold pitch. " +
          "Every send gate applies to it " +
          "(409 `lead_opted_out` org-wide, 409 `lead_not_a_prospect`, the address verifier, 402 `insufficient_credits`). " +
          "Refused 409 `brand_sequence_queued` while a step of this brand's sequence to the person is still waiting to leave (two of our emails would race). " +
          "Lands in the SAME thread as the brand's latest email to the person (same mailbox, `Re:` that subject, In-Reply-To/References) when that mailbox can still send; else a new thread with this `subject`. " +
          "Each follow-up is its own send keyed on the caller's `x-run-id` (a retry with the same run = `200 duplicate`; the next follow-up of the same campaign = a new run). Billed like any send. Absent/false = a normal send.",
      ),
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
    code: z.enum(["lead_opted_out", "lead_not_a_prospect", "email_not_verified"]),
    details: z.string(),
    verdict: z
      .string()
      .optional()
      .describe("email_not_verified only — our verifier's (BounceVerify) verdict: `invalid` or `risky`"),
    reason: z
      .string()
      .optional()
      .describe("email_not_verified only — `verdict_invalid` | `verdict_risky` | `verdict_unrecognised`"),
    until: z
      .string()
      .nullable()
      .optional()
      .describe("email_not_verified only — when the verdict may be re-asked (verdict time + 30 days)"),
  })
  .openapi("SendRefusal");

export const SendCreditRefusalSchema = z
  .object({
    error: z
      .string()
      .describe(
        "\"Insufficient credits\" for a balance shortfall; names the cause otherwise " +
          "(\"Sales funnel max budget reached\" / \"Sales funnel max budget unreadable\").",
      ),
    code: z.literal("insufficient_credits").describe("Unchanged for every refusal to pay, so callers keep backing off on it."),
    details: z.string().optional(),
    balance_cents: z.union([z.number(), z.string()]).optional(),
    required_cents: z.union([z.number(), z.string()]).optional(),
    reason: z
      .string()
      .optional()
      .describe(
        "billing-service's reason, verbatim, present ONLY when the refusal is not a balance shortfall: " +
          "`sales_funnel_max_budget_reached` / `sales_funnel_max_budget_unreadable` (raise the campaign's max budget; adding credit changes nothing).",
      ),
    sales_funnel_cap: z
      .record(z.string(), z.unknown())
      .optional()
      .describe(
        "billing-service's `sales_funnel_cap`, verbatim, present only with `reason` " +
          "({ salesFunnelId, salesFunnelCampaignId, amountCents, period, consumedCents, reservedCents }).",
      ),
  })
  .openapi("SendCreditRefusal");

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
    402: {
      description:
        "Refused: the org cannot afford the sequence (every email sent to a lead is billed, " +
        "`instantly-account-email-sent` + `instantly-domain-email-sent` per step for a `valid` address, " +
        "their `-catch-all` (×2) / `-unknown` (×3) twins for those verdicts). No email was " +
        "sent, the provisioned costs were cancelled and nothing was queued. " +
        "Body: `{ error, code: \"insufficient_credits\", balance_cents, required_cents }`. " +
        "Also answered (without the cents) when the org cannot pay to verify the address. " +
        "When billing refused for the sales funnel campaign's MAX BUDGET (not the balance), `error` names it " +
        "(\"Sales funnel max budget reached\") and billing's `reason` + `sales_funnel_cap` ride along verbatim; `code` is unchanged.",
      content: { "application/json": { schema: SendCreditRefusalSchema } },
    },
    409: {
      description:
        "Refused, not a transport failure — no email was sent and nothing was billed. " +
        "No send is refused because the brand emailed the person recently: that is the serve-time " +
        "tactic \"Fresh contacts only\" (lead-service), never a send gate; a person the caller names is sent to. " +
        "`code: \"lead_opted_out\"` = the recipient asked this org to stop. " +
        "`code: \"lead_not_a_prospect\"` = the recipient told the brand they are not a prospect. " +
        "`code: \"email_signature_conflict\"` = two brands of this send set different email signatures (PUT /orgs/email-signature). " +
        "`code: \"email_not_verified\"` = our verifier (BounceVerify, via apollo-service) calls the " +
        "address `invalid` or `risky`: no email ever leaves our mailboxes for it. Every address is " +
        "verified before its sequence is queued (a verdict under 30 days old is reused for free; " +
        "otherwise verified now and billed to the org on this send's run). " +
        "A different lead_id for an address already on file is NOT refused: the email is the " +
        "identity and the stored rows are re-keyed onto the caller's lead_id.",
      content: { "application/json": { schema: SendRefusalSchema } },
    },
    500: {
      description: "Failed to send",
      content: { "application/json": { schema: ErrorSchema } },
    },
    502: {
      description:
        "No verdict could be obtained for the address (verifier, runs or billing unreachable): " +
        "NOT sent, nothing queued or billed for the email. Body `{ error, code: \"email_verification_unavailable\", details }`. Retry later.",
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

const QUEUED_EMAILS_DESCRIPTION =
  "Emails queued right now and not yet sent for this scope: every scheduled sequence step (all steps counted, " +
  "same grain as `sent`) of a LIVE per-lead campaign (status active, delivery status contacted or sent). " +
  "A current snapshot, not dated. Holds of stopped, paused, completed, replied or bounced sequences never count " +
  "(they will never be sent). Scoped by the same query params as every other figure (org, runIds, brandId, " +
  "campaignId, goal, brandProfileId, audienceId, workflowSlugs, featureSlugs). 0 = nothing waiting; " +
  "null = the queue could not be read (unknown, never a 0).";

const StatsEmailStatsSchema = EmailStatsSchema.extend({
  queued: z.number().int().nullable().describe(QUEUED_EMAILS_DESCRIPTION),
});

const StatsGroupedEmailStatsSchema = EmailStatsSchema.extend({
  queued: z
    .number()
    .int()
    .nullable()
    .optional()
    .describe(
      QUEUED_EMAILS_DESCRIPTION +
        " Per group key for groupBy=campaignId|brandId|workflowSlug|featureSlug|leadEmail|audienceId; " +
        "ABSENT for groupBy=day (a queue is a snapshot, it has no day).",
    ),
});

const StatsResponseSchema = z
  .object({
    recipientStats: RecipientStatsSchema,
    emailStats: StatsEmailStatsSchema,
  })
  .openapi("StatsResponse");

const StatsGroupedEntrySchema = z.object({
  key: z.string().describe("Group key. For groupBy=day this is YYYY-MM-DD in the requested timezone."),
  recipientStats: RecipientStatsSchema,
  emailStats: StatsGroupedEmailStatsSchema,
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
      z
        .object({
          workflowSlugs: z.array(z.string().trim().min(1)).min(1).optional().describe("Workflow slugs included in this public-safe group (fleet per workflow)."),
          campaignIds: z
            .array(z.string().trim().min(1))
            .min(1)
            .max(5000)
            .optional()
            .describe(
              "Caller campaign ids (`instantly_campaigns.campaign_id`) included in this group, e.g. one campaign's units, or every campaign of one pipe across the fleet. " +
                "Exclusive with `workflowSlugs`. The ids are never echoed (the entry serves `campaignCount`).",
            ),
        })
        .refine((g) => (g.workflowSlugs ? 1 : 0) + (g.campaignIds ? 1 : 0) === 1, {
          message: "A group takes exactly one of workflowSlugs or campaignIds",
        }),
    ),
  })
  .openapi("EngagementLatencyGroupedRequest");

export type EngagementLatencyGroupedRequest = z.infer<typeof EngagementLatencyGroupedRequestSchema>;

const EngagementLatencyGroupedEntrySchema = z
  .object({
    key: z.string().describe("Caller-owned public group key, for example a workflow dynasty slug."),
    workflowSlugs: z.array(z.string()).describe("The group's workflow slugs; empty for a campaign id group."),
    campaignCount: z.number().int().describe("How many distinct campaign ids scoped the group; 0 for a workflow slug group."),
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
  summary: "Get public-safe engagement latency for workflow slug or campaign id groups",
  description:
    "Computes aggregate elapsed time from each recipient's first real send to first link click and first positive reply for caller-owned groups. " +
    "A group is EITHER a workflow slug set OR a campaign id set (one campaign's units, or every campaign of one pipe across the fleet: the caller owns which campaigns form a pipe). " +
    "Since inception, no time window. Use group keys such as workflow dynasty slugs when the consumer owns dynasty metadata. Returns no per-recipient rows or campaign internals.",
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

// ─── Bounced emails (fleet-wide) ────────────────────────────────────────────

export const BouncedEmailsRequestSchema = z
  .object({
    emails: z
      .array(z.string().min(1))
      .min(1)
      .max(1000)
      .describe("Addresses to look up (case-insensitive, trimmed). Not validated as RFC emails: one odd address must not 400 the whole batch."),
  })
  .openapi("BouncedEmailsRequest");

const BouncedEmailSchema = z.object({
  email: z.string().describe("The address, lowercased and trimmed"),
  firstBouncedAt: z.string().describe("ISO timestamp of the first recorded bounce on any of our sends"),
});

export const BouncedEmailsResponseSchema = z
  .object({
    bounced: z
      .array(BouncedEmailSchema)
      .describe("The subset of the requested addresses that bounced. An address absent here has no recorded bounce."),
  })
  .openapi("BouncedEmailsResponse");

registry.registerPath({
  method: "post",
  path: "/internal/bounced-emails",
  summary: "Which of these addresses bounced on one of our sends (fleet-wide)",
  description:
    "A bounce is a fact about the address, not the sender, so this read is NOT org-scoped: it answers across every org " +
    "and brand. Every recorded `email_bounced` event is a permanent failure (temporary delivery delays are never promoted " +
    "and were retracted), so all are hard bounces. Fails loud (500) rather than returning an empty list on a read error.",
  request: {
    body: {
      content: { "application/json": { schema: BouncedEmailsRequestSchema } },
    },
  },
  responses: {
    200: {
      description: "The bounced subset (possibly empty)",
      content: { "application/json": { schema: BouncedEmailsResponseSchema } },
    },
    400: {
      description: "Invalid request",
      content: { "application/json": { schema: ErrorSchema } },
    },
    401: { description: "Unauthorized" },
    500: {
      description: "The bounce record could not be read",
      content: { "application/json": { schema: ErrorSchema } },
    },
  },
});

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
    skipped: z
      .array(TransferBrandUpdatedTableSchema.extend({ reason: z.string() }))
      .optional()
      .describe("Rows of the brand that could not move (multi-brand campaigns). Absent when nothing was left behind."),
  })
  .openapi("TransferBrandResponse");

registry.registerPath({
  method: "post",
  path: "/internal/transfer-brand",
  summary: "Transfer solo-brand rows from one org to another",
  description:
    "Moves the brand's campaigns (solo-brand only) and every row tied to them (leads, messages, gold status, " +
    "bronze webhook/analytics/emails/leads/config mirrors, manual qualifications and withdrawals, scheduled replies) " +
    "from sourceOrgId to targetOrgId. When targetBrandId is present, rewrites the brand id too. Standing opt-outs of " +
    "the brand's leads are COPIED to the target org (never removed from the source). Multi-brand campaigns are left " +
    "and reported under `skipped`. Idempotent: a second call reports zero everywhere.",
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
  "lead_opt_out_requested",
  "lead_neutral",
  "lead_off_topic",
  "lead_already_customer",
  "lead_is_client",
  "lead_out_of_office",
  "auto_reply_received",
] as const;

export const ReplyKindSchema = z
  .enum(REPLY_KIND_VALUES)
  .describe(
    "What KIND of reply arrived, and nothing about how far the deal got. Positive splits four ways: lead_interested (personally interested), lead_referral (not personally interested but relevant — points at the right person), lead_info_requested (wants to know more), lead_meeting_requested (wants to book). Negative splits by whether the no is about the moment or the person: lead_not_interested (declines today — recyclable), lead_wrong_person (not the right contact, hands nothing back) and lead_changed_job (has left the role we were selling to). The last two are objective facts about the person and permanent for the lead; the first is not. lead_opt_out_requested: they asked us to stop. lead_off_topic: a real reply about something other than buying the offer (a partnership, hiring, investors, a vendor pitching us, press) — reported neutral, never answered by the automated responder, escalated to a human. lead_already_customer: they already buy from the client (a won sale our outreach did not cause); lead_is_client: they are the client or its own team. Both are not prospects: reported neutral, never celebrated, every sequence of the brand to them stops and none is sent again. Deal outcomes (a booked meeting, a closed deal) are lead outcomes owned by the lead-outcomes service, not reply kinds.",
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
      .describe("Campaign ID — when provided, returns campaign-scoped status (brandId only scopes `sendRefusal`)"),
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
    ),  sendRefusal: z
    .object({
      code: z
        .string()
        .describe("The 409 `code` `POST /orgs/send` would answer: `lead_opted_out`, `lead_not_a_prospect` or `email_not_verified` (our verifier called the address invalid / risky; until = when it may be re-verified)."),
      until: z
        .string()
        .nullable()
        .describe("ISO 8601 instant the refusal lapses (the moment the send gate lets the send through again, e.g. when the address may be re-verified). Null = never lapses on its own (opt-out, not a prospect)."),
    })
    .optional()
    .describe(
      "Present only on the `campaign` and `brand` scopes, and only when `POST /orgs/send` would REFUSE this person for this org and brand (`x-brand-id`, else the body's `brandId`). Read from the same lookups the send gates run, so the two answers cannot disagree. When several rules stand, the one that lasts longest is stated. Absent = no refusal.",
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
  "lead_off_topic",
  "lead_already_customer",
  "lead_is_client",
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
      .optional()
      .describe(
        "Logical campaign id — the same key manual qualifications and opt-outs use. OPTIONAL: omit it and the thread is found from `email` alone. A thread is a campaign of this org in which the prospect wrote back. Exactly one → the reply goes into it. Several → 409 `ambiguous_thread` listing each candidate (`campaign_id` + `last_message_at`); call again naming one. None but we did email them → 409 `no_reply_to_thread`. Never emailed by this org → 404 `campaign_not_found`. When given, it is used as-is.",
      ),
    email: z
      .string()
      .email()
      .describe("The prospect who replied. Matched case-insensitively. Enough on its own to find the thread when `campaign_id` is omitted."),
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
        "Who asked for this answer to be sent. It decides ONE thing: whether the human-takeover gate applies. An `automation` reply is refused (409 `human_took_over`) once a person has answered the thread since the prospect last wrote and that answer is not yet counted as our touch; a `human` reply is never gated. Absent resolves to `automation` — the automated responder is the only caller today, so the gate is live without waiting on it, and a human surface declares `human` explicitly.",
      ),
    attachments: z
      .array(
        z.object({
          document_id: z
            .string()
            .uuid()
            .describe("An offer document id (UUID) from brand-service, of THIS org (x-org-id)."),
        }),
      )
      .max(5)
      .optional()
      .describe(
        "Offer documents to attach to the reply as real files (a case study, a one-pager). This service reads each one from brand-service, bytes included, as this org, and attaches it under brand-service's filename and type. Up to 5, 10 MB together. FAIL LOUD, nothing sent: unknown or another org's document = 404 `attachment_not_found`; over the limits = 422 `attachment_too_large`; brand-service unreadable = 502 `attachment_unavailable`; a thread on the Instantly pipe (no attachment support) = 422 `attachments_unsupported_on_transport`. Absent or empty = the reply exactly as before. Replies only: cold sequence steps never carry a file.",
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

const SentAttachmentSchema = z
  .object({
    documentId: z.string().describe("The brand-service offer document id"),
    filename: z.string(),
    contentType: z.string(),
    sizeBytes: z.number().int(),
    md5: z.string().describe("Hex md5 of the exact bytes attached"),
  })
  .openapi("SentAttachment");

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
    attachments: z
      .array(SentAttachmentSchema)
      .optional()
      .describe("The files this reply carried. Absent when it carried none."),
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
        "ambiguous_thread",
        "no_reply_to_thread",
        "sending_account_unresolved",
        "mailbox_credential_unavailable",
        "human_took_over",
        "prospect_wrote_elsewhere",
        "reply_dispatch_failed",
        "attachment_not_found",
        "attachment_too_large",
        "attachment_unavailable",
        "attachments_unsupported_on_transport",
      ])
      .describe(
        "campaign_not_found: no campaign in this org for the given email. ambiguous_thread: `campaign_id` was omitted and the prospect wrote back in several campaigns; `candidates` lists them, name one. no_reply_to_thread: the lead never wrote back, so there is nothing to thread onto. sending_account_unresolved: we cannot tell which mailbox contacted them. mailbox_credential_unavailable: the mailbox is on our own sender and we hold no credential for it. human_took_over: a person already answered this thread since the prospect last wrote, so an automated reply is refused; that answer counts as our touch and the next follow-up is scheduled from it (a thread held in a person's own mailbox is handed to them instead). prospect_wrote_elsewhere: the prospect's newest message went to one of our people's own mailbox (their Gmail), which we cannot send from; refused for every sender, answer from that mailbox. reply_dispatch_failed: the transport refused the send. attachment_not_found: a named offer document does not exist in this org (or is another org's). attachment_too_large: more than 5 documents, or over 10 MB together. attachment_unavailable: brand-service could not serve a document. attachments_unsupported_on_transport: the thread runs on Instantly, which cannot carry a file. Every attachment refusal sends nothing.",
      ),
    candidates: z
      .array(
        z.object({
          campaign_id: z.string().describe("Pass this back as `campaign_id` to reply in that thread"),
          last_message_at: z
            .string()
            .nullable()
            .describe("ISO time of the newest email in that thread, either direction"),
        }),
      )
      .optional()
      .describe("Only on `ambiguous_thread`: every thread the prospect wrote back in, newest first"),
  })
  .openapi("ReplyToLeadError");

registry.registerPath({
  method: "post",
  path: "/orgs/replies",
  summary: "Reply to a lead who replied, in their existing thread",
  description:
    "Answer a prospect who wrote back, in the SAME email thread, from the SAME mailbox that originally contacted them, under the SAME persona they have been corresponding with.\n\n" +
    "**The sending identity is resolved here, never supplied.** Which mailbox answers comes from `instantly_campaigns.account_email` (persisted at send time), with the mailbox recorded on the inbound message as the fallback for a historical row. The display name and signature follow that account. A caller-supplied from-address would let a reply arrive from a mailbox this prospect has never heard from — the exact failure this endpoint exists to prevent.\n\n" +
    "**Finding the thread from the address alone.** `campaign_id` is optional: with only `email`, the thread is the one campaign of this org where that prospect wrote back. Several such campaigns → `409 ambiguous_thread` with the `candidates` (campaign + last message date) so the caller can name one; never answered → `409 no_reply_to_thread`; never emailed → `404 campaign_not_found`. Naming the campaign behaves exactly as before.\n\n" +
    "**Threaded or nothing.** On the Instantly transport the reply goes out through `POST /emails/reply`, threaded onto the prospect's latest inbound message; on the self-send transport we dispatch it ourselves with `In-Reply-To` / `References` over the conversation we already hold in bronze. There is NO fallback to a fresh email: if the thread cannot be found the call fails with `code: \"no_reply_to_thread\"`.\n\n" +
    "**The agency inbox is CC'd, visibly.** Every reply carries it as a CC (never a BCC) so a human can read the exchange and be pulled into it by a reply-all. Cold sequence sends carry no CC — this applies only to the one-to-one answer.\n\n" +
    "**Signature:** send the prospect-facing words only. This service appends the account's persona signature (idempotently — a body re-sent never stacks signatures). Deliberately NO unsubscribe footer: a one-to-one answer is not bulk mail.\n\n" +
    "**The answer goes out immediately, at any hour, on any day.** A prospect who just wrote to us is at their inbox now, so a one-to-one answer is never held for their business hours (cold sequence steps still are). It is still NOT a sequence step: no hold, no step number, no capacity consumed.\n\n" +
    "**A human takeover stops the automated responder.** When `sent_by` is `automation` (the default), the reply is refused with `409 human_took_over` if a PERSON has answered this thread since the prospect last wrote — read from what actually went out, both from this service and from Instantly's own inbox. That answer then counts as one of our touches: the prospect's next follow-up is scheduled from it (the usual growing interval), and once that date comes the automated follow-up goes out. A `human` reply is never gated. Nothing is prepared or sent on a refusal.\n\n" +
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
      description: "No campaign in this org for the given email (`campaign_not_found`), or a named offer document is not this org's (`attachment_not_found`)",
      content: { "application/json": { schema: ReplyToLeadErrorSchema } },
    },
    422: {
      description:
        "An attachment cannot be carried, nothing sent: `attachment_too_large` or `attachments_unsupported_on_transport`",
      content: { "application/json": { schema: ReplyToLeadErrorSchema } },
    },
    409: {
      description:
        "Refused, nothing sent: `ambiguous_thread` (several threads, see `candidates`), `no_reply_to_thread`, `sending_account_unresolved`, `mailbox_credential_unavailable`, or `human_took_over` (a person already answered this thread).",
      content: { "application/json": { schema: ReplyToLeadErrorSchema } },
    },
    502: {
      description: "The transport refused the send (`reply_dispatch_failed`), or brand-service could not serve a named document (`attachment_unavailable`)",
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
      .describe("How many messages of the exchange the brand's team was sent (0 when the rep hand-over went out or on a repeat)"),
    followupsStopped: z
      .boolean()
      .describe(
        "Whether the follow-up ladder was emptied. False when lead-service holds no row for this person on this campaign — a real state, not a failure; the human was still told.",
      ),
    handoff: z
      .enum(["rep", "team", "none", "already_escalated"])
      .describe(
        "rep: the prospect got an immediate hand-over in their own thread, the brand's rep in Cc and the agency inbox in Bcc. team: the brand names no rep (or the hand-over could not go out), so the prospect got nothing and every member of the org that owns the brand got the prospect's reply with a link to the brand's Unibox (staff are never the recipient). none: nobody was emailed (an off-topic reply on a brand with no AI responder: the client answers it in the Unibox). already_escalated: this thread was escalated before; nothing was sent and nothing stopped.",
      ),
    handedTo: z.string().nullable().describe("Who now owns the thread: the rep's address, or the team members emailed (comma-separated). Null when nobody was emailed."),
    replyRead: z
      .boolean()
      .nullable()
      .describe("Whether the prospect's reply could be read, and so is verbatim in what was sent. Null when not checked."),
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
    "The automated responder cannot answer what this prospect asked. EXACTLY ONCE per thread: the client is told in the positive-reply celebration; when the brand names a sales rep the prospect gets an immediate hand-over in their own thread (the rep in Cc, the agency inbox in Bcc, the history quoted, nothing answered or promised), otherwise the prospect gets nothing and every member of the org that owns the brand gets the prospect's reply verbatim with a link to the brand's Unibox (staff are never the recipient); the follow-up ladder stops once. Afterwards the automated responder never writes on that thread again (`handed_over`). The `question` is a hint for the hand-over draft and is shown to nobody.\n\n" +
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
    outreachFact: z
      .object({
        subjectKey: z
          .string()
          .describe(
            "The `subjectKey` of the `email_sent` fact in GET /internal/outreach-facts, byte for byte (`ievt:<event id>`).",
          ),
        step: z
          .number()
          .int()
          .nullable()
          .describe("That fact's step (1 = first email). Null where the feed serves none (poll-only sends)."),
        position: z
          .enum(["first", "followup"])
          .describe("That fact's position: first email or a follow-up."),
      })
      .nullable()
      .describe(
        "WHICH served `email_sent` outreach fact this outbound message is, tied by the stored email's identity (never by time). Null on every inbound message, and on an outbound one no served fact recorded (a manual answer, a send the event stream never saw, a send younger than the feed's ~2 min emission tick).",
      ),
    attachments: z
      .array(SentAttachmentSchema)
      .optional()
      .describe("Offer documents this answer of ours carried as files (POST /orgs/replies `attachments`). Absent when none."),
  })
  .openapi("ConversationMessage");

const ConversationSourceSchema = z
  .enum(["mirror", "self_send", "provider"])
  .describe(
    "Where the messages were read from. mirror: our own copy — the bronze mirror of the Instantly Unibox, or for a sequence never mirrored the emails and replies our webhook events recorded; the normal case, and the one that survives the Instantly plan being cancelled. self_send: the sequence we dispatched ourselves. provider: read live from Instantly because neither our mirror nor our event record held the words of a sequence that did exchange mail.",
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

export const LeadSendingScheduleQuerySchema = z
  .object({
    email: z
      .string()
      .trim()
      .min(1)
      .describe("The lead's email address (the lead identity in this service, matched case-insensitively)"),
    brand_id: z
      .string()
      .uuid()
      .optional()
      .describe("Only consider this lead's sequences that carry this brand"),
  })
  .openapi("LeadSendingScheduleQuery");

export const LeadSendingScheduleSchema = z
  .object({
    weekdays: z
      .array(
        z.enum([
          "sunday",
          "monday",
          "tuesday",
          "wednesday",
          "thursday",
          "friday",
          "saturday",
        ]),
      )
      .describe("Local days of the week a cold email may be sent, in `timezone`"),
    startHour: z
      .number()
      .int()
      .describe("Local hour (0-23, in `timezone`) the sending window opens, inclusive"),
    endHour: z
      .number()
      .int()
      .describe("Local hour (0-23, in `timezone`) the sending window closes, exclusive"),
    timezone: z
      .string()
      .describe("IANA timezone the window is evaluated in for this lead"),
    timezoneIsDefault: z
      .boolean()
      .describe(
        "True when we hold no usable timezone for this lead and `timezone` is the fleet default we fall back to",
      ),
    hasSequence: z
      .boolean()
      .describe(
        "True when this service holds a sequence for this lead (in this brand when brand_id is given). False = the schedule that WOULD apply",
      ),
  })
  .openapi("LeadSendingSchedule");

export const LeadSendingScheduleResponseSchema = z
  .object({
    success: z.literal(true),
    schedule: LeadSendingScheduleSchema,
  })
  .openapi("LeadSendingScheduleResponse");

registry.registerPath({
  method: "get",
  path: "/orgs/sending-schedule",
  summary: "When a lead may be emailed",
  description:
    "The sending window that governs cold email to ONE lead: which local weekdays, which local hours, and in which timezone. Every value is read from the same constants the send path uses, so a consumer never re-types the window and this read moves the day the window does.\n\n" +
    "**Timezone.** The zone persisted on the lead's most recent sequence in this org (and brand, when `brand_id` is given), falling back to the zone shipped in that sequence's campaign schedule. No sequence, or no usable zone, = the fleet default zone with `timezoneIsDefault: true`.\n\n" +
    "**Never 404.** A lead we hold nothing for gets the schedule that WOULD apply (`hasSequence: false`, default zone).\n\n" +
    "**Cost:** none. It sends nothing and declares nothing.",
  request: {
    headers: TrackingHeadersSchema,
    query: LeadSendingScheduleQuerySchema,
  },
  responses: {
    200: {
      description: "The lead's sending schedule",
      content: { "application/json": { schema: LeadSendingScheduleResponseSchema } },
    },
    400: {
      description: "Invalid query",
      content: { "application/json": { schema: ErrorSchema } },
    },
    401: { description: "Unauthorized" },
    500: {
      description: "The schedule could not be read",
      content: { "application/json": { schema: ErrorSchema } },
    },
  },
});

export const CampaignSendQueueQuerySchema = z
  .object({
    campaignIds: z
      .string()
      .transform((raw) => Array.from(new Set(raw.split(",").map((id) => id.trim()).filter(Boolean))))
      .pipe(z.array(z.string()).min(1).max(500))
      .describe("Comma-separated caller campaign ids (`instantly_campaigns.campaign_id`), 1 to 500."),
  })
  .openapi("CampaignSendQueueQuery");

const SendQueueTimezoneSchema = z
  .object({
    timezone: z.string().describe("IANA timezone the leads are emailed in."),
    timezoneIsDefault: z.boolean().describe("True when no usable zone is stored for these leads: the fleet default the send runs in."),
    leadsAwaitingFirstEmail: z.number().int(),
    windowOpensAt: z.string().describe("ISO instant the sending window next opens in this zone (at or after `asOf`; `asOf` itself while it is open)."),
  })
  .openapi("SendQueueTimezone");

export const CampaignSendQueueResponseSchema = z
  .object({
    campaignIds: z.array(z.string()),
    asOf: z.string().describe("ISO instant the snapshot was read."),
    leadsAwaitingFirstEmail: z.number().int().describe("Leads whose first email is written and queued, not sent yet (live sequence, delivery status `contacted`, >= 1 provisioned step)."),
    leadsWithFollowUpQueued: z.number().int().describe("Leads already emailed whose next step is queued."),
    queuedEmails: z.number().int().describe("Every queued email, all steps (the grain of `emailStats.queued`)."),
    firstEmailsFrom: z
      .string()
      .nullable()
      .describe("Earliest ISO instant the sending window opens for a lead awaiting its first email: the earliest such an email MAY leave, never a promise (mailbox capacity can push it later). Null when none awaits."),
    sendWindow: z
      .object({
        weekdays: z.array(z.string()).describe("Local weekdays we send on."),
        startHour: z.number().int().describe("Local hour the window opens."),
        endHour: z.number().int().describe("Local hour the window closes."),
        timezoneRule: z.literal("lead_local").describe("Hours are local to each lead (its own timezone)."),
      })
      .describe("The send path's own window constants."),
    timezones: z.array(SendQueueTimezoneSchema).describe("Leads awaiting their first email per timezone, earliest window first."),
  })
  .openapi("CampaignSendQueueResponse");

registry.registerPath({
  method: "get",
  path: "/orgs/send-queue",
  summary: "What is queued to send for some campaigns, and when the window next opens",
  description:
    "A SNAPSHOT of the send queue for the given campaigns of the caller's org: leads awaiting their first email, leads with a follow-up step queued, queued emails, and when the sending window next opens for the leads awaiting their first email (each lead's own local business hours, the send path's own constants and `nextLocalSendInstant`).\n\n" +
    "A queued sequence = a live per-lead campaign (`status='active'`, delivery `contacted|sent`) holding >= 1 provisioned step. Another org's campaign reads as nothing queued.\n\n" +
    "**Fail loud:** a read failure is a 500, never zeros.\n\n**Cost:** none.",
  request: {
    headers: TrackingHeadersSchema,
    query: CampaignSendQueueQuerySchema,
  },
  responses: {
    200: { description: "The send queue snapshot", content: { "application/json": { schema: CampaignSendQueueResponseSchema } } },
    400: { description: "Invalid query", content: { "application/json": { schema: ErrorSchema } } },
    401: { description: "Unauthorized" },
    500: { description: "The queue could not be read", content: { "application/json": { schema: ErrorSchema } } },
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

export const WrittenToLeadsQuerySchema = z
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
    limit: z.coerce
      .number()
      .int()
      .positive()
      .max(5000)
      .optional()
      .describe("Page size, 1-5000. Omitted = 1000."),
    cursor: z
      .string()
      .min(1)
      .optional()
      .describe("The previous page's `nextCursor`, verbatim. Omitted = first page."),
  })
  .openapi("WrittenToLeadsQuery");

const WrittenToLeadSchema = z
  .object({
    campaignId: z
      .string()
      .nullable()
      .describe("The caller's own campaign id. Null on a platform send."),
    instantlyCampaignId: z
      .string()
      .describe("This service's per-lead sequence id, the identity `GET /orgs/conversations` takes"),
    leadEmail: z.string(),
    brandIds: z.array(z.string()),
    firstSentAt: z.string().describe("Our first real send in this sequence (ISO 8601 UTC)"),
    lastSentAt: z
      .string()
      .describe("Our latest real send in this sequence, bounced or not (ISO 8601 UTC)"),
    engaged: z
      .boolean()
      .describe("True iff `GET /orgs/engaged-leads` lists this row too: `(replied AND NOT unsubscribed) OR clicked`"),
    replied: z.boolean().describe("A human reply was received. Autoresponders never set it (see `replyKind`)."),
    clicked: z.boolean().describe("The lead clicked a link WE sent"),
    unsubscribed: z.boolean(),
    bounced: z.boolean(),
    firstRepliedAt: z.string().nullable(),
    firstClickedAt: z.string().nullable(),
    replyClassification: z
      .string()
      .nullable()
      .describe("positive | negative | neutral. Null when no reply is qualified."),
    replyKind: z
      .string()
      .nullable()
      .describe("The finer reading of the latest statement, incl. `auto_reply_received` / `lead_out_of_office`. Null when none is on record."),
    disqualified: z
      .boolean()
      .describe("True only for a kind that is permanent about the PERSON (wrong person, changed job)"),
  })
  .openapi("WrittenToLead");

export const WrittenToLeadsResponseSchema = z
  .object({
    success: z.literal(true),
    count: z.number().int().describe("Rows on THIS page"),
    leads: z
      .array(WrittenToLeadSchema)
      .describe("Ordered by (instantlyCampaignId, leadEmail) ascending, the keyset the cursor walks"),
    nextCursor: z
      .string()
      .nullable()
      .describe("Pass as `cursor` for the next page. Null = this was the last page."),
  })
  .openapi("WrittenToLeadsResponse");

registry.registerPath({
  method: "get",
  path: "/orgs/written-to-leads",
  summary: "Every lead we have actually written to (paged)",
  description:
    "One row per (sequence, lead) with at least one REAL `email_sent`, whether or not the lead ever answered. The superset of `GET /orgs/engaged-leads`: a Unibox lists every conversation, including the ones where only we wrote. `engaged` marks the rows engaged-leads would also return.\n\n" +
    "**Population.** Exactly the brand's `sent` rows on `POST /orgs/status` (read from the same gold projection). A lead contacted but not yet sent is NOT listed.\n\n" +
    "**Paging.** Keyset on `(instantlyCampaignId, leadEmail)`. Read until `nextCursor` is null. Default page 1000, max 5000. A row promoted between two pages never shifts a page boundary.\n\n" +
    "**Cost:** none. It sends nothing and declares nothing.",
  request: {
    headers: TrackingHeadersSchema,
    query: WrittenToLeadsQuerySchema,
  },
  responses: {
    200: {
      description: "One page of written-to leads (possibly empty)",
      content: { "application/json": { schema: WrittenToLeadsResponseSchema } },
    },
    400: {
      description: "Invalid query or cursor",
      content: { "application/json": { schema: ErrorSchema } },
    },
    401: { description: "Unauthorized" },
    500: {
      description: "The written-to leads could not be read",
      content: { "application/json": { schema: ErrorSchema } },
    },
  },
});

export const EmailSequencesQuerySchema = z
  .object({
    brandId: z.string().uuid().describe("The brand whose sequences to list (required)"),
    limit: z.coerce
      .number()
      .int()
      .positive()
      .max(2000)
      .optional()
      .describe("Sequences per page, 1-2000. Omitted = 500."),
    cursor: z
      .string()
      .min(1)
      .optional()
      .describe("The previous page's `nextCursor`, verbatim. Omitted = first page."),
  })
  .openapi("EmailSequencesQuery");

const EmailSequenceStepSchema = z
  .object({
    step: z.number().int().describe("1-based position in the sequence. Step 1 is the first email."),
    kind: z.enum(["first_email", "follow_up"]),
    status: z
      .enum(["sent", "bounced", "queued", "stopped"])
      .describe(
        "`sent`: it went out (see `sentAt`). `bounced`: it went out and the receiving server refused it (see `bouncedAt`); it was NOT delivered. `queued`: not sent yet, it goes out in the prospect's business hours. `stopped`: it will not go out (the prospect replied, bounced, opted out or booked, or the sequence was stopped).",
      ),
    sentAt: z.string().nullable().describe("When it went out (ISO 8601 UTC). Null unless `sent` or `bounced`."),
    bouncedAt: z.string().nullable().describe("When the receiving server refused it (ISO 8601 UTC). Null unless `bounced`."),
  })
  .openapi("EmailSequenceStep");

const EmailSequenceSchema = z
  .object({
    leadId: z
      .string()
      .nullable()
      .describe("The lead id, the same `leadId` content-generation-service carries on the generated email"),
    leadEmail: z.string(),
    campaignId: z.string().nullable().describe("The campaign this sequence belongs to"),
    steps: z.array(EmailSequenceStepSchema).describe("Ordered by `step`"),
  })
  .openapi("EmailSequence");

export const EmailSequencesResponseSchema = z
  .object({
    brandId: z.string(),
    count: z.number().int().describe("Sequences on THIS page"),
    sequences: z.array(EmailSequenceSchema),
    nextCursor: z
      .string()
      .nullable()
      .describe("Pass as `cursor` for the next page. Null = this was the last page."),
  })
  .openapi("EmailSequencesResponse");

registry.registerPath({
  method: "get",
  path: "/orgs/email-sequences",
  summary: "A brand's email sequences, each email sent or queued (paged)",
  description:
    "Every sequence we run for one brand of the org: one row per sequence (a prospect emailed again by a later campaign has two), with every step (first email + follow-ups) and what happened to it: `sent` with `sentAt`, `bounced` (sent, then refused by the receiving server: `sentAt` + `bouncedAt`, not delivered), `queued`, or `stopped`. Read from our own send records (send holds, self-send steps, real `email_sent` events), so the counts match our dispatch log.\n\n" +
    "**Join** with the generated email bodies (content-generation-service, `GET /v1/emails?brandId=`) on `leadId`.\n\n" +
    "**Paging.** Keyset, opaque cursor. Read until `nextCursor` is null. Default page 500, max 2000.\n\n" +
    "**Scope.** Org-scoped on `x-org-id`: a brand of another org lists nothing.\n\n" +
    "**Cost:** none. It sends nothing and declares nothing.",
  request: {
    headers: TrackingHeadersSchema,
    query: EmailSequencesQuerySchema,
  },
  responses: {
    200: {
      description: "One page of sequences (possibly empty)",
      content: { "application/json": { schema: EmailSequencesResponseSchema } },
    },
    400: {
      description: "Missing/invalid brandId, invalid limit or cursor",
      content: { "application/json": { schema: ErrorSchema } },
    },
    401: { description: "Unauthorized" },
    500: {
      description: "The sequences could not be read",
      content: { "application/json": { schema: ErrorSchema } },
    },
  },
});

// ─── Email signature (per brand) ────────────────────────────────────────────

export const EmailSignatureQuerySchema = z
  .object({
    brandId: z.string().min(1).describe("The brand whose email signature to read or reset"),
  })
  .openapi("EmailSignatureQuery");

export const EmailSignatureBodySchema = z
  .object({
    brandId: z.string().min(1).describe("The brand the signature is for"),
    signature: z
      .string()
      .min(1)
      .max(2000)
      .describe(
        "Your signature as plain text, one line per line (use \\n between lines), at most 1,000 characters and 12 lines. " +
          "Example: `Jane Doe\\nHead of Sales, Acme\\nacme.com`. HTML is not interpreted (it is shown as written). " +
          "Leave out the `--` line: it is added above the signature automatically. " +
          "Placeholders `{first_name}`, `{last_name}`, `{full_name}`, `{sender_email}` are filled with the mailbox that sends each email, so the signature matches the From name: " +
          "`{first_name} {last_name}\\nDefiniteJobs` signs `Ruth Chandler` from ruth@ and `Fallon …` from fallon@. Any other `{…}` or stray brace is refused.",
      ),
  })
  .openapi("EmailSignatureBody");

export const EmailSignatureResponseSchema = z
  .object({
    brandId: z.string(),
    signature: z
      .string()
      .nullable()
      .describe(
        "The signature you set, as plain text. Null = you set none: emails end with our default signature (the sending mailbox's name, then `Distribute.you | Marketing Agency`).",
      ),
    updatedAt: z.string().nullable().describe("When you last set it (ISO 8601 UTC). Null when you set none."),
  })
  .openapi("EmailSignature");

const EMAIL_SIGNATURE_RULES =
  "**What it changes.** Every email we send for this brand ends with a signature. By default it is ours: the sending mailbox's name, then `Distribute.you | Marketing Agency`. " +
  "Set your own and every NEW email thread for this brand ends with it instead: the email itself (a cold email, or an email you pushed through the API with the `email_pushed_via_api` trigger), every follow-up, and every reply we send in that thread (AI or human).\n\n" +
  "**One signature per thread.** A thread keeps the signature it started with: changing or removing yours later applies to new threads only, so a conversation never switches signature halfway.\n\n" +
  "**Sender placeholders.** We send from several mailboxes, each under its own name in the From line. Write `{first_name}`, `{last_name}`, `{full_name}` or `{sender_email}` in your signature and each email fills them with the mailbox that sends it, so the name you sign with always matches the From. A thread keeps one mailbox, so it keeps one signature.\n\n" +
  "**Several brands.** An email sent for several brands at once uses the signature they share; if two of them set different signatures, the send is refused (409 `email_signature_conflict`).\n\n" +
  "**Scope.** Org-scoped on `x-org-id`. **Cost:** none.";

registry.registerPath({
  method: "get",
  path: "/orgs/email-signature",
  summary: "Read the email signature of a brand (yours, or our default)",
  description: "The signature emails of this brand end with. `signature: null` = our default.\n\n" + EMAIL_SIGNATURE_RULES,
  request: { headers: TrackingHeadersSchema, query: EmailSignatureQuerySchema },
  responses: {
    200: {
      description: "The brand's signature (null = our default)",
      content: { "application/json": { schema: EmailSignatureResponseSchema } },
    },
    400: { description: "Missing brandId", content: { "application/json": { schema: ErrorSchema } } },
    401: { description: "Unauthorized" },
  },
});

registry.registerPath({
  method: "put",
  path: "/orgs/email-signature",
  summary: "Change my email signature: replace our default signature with yours for a brand",
  description:
    "Replace the signature at the bottom of every email we send for this brand (cold emails, emails you push, follow-ups, AI replies) with your own. Idempotent: setting it again overwrites it.\n\n" +
    EMAIL_SIGNATURE_RULES,
  request: {
    headers: TrackingHeadersSchema,
    body: { content: { "application/json": { schema: EmailSignatureBodySchema } } },
  },
  responses: {
    200: {
      description: "Saved. New threads of this brand end with it.",
      content: { "application/json": { schema: EmailSignatureResponseSchema } },
    },
    400: {
      description: "Missing brandId, or a signature that is empty, too long, has too many lines, contains a `--` line, an unknown `{placeholder}` or a stray brace",
      content: { "application/json": { schema: ErrorSchema } },
    },
    401: { description: "Unauthorized" },
  },
});

registry.registerPath({
  method: "delete",
  path: "/orgs/email-signature",
  summary: "Go back to our default email signature for a brand",
  description: "Removes your signature: new threads of this brand end with our default signature again. Threads already started keep theirs. Idempotent.\n\n" + EMAIL_SIGNATURE_RULES,
  request: { headers: TrackingHeadersSchema, query: EmailSignatureQuerySchema },
  responses: {
    200: {
      description: "Back to our default (`signature: null`)",
      content: { "application/json": { schema: EmailSignatureResponseSchema } },
    },
    400: { description: "Missing brandId", content: { "application/json": { schema: ErrorSchema } } },
    401: { description: "Unauthorized" },
  },
});

export const ReplyVerdictsQuerySchema = z
  .object({
    emails: z
      .array(z.string().email())
      .min(1)
      .max(1000)
      .describe("Lead addresses (case-insensitive). Every real reply this org holds for them is returned."),
    brandId: z.string().optional().describe("Only replies on campaigns of this brand"),
    campaignId: z.string().optional().describe("Only replies on this logical campaign"),
  })
  .openapi("ReplyVerdictsQuery");

const ReplyVerdictSchema = z
  .object({
    kind: z.string().describe("The reply kind (same vocabulary as `replyKind` on POST /orgs/status)"),
    classification: z.enum(["positive", "negative", "neutral"]).nullable(),
    producerType: z.enum(["human", "instantly", "model"]).describe("Who produced the current verdict. A person beats everyone."),
    producer: z.string().describe("e.g. `manual`, `instantly:webhook`, `deepseek-flash`"),
    attribution: z
      .enum(["exact", "latest_before"])
      .describe("`exact` = the producer named this reply; `latest_before` = the verdict judged the latest reply before it on the thread"),
    confidence: z.number().nullable(),
    decidedAt: z.string().describe("ISO 8601 UTC"),
    automatedAnswer: z
      .boolean()
      .describe("A machine answered (out-of-office, autoresponder). No person engaged; the lead has not replied in any sense that stops outreach."),
    stopRequested: z
      .boolean()
      .describe("They asked us to stop writing to them. A legal obligation, not a judgement about the offer."),
    notOurTarget: z
      .boolean()
      .describe("They are not who we sell to: the wrong contact, or they left the role. A fact about the person; a plain 'not interested' is NOT this (that lead stays recyclable)."),
    handedToPerson: z
      .boolean()
      .describe("The reply hands the lead to a person rather than to the automated responder: a referral ('not me, write to X') or a reply about something other than the offer. Both classify `neutral` like a plain neutral reply; this is what separates them."),
    positiveSignal: z
      .enum(["interest", "information_request", "meeting_request"])
      .nullable()
      .describe("Which buying signal a positive reply carries: plain interest, a request for information (a question about the offer), or a request for a meeting. Null for every non-positive verdict."),
    declinedOffer: z
      .boolean()
      .describe("They said no to the offer for now. Recyclable; NOT a stop request (`stopRequested`) and NOT `notOurTarget`."),
    notOurTargetReason: z
      .enum(["wrong_contact", "left_role", "already_customer", "is_the_client"])
      .nullable()
      .describe("Why `notOurTarget`: the wrong contact (hands nobody back), they left the role or company, they already buy from the client, or they ARE the client. Null when `notOurTarget` is false."),
    handoffReason: z
      .enum(["referral", "unrelated_proposal"])
      .nullable()
      .describe("Why `handedToPerson`: they pointed us at someone else (`referral`), or the conversation is about something other than buying the offer (`unrelated_proposal`; its subtype is `judgments.proposalType`). Null when `handedToPerson` is false."),
  })
  .openapi("ReplyVerdict");

const ReplyJudgmentSchema = z.object({
  value: z.string(),
  confidence: z.number().nullable().describe("0..1, how concentrated the judgment engine's distribution was"),
});

const ReplyJudgmentsSchema = z
  .object({
    proposalType: ReplyJudgmentSchema.extend({ value: z.enum(["partnership", "hiring", "investment", "other"]) })
      .nullable()
      .describe("For an `unrelated_proposal` reply: what it proposes (partnership / reseller / affiliate; a job or recruiting; money in the company: investing, fundraising, acquisition; anything else, e.g. a vendor pitching us, press). Null otherwise or until judged."),
    question: ReplyJudgmentSchema.extend({ value: z.enum(["none", "answerable", "needs_sender_company"]) })
      .nullable()
      .describe("For a reply a PERSON wrote: `none` = asks no question; `answerable` = a question the outreach team can answer right away from the offer itself; `needs_sender_company` = only the company we write for can answer it, so it must be escalated to them. Null for an automated answer, a reply with no stored words, or until judged."),
  })
  .describe("Judgments about the reply's words (chat-service judgment engine), each judged ONCE per reply and kept. The verdict's kind stays the classifier's; these answer finer questions the kind leaves open.");

const ReplyEscalationSchema = z
  .object({
    escalatedAt: z.string().describe("ISO 8601 UTC"),
    handedTo: z.string().nullable().describe("Who now owns the conversation (the brand's rep, the agency inbox, or `reassured`)"),
  })
  .nullable()
  .describe("The thread was actually handed to a person (the responder's escalation) after THIS reply and before any later one. Null when it never was.");

const ReplyViewSchema = z.object({
        replyId: z.string().describe("Stable per-reply id (`ie:<instantly email id>`, `imap:<row id>`, `manual:<qualification row id>` for a reply a person recorded by hand, or `ievt:<event id>` for a reply Instantly qualified but whose message was never mirrored; the last two carry null `fromEmail` / `subject`)"),
        leadEmail: z.string(),
        instantlyCampaignId: z.string().describe("The per-lead thread"),
        campaignId: z.string().nullable().describe("The logical campaign (campaign-service id)"),
        brandIds: z.array(z.string()),
        transport: z.string().describe("`instantly` | `smtp` | ... ; `manual` = a reply a person recorded by hand (no message mirrored, so `fromEmail` and `subject` are null)"),
        fromEmail: z.string().nullable().describe("Who actually wrote it (can differ from the lead: an assistant, a shared inbox)"),
        subject: z.string().nullable(),
        receivedAt: z.string().describe("ISO 8601 UTC"),
        verdict: ReplyVerdictSchema.nullable().describe("The reply's CURRENT verdict; null only while it is still being classified"),
        verdictCount: z.number().int().describe("How many verdicts were ever produced for this reply (bronze history)"),
        judgments: ReplyJudgmentsSchema,
        escalation: ReplyEscalationSchema,
      });

export const ReplyVerdictsResponseSchema = z
  .object({ replies: z.array(ReplyViewSchema) })
  .openapi("ReplyVerdictsResponse");

registry.registerPath({
  method: "post",
  path: "/orgs/reply-verdicts/query",
  summary: "Per-reply verdicts for a set of leads",
  description:
    "Every real inbound reply this org holds for the given leads, each with its CURRENT verdict — oldest first per lead. A read (POST only so up to 1000 addresses fit in a body); it sends nothing and declares nothing.\n\n" +
    "**Why per reply.** The per-(campaign × lead) `replyKind` keeps only the latest verdict, so an older verdict could hide a newer reply (an out-of-office on 09-24 masked a referral on 09-28). Here each reply carries its own verdict, so a consumer can roll up lead × offer (group by `campaignId`) or lead × brand (`brandIds`) itself.\n\n" +
    "**Current verdict:** a person's statement beats Instantly's and the model's; then the most recent. A withdrawn statement no longer counts. Real replies only: our own staff, our own sending mailboxes and mail servers are not replies.\n\n" +
    "**Freshness:** a projection refreshed every 2 minutes.",
  request: {
    headers: TrackingHeadersSchema,
    body: { content: { "application/json": { schema: ReplyVerdictsQuerySchema } } },
  },
  responses: {
    200: {
      description: "The replies (possibly empty)",
      content: { "application/json": { schema: ReplyVerdictsResponseSchema } },
    },
    400: { description: "Invalid body", content: { "application/json": { schema: ErrorSchema } } },
    401: { description: "Unauthorized" },
    500: { description: "Could not be read", content: { "application/json": { schema: ErrorSchema } } },
  },
});

export const OutreachFactsQuerySchema = z.object({
  since: z
    .string()
    .regex(/^\d+$/, "since must be a cursor returned by this route (digits)")
    .optional()
    .describe("Cursor: return facts AFTER this `seq` (exclusive). Omit to start from the beginning."),
  limit: z.coerce.number().int().min(1).max(1000).optional().describe("Page size, default 500, max 1000"),
  orgId: z.string().optional().describe("Only facts of this org (internal org UUID)"),
  brandId: z.string().optional().describe("Only facts on campaigns of this brand"),
  email: z.string().optional().describe("Only facts about this person (case-insensitive)"),
});

export const OutreachFactsSyncSchema = z
  .object({
    judgeLimit: z.number().int().min(0).max(100000).optional().describe("Replies to judge before emitting (default 100)"),
    sinceDays: z.number().int().min(1).nullable().optional().describe("Event scan window by insertion time; null/omitted = whole history"),
  })
  .openapi("OutreachFactsSync");

const OutreachFactSchema = z
  .object({
    seq: z.string().describe("The fact's position in the feed (digits). Pass the last one as `since` to resume."),
    type: z
      .enum(["email_sent", "email_opened", "link_clicked", "email_bounced", "unsubscribed", "reply", "reply_sent", "withdrawn"])
      .describe("What happened. Exactly one of the per-type objects below is non-null (the one matching `type`)."),
    subjectKey: z
      .string()
      .describe("What the fact is about: `ievt:<event id>` for an email event, `reply:<replyId>` for a reply, `rsent:<id>` for a reply we sent. Every fact about the same thing shares it."),
    supersedesSeq: z
      .string()
      .nullable()
      .describe("A CORRECTION: this fact replaces the fact with that `seq` (a reply whose verdict, judgments or escalation changed), or withdraws it (`type: withdrawn`). Null for a first statement. Facts are never edited."),
    occurredAt: z.string().describe("ISO 8601 UTC, when it happened (for a withdrawal: when we learned it)"),
    recordedAt: z.string().describe("ISO 8601 UTC, when the fact entered the feed"),
    leadEmail: z.string().describe("The person (stored casing; match case-insensitively)"),
    orgId: z.string().nullable().describe("Internal org UUID; null for a platform send or a thread whose campaign row was lost"),
    campaignId: z.string().nullable().describe("The logical campaign (campaign-service id)"),
    instantlyCampaignId: z.string().describe("The per-lead sequence (thread)"),
    brandIds: z.array(z.string()),
    transport: z.string().nullable().describe("`instantly` | `smtp` (our own sender) | `manual`"),
    send: z
      .object({
        step: z.number().int().nullable().describe("1-based step of the sequence; null when the provider reported the send without one"),
        position: z.enum(["first", "followup"]).describe("The first email of the sequence, or a follow-up"),
        positionBasis: z.enum(["step", "order"]).describe("`step` = from the step number; `order` = no step was reported, placed by its order among this thread's sends"),
        accountEmail: z.string().nullable().describe("The mailbox that sent it"),
        localSentAt: z
          .string()
          .nullable()
          .describe("When it went out on the PROSPECT's wall clock, `YYYY-MM-DDTHH:mm:ss` (no offset), in `sequence.timezone`. Null when that zone is unknown or not a zone we can read."),
        localWeekday: z
          .string()
          .nullable()
          .describe("The day of the week it went out in the prospect's timezone (`Monday`..`Sunday`); null exactly when `localSentAt` is."),
      })
      .nullable(),
    open: z.object({ step: z.number().int().nullable() }).nullable(),
    click: z
      .object({
        step: z.number().int().nullable(),
        url: z.string().nullable().describe("The URL clicked, as our own click tracker recorded it. Null when the provider's tracker did not tell us (Instantly-sent mail)."),
      })
      .nullable(),
    bounce: z.object({ step: z.number().int().nullable() }).nullable(),
    unsubscribe: z
      .object({
        step: z.number().int().nullable(),
        via: z.enum(["link", "recorded"]).describe("`link` = they used the unsubscribe link/header; `recorded` = an opt-out recorded for them (stated to staff, or asked for in a reply)"),
      })
      .nullable(),
    reply: ReplyViewSchema.nullable().describe("A real reply with its CURRENT verdict, distinctions, judgments and escalation (same shape as one item of POST /orgs/reply-verdicts/query)."),
    replySent: z
      .object({
        via: z
          .enum(["replies_route", "staff_client", "provider_unibox"])
          .describe("`replies_route` = sent through POST /orgs/replies (a human's or the automation's answer, either transport); `staff_client` = one of our people answered from their own mail client and copied the mailbox; `provider_unibox` = typed into Instantly's Unibox"),
        sentBy: z.enum(["human", "automation"]).nullable().describe("Who wrote it; null when the record does not say"),
        accountEmail: z.string().nullable().describe("The mailbox (or person) that sent it"),
        subject: z.string().nullable(),
        attachments: z
          .array(SentAttachmentSchema)
          .optional()
          .describe("Offer documents the answer carried as files. Absent when none."),
      })
      .nullable()
      .describe("An email WE sent into the prospect's thread outside the sequence (an answer, a follow-up to their reply). Its `occurredAt` is when it went out. Stated once, never corrected."),
    withdrawal: z
      .object({
        withdrawnSeq: z.string().describe("The fact withdrawn (also in `supersedesSeq`)"),
        withdrawnType: z.string().describe("Its type"),
        reason: z
          .enum(["source_removed", "statement_withdrawn"])
          .describe("`source_removed` = it turned out not to have happened (a click proven to be a link scanner, a bounce retracted as a delayed notice, a reply placeholder replaced by the real message); `statement_withdrawn` = staff withdrew a recorded opt-out"),
      })
      .nullable(),
    sequence: z
      .object({
        timezone: z
          .string()
          .nullable()
          .describe("The timezone the prospect is read in, as stored (IANA name). Null = this service holds none for the thread (the lead's own record lives in lead-service)."),
        timezoneBasis: z
          .enum(["sequence", "schedule"])
          .nullable()
          .describe("Where `timezone` comes from: `sequence` = the zone the sequence was scheduled in (the lead's zone as the sender was given it; first choice); `schedule` = the zone of the provider campaign's stored sending schedule (older Instantly threads); null when neither is known."),
        openTracking: z
          .enum(["on", "off", "unplaceable"])
          .describe("Whether this thread's emails carried the open-tracking pixel. `off` = a thread our own sender dispatched (it never carries one) or an Instantly campaign created with open tracking off; `on` = an Instantly campaign whose first stored settings had it on; `unplaceable` = an Instantly thread whose settings were never stored, so it cannot be placed in either arm."),
      })
      .describe("What the thread (`instantlyCampaignId`) says, on EVERY fact of it, read live at each page (also on facts stated before this field existed). Stable for a thread: a consumer may keep it once per `instantlyCampaignId`."),
  })
  .openapi("OutreachFact");

export const OutreachFactsResponseSchema = z
  .object({
    facts: z.array(OutreachFactSchema),
    nextCursor: z.string().describe("Pass as `since` for the next page (unchanged when the page is empty)"),
    hasMore: z.boolean(),
  })
  .openapi("OutreachFactsPage");

registry.registerPath({
  method: "get",
  path: "/internal/outreach-facts",
  summary: "Outreach fact feed (fleet-wide, cursor-paginated)",
  description:
    "Every dated thing our cold-email outreach did or saw for a person, in ONE total order, for a consumer to COPY into its own store and then follow incrementally: each email we sent (first or follow-up), each open, click (with the URL when we recorded it), bounce and unsubscribe, and each real reply with its verdict and finer distinctions, and each email we sent into the thread outside the sequence (`reply_sent`: our answers, manual or automated). Read-only, DB-only, no cost.\n\n" +
    "**Append-only.** A fact is never edited. A reply whose verdict, judgments or escalation changes gets a NEW `reply` fact naming the one it supersedes; something that turned out not to have happened gets a `withdrawn` fact. A consumer keeps, per `subjectKey`, the latest fact.\n\n" +
    "**Paging.** Start with no `since`; pass `nextCursor` back until `hasMore` is false; then poll with the last cursor. A cursor never skips a fact (emission is serialized).\n\n" +
    "**Coverage.** From the first stored event (2026-02-10). Real events only (no inferred projections). Opens on our own sender are not observed (no pixel). Spam complaints are not captured. Freshness: emitted every 2 minutes. A reply is emitted the tick it lands, with its verdict and judgments when already known, else null; they arrive as a correction. A reply typed into Instantly's Unibox (`provider_unibox`) is stated once our mirror of the thread picks it up.\n\n" +
    "**Thread context.** Every fact carries `sequence`: the prospect's timezone (and on each send, its local wall-clock time and weekday) and whether the thread's emails carried the open pixel.",
  request: { query: OutreachFactsQuerySchema },
  responses: {
    200: { description: "A page of facts", content: { "application/json": { schema: OutreachFactsResponseSchema } } },
    400: { description: "Invalid query", content: { "application/json": { schema: ErrorSchema } } },
    401: { description: "Unauthorized" },
    500: { description: "Could not be read", content: { "application/json": { schema: ErrorSchema } } },
  },
});

registry.registerPath({
  method: "post",
  path: "/internal/outreach-facts/sync",
  summary: "Run one outreach-fact emission pass by hand",
  description:
    "Judges up to `judgeLimit` replies still owed a judgment, then appends every new fact. The in-process worker does the same every 2 minutes; this is for ops. Idempotent.",
  request: { body: { content: { "application/json": { schema: OutreachFactsSyncSchema } } } },
  responses: {
    200: { description: "What the pass did" },
    400: { description: "Invalid body", content: { "application/json": { schema: ErrorSchema } } },
    401: { description: "Unauthorized" },
    500: { description: "The pass failed", content: { "application/json": { schema: ErrorSchema } } },
  },
});

const DeliveryFiguresShape = {
  sent: z.number().int().describe("Sequences whose first real email left (one address emailed under one campaign)"),
  delivered: z.number().int().describe("Of `sent`, sequences with no bounce on any step (sent AND NOT bounced, the gold `delivered` flag)"),
  bounced: z.number().int().describe("Of `sent`, sequences where any step bounced. `delivered + bounced = sent`"),
  deliveryRate: z.number().nullable().describe("delivered / sent, 0..1, 4 decimals. Null when `sent` is 0 (no rate over zero)"),
  bounceRate: z.number().nullable().describe("bounced / sent, 0..1, 4 decimals. Null when `sent` is 0"),
};

const DeliveryByVerdictResponseSchema = z
  .object({
    basis: z.literal("sequences").describe("Unit counted: one SEQUENCE = one address emailed under one campaign, counted once its first real email left (Instantly and our own sender alike). Same population and flags as every gold stats read"),
    since: z.string().nullable().describe("ISO: first real email of the earliest counted sequence (inception)"),
    sentThrough: z.string().describe("ISO: sequences first sent after this instant are not counted yet (the mirror of the verifier's history is complete up to here)"),
    verificationsSince: z.string().nullable().describe("ISO: earliest verification on record. Sequences sent before it can only be `never_verified`"),
    buckets: z
      .array(
        z.object({
          verdict: z
            .enum(["valid", "catch_all", "unknown", "invalid", "risky", "no_verdict", "never_verified"])
            .describe("Latest verdict our verifier (BounceVerify via apollo-service, any org, any source) returned for the address AT OR BEFORE the sequence's first email. `no_verdict` = only calls that returned no verdict before the send. `never_verified` = no verification before the send"),
          ...DeliveryFiguresShape,
        }),
      )
      .describe("All 7 buckets, always present (zero when empty), in this order. They partition the counted sequences"),
    bucketsByLatestVerdict: z
      .array(z.object({ verdict: z.enum(["valid", "catch_all", "unknown", "invalid", "risky", "no_verdict", "never_verified"]), ...DeliveryFiguresShape }))
      .describe("The SAME sequences bucketed by the LATEST verdict on record today, INCLUDING verifications made after the send (most addresses were verified only after we emailed them). Answers 'how do addresses our verifier calls X fare', NOT 'what did we know when sending'. Same 7 buckets, same total"),
    total: z.object(DeliveryFiguresShape).describe("Sum of the buckets; checked against an independent count of the same population (the read fails 500 if they differ)"),
    sentAfterThrough: z.number().int().describe("Sequences first sent after `sentThrough`: counted once the mirror catches up"),
  })
  .openapi("DeliveryByVerdictResponse");

registry.registerPath({
  method: "get",
  path: "/internal/delivery-by-verdict",
  summary: "Fleet delivery split by the verifier verdict the address had at send",
  description:
    "Platform-scoped (every org), since the first email we sent. For each verdict our verifier (BounceVerify, through apollo-service) held on the address when the sequence's first email left: how many sequences were sent, delivered (no bounce) and bounced, and the rates. Verdicts are read from this service's mirror of apollo-service's verification history (copied every few minutes); a verdict is never invented. Cached 10 min per replica (refresh-ahead at 5). 503 `verification_history_not_synced` until the mirror has reached the end of the history once.",
  responses: {
    200: { description: "The split", content: { "application/json": { schema: DeliveryByVerdictResponseSchema } } },
    401: { description: "Unauthorized" },
    500: { description: "Could not be read, or buckets do not reconcile with the total", content: { "application/json": { schema: ErrorSchema } } },
    503: { description: "Verification history not mirrored yet", content: { "application/json": { schema: ErrorSchema } } },
  },
});

const BlocklistKeySchema = z.enum(["surbl", "spamhaus_dbl", "uribl"]);

const DomainBlocklistReadSchema = z
  .object({
    asOf: z.string().describe("ISO instant the read was computed"),
    monitoredSince: z.string().nullable().describe("ISO instant of the first honest answer of any list. Listing series are null before it (nobody looked; not a clean day)"),
    lists: z
      .array(
        z.object({
          key: BlocklistKeySchema,
          name: z.string().describe("SURBL, Spamhaus DBL, URIBL"),
          zone: z.string(),
          removalUrl: z.string().describe("Where a person starts a removal request"),
          removalAutomated: z.boolean().describe("True = we file the removal form ourselves, once a week per listed domain (SURBL); false = a person files it from removalUrl (Spamhaus, URIBL)"),
          removalNote: z.string(),
        }),
      )
      .describe("The lists checked daily, in display order"),
    domains: z
      .array(
        z.object({
          domain: z.string(),
          mailboxes: z.array(z.string()).describe("Sending addresses on the domain (present accounts)"),
          listed: z.boolean().describe("True when ANY list lists it right now"),
          outOfNewSequences: z.boolean().describe("Always false since SURBL-FILL-LAST-1021: a listing no longer takes a mailbox out of new sequences (it fills last within its tier)"),
          lists: z.array(
            z.object({
              list: BlocklistKeySchema,
              state: z.enum(["listed", "clean", "unknown"]).describe("`unknown` = the list never answered us honestly for this domain"),
              answer: z.string().nullable().describe("The A record the list answered, e.g. 127.0.0.64"),
              labels: z.array(z.string()).describe("The list's own sub-list names, e.g. ABUSE"),
              listedSince: z.string().nullable().describe("ISO: when our check first saw the current listing"),
              lastCheckedAt: z.string().nullable(),
              lastAnsweredAt: z.string().nullable().describe("ISO: last run where the list answered honestly. The state is as of this instant"),
              lastOutcome: z.string().nullable().describe("listed | clean | unanswered (refused / canary failed: state kept)"),
              lastError: z.string().nullable(),
            }),
          ),
        }),
      )
      .describe("Every sending domain, alphabetical"),
    events: z
      .array(
        z.object({
          id: z.string(),
          domain: z.string(),
          list: z.string(),
          type: z.enum(["listed", "delisted", "removal_link_issued", "removal_requested", "removal_failed"]),
          answer: z.string().nullable(),
          labels: z.array(z.string()),
          mailboxes: z.array(z.string()).describe("Sending addresses on the domain at the event"),
          detail: z.unknown().describe("listed: {zone, firstBlocklistBounceAt, actionTaken}; removal_link_issued: {method, url, note}; removal_requested / removal_failed (automated): {method:'automated', attemptId, outcome, httpStatus, error, answer}"),
          occurredAt: z.string(),
          alert: z
            .object({ status: z.string(), attempts: z.number().int(), lastError: z.string().nullable(), alertedAt: z.string().nullable() })
            .nullable()
            .describe("The ONE staff email of a `listed` event (pending until delivered); null for other types"),
        }),
      )
      .describe("Append-only history, newest first"),
    series: z
      .object({
        days: z.array(z.string()).describe("UTC days of the window, oldest first"),
        domainsListed: z.array(z.object({ day: z.string(), total: z.number().int().nullable(), byList: z.record(z.string(), z.number().int()).nullable() })).describe("Domains listed at any moment of the day, total (distinct domains) and per list"),
        mailboxesOutOfNewSequences: z.array(z.object({ day: z.string(), mailboxes: z.number().int().nullable() })).describe("Mailboxes on a listed domain that day (out of new sequences)"),
        sendsFromListedDomains: z.array(z.object({ day: z.string(), sends: z.number().int().nullable() })).describe("Real emails sent that day from a domain listed that day (follow-ups keep going: expect non-zero)"),
        blocklistBounces: z.array(z.object({ day: z.string(), total: z.number().int(), byList: z.object({ surbl: z.number().int(), spamhaus: z.number().int(), uribl: z.number().int(), other_list: z.number().int() }) })).describe("Bounces (DSNs read in our mailboxes) whose text cites a blocklist, by the list cited. Backfilled from the first DSN we hold"),
        removalRequests: z.array(z.object({ day: z.string(), linksIssued: z.number().int(), requested: z.number().int(), failed: z.number().int() })).describe("Per day: removal links issued to staff (one per new listing), removal requests SENT to a list (automated SURBL form, or recorded by a person), and automated attempts that FAILED (form refused it, network, owner record unreadable; retried the next day)"),
        delistings: z.array(z.object({ day: z.string(), delistings: z.number().int() })),
      })
      .describe("One point per day of the window, zero included; null = before monitoring started"),
  })
  .openapi("DomainBlocklistRead");

registry.registerPath({
  method: "get",
  path: "/internal/domain-blocklist",
  summary: "Sending domains on public blocklists: current state, history, daily series (staff)",
  description:
    "Platform-scoped, read-only. Every sending domain is checked once a day on SURBL multi, Spamhaus DBL and URIBL (each behind its published test name, so a refused query is never read as a listing or as clean). A listed domain stays in production when its inbox test passes and fills AFTER the clean mailboxes of its price tier (SURBL-FILL-LAST-1021). Cached 5 min per replica.",
  request: { query: z.object({ days: z.string().optional().describe("Window in days, 1..365, default 60") }) },
  responses: {
    200: { description: "The read", content: { "application/json": { schema: DomainBlocklistReadSchema } } },
    400: { description: "Bad `days`", content: { "application/json": { schema: ErrorSchema } } },
    401: { description: "Unauthorized" },
    500: { description: "Could not be read", content: { "application/json": { schema: ErrorSchema } } },
  },
});

// ─── Microsoft seed placement (MS-SEED-PLACEMENT-1018) ──────────────────────

const MicrosoftAuthorizationStartSchema = z
  .object({
    email: z.string().describe("The Microsoft mailbox being authorized (lowercased)"),
    userCode: z.string().describe("Code the person types at verificationUri"),
    verificationUri: z.string().describe("Where the person signs in (https://microsoft.com/devicelogin)"),
    expiresAt: z.string().describe("ISO8601: the code stops working at this time (~15 min)"),
    message: z.string().describe("Microsoft's own instruction sentence"),
    instructions: z.string().describe("The steps, in one sentence"),
  })
  .openapi("MicrosoftAuthorizationStart");

registry.registerPath({
  method: "post",
  path: "/internal/seed-placement/microsoft/authorize",
  summary: "Start authorizing a Microsoft seed mailbox (OAuth device code) (staff)",
  description:
    "Platform-scoped. Microsoft refuses password IMAP, so a Microsoft seed receiver (Outlook.com or Microsoft 365) is read through Microsoft Graph with a delegated token. This call asks Microsoft for a device code and answers it; a person opens verificationUri, types userCode and signs in AS that mailbox. The service polls in the background, refuses a sign-in by any other account, and stores the refresh token (rotated on every use, refreshed daily). Calling it again for an authorized mailbox re-authorizes it without losing the current token until the new one lands.",
  request: {
    body: { content: { "application/json": { schema: z.object({ email: z.string().describe("The Microsoft mailbox address") }) } } },
  },
  responses: {
    200: { description: "Code to type", content: { "application/json": { schema: MicrosoftAuthorizationStartSchema } } },
    400: { description: "No email", content: { "application/json": { schema: ErrorSchema } } },
    401: { description: "Unauthorized" },
    502: { description: "Microsoft refused the device code request", content: { "application/json": { schema: ErrorSchema } } },
  },
});

registry.registerPath({
  method: "get",
  path: "/internal/seed-placement/microsoft",
  summary: "Microsoft seed mailboxes and their authorization state (staff)",
  description: "Platform-scoped, read-only. Never returns a token.",
  responses: {
    200: {
      description: "Mailboxes",
      content: {
        "application/json": {
          schema: z.object({
            mailboxes: z.array(
              z.object({
                email: z.string(),
                status: z.enum(["pending", "authorized", "failed", "expired"]),
                accountKind: z.string().nullable().describe("consumer (Outlook.com) | business (Microsoft 365)"),
                pendingCode: z
                  .object({ userCode: z.string().nullable(), verificationUri: z.string().nullable(), expiresAt: z.string() })
                  .nullable(),
                authorizedAt: z.string().nullable(),
                lastRefreshedAt: z.string().nullable(),
                lastError: z.string().nullable(),
              }),
            ),
          }),
        },
      },
    },
    401: { description: "Unauthorized" },
  },
});

const SeedPlacementCountsSchema = z.object({
  seeds: z.number().int().describe("Seeds sent that day (successful dispatches)"),
  inbox: z.number().int(),
  spam: z.number().int(),
  missing: z.number().int().describe("Sent and never found in Inbox nor spam"),
  inboxPct: z.number().nullable().describe("inbox / seeds, 0..100; null on a day with no seed"),
  spamPct: z.number().nullable(),
});
const SeedPlacementMsCountsSchema = SeedPlacementCountsSchema.extend({
  inboxFocused: z.number().int().describe("Inbox seeds Microsoft sorted to Focused"),
  inboxOther: z.number().int().describe("Inbox seeds Microsoft sorted to Other"),
});

registry.registerPath({
  method: "get",
  path: "/internal/seed-placement/daily",
  summary: "Seed inbox placement per day, Google and Microsoft (staff chart)",
  description:
    "Platform-scoped, read-only. The stored daily series (`seed_placement_daily`, recomputed from bronze on every seed sync): per UTC day the seeds left, Google inbox %, Microsoft 365 inbox % and Outlook.com inbox % (apart: they filter apart), and the same split per SENDING vendor (gandi = the legacy shared-IP fleet, primeforge = the Google Workspace fleet that sends most prospect mail). Every day of the window is emitted; a day with no test has 0 seeds and null percentages.",
  request: { query: z.object({ days: z.string().optional().describe("Window in days, 1..400, default 90") }) },
  responses: {
    200: {
      description: "Series",
      content: {
        "application/json": {
          schema: z.object({
            asOf: z.string(),
            from: z.string(),
            to: z.string(),
            days: z.array(
              z.object({
                day: z.string().describe("YYYY-MM-DD (UTC)"),
                google: SeedPlacementCountsSchema,
                microsoft: SeedPlacementMsCountsSchema.describe("A Microsoft 365 business tenant receiver"),
                microsoftConsumer: SeedPlacementMsCountsSchema.describe(
                  "An Outlook.com consumer receiver, measured apart from Microsoft 365",
                ),
                byVendor: z.array(
                  z.object({
                    vendor: z.string(),
                    google: SeedPlacementCountsSchema,
                    microsoft: SeedPlacementMsCountsSchema,
                    microsoftConsumer: SeedPlacementMsCountsSchema,
                  }),
                ),
              }),
            ),
          }),
        },
      },
    },
    400: { description: "Bad `days`", content: { "application/json": { schema: ErrorSchema } } },
    401: { description: "Unauthorized" },
  },
});

registry.registerPath({
  method: "post",
  path: "/internal/seed-placement/daily/refresh",
  summary: "Recompute the seed placement daily series from bronze now (staff)",
  description: "Platform-scoped. Every seed sync and every boot does the same.",
  responses: {
    200: { description: "Rows written", content: { "application/json": { schema: z.object({ rows: z.number().int() }) } } },
    401: { description: "Unauthorized" },
  },
});

// ─── Sender health (SENDER-HEALTH-1019) ─────────────────────────────────────

const ProviderCountsSchema = z.object({
  google: z.number().int(),
  microsoft: z.number().int().describe("Microsoft 365 business tenants"),
  microsoft_consumer: z.number().int().describe("Outlook.com / Hotmail / Live"),
});
const ProviderListsSchema = z.object({
  google: z.array(z.string()),
  microsoft: z.array(z.string()),
  microsoft_consumer: z.array(z.string()),
});

registry.registerPath({
  method: "get",
  path: "/internal/sender-health",
  summary: "Mailboxes kept out of prospect mail, and why (staff chart)",
  description:
    "Platform-scoped, read-only. `current` = the latest dispatch tick's lists: `refused` = real mailboxes the relay refuses with no accepted send since (prospect, seed and warmup mail: they send nothing); `mostlySpam[provider]` = sending addresses with more than half of at least 10 found seeds in spam at that provider (no email, follow-ups included, to that provider's prospects). `days` = the stored daily series (`sender_health_daily`, written every dispatch tick): mailbox counts at day end, held emails = the day's max per tick. A day with no tick is null, never 0.",
  request: { query: z.object({ days: z.string().optional().describe("Window in days, 1..400, default 90") }) },
  responses: {
    200: {
      description: "State and series",
      content: {
        "application/json": {
          schema: z.object({
            asOf: z.string(),
            from: z.string(),
            to: z.string(),
            current: z
              .object({
                day: z.string(),
                updatedAt: z.string(),
                refused: z.array(z.string()),
                mostlySpam: ProviderListsSchema,
              })
              .nullable(),
            days: z.array(
              z.object({
                day: z.string().describe("YYYY-MM-DD (UTC)"),
                refusedMailboxes: z.number().int().nullable(),
                mostlySpamMailboxes: ProviderCountsSchema.nullable(),
                heldPlacement: z.number().int().nullable().describe("First emails held for placement (rehomed or waiting)"),
                heldMostlySpam: z.number().int().nullable().describe("Follow-ups held: mailbox mostly spam at the prospect's provider"),
                skippedRefused: z.number().int().nullable().describe("Steps skipped on a refused or blind mailbox"),
              }),
            ),
          }),
        },
      },
    },
    400: { description: "Bad `days`", content: { "application/json": { schema: ErrorSchema } } },
    401: { description: "Unauthorized" },
  },
});

// ─── Lead source yield (SOURCE-YIELD-1016) ──────────────────────────────────

const LeadSourceYieldMeasuresSchema = z.object({
  firstEmails: z.number().int().describe("Sequences whose FIRST email left (step 1, every transport)"),
  hardBounces: z.number().int().describe("Of those, sequences with a permanent bounce on any step"),
  hardBounceRate: z.number().nullable().describe("hardBounces / firstEmails, 0..1; null when no first email"),
  refusals: z.number().int().describe("Sequences a receiving server refused AFTER accepting (a delivery-failure notice); our own sender only, from refusalsMeasuredSince"),
  refusalRate: z.number().nullable().describe("refusals / firstEmails, 0..1"),
  blocklistRefusals: z.number().int().describe("Refusals whose notice cites a blocklist (SURBL, Spamhaus, URIBL, any RBL)"),
  firstEmailsFromListedDomains: z.number().int().describe("First emails sent from a domain listed on any blocklist TODAY"),
  listedDomainShare: z.number().nullable().describe("firstEmailsFromListedDomains / firstEmails, 0..1"),
});

const LeadSourceYieldPointSchema = z.object({
  day: z.string().describe("UTC day of the FIRST email (cohort)"),
  total: z.number().int().describe("Every sequence once (not the sum of byKey)"),
  byKey: z.record(z.string(), z.number().int()).describe("Per key of `series.keys`, zero included"),
});

const LeadSourceYieldReadSchema = z
  .object({
    asOf: z.string(),
    countingRule: z.literal("per_tag"),
    countingRuleNote: z.string(),
    pseudoKeys: z.object({ noSourceRecorded: z.string(), unknown: z.string() }).describe("The two keys that are not sources: lead-service named none / we hold no answer"),
    provenanceSyncedAt: z.string().nullable(),
    listedDomainsToday: z.array(z.string()),
    refusalsMeasuredSince: z.string().nullable(),
    total: LeadSourceYieldMeasuresSchema.describe("Every first email once"),
    coverage: z.object({
      firstEmails: z.number().int(),
      withSource: z.number().int(),
      noSourceRecorded: z.number().int(),
      unknown: z.number().int(),
    }),
    sources: z
      .array(LeadSourceYieldMeasuresSchema.extend({ key: z.string(), kind: z.enum(["source", "no_source_recorded", "unknown"]) }))
      .describe("Since inception, ranked by hard-bounce rate (desc) then first emails; the two pseudo keys last"),
    series: z
      .object({
        days: z.array(z.string()),
        keys: z.array(z.string()).describe("Same order as `sources`"),
        firstEmails: z.array(LeadSourceYieldPointSchema),
        hardBounces: z.array(LeadSourceYieldPointSchema),
        refusals: z.array(LeadSourceYieldPointSchema),
        blocklistRefusals: z.array(LeadSourceYieldPointSchema),
        firstEmailsFromListedDomains: z.array(LeadSourceYieldPointSchema),
      })
      .describe("One point per day of the window, zero included; one chart per measure"),
  })
  .openapi("LeadSourceYieldRead");

registry.registerPath({
  method: "get",
  path: "/internal/lead-source-yield",
  summary: "Lead sources ranked by the bounces, refusals and blocklist hits their people produce (staff)",
  description:
    "Platform-scoped, read-only. Every sequence whose first email left is joined, on the lowercased address and (org, brand), to every source tag lead-service holds for that person (our copy, refreshed every 30 min; never a live call). Counting rule `per_tag`: a person found by several sources counts once under EACH tag. `(no source recorded)` = lead-service named none; `(unknown)` = no answer held yet (never fetched, lead-service unavailable, or no org/brand). Cached 10 min per replica, refresh-ahead at 5.",
  request: { query: z.object({ days: z.string().optional().describe("Series window in days, 1..365, default 60 (the ranking is since inception)") }) },
  responses: {
    200: { description: "The read", content: { "application/json": { schema: LeadSourceYieldReadSchema } } },
    400: { description: "Bad `days`", content: { "application/json": { schema: ErrorSchema } } },
    401: { description: "Unauthorized" },
    500: { description: "Could not be read", content: { "application/json": { schema: ErrorSchema } } },
  },
});

registry.registerPath({
  method: "post",
  path: "/internal/lead-source-yield/sync",
  summary: "Copy lead-service's provenance for the people we emailed, now",
  description: "Asks lead-service for every (org, brand, address) with a first email and no answer stored, plus recent ones due a daily re-check, in batches of 1000. A 502 or failed batch stores nothing (those people stay unknown). The worker runs it every 30 min.",
  responses: {
    200: { description: "`missing`, `stale`, `batches`, `asked`, `inserted`, `confirmed`, `unavailableBatches`, `failedBatches`, `errors`" },
    401: { description: "Unauthorized" },
    500: { description: "The pass failed", content: { "application/json": { schema: ErrorSchema } } },
  },
});

registry.registerPath({
  method: "post",
  path: "/internal/domain-blocklist/check",
  summary: "Run the daily blocklist check now",
  description: "Checks every sending domain on every list, writes state + events, reconciles the lifecycle on a change. Emails nobody (the admin chart is the surface). The in-process worker runs it daily. Idempotent: a listing already recorded writes nothing.",
  responses: {
    200: { description: "What the check found: `domains`, per-list `answered/listed/clean/unanswered`, `listed`, `delisted`, `lifecycle`" },
    401: { description: "Unauthorized" },
    500: { description: "The check failed", content: { "application/json": { schema: ErrorSchema } } },
  },
});

registry.registerPath({
  method: "post",
  path: "/internal/domain-blocklist/removals",
  summary: "File every due automated removal request now (SURBL)",
  description: "For every domain SURBL lists, sends SURBL's removal form unless one reached the form within 7 days, with the owner's details read from kevinlourd.com, a real email of ours and our anti-spam terms. Every attempt is stored (bronze `domain_blocklist_removal_attempts`, event `removal_requested` or `removal_failed`). The daily worker does the same after its check.",
  responses: {
    200: { description: "`{due, sent, refused, notListed, failed, skippedCoolingDown}`" },
    401: { description: "Unauthorized" },
    500: { description: "Could not run", content: { "application/json": { schema: ErrorSchema } } },
  },
});

registry.registerPath({
  method: "post",
  path: "/internal/domain-blocklist/removal-requests",
  summary: "Record a removal request a person filed on a list's form",
  description: "Append-only `removal_requested` event, for a list we do not file ourselves (Spamhaus, URIBL). SURBL is filed automatically once a week per listed domain.",
  request: {
    body: {
      content: {
        "application/json": {
          schema: z.object({ domain: z.string(), list: BlocklistKeySchema, note: z.string().optional(), requestedBy: z.string().optional() }),
        },
      },
    },
  },
  responses: {
    201: { description: "Recorded: `{id}`" },
    400: { description: "Missing domain or unknown list", content: { "application/json": { schema: ErrorSchema } } },
    401: { description: "Unauthorized" },
  },
});

registry.registerPath({
  method: "post",
  path: "/internal/delivery-by-verdict/sync",
  summary: "Run one pass of the verification-history mirror by hand",
  description: "Copies new rows of apollo-service's verification history from the saved cursor to the end of the feed. The in-process worker does the same every 5 minutes. Idempotent.",
  responses: {
    200: { description: "What the pass did: `pages`, `rows`, `caughtUp`, `cursor`" },
    401: { description: "Unauthorized" },
    500: { description: "The pass failed", content: { "application/json": { schema: ErrorSchema } } },
  },
});

registry.registerPath({
  method: "post",
  path: "/internal/incurred-send-costs",
  summary: "Declare what the unbilled 2026-08-24 to 2026-10-02 sends really cost, never charged",
  description:
    "Every queued step of 2026-08-24 to 2026-10-02 carried no cost (the owner absorbed the sending infrastructure). For each such step whose email really left, declares on the step's own run the two email costs every send of that era carried (instantly-account-email-sent + instantly-domain-email-sent), through runs-service's incurred-costs capability: real spend, born never charged (no balance, usage total, bill, pacing or Stripe moves), dated at the email's real send instant, counted by every incurred read. Body `{dryRun (default true), limit}`. Dry run prices every item and writes nothing (200 + summary: sends, items to record / already recorded / invalid, total incurred USD, per org). `dryRun:false` answers 202 and applies in the background (log `incurred-send-costs: done`). Idempotent per item. Manual only.",
  responses: {
    200: { description: "Dry-run summary" },
    202: { description: "Apply accepted, running in the background" },
    401: { description: "Unauthorized" },
    500: { description: "The dry run failed", content: { "application/json": { schema: ErrorSchema } } },
  },
});

registry.registerPath({
  method: "post",
  path: "/internal/send-outcomes/backfill",
  summary: "Declare the message_sent of every past send to the runs ledger, dated when the email left",
  description:
    "COST-BACKFILL-1023-INSTANTLY. Walks every undeclared step hold (actual, plus cancelled ones whose email really left) and declares on its email-send-step run the `message_sent` it produced and the `message_written` + `email_checked` it consumed (subject = the lead in lead-service), with `occurredAt` = the step's earliest non-withdrawn `email_sent`, through runs-service `POST /v1/platform-runs/outcomes/batch`. A hold with no lead id, two candidate leads or no sent email is marked and never declared. Body `{dryRun (default true), limit, redate}`: dry run plans every page and writes nothing (200 + summary); `dryRun:false` answers 202 and runs in the background (log `send-outcomes-backfill: done`). `redate:true` re-states the holds ALREADY declared with runs-service's `redate`, moving each occurrence declared late back to when the email left (one already on time stays). Idempotent and resumable. Manual only.",
  responses: {
    200: { description: "Dry-run summary: holds read, runs to declare, holds with no lead id / ambiguous / no sent email" },
    202: { description: "Accepted, running in the background" },
    401: { description: "Unauthorized" },
    500: { description: "The dry run failed", content: { "application/json": { schema: ErrorSchema } } },
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

// ─── Follow-up cuts (leads emailed in error) ─────────────────────────────────

export const FollowupCutsBodySchema = z
  .object({
    campaignId: z.string().min(1).describe("The campaign (campaign-service id) that emailed these leads"),
    leadEmails: z
      .array(z.string().email())
      .min(1)
      .max(500)
      .describe("The leads of that campaign whose queued follow-ups stop. Every other lead keeps its follow-ups."),
    reason: z.string().min(1).max(2000).describe("Why: stored on the audit row"),
  })
  .openapi("FollowupCutsBody", {
    example: {
      campaignId: "fb201499-4f90-40da-9cc3-f612392664f2",
      leadEmails: ["alice@example.com"],
      reason: "Sourcing bug handed this campaign people it was not meant to write to",
    },
  });

export const FollowupCutsListQuerySchema = z.object({
  campaignId: z.string().min(1).describe("The campaign whose cuts to list"),
});

const FollowupCutLeadSchema = z.object({
  email: z.string(),
  outcome: z.enum(["cut", "already_cut", "failed"]),
  holdsCancelled: z.number().int().describe("Queued follow-up steps refunded by this call"),
  cutAt: z.string().nullable(),
  cutBy: z.string().nullable(),
  error: z.string().optional(),
});

export const FollowupCutsResponseSchema = z
  .object({
    campaignId: z.string(),
    cut: z.number().int(),
    alreadyCut: z.number().int(),
    failed: z.number().int(),
    leads: z.array(FollowupCutLeadSchema),
  })
  .openapi("FollowupCutsResponse");

export const FollowupCutsListResponseSchema = z
  .object({
    cuts: z.array(
      z.object({
        campaignId: z.string(),
        email: z.string(),
        reason: z.string(),
        cutBy: z.string(),
        holdsCancelled: z.number().int(),
        cutAt: z.string(),
      }),
    ),
  })
  .openapi("FollowupCutsListResponse");

registry.registerPath({
  method: "post",
  path: "/orgs/followup-cuts",
  summary: "Stop the queued follow-ups of named leads a campaign emailed in error",
  description:
    "For leads a campaign should never have written to (a sourcing mistake): the rest of their sequence will not go out. Every other lead of the campaign keeps its follow-ups.\n\n" +
    "**Not an opt-out.** Nothing is recorded as unsubscribe, opt-out or suppression: those people never asked to stop. Their queued steps are refunded and the sequence is marked stopped, exactly like any other stop.\n\n" +
    "**All or nothing on names:** an email the campaign never wrote to answers 404 `leads_not_in_campaign` and nothing is cut. **Idempotent:** a lead already cut answers `already_cut`. **Audited:** who (x-user-id), when, why, steps refunded (`GET /orgs/followup-cuts`). **Fail loud:** a lead whose queued steps could not all be cancelled is `failed` and the call answers 502 (retry re-runs only those).",
  request: {
    headers: TrackingHeadersSchema,
    body: { content: { "application/json": { schema: FollowupCutsBodySchema } } },
  },
  responses: {
    200: { description: "Every named lead is cut (or already was)", content: { "application/json": { schema: FollowupCutsResponseSchema } } },
    400: { description: "Invalid body or missing x-user-id", content: { "application/json": { schema: ErrorSchema } } },
    401: { description: "Unauthorized" },
    404: { description: "A named email has no sequence in this campaign; nothing cut", content: { "application/json": { schema: ErrorSchema } } },
    502: { description: "At least one lead could not be cut", content: { "application/json": { schema: FollowupCutsResponseSchema } } },
  },
});

registry.registerPath({
  method: "get",
  path: "/orgs/followup-cuts",
  summary: "The follow-up cuts of a campaign (audit)",
  description: "Every lead whose follow-ups were cut in this campaign, with who, when, why and steps refunded.",
  request: { query: FollowupCutsListQuerySchema, headers: TrackingHeadersSchema },
  responses: {
    200: { description: "Cuts", content: { "application/json": { schema: FollowupCutsListResponseSchema } } },
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

const MicrosoftPlacementSchema = z
  .object({
    verdict: z
      .enum(["inbox", "junk", "unmeasured"])
      .describe(
        "Where this provider puts this mailbox's mail, over the newest seed tests pooled until 10 seeds (under 16 days): inbox = at least 90% of the seeds found sat in Inbox (Focused or Other at Microsoft); junk = below; unmeasured = no seed found or evidence too old. A FIRST email to a prospect this provider hosts needs inbox; never a demotion.",
      ),
    mostlySpam: z
      .boolean()
      .describe("More than half of at least 10 found seeds sat in spam: NO email (follow-ups included) to a prospect this provider hosts leaves from this mailbox"),
    testsPooled: z.number().int().describe("Seed tests pooled into this verdict"),
    seedTotal: z.number().int().describe("Seeds sent to this provider's receivers in the pooled tests"),
    inboxCount: z.number().int().describe("Of those, found in Inbox"),
    spamCount: z.number().int().describe("Of those, found in Junk"),
    inboxPctOfFound: z.number().nullable().describe("inbox / (inbox + junk), 0..100; null when no seed was found"),
    testedAt: z.string().describe("ISO8601 time the test's seeds left"),
  })
  .openapi("ProviderPlacement");

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
        "Short reason when blocked — 'inbound_unreadable' when we send from this mailbox but never read its replies (see inboundUnreadable), else the account's lifecycle_status (in_recovery / deactivated_by_instantly / deactivated_by_user), or 'unclassified' when the lifecycle has not yet run; null when send-eligible.",
      ),
    inboundUnreadable: z
      .boolean()
      .describe(
        "True when this mailbox sent at least 20 emails in the last 14 days and not one inbound message was ingested from it: prospects' replies there are invisible. Such a mailbox gets no new sequence and the dispatcher sends nothing from it until a message is read again.",
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
    microsoftPlacement: MicrosoftPlacementSchema.nullable().describe(
      "Microsoft 365 (business tenant) placement from our own Microsoft seed receiver, served beside inboxPlacement. Decides for prospects hosted by a Microsoft 365 tenant. Null when never measured there.",
    ),
    microsoftConsumerPlacement: MicrosoftPlacementSchema.nullable().describe(
      "Outlook.com (consumer) placement, measured apart from Microsoft 365 (SENDER-HEALTH-1019). Decides only for Outlook.com / Hotmail / Live prospects. Null when never measured there.",
    ),
    googlePlacement: MicrosoftPlacementSchema.nullable().describe(
      "Google placement as the dispatcher routes on it (same pooled evidence as the lifecycle gate). mostlySpam = no email to a Google-hosted prospect leaves from this mailbox. Null when never measured.",
    ),
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
    queuedFirstOverdueSequences: z
      .number()
      .int()
      .describe(
        "STUCK subset of queuedFirstUnsentSequences — never-sent first emails whose sequence was assigned to this account on a UTC day STRICTLY BEFORE today, i.e. the first email was owed on an earlier day and has not gone out. Always <= queuedFirstUnsentSequences; not part of any partition. Non-zero = a stall to investigate, never work for today.",
      ),
    queuedFirstDueTodaySequences: z
      .number()
      .int()
      .describe(
        "Never-sent first emails whose sequence was assigned TODAY (UTC) = queuedFirstUnsentSequences - queuedFirstOverdueSequences. The 'first emails due today' figure without the stuck backlog. Note: send selection and the dispatcher still count stuck first emails as load on today's capacity (they are owed now), so selector load = queuedFirstUnsentSequences + queuedNextToday.",
      ),
    newSequencesToday: z
      .number()
      .int()
      .describe(
        "Sequences ASSIGNED to this account today (UTC) — campaign rows created today whose account is this address, whatever happened to them since (sent, queued, cancelled). Answers 'how many new leads did the waterfall give this mailbox today'; distinct from queuedFirstUnsentSequences, which counts every never-sent first email of any age.",
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
    "Platform-scoped, cached 60s. From `mailboxes` (migration 0053): provider, pool type (google-workspace / gandi-relay / mailforge-relay / dfy-google), subscription (standard / prewarmed / dfy), credential source, vendor creation / prewarm / import dates, absence; its addresses with lifecycle and transport; `sustainedDaily` + `effectiveDailyCap` (the selector's own `capForAccount`, minimum across aliases) + `rampProjection` (the cap trajectory to the ceiling if the room is used) + `warmupBudgetToday`; `queuedSteps` (queued-but-unsent steps, the sum of its addresses' `queueSize`); `delivery` pooled across aliases with `evidenceExpiresAt`; `volume7d` / `volume30d` by typology; `cost` from the mailbox-month rate with a labelled paid-to-date estimate.",
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

const SentCountsShape = {
  toLeads: z.number().int().describe("`outreach` sent: cold sequence steps to a lead, Instantly + SMTP"),
  manualReplies: z.number().int().describe("`manual_reply` sent: a human's answer to a lead — reaches a lead but is kept apart from cold outreach"),
  warmup: z.number().int().describe("`warmup` sent: our own warmup mesh"),
  warmupReplies: z.number().int().describe("`warmup_reply` sent: replies inside the warmup mesh"),
  seeds: z.number().int().describe("`seed` sent: inbox-placement / seed tests"),
};

const EntityScopeQueryShape = {
  account: z.string().optional().describe("One sending address"),
  mailbox: z.string().optional().describe("One real mailbox (its login): the unit a daily quota belongs to"),
  domain: z.string().optional().describe("One sending domain"),
  groupBy: z.enum(["account", "mailbox", "domain"]).optional().describe("Also serve the series per entity of this grain"),
};

const SentPeriodSchema = z
  .object({
    periodStart: z.string().describe("YYYY-MM-DD, UTC, inclusive (a week starts Monday)"),
    periodEnd: z.string().describe("YYYY-MM-DD, UTC, exclusive"),
    inProgress: z.boolean().describe("True on the current period only — it is still filling"),
    ...SentCountsShape,
    leadsEmailed: z.number().int().describe("Distinct lead addresses sent an `outreach` or `manual_reply` in the period — does NOT add across periods"),
  })
  .openapi("SentPeriod");

const SentPerPeriodResponseSchema = z
  .object({
    grain: z.enum(["day", "week", "month"]),
    timezone: z.literal("UTC"),
    since: z.string().nullable().describe("The `since` asked for (ISO), null = from the first send"),
    asOf: z.string(),
    totals: z.object(SentCountsShape).describe("Sum of the periods' counts"),
    periods: z.array(SentPeriodSchema).describe("Oldest first, no gaps, zeros included, last one is the current period"),
    scope: z.object({ account: z.string().optional(), mailbox: z.string().optional(), domain: z.string().optional() }).describe("The account / mailbox / domain filter applied; empty = the fleet"),
    groupBy: z.enum(["account", "mailbox", "domain"]).optional(),
    entities: z
      .array(z.object({ key: z.string(), totals: z.object(SentCountsShape), periods: z.array(SentPeriodSchema) }))
      .optional()
      .describe("With `groupBy` only: the same periods per entity, zeros included, entities that sent at least once. Per-period sums equal `periods` (`leadsEmailed` is distinct and does not add)."),
  })
  .openapi("SentPerPeriodResponse");

registry.registerPath({
  method: "get",
  path: "/internal/ops/sent-per-period",
  summary: "Emails sent per day / week / month, to leads apart from our own mail (warmup, warmup replies, seeds)",
  description:
    "Platform-scoped, cached 60s per (grain, since). Counts outbound `messages` rows whose outcome is `sent` (a hand-off bounce never left us) per UTC period and per purpose, plus distinct leads emailed. Every period from the first send (or from the period containing `since`) to the current one, zeros included; the current period carries `inProgress: true`. Independent of `/stats` (customer totals are untouched).",
  request: {
    query: z.object({
      grain: z.enum(["day", "week", "month"]).describe("REQUIRED"),
      since: z.string().optional().describe("ISO timestamp; the series starts at the period containing it"),
      ...EntityScopeQueryShape,
    }),
  },
  responses: {
    200: { description: "Periods", content: { "application/json": { schema: SentPerPeriodResponseSchema } } },
    400: { description: "grain missing / unknown, or since not a timestamp" },
    401: { description: "Unauthorized" },
  },
});

const ScopeOnlyQuery = {
  account: EntityScopeQueryShape.account,
  mailbox: EntityScopeQueryShape.mailbox,
  domain: EntityScopeQueryShape.domain,
};

registry.registerPath({
  method: "get",
  path: "/internal/ops/placement-tests",
  summary: "Placement tests, newest first, with inbox / spam / missing per inbox provider (MAILBOX-MONITORING-1019)",
  description:
    "Platform-scoped. One row per test (`seed:<uuid>` = our seed harness, any other id = an Instantly paid test), counted from the silver `instantly_placement_results` over the senders in scope: total and per provider (google = Google, microsoft = Microsoft 365, microsoftConsumer = Outlook.com, other). Cursor pagination on (testedAt, testId).",
  request: {
    query: z.object({
      ...ScopeOnlyQuery,
      limit: z.number().int().min(1).max(500).optional().describe("Default 50"),
      cursor: z.string().optional().describe("Opaque, from the previous page's `nextCursor`"),
    }),
  },
  responses: {
    200: { description: "Tests page", content: { "application/json": { schema: z.object({ scope: OpsObject, tests: z.array(OpsObject), nextCursor: z.string().nullable() }).openapi("OpsPlacementTestsResponse") } } },
    400: { description: "Bad filter or cursor" },
    401: { description: "Unauthorized" },
  },
});

registry.registerPath({
  method: "get",
  path: "/internal/ops/placement-tests/{testId}",
  summary: "One placement test, seed by seed: sender, receiver, inbox provider, where it landed",
  description:
    "Platform-scoped. Seed tests list every seed sent, never-found ones as `missing` (`missingSeedsListed: true`); an Instantly test lists only the seeds Instantly found (its missing seeds exist only in the list's counts). `placement` null = the seed never left (`dispatchOutcome` not `sent`), not counted. Totals per provider and per sender are recomputed from the seeds listed. Scope filters narrow the senders.",
  request: { params: z.object({ testId: z.string() }), query: z.object(ScopeOnlyQuery) },
  responses: {
    200: { description: "Test detail", content: { "application/json": { schema: OpsObject.openapi("OpsPlacementTestDetail") } } },
    404: { description: "Unknown test" },
    401: { description: "Unauthorized" },
  },
});

registry.registerPath({
  method: "get",
  path: "/internal/ops/capacity-daily",
  summary: "Capacity per day since inception and the ramp ahead, per account / mailbox / domain / fleet",
  description:
    "Platform-scoped, cached 60s. PAST: the capacity-history reconstruction kept per real mailbox (in_production addresses, operator limit = min across them as in force THAT day: the latest of an Instantly snapshot and a lifecycle flip's limit, since the snapshots stopped on 2026-10-12; `limitUnknownAddresses` counts addresses with neither, which add 0, never a guess; volume sustained over the ramp window, ramp cap, capacity = min); `phase: today` includes today so far. FUTURE (`horizonDays`, default 30, max 90): the ramp rule re-applied from today's volume for the mailboxes in production today, assuming each uses its room (never a promise). `rampRules` states the rule. Mailbox figures add up to domain and fleet; at the account grain each address carries its mailbox's figure (`additive: false`).",
  request: { query: z.object({ ...EntityScopeQueryShape, horizonDays: z.number().int().min(0).max(90).optional() }) },
  responses: {
    200: { description: "Capacity days", content: { "application/json": { schema: OpsObject.openapi("OpsCapacityDaily") } } },
    400: { description: "Bad filter" },
    401: { description: "Unauthorized" },
  },
});

const OpsLifecycleQuery = z
  .enum(["in_production", "in_recovery", "deactivated_by_instantly", "deactivated_by_user"])
  .optional()
  .describe("Only addresses whose CURRENT lifecycle status is this one (warmups: a mailbox with one such address). Absent = every address.");

registry.registerPath({
  method: "get",
  path: "/internal/ops/schedule",
  summary: "From today on, per day: queued sequence steps at their theoretical date, planned warmups, planned seed tests",
  description:
    "Platform-scoped, cached 60s. Queued steps sit on their THEORETICAL date: previous send + configured step delay, chained (a never-sent sequence is anchored on the day it was assigned). No capacity, sending window or holiday is applied. Steps already past their theoretical date are `totals.overdueQueuedSteps`, never moved onto a day. Warmups = the warmup budget rule on the ramp's projected cap for the mailboxes that sent warmup in the last 7 days (sends; replies come on top). Seed tests = the seed cadence rule for the testable pool, each test expected to carry the address's latest seed count. `horizonDays` default 30, max 90; today is `isToday`.",
  request: {
    query: z.object({
      ...EntityScopeQueryShape,
      horizonDays: z.number().int().min(0).max(90).optional(),
      lifecycle: OpsLifecycleQuery,
    }),
  },
  responses: {
    200: { description: "Schedule days", content: { "application/json": { schema: OpsObject.openapi("OpsSchedule") } } },
    400: { description: "Bad filter" },
    401: { description: "Unauthorized" },
  },
});

registry.registerPath({
  method: "get",
  path: "/internal/ops/queued-steps",
  summary: "Every queued sequence step at its theoretical date, oldest first",
  description:
    "Platform-scoped. One row per un-sent step: sequence (instantlyCampaignId), lead, org, caller campaign, account / mailbox / domain, step, first email or follow-up, the send it is anchored on, theoretical date, overdue. Same cadence as `/internal/ops/schedule`. Offset pagination.",
  request: {
    query: z.object({
      ...ScopeOnlyQuery,
      lifecycle: OpsLifecycleQuery,
      limit: z.number().int().min(1).max(1000).optional().describe("Default 100"),
      offset: z.number().int().min(0).optional(),
      overdue: z.enum(["true", "false"]).optional(),
    }),
  },
  responses: {
    200: { description: "Steps page", content: { "application/json": { schema: OpsObject.openapi("OpsQueuedSteps") } } },
    400: { description: "Bad filter" },
    401: { description: "Unauthorized" },
  },
});

registry.registerPath({
  method: "get",
  path: "/internal/ops/placement-alerts",
  summary: "Inbox placement per inbox provider: daily evaluation, its bar and sources, the episodes below the bar",
  description:
    "Platform-scoped, cached 60s. Per (day, provider): the fleet's seeds and the seeds of the production mailboxes the automatic cut (SENDER-HEALTH-1019) still lets mail that provider, inbox % of FOUND seeds, mailboxes cut, whether the day is measured (>= 20 found seeds) and below the 80 % bar (market consensus, sources served). An episode opens on the first measured day below, closes on the first measured day back above. No email is sent (PLACEMENT-ALERT-NO-EMAIL-1022): `alertStatus`/`alertedAt` are history from before 2026-10-22.",
  request: { query: z.object({ days: z.number().int().min(1).max(400).optional().describe("Default 30") }) },
  responses: {
    200: { description: "Evaluation", content: { "application/json": { schema: OpsObject.openapi("OpsPlacementAlerts") } } },
    400: { description: "Bad days" },
    401: { description: "Unauthorized" },
  },
});

registry.registerPath({
  method: "post",
  path: "/internal/ops/placement-alerts/run",
  summary: "One pass of the inbox-placement evaluation now; records the episodes, never emails; dryRun (default true) writes nothing",
  request: { body: { content: { "application/json": { schema: z.object({ dryRun: z.boolean().optional() }) } } } },
  responses: {
    200: { description: "Pass summary: evaluations, episodes written", content: { "application/json": { schema: OpsObject } } },
    401: { description: "Unauthorized" },
  },
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
