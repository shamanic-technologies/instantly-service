/**
 * Silver-layer promotion — derives canonical entities (`instantly_events`,
 * `instantly_campaigns.delivery_status`, `instantly_campaigns.reply_classification`,
 * `sequence_costs.status`) from bronze rows.
 *
 * Idempotency: silver event inserts use the `instantly_events_dedupe_idx`
 * unique index. If a row already exists, the insert is a no-op (empty returning)
 * and side effects (delivery_status update, cost lifecycle) are SKIPPED — they
 * already fired for that event.
 *
 * Source attribution: every silver event row carries `source` ('webhook',
 * 'poll_emails', 'poll_leads') and `source_row_id` (FK into the bronze table)
 * so it can be traced back.
 */
import { db } from "../db";
import { instantlyCampaigns, instantlyEvents, sequenceCosts } from "../db/schema";
import { and, eq, isNull } from "drizzle-orm";
import { updateCostStatus, type IdentityContext } from "./runs-client";
import type { LeadFull, EmailRecord } from "./instantly-client";

const SEQUENCE_STOP_EVENTS = new Set([
  "reply_received",
  "email_bounced",
  "lead_unsubscribed",
  "lead_not_interested",
]);

// Events that are at-most-1 per (campaign, lead, step) regardless of timestamp.
// Backed by partial unique index `instantly_events_one_shot_dedupe_idx`. When a
// real webhook/poll arrives after a synthetic inference, the existing row is
// upgraded in place (inferred=true → inferred=false, real timestamp wins).
const ONE_SHOT_EVENT_TYPES = new Set([
  "email_sent",
  "email_bounced",
  "lead_unsubscribed",
  "reply_received",
]);

function isOneShotEvent(eventType: string): boolean {
  return ONE_SHOT_EVENT_TYPES.has(eventType);
}

// Maps an Instantly webhook event_type to the silver `delivery_status` value
// it should set on `instantly_campaigns`. Aligns with the 4-stage funnel
// (contacted → sent → delivered → terminal). POST /send writes `contacted`
// directly; webhook `email_sent` promotes to `sent` (stage 3). Stage 4
// `delivered` is derived in queries (sent − bounced), never written as a
// status. `campaign_completed` is sequence-level and does not change delivery.
const DELIVERY_STATUS_MAP: Record<string, string> = {
  email_sent: "sent",
  reply_received: "replied",
  email_bounced: "bounced",
  lead_unsubscribed: "unsubscribed",
};

const REPLY_CLASSIFICATION_MAP: Record<string, "positive" | "negative" | "neutral"> = {
  lead_interested: "positive",
  lead_meeting_booked: "positive",
  lead_closed: "positive",
  lead_not_interested: "negative",
  lead_wrong_person: "negative",
  lead_neutral: "neutral",
  lead_out_of_office: "neutral",
  auto_reply_received: "neutral",
};

/** Instantly lead.status values that mean the lead is terminal */
const LEAD_STATUS_BOUNCED = -1;
const LEAD_STATUS_UNSUBSCRIBED = -2;

export type EventSource = "webhook" | "poll_emails" | "poll_leads" | "inferred";

export interface PromoteEventInput {
  eventType: string;
  instantlyCampaignId: string;
  leadEmail: string | null;
  accountEmail: string | null;
  step: number | null;
  variant: number | null;
  timestamp: Date;
  source: EventSource;
  sourceRowId: string;
  rawPayload?: unknown;
  // Set true for synthetic predecessors emitted by inferPredecessors. Skips
  // side effects (delivery_status, cost lifecycle) — those only fire for real
  // external signals. Recursion still runs so cascade rules (e.g. sent step N
  // ⇒ sent steps 1..N-1) propagate through inferred chains.
  inferred?: boolean;
  inferredFromEventId?: string | null;
  inferredRule?: string | null;
}

export interface PromoteEventResult {
  promoted: boolean;
  silverEventId: string | null;
}

interface CampaignRow {
  campaignId: string | null;
  instantlyCampaignId: string;
  orgId: string | null;
  userId: string | null;
  runId: string | null;
}

async function findCampaign(instantlyCampaignId: string): Promise<CampaignRow | null> {
  const [campaign] = await db
    .select()
    .from(instantlyCampaigns)
    .where(eq(instantlyCampaigns.instantlyCampaignId, instantlyCampaignId));
  return campaign ?? null;
}

async function updateDeliveryStatus(
  instantlyCampaignId: string,
  eventType: string,
): Promise<void> {
  const newStatus = DELIVERY_STATUS_MAP[eventType];
  if (!newStatus) return;

  await db
    .update(instantlyCampaigns)
    .set({ deliveryStatus: newStatus, updatedAt: new Date() })
    .where(eq(instantlyCampaigns.instantlyCampaignId, instantlyCampaignId));
  console.log(
    `[instantly-service] silver: deliveryStatus='${newStatus}' campaign=${instantlyCampaignId}`,
  );
}

async function updateReplyClassification(
  instantlyCampaignId: string,
  eventType: string,
): Promise<void> {
  const classification = REPLY_CLASSIFICATION_MAP[eventType];
  if (!classification) return;

  await db
    .update(instantlyCampaigns)
    .set({ replyClassification: classification, updatedAt: new Date() })
    .where(eq(instantlyCampaigns.instantlyCampaignId, instantlyCampaignId));
  console.log(
    `[instantly-service] silver: replyClassification='${classification}' campaign=${instantlyCampaignId}`,
  );
}

/**
 * When `email_sent` arrives for any step, convert that step's provisioned
 * email costs to actual. Each step has 2 email costs (account + domain).
 * Step 1's costs were inserted as provisioned at /send time (POST /send no
 * longer marks them actual upfront), so this handler runs for every step.
 */
async function handleEmailSent(
  campaign: CampaignRow,
  leadEmail: string,
  step: number,
): Promise<void> {
  if (!campaign.campaignId) return;

  const costs = await db
    .select()
    .from(sequenceCosts)
    .where(
      and(
        eq(sequenceCosts.campaignId, campaign.campaignId),
        eq(sequenceCosts.leadEmail, leadEmail),
        eq(sequenceCosts.step, step),
        eq(sequenceCosts.status, "provisioned"),
      ),
    );

  if (costs.length === 0) return;

  for (const cost of costs) {
    const identity: IdentityContext = {
      orgId: campaign.orgId || "system",
      userId: campaign.userId || "00000000-0000-0000-0000-000000000000",
      runId: cost.runId,
    };

    try {
      await updateCostStatus(cost.runId, cost.costId, "actual", identity);
      await db
        .update(sequenceCosts)
        .set({ status: "actual", updatedAt: new Date() })
        .where(eq(sequenceCosts.id, cost.id));
      console.log(
        `[instantly-service] silver: cost ${cost.costId} provisioned→actual step=${step}`,
      );
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : String(error);
      console.error(
        `[instantly-service] silver: failed to convert cost ${cost.costId}: ${message}`,
      );
    }
  }
}

/**
 * When sequence stops (reply, bounce, unsub, not_interested), cancel all
 * remaining provisioned costs for this lead.
 */
async function cancelRemainingProvisions(
  campaign: CampaignRow,
  leadEmail: string,
): Promise<void> {
  if (!campaign.campaignId) return;

  const remaining = await db
    .select()
    .from(sequenceCosts)
    .where(
      and(
        eq(sequenceCosts.campaignId, campaign.campaignId),
        eq(sequenceCosts.leadEmail, leadEmail),
        eq(sequenceCosts.status, "provisioned"),
      ),
    );

  for (const cost of remaining) {
    const identity: IdentityContext = {
      orgId: campaign.orgId || "system",
      userId: campaign.userId || "00000000-0000-0000-0000-000000000000",
      runId: cost.runId,
    };
    try {
      await updateCostStatus(cost.runId, cost.costId, "cancelled", identity);
      await db
        .update(sequenceCosts)
        .set({ status: "cancelled", updatedAt: new Date() })
        .where(eq(sequenceCosts.id, cost.id));
      console.log(
        `[instantly-service] silver: cost ${cost.costId} cancelled step=${cost.step}`,
      );
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : String(error);
      console.error(
        `[instantly-service] silver: failed to cancel cost ${cost.costId}: ${message}`,
      );
    }
  }
}

/**
 * Inference rule: a real (or already-inferred) event implies its predecessor
 * events must also have occurred. Stored as a function returning an array of
 * partial event descriptors (without inferred markers — those are added by
 * `inferPredecessors`).
 *
 * Rules:
 *   - `email_opened`        ⇒ `email_sent` (same step)            [opened_implies_sent]
 *   - `email_link_clicked`  ⇒ `email_opened` + `email_sent`       [clicked_implies_opened, clicked_implies_sent]
 *   - `reply_received`      ⇒ `email_sent`                        [replied_implies_sent]
 *   - `email_bounced`       ⇒ `email_sent`                        [bounced_implies_sent]
 *   - `lead_unsubscribed`   ⇒ `email_sent`                        [unsubscribed_implies_sent]
 *   - `email_sent` step N   ⇒ `email_sent` steps 1..N-1           [sent_cascade]
 *
 * Step is required for any inference — if the trigger event has no step we
 * cannot place the predecessor in the sequence and skip.
 */
interface PredecessorDescriptor {
  eventType: string;
  step: number;
  rule: string;
}

function computePredecessors(input: PromoteEventInput): PredecessorDescriptor[] {
  if (input.step == null) return [];
  const step = input.step;

  switch (input.eventType) {
    case "email_opened":
      return [{ eventType: "email_sent", step, rule: "opened_implies_sent" }];

    case "email_link_clicked":
      return [
        { eventType: "email_opened", step, rule: "clicked_implies_opened" },
        { eventType: "email_sent", step, rule: "clicked_implies_sent" },
      ];

    case "reply_received":
      return [{ eventType: "email_sent", step, rule: "replied_implies_sent" }];

    case "email_bounced":
      return [{ eventType: "email_sent", step, rule: "bounced_implies_sent" }];

    case "lead_unsubscribed":
      return [{ eventType: "email_sent", step, rule: "unsubscribed_implies_sent" }];

    case "email_sent": {
      const out: PredecessorDescriptor[] = [];
      for (let s = 1; s < step; s++) {
        out.push({ eventType: "email_sent", step: s, rule: "sent_cascade" });
      }
      return out;
    }

    default:
      return [];
  }
}

/**
 * Emit synthetic predecessor events for a freshly-promoted trigger event.
 * Synthetic rows carry `inferred=true`, `source='inferred'`, and trace back
 * to the trigger via `inferred_from_event_id`. Timestamp matches the trigger
 * (per design: synthetic rows are not chronologically real — they are logical
 * projections, so co-locating them at the trigger ts avoids backdating).
 *
 * Recursion is safe: synthetic events also go through `promoteEvent`, so
 * cascade rules (e.g. sent step 3 ⇒ sent steps 1, 2) propagate. The dedup
 * indexes terminate the chain (each predecessor inserted at most once).
 */
async function inferPredecessors(
  trigger: PromoteEventInput,
  triggerEventId: string,
): Promise<void> {
  const predecessors = computePredecessors(trigger);
  if (predecessors.length === 0) return;

  for (const pred of predecessors) {
    await promoteEvent({
      eventType: pred.eventType,
      instantlyCampaignId: trigger.instantlyCampaignId,
      leadEmail: trigger.leadEmail,
      accountEmail: trigger.accountEmail,
      step: pred.step,
      variant: null,
      timestamp: trigger.timestamp,
      source: "inferred",
      sourceRowId: triggerEventId,
      rawPayload: null,
      inferred: true,
      inferredFromEventId: triggerEventId,
      inferredRule: pred.rule,
    });
  }
}

/**
 * Look up an existing one-shot silver row by natural key (campaign, lead,
 * event_type, step) — ignoring timestamp. Used by the upsert path to detect
 * "real event arrived after synthetic inference" so the row can be upgraded.
 */
async function findOneShotEvent(
  instantlyCampaignId: string,
  leadEmail: string | null,
  eventType: string,
  step: number | null,
): Promise<{ id: string; inferred: boolean } | null> {
  const conditions = [
    eq(instantlyEvents.campaignId, instantlyCampaignId),
    eq(instantlyEvents.eventType, eventType),
    leadEmail === null
      ? isNull(instantlyEvents.leadEmail)
      : eq(instantlyEvents.leadEmail, leadEmail),
    step === null ? isNull(instantlyEvents.step) : eq(instantlyEvents.step, step),
  ];

  const [row] = await db
    .select({ id: instantlyEvents.id, inferred: instantlyEvents.inferred })
    .from(instantlyEvents)
    .where(and(...conditions));

  return row ?? null;
}

/**
 * Upgrade a synthetic (inferred=true) row to a real row in place. Used when a
 * real webhook/poll arrives after inference already projected the event. The
 * row's silver id is preserved so downstream references stay stable.
 */
async function upgradeInferredRow(
  rowId: string,
  realInput: PromoteEventInput,
): Promise<void> {
  await db
    .update(instantlyEvents)
    .set({
      inferred: false,
      source: realInput.source,
      sourceRowId: realInput.sourceRowId,
      timestamp: realInput.timestamp,
      rawPayload: realInput.rawPayload as Record<string, unknown> | null | undefined,
      inferredFromEventId: null,
      inferredRule: null,
    })
    .where(eq(instantlyEvents.id, rowId));
  console.log(
    `[instantly-service] silver: upgraded inferred → real event=${realInput.eventType} campaign=${realInput.instantlyCampaignId} lead=${realInput.leadEmail ?? "null"} step=${realInput.step ?? "null"}`,
  );
}

interface InsertResult {
  /** True if a new row was inserted into silver (first-time promotion). */
  promoted: boolean;
  /** True if an existing inferred row was upgraded to real. */
  upgraded: boolean;
  silverEventId: string | null;
}

/**
 * Insert or upgrade a silver event row. Handles the one-shot upgrade case:
 * if a synthetic row exists for the same natural key, a real event upgrades it
 * in place instead of inserting a duplicate. Synthetic events with a real row
 * already in place are a no-op.
 */
async function insertOrUpgradeSilverEvent(
  input: PromoteEventInput,
): Promise<InsertResult> {
  const inserted = await db
    .insert(instantlyEvents)
    .values({
      eventType: input.eventType,
      campaignId: input.instantlyCampaignId,
      leadEmail: input.leadEmail,
      accountEmail: input.accountEmail,
      step: input.step,
      variant: input.variant,
      timestamp: input.timestamp,
      rawPayload: input.rawPayload as Record<string, unknown> | null | undefined,
      source: input.source,
      sourceRowId: input.sourceRowId,
      inferred: input.inferred ?? false,
      inferredFromEventId: input.inferredFromEventId ?? null,
      inferredRule: input.inferredRule ?? null,
    })
    .onConflictDoNothing()
    .returning({ id: instantlyEvents.id });

  if (inserted.length > 0) {
    return { promoted: true, upgraded: false, silverEventId: inserted[0].id };
  }

  // Conflict hit. For one-shot events, the conflict may be on the partial
  // unique index `(campaign, lead, event_type, step)` (timestamp-independent).
  // If the existing row is synthetic and the incoming event is real, upgrade.
  if (isOneShotEvent(input.eventType)) {
    const existing = await findOneShotEvent(
      input.instantlyCampaignId,
      input.leadEmail,
      input.eventType,
      input.step,
    );
    if (existing && existing.inferred && !input.inferred) {
      await upgradeInferredRow(existing.id, input);
      return { promoted: false, upgraded: true, silverEventId: existing.id };
    }
    return {
      promoted: false,
      upgraded: false,
      silverEventId: existing?.id ?? null,
    };
  }

  return { promoted: false, upgraded: false, silverEventId: null };
}

/**
 * Promote a single event into silver. Idempotent: if a row matching the
 * dedupe key already exists, returns { promoted: false } and skips side
 * effects. Upgrade path: if the existing row was synthetic (inferred=true)
 * and the incoming event is real, the row is upgraded in place — side effects
 * fire because this is the first time the real signal is observed.
 *
 * Inference: after a successful insert/upgrade, deterministic predecessor
 * rules (opened ⇒ sent, replied ⇒ sent, sent N ⇒ sent 1..N-1, etc.) synthesize
 * missing predecessor rows with `inferred=true`. Synthetic events themselves
 * also recurse so cascade chains complete in one pass; dedup terminates the
 * recursion naturally.
 *
 * Returns { promoted: true, silverEventId } when a new row was inserted (or
 * an inferred row was upgraded), in which case side effects (delivery_status,
 * cost lifecycle) have fired for real events.
 */
export async function promoteEvent(input: PromoteEventInput): Promise<PromoteEventResult> {
  const campaign = await findCampaign(input.instantlyCampaignId);
  if (!campaign) {
    return { promoted: false, silverEventId: null };
  }

  const result = await insertOrUpgradeSilverEvent(input);

  if (!result.promoted && !result.upgraded) {
    return { promoted: false, silverEventId: result.silverEventId };
  }

  // Side effects fire only for real (non-inferred) events. Synthetic events
  // are stats-only projection; delivery_status and cost lifecycle are driven
  // by actual external signals.
  if (!input.inferred) {
    await updateDeliveryStatus(input.instantlyCampaignId, input.eventType);
    await updateReplyClassification(input.instantlyCampaignId, input.eventType);

    if (input.leadEmail) {
      if (input.eventType === "email_sent" && input.step) {
        await handleEmailSent(campaign, input.leadEmail, input.step);
      } else if (SEQUENCE_STOP_EVENTS.has(input.eventType)) {
        await cancelRemainingProvisions(campaign, input.leadEmail);
      }
    }
  }

  // Run inference on the freshly-promoted event. Inferred trigger events
  // also recurse so cascade rules (e.g. sent step N ⇒ sent 1..N-1) propagate
  // through synthetic chains. Termination is guaranteed by the dedup indexes.
  if (result.silverEventId && result.promoted) {
    await inferPredecessors(input, result.silverEventId);
  }

  return { promoted: true, silverEventId: result.silverEventId };
}

/**
 * Promote a bronze webhook payload row into silver. Returns the silver promotion
 * result (caller decides what to do with it, e.g. response shape).
 */
export async function promoteFromWebhookPayload(params: {
  bronzeRowId: string;
  payload: {
    event_type: string;
    campaign_id: string;
    lead_email?: string | null;
    email_account?: string | null;
    step?: number | null;
    variant?: number | null;
    timestamp?: string | null;
  };
  rawPayload?: unknown;
}): Promise<PromoteEventResult> {
  const { bronzeRowId, payload, rawPayload } = params;
  return promoteEvent({
    eventType: payload.event_type,
    instantlyCampaignId: payload.campaign_id,
    leadEmail: payload.lead_email ?? null,
    accountEmail: payload.email_account ?? null,
    step: payload.step ?? null,
    variant: payload.variant ?? null,
    timestamp: payload.timestamp ? new Date(payload.timestamp) : new Date(),
    source: "webhook",
    sourceRowId: bronzeRowId,
    rawPayload,
  });
}

/**
 * Parse Instantly's `step` field (string like "step-1" or "1") into an integer.
 * Returns null if unparseable or null input.
 */
function parseInstantlyStep(raw: string | null | undefined): number | null {
  if (!raw) return null;
  // Instantly's `step` is "step-1", "step-2", etc. Strip non-digit prefix, then
  // parse positive integer. Do NOT use `/(-?\d+)/` — the hyphen in "step-2"
  // would be matched as negative sign yielding -2 instead of 2.
  const match = /(\d+)/.exec(raw);
  if (!match) return null;
  const parsed = parseInt(match[1], 10);
  return Number.isFinite(parsed) ? parsed : null;
}

/**
 * Promote a bronze /emails record into silver. Maps Instantly's ue_type:
 *   1 (sent from campaign) → email_sent
 *   2 (received)           → reply_received
 *   3 (manual sent)        → skipped (not a campaign event)
 *   4 (scheduled)          → skipped (not yet sent)
 */
export async function promoteFromEmailRecord(params: {
  bronzeRowId: string;
  email: EmailRecord;
}): Promise<PromoteEventResult> {
  const { bronzeRowId, email } = params;

  let eventType: string;
  switch (email.ue_type) {
    case 1:
      eventType = "email_sent";
      break;
    case 2:
      eventType = "reply_received";
      break;
    default:
      return { promoted: false, silverEventId: null };
  }

  if (!email.campaign_id || !email.lead) {
    return { promoted: false, silverEventId: null };
  }

  return promoteEvent({
    eventType,
    instantlyCampaignId: email.campaign_id,
    leadEmail: email.lead,
    accountEmail: email.eaccount ?? null,
    step: parseInstantlyStep(email.step),
    variant: null,
    timestamp: new Date(email.timestamp_email),
    source: "poll_emails",
    sourceRowId: bronzeRowId,
  });
}

/**
 * Promote a bronze /leads/list row into silver delivery_status. Derives
 * delivery_status from Instantly's lead.status (-1=bounced, -2=unsubscribed)
 * and from timestamp_last_reply (any non-null reply timestamp = replied).
 *
 * Returns whether delivery_status was changed (i.e. silver was promoted).
 */
export async function promoteFromLead(params: {
  bronzeRowId: string;
  instantlyCampaignId: string;
  lead: LeadFull;
}): Promise<PromoteEventResult> {
  const { bronzeRowId, instantlyCampaignId, lead } = params;

  // Map lead status to synthetic event type for promoteEvent's dedupe + side effects
  let syntheticEventType: string | null = null;
  let syntheticTimestamp: Date | null = null;

  if (lead.status === LEAD_STATUS_BOUNCED) {
    syntheticEventType = "email_bounced";
    syntheticTimestamp = lead.timestamp_last_contact
      ? new Date(lead.timestamp_last_contact)
      : new Date();
  } else if (lead.status === LEAD_STATUS_UNSUBSCRIBED) {
    syntheticEventType = "lead_unsubscribed";
    syntheticTimestamp = lead.timestamp_last_contact
      ? new Date(lead.timestamp_last_contact)
      : new Date();
  } else if (lead.timestamp_last_reply) {
    syntheticEventType = "reply_received";
    syntheticTimestamp = new Date(lead.timestamp_last_reply);
  }

  if (!syntheticEventType || !syntheticTimestamp) {
    return { promoted: false, silverEventId: null };
  }

  return promoteEvent({
    eventType: syntheticEventType,
    instantlyCampaignId,
    leadEmail: lead.email,
    accountEmail: null,
    step: lead.email_replied_step ?? null,
    variant: null,
    timestamp: syntheticTimestamp,
    source: "poll_leads",
    sourceRowId: bronzeRowId,
  });
}

/**
 * Backfill synthetic open events from a /leads/list snapshot. Inserts ONE
 * `email_opened` silver row per lead with email_open_count > 0, dated at
 * timestamp_last_open and stepped at email_opened_step. Imperfect (collapses
 * multi-step opens to last step) but recovers per-lead opened flag for /stats.
 *
 * Returns the count of new opens inserted.
 */
export async function promoteSyntheticOpensFromLead(params: {
  bronzeRowId: string;
  instantlyCampaignId: string;
  lead: LeadFull;
}): Promise<{ promoted: boolean }> {
  const { bronzeRowId, instantlyCampaignId, lead } = params;
  if (!lead.email_open_count || lead.email_open_count <= 0) {
    return { promoted: false };
  }
  if (!lead.timestamp_last_open) return { promoted: false };

  const result = await promoteEvent({
    eventType: "email_opened",
    instantlyCampaignId,
    leadEmail: lead.email,
    accountEmail: null,
    step: lead.email_opened_step ?? null,
    variant: lead.email_opened_variant ?? null,
    timestamp: new Date(lead.timestamp_last_open),
    source: "poll_leads",
    sourceRowId: bronzeRowId,
  });
  return { promoted: result.promoted };
}

// Refresh window for not_sending_status_seen_at — if the value is unchanged
// AND was seen within this window, skip the DB write to keep the silver row
// quiet. Window matches typical reconcile cadence.
const NOT_SENDING_STATUS_REFRESH_MS = 15 * 60 * 1000;

/**
 * Promote a bronze GET /campaigns/{id} payload into silver
 * `instantly_campaigns.not_sending_status` + `not_sending_status_seen_at`.
 *
 * Idempotency: writes only if (a) value changed, or (b) value unchanged but
 * `seen_at` is older than NOT_SENDING_STATUS_REFRESH_MS. This keeps the silver
 * row from churning every cycle when nothing changes.
 *
 * Returns { promoted: true } on write, { promoted: false } on skip / unknown
 * campaign.
 */
export async function promoteFromCampaignConfig(params: {
  bronzeRowId: string;
  instantlyCampaignId: string;
  notSendingStatus: number | null;
  now?: Date;
}): Promise<{ promoted: boolean }> {
  const { instantlyCampaignId, notSendingStatus } = params;
  const now = params.now ?? new Date();

  const [row] = await db
    .select({
      notSendingStatus: instantlyCampaigns.notSendingStatus,
      notSendingStatusSeenAt: instantlyCampaigns.notSendingStatusSeenAt,
    })
    .from(instantlyCampaigns)
    .where(eq(instantlyCampaigns.instantlyCampaignId, instantlyCampaignId));

  if (!row) {
    return { promoted: false };
  }

  const currentValue = row.notSendingStatus ?? null;
  const currentSeenAt = row.notSendingStatusSeenAt;
  const valueChanged = currentValue !== notSendingStatus;
  const stale =
    !currentSeenAt ||
    now.getTime() - currentSeenAt.getTime() > NOT_SENDING_STATUS_REFRESH_MS;

  if (!valueChanged && !stale) {
    return { promoted: false };
  }

  await db
    .update(instantlyCampaigns)
    .set({
      notSendingStatus,
      notSendingStatusSeenAt: now,
      updatedAt: now,
    })
    .where(eq(instantlyCampaigns.instantlyCampaignId, instantlyCampaignId));

  if (valueChanged) {
    console.log(
      `[instantly-service] silver: notSendingStatus ${currentValue ?? "null"}→${notSendingStatus ?? "null"} campaign=${instantlyCampaignId}`,
    );
  }

  return { promoted: true };
}

/**
 * Backfill synthetic click events from a /leads/list snapshot. Same shape as
 * opens. Note: Instantly does not expose timestamp_last_click directly; we use
 * timestamp_last_contact as a proxy (best available).
 */
export async function promoteSyntheticClicksFromLead(params: {
  bronzeRowId: string;
  instantlyCampaignId: string;
  lead: LeadFull;
}): Promise<{ promoted: boolean }> {
  const { bronzeRowId, instantlyCampaignId, lead } = params;
  if (!lead.email_click_count || lead.email_click_count <= 0) {
    return { promoted: false };
  }

  const fallbackTs = lead.timestamp_last_open ?? lead.timestamp_last_contact;
  if (!fallbackTs) return { promoted: false };

  const result = await promoteEvent({
    eventType: "email_link_clicked",
    instantlyCampaignId,
    leadEmail: lead.email,
    accountEmail: null,
    step: lead.email_clicked_step ?? null,
    variant: null,
    timestamp: new Date(fallbackTs),
    source: "poll_leads",
    sourceRowId: bronzeRowId,
  });
  return { promoted: result.promoted };
}

/**
 * Run inference for an existing silver event. Used by the backfill CLI to
 * project predecessor events for rows that pre-date inference logic (or for
 * events that landed via a path that skipped `inferPredecessors`).
 *
 * Idempotent: predecessors that already exist (real or inferred) dedup via
 * the silver indexes. No side effects (delivery_status / cost lifecycle).
 */
export async function backfillInferenceForEvent(args: {
  silverEventId: string;
  eventType: string;
  instantlyCampaignId: string;
  leadEmail: string | null;
  accountEmail: string | null;
  step: number | null;
  timestamp: Date;
}): Promise<void> {
  await inferPredecessors(
    {
      eventType: args.eventType,
      instantlyCampaignId: args.instantlyCampaignId,
      leadEmail: args.leadEmail,
      accountEmail: args.accountEmail,
      step: args.step,
      variant: null,
      timestamp: args.timestamp,
      // Source on the trigger event is unused by inferPredecessors (only the
      // synthetic rows it emits carry source='inferred'). Pass any valid value.
      source: "webhook",
      sourceRowId: args.silverEventId,
      inferred: false,
    },
    args.silverEventId,
  );
}
