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
import { and, eq } from "drizzle-orm";
import { updateCostStatus, type IdentityContext } from "./runs-client";
import type { LeadFull, EmailRecord } from "./instantly-client";

const SEQUENCE_STOP_EVENTS = new Set([
  "reply_received",
  "email_bounced",
  "lead_unsubscribed",
  "lead_not_interested",
]);

const DELIVERY_STATUS_MAP: Record<string, string> = {
  email_sent: "sent",
  campaign_completed: "delivered",
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

export type EventSource = "webhook" | "poll_emails" | "poll_leads";

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
 * When a follow-up email is sent, convert all matching provisioned costs to actual.
 * Each step has 2 email costs (account + domain).
 */
async function handleFollowUpSent(
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
 * Promote a single event into silver. Idempotent: if a row matching the
 * dedupe key (campaign_id, lead_email, event_type, timestamp, step) already
 * exists, returns { promoted: false } and skips side effects.
 *
 * Returns { promoted: true, silverEventId } on first insert, in which case
 * side effects (delivery_status update, cost lifecycle) have fired.
 */
export async function promoteEvent(input: PromoteEventInput): Promise<PromoteEventResult> {
  const campaign = await findCampaign(input.instantlyCampaignId);
  if (!campaign) {
    return { promoted: false, silverEventId: null };
  }

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
    })
    .onConflictDoNothing()
    .returning({ id: instantlyEvents.id });

  if (inserted.length === 0) {
    return { promoted: false, silverEventId: null };
  }

  await updateDeliveryStatus(input.instantlyCampaignId, input.eventType);
  await updateReplyClassification(input.instantlyCampaignId, input.eventType);

  if (input.leadEmail) {
    if (input.eventType === "email_sent" && input.step && input.step > 1) {
      await handleFollowUpSent(campaign, input.leadEmail, input.step);
    } else if (SEQUENCE_STOP_EVENTS.has(input.eventType)) {
      await cancelRemainingProvisions(campaign, input.leadEmail);
    }
  }

  return { promoted: true, silverEventId: inserted[0].id };
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
