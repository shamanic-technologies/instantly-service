/**
 * Manual reply qualifications — human users override Instantly's automatic
 * reply classification via POST /orgs/manual-qualifications.
 *
 * Bronze table is append-only. Idempotence is enforced here: if the latest
 * row for (org, instantly_campaign, lead) already has the requested status,
 * no new bronze row is inserted and no side effects fire — the existing row
 * is returned. Otherwise: insert bronze, mirror as a silver event row with
 * source='manual', and update instantly_campaigns reply_classification +
 * reply_classification_source='manual' (manual wins over webhook auto).
 */
import { db } from "../db";
import {
  instantlyCampaigns,
  instantlyEvents,
  instantlyManualQualificationsRaw,
} from "../db/schema";
import { and, desc, eq } from "drizzle-orm";
import { promoteEvent } from "./silver-promote";
import { refreshLeadStatusCurrent } from "./status-gold";
import { resolveInstantlyApiKey } from "./key-client";
import { updateCampaignStatus } from "./instantly-client";
import { isSelfSendCampaignId } from "./self-send/transport";
import { stopSelfSendSequence } from "./self-send/stop-sequence";

// Mirrors the 8 keys of REPLY_CLASSIFICATION_MAP in silver-promote.ts. Kept in
// sync deliberately: when a human qualifies a reply, the status is the same
// event_type Instantly would have fired had it detected the reply itself.
export const MANUAL_QUALIFICATION_STATUSES = [
  "lead_interested",
  "lead_meeting_booked",
  "lead_closed",
  "lead_not_interested",
  "lead_wrong_person",
  "lead_neutral",
  "lead_out_of_office",
  "auto_reply_received",
] as const;

export type ManualQualificationStatus = (typeof MANUAL_QUALIFICATION_STATUSES)[number];

const MANUAL_QUALIFICATION_CLASSIFICATION: Record<
  ManualQualificationStatus,
  "positive" | "negative" | "neutral"
> = {
  lead_interested: "positive",
  lead_meeting_booked: "positive",
  lead_closed: "positive",
  lead_not_interested: "negative",
  lead_wrong_person: "negative",
  lead_neutral: "neutral",
  lead_out_of_office: "neutral",
  auto_reply_received: "neutral",
};

/**
 * The manual statuses that assert the prospect actually ENGAGED — i.e. the
 * sequence must stop.
 *
 * `lead_out_of_office` and `auto_reply_received` are deliberately EXCLUDED: an
 * autoresponder is not a reply (RFC 3834), the prospect is back at their desk
 * next week and has not engaged. Stopping on one would end the outreach — and
 * refund the spend — for a lead who never answered. Same reasoning as
 * `auto_reply_received` being absent from `SEQUENCE_STOP_EVENTS` in
 * silver-promote.ts and from the self-send inbound classifier.
 *
 * This single predicate gates BOTH halves of "the sequence stopped": the
 * synthesized `reply_received` event (which cancels the lead's remaining
 * provisioned holds) AND the Instantly pause. Gating only one of the two would
 * leave the two sides contradicting each other — holds refunded locally while
 * Instantly keeps dispatching, or vice versa.
 */
export const SEQUENCE_STOPPING_MANUAL_STATUSES = new Set<ManualQualificationStatus>([
  "lead_interested",
  "lead_meeting_booked",
  "lead_closed",
  "lead_not_interested",
  "lead_wrong_person",
  "lead_neutral",
]);

/** True iff this manual qualification means the sequence must stop. */
export function isSequenceStoppingQualification(
  status: ManualQualificationStatus,
): boolean {
  return SEQUENCE_STOPPING_MANUAL_STATUSES.has(status);
}

export interface ManualQualificationRow {
  id: string;
  orgId: string;
  campaignId: string;
  instantlyCampaignId: string;
  leadEmail: string;
  status: ManualQualificationStatus;
  qualifiedBy: string;
  notes: string | null;
  qualifiedAt: Date;
}

export interface InsertManualQualificationInput {
  orgId: string;
  campaignId: string;
  instantlyCampaignId: string;
  leadEmail: string;
  status: ManualQualificationStatus;
  qualifiedBy: string;
  notes?: string;
  payload: unknown;
}

export interface InsertManualQualificationResult {
  /** True if a new bronze row was inserted; false on idempotent no-op. */
  inserted: boolean;
  row: ManualQualificationRow;
}

function toRow(raw: {
  id: string;
  orgId: string;
  campaignId: string;
  instantlyCampaignId: string;
  leadEmail: string;
  status: string;
  qualifiedBy: string;
  notes: string | null;
  qualifiedAt: Date;
}): ManualQualificationRow {
  return {
    id: raw.id,
    orgId: raw.orgId,
    campaignId: raw.campaignId,
    instantlyCampaignId: raw.instantlyCampaignId,
    leadEmail: raw.leadEmail,
    status: raw.status as ManualQualificationStatus,
    qualifiedBy: raw.qualifiedBy,
    notes: raw.notes,
    qualifiedAt: raw.qualifiedAt,
  };
}

async function findLatestManualQualification(
  orgId: string,
  instantlyCampaignId: string,
  leadEmail: string,
): Promise<ManualQualificationRow | null> {
  const [row] = await db
    .select()
    .from(instantlyManualQualificationsRaw)
    .where(
      and(
        eq(instantlyManualQualificationsRaw.orgId, orgId),
        eq(instantlyManualQualificationsRaw.instantlyCampaignId, instantlyCampaignId),
        eq(instantlyManualQualificationsRaw.leadEmail, leadEmail),
      ),
    )
    .orderBy(desc(instantlyManualQualificationsRaw.qualifiedAt))
    .limit(1);
  return row ? toRow(row) : null;
}

/**
 * Insert a new manual qualification row in bronze. Idempotent: if the latest
 * row for (org, instantly_campaign, lead_email) already matches `status`,
 * returns { inserted: false, row: existing } without writing.
 */
export async function insertManualQualification(
  input: InsertManualQualificationInput,
): Promise<InsertManualQualificationResult> {
  const existing = await findLatestManualQualification(
    input.orgId,
    input.instantlyCampaignId,
    input.leadEmail,
  );

  if (existing && existing.status === input.status) {
    return { inserted: false, row: existing };
  }

  const [inserted] = await db
    .insert(instantlyManualQualificationsRaw)
    .values({
      orgId: input.orgId,
      campaignId: input.campaignId,
      instantlyCampaignId: input.instantlyCampaignId,
      leadEmail: input.leadEmail,
      status: input.status,
      qualifiedBy: input.qualifiedBy,
      notes: input.notes ?? null,
      payload: input.payload as Record<string, unknown>,
    })
    .returning();

  return { inserted: true, row: toRow(inserted) };
}

export interface ApplyManualQualificationSideEffectsInput {
  bronzeRowId: string;
  orgId: string;
  instantlyCampaignId: string;
  leadEmail: string;
  status: ManualQualificationStatus;
  qualifiedAt: Date;
  rawPayload: unknown;
}

/**
 * Pause the lead's Instantly campaign after a sequence-stopping manual
 * qualification.
 *
 * Load-bearing: a manual qualification exists PRECISELY because Instantly did
 * not detect the reply itself — so Instantly's own stop-on-reply can never
 * fire, and without this pause it keeps dispatching the remaining steps to a
 * prospect who already answered. Cancelling the local cost holds (which the
 * synthesized `reply_received` does) only refunds the spend; it tells Instantly
 * nothing.
 *
 * Minimal by design — this only PAUSES on Instantly, exactly like
 * `maybeStopOnClickForSignup`. The nightly reconcile then discovers the paused
 * Instantly status and its `finish` closure cancels any residual provisioned
 * holds, deletes the contact and marks the local row terminal. Do NOT duplicate
 * those here, and do NOT write a local terminal status (a locally-terminal row
 * is SKIPPED by `reconcileAll`, so the finish closure would never run).
 *
 * Fail-soft: any error (key resolution, Instantly) is swallowed and logged. The
 * caller runs inside a request handler whose bronze row is already committed;
 * throwing here would 500 a qualification that did land.
 */
async function pauseSequenceOnInstantly(
  orgId: string,
  instantlyCampaignId: string,
  leadEmail: string,
  status: ManualQualificationStatus,
): Promise<void> {
  try {
    // A sequence WE dispatch has no Instantly campaign to pause, and reconcile
    // skips a `self:` row outright — so the stop has to happen locally, holds
    // included, or the lead would keep receiving followups after a human said
    // they had already replied.
    if (isSelfSendCampaignId(instantlyCampaignId)) {
      await stopSelfSendSequence(
        { instantlyCampaignId, campaignId: null, orgId, userId: null, runId: null },
        leadEmail,
        `manual qualification status=${status}`,
      );
      return;
    }

    const { key } = await resolveInstantlyApiKey(orgId, "system", {
      method: "POST",
      path: "/orgs/manual-qualifications",
    });
    await updateCampaignStatus(key, instantlyCampaignId, "paused");
    console.log(
      `[instantly-service] manual qualification: paused campaign=${instantlyCampaignId} lead=${leadEmail} status=${status}`,
    );
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.warn(
      `[instantly-service] manual qualification: Instantly pause failed for campaign=${instantlyCampaignId} lead=${leadEmail} — ${message}; sequence continues on Instantly`,
    );
  }
}

/**
 * Side effects after a manual qualification is inserted into bronze:
 *  1. **Stop the sequence, on BOTH sides** — but only for a status that
 *     asserts real engagement (`isSequenceStoppingQualification`; an
 *     autoresponder / out-of-office is not a reply, see that predicate):
 *     a. Synthesize a `reply_received` silver event (source='manual'). The
 *        human is asserting "this lead replied — Instantly missed it", so the
 *        reply event MUST exist in silver for `/orgs/status` to report
 *        `replied=true`. Routed through `promoteEvent` so the normal side
 *        effects fire: `delivery_status='replied'` AND remaining provisioned
 *        costs are cancelled.
 *     b. PAUSE the campaign on Instantly. (a) alone stops nothing on
 *        Instantly's side — see `pauseSequenceOnInstantly`.
 *  2. Mirror the lead-status event (`lead_interested` / `lead_not_interested`
 *     / etc.) in silver via direct insert — for EVERY status, stopping or not,
 *     because the human's qualification is a fact worth recording either way.
 *     Kept as a direct insert so we can also set
 *     `replyClassificationSource='manual'` below — going through
 *     `promoteEvent` would update `replyClassification` from the status map
 *     but not the source field.
 *  3. Set `reply_classification` to the derived positive/negative/neutral
 *     value and pin `reply_classification_source='manual'` so subsequent
 *     webhook events do not overwrite the manual choice.
 */
export async function applyManualQualificationSideEffects(
  input: ApplyManualQualificationSideEffectsInput,
): Promise<void> {
  // 1. The lead engaged ⇒ stop the sequence on both sides.
  if (isSequenceStoppingQualification(input.status)) {
    // 1a. Synthesize the reply_received event so `/orgs/status.replied` reports
    //     true. `promoteEvent` handles the one-shot dedupe: if a real reply
    //     event already exists (Instantly auto-detected too), this is a no-op.
    await promoteEvent({
      eventType: "reply_received",
      instantlyCampaignId: input.instantlyCampaignId,
      leadEmail: input.leadEmail,
      accountEmail: null,
      step: null,
      variant: null,
      timestamp: input.qualifiedAt,
      rawPayload: input.rawPayload,
      source: "manual",
      sourceRowId: input.bronzeRowId,
      inferred: false,
    });

    // 1b. Instantly never saw the reply — tell it to stop dispatching.
    await pauseSequenceOnInstantly(
      input.orgId,
      input.instantlyCampaignId,
      input.leadEmail,
      input.status,
    );
  }

  // 2. Mirror the lead-status event in silver (direct insert — source field is
  //    set to 'manual' explicitly below; promoteEvent's auto-update would not
  //    touch `reply_classification_source`).
  await db.insert(instantlyEvents).values({
    eventType: input.status,
    campaignId: input.instantlyCampaignId,
    leadEmail: input.leadEmail,
    accountEmail: null,
    step: null,
    variant: null,
    timestamp: input.qualifiedAt,
    rawPayload: input.rawPayload as Record<string, unknown> | null | undefined,
    source: "manual",
    sourceRowId: input.bronzeRowId,
    inferred: false,
    inferredFromEventId: null,
    inferredRule: null,
  });

  // 3. Pin reply_classification + source='manual'. Manual wins over webhook
  //    auto — subsequent webhook events do not overwrite.
  await db
    .update(instantlyCampaigns)
    .set({
      replyClassification: MANUAL_QUALIFICATION_CLASSIFICATION[input.status],
      replyClassificationSource: "manual",
      updatedAt: new Date(),
    })
    .where(eq(instantlyCampaigns.instantlyCampaignId, input.instantlyCampaignId));

  await refreshLeadStatusCurrent(input.instantlyCampaignId, input.leadEmail);

  console.log(
    `[instantly-service] manual qualification applied: campaign=${input.instantlyCampaignId} lead=${input.leadEmail} status=${input.status}`,
  );
}

export interface ListManualQualificationsInput {
  orgId: string;
  campaignId?: string;
  leadEmail?: string;
  limit?: number;
}

/**
 * List manual qualifications scoped to an org, sorted by qualified_at DESC.
 * Optional filters by logical `campaign_id` and `lead_email`. All requests are
 * org-scoped — cross-org reads return empty.
 */
export async function listManualQualifications(
  input: ListManualQualificationsInput,
): Promise<ManualQualificationRow[]> {
  const conditions = [eq(instantlyManualQualificationsRaw.orgId, input.orgId)];
  if (input.campaignId) {
    conditions.push(eq(instantlyManualQualificationsRaw.campaignId, input.campaignId));
  }
  if (input.leadEmail) {
    conditions.push(eq(instantlyManualQualificationsRaw.leadEmail, input.leadEmail));
  }

  const rows = await db
    .select()
    .from(instantlyManualQualificationsRaw)
    .where(and(...conditions))
    .orderBy(desc(instantlyManualQualificationsRaw.qualifiedAt))
    .limit(input.limit ?? 200);

  return rows.map(toRow);
}
