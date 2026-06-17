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
  instantlyCampaignId: string;
  leadEmail: string;
  status: ManualQualificationStatus;
  qualifiedAt: Date;
  rawPayload: unknown;
}

/**
 * Side effects after a manual qualification is inserted into bronze:
 *  1. **Synthesize a `reply_received` silver event** (source='manual'). The
 *     human is asserting "this lead replied — Instantly missed it", so the
 *     reply event MUST exist in silver for `/orgs/status` to report
 *     `replied=true`. Routed through `promoteEvent` so the normal side
 *     effects fire: `delivery_status='replied'` AND remaining provisioned
 *     costs are cancelled (sequence stops on reply).
 *  2. Mirror the lead-status event (`lead_interested` / `lead_not_interested`
 *     / etc.) in silver via direct insert. Kept as a direct insert so we
 *     can also set `replyClassificationSource='manual'` below — going
 *     through `promoteEvent` would update `replyClassification` from the
 *     status map but not the source field.
 *  3. Set `reply_classification` to the derived positive/negative/neutral
 *     value and pin `reply_classification_source='manual'` so subsequent
 *     webhook events do not overwrite the manual choice.
 */
export async function applyManualQualificationSideEffects(
  input: ApplyManualQualificationSideEffectsInput,
): Promise<void> {
  // 1. Synthesize the reply_received event so `/orgs/status.replied` reports
  //    true. `promoteEvent` handles the one-shot dedupe: if a real reply
  //    event already exists (Instantly auto-detected too), this is a no-op.
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
