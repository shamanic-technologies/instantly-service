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
  instantlyManualQualificationWithdrawals,
  instantlyManualQualificationsRaw,
} from "../db/schema";
import { and, desc, eq, isNull, sql } from "drizzle-orm";
import { promoteEvent } from "./silver-promote";
import { refreshLeadStatusCurrent } from "./status-gold";
import { resolveInstantlyApiKey } from "./key-client";
import { updateCampaignStatus } from "./instantly-client";
import { isSelfSendCampaignId } from "./self-send/transport";
import { stopSelfSendSequence } from "./self-send/stop-sequence";
import {
  ACCEPTED_QUALIFICATION_STATUSES,
  REPLY_KINDS,
  REPLY_KIND_CLASSIFICATION,
  isReplyKind,
  isSequenceStoppingReplyKind,
  resolveReplyKind,
  type AcceptedQualificationStatus,
  type ReplyKind,
} from "./reply-kind";

/**
 * What the write path accepts: the reply-kind vocabulary plus the two legacy
 * deal-progress values the staff console is still sending today. Every value is
 * RESOLVED to a reply kind at write (`resolveReplyKind`) — see lib/reply-kind.
 * Removing the legacy pair from this list is a separate, later change, once
 * both dashboards ship their new pickers.
 */
export const MANUAL_QUALIFICATION_STATUSES = ACCEPTED_QUALIFICATION_STATUSES;

export type ManualQualificationStatus = AcceptedQualificationStatus;

/** True iff this manual qualification means the sequence must stop. */
export function isSequenceStoppingQualification(
  status: ManualQualificationStatus,
): boolean {
  return isSequenceStoppingReplyKind(resolveReplyKind(status));
}

export interface ManualQualificationRow {
  id: string;
  orgId: string;
  campaignId: string;
  instantlyCampaignId: string;
  leadEmail: string;
  /** The raw human statement, exactly as it was clicked. Append-only. */
  status: ManualQualificationStatus;
  /** The reply kind `status` resolves to — the new vocabulary, frozen at write. */
  replyKind: ReplyKind;
  qualifiedBy: string;
  notes: string | null;
  qualifiedAt: Date;
  /** When this statement was WITHDRAWN, or null while it still stands. A
   *  withdrawn statement is kept for audit and must not be rendered as a kind
   *  anybody stands behind. */
  withdrawnAt: Date | null;
  /** Who withdrew it, or null while it still stands. */
  withdrawnBy: string | null;
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

function toRow(
  raw: {
    id: string;
    orgId: string;
    campaignId: string;
    instantlyCampaignId: string;
    leadEmail: string;
    status: string;
    replyKind: string;
    qualifiedBy: string;
    notes: string | null;
    qualifiedAt: Date;
  },
  withdrawal?: { withdrawnAt: Date; withdrawnBy: string } | null,
): ManualQualificationRow {
  return {
    id: raw.id,
    orgId: raw.orgId,
    campaignId: raw.campaignId,
    instantlyCampaignId: raw.instantlyCampaignId,
    leadEmail: raw.leadEmail,
    status: raw.status as ManualQualificationStatus,
    replyKind: raw.replyKind as ReplyKind,
    qualifiedBy: raw.qualifiedBy,
    notes: raw.notes,
    qualifiedAt: raw.qualifiedAt,
    withdrawnAt: withdrawal?.withdrawnAt ?? null,
    withdrawnBy: withdrawal?.withdrawnBy ?? null,
  };
}

/**
 * The STANDING human statement for a (org, instantly_campaign, lead) pair — the
 * latest bronze row that has NOT been withdrawn, or null when nobody currently
 * stands behind a kind for this lead.
 *
 * "Latest that is not withdrawn", not "latest": a withdrawn statement is still
 * the most recent ROW (bronze is append-only, nothing is rewritten), so reading
 * the plain latest would report a kind the person has explicitly retracted.
 */
export async function findStandingManualQualification(
  orgId: string,
  instantlyCampaignId: string,
  leadEmail: string,
): Promise<ManualQualificationRow | null> {
  const [row] = await db
    .select({ q: instantlyManualQualificationsRaw })
    .from(instantlyManualQualificationsRaw)
    .leftJoin(
      instantlyManualQualificationWithdrawals,
      eq(
        instantlyManualQualificationWithdrawals.qualificationId,
        instantlyManualQualificationsRaw.id,
      ),
    )
    .where(
      and(
        eq(instantlyManualQualificationsRaw.orgId, orgId),
        eq(instantlyManualQualificationsRaw.instantlyCampaignId, instantlyCampaignId),
        eq(instantlyManualQualificationsRaw.leadEmail, leadEmail),
        isNull(instantlyManualQualificationWithdrawals.id),
      ),
    )
    .orderBy(desc(instantlyManualQualificationsRaw.qualifiedAt))
    .limit(1);
  return row ? toRow(row.q) : null;
}

/**
 * Insert a new manual qualification row in bronze. Idempotent: if the STANDING
 * statement for (org, instantly_campaign, lead_email) already matches `status`,
 * returns { inserted: false, row: existing } without writing. A withdrawn
 * statement is not standing, so re-stating the same kind after a withdrawal
 * records it again rather than being swallowed as a no-op.
 */
export async function insertManualQualification(
  input: InsertManualQualificationInput,
): Promise<InsertManualQualificationResult> {
  const existing = await findStandingManualQualification(
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
      // Resolve at WRITE, never at read: bronze keeps the raw intent, every
      // reader downstream sees the new vocabulary only. Throws on an
      // unresolvable status — a statement we cannot record fails loudly.
      replyKind: resolveReplyKind(input.status),
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
  /** The reply kind the statement resolved to at write. */
  replyKind: ReplyKind;
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
  if (isSequenceStoppingReplyKind(input.replyKind)) {
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
    // The RESOLVED kind, not the raw statement — silver carries the new
    // vocabulary only, so no reader ever has to translate a legacy value.
    eventType: input.replyKind,
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
      replyClassification: REPLY_KIND_CLASSIFICATION[input.replyKind],
      replyClassificationSource: "manual",
      updatedAt: new Date(),
    })
    .where(eq(instantlyCampaigns.instantlyCampaignId, input.instantlyCampaignId));

  await refreshLeadStatusCurrent(input.instantlyCampaignId, input.leadEmail);

  console.log(
    `[instantly-service] manual qualification applied: campaign=${input.instantlyCampaignId} lead=${input.leadEmail} status=${input.status} replyKind=${input.replyKind}`,
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
    .select({
      q: instantlyManualQualificationsRaw,
      w: {
        withdrawnAt: instantlyManualQualificationWithdrawals.withdrawnAt,
        withdrawnBy: instantlyManualQualificationWithdrawals.withdrawnBy,
      },
    })
    .from(instantlyManualQualificationsRaw)
    .leftJoin(
      instantlyManualQualificationWithdrawals,
      eq(
        instantlyManualQualificationWithdrawals.qualificationId,
        instantlyManualQualificationsRaw.id,
      ),
    )
    .where(and(...conditions))
    .orderBy(desc(instantlyManualQualificationsRaw.qualifiedAt))
    .limit(input.limit ?? 200);

  // Every statement ever made is returned, withdrawn ones included — that is
  // the audit. The caller tells them apart by `withdrawnAt`; a row carrying one
  // is a kind nobody stands behind and must not be rendered as current.
  return rows.map((row) =>
    toRow(row.q, row.w?.withdrawnAt ? { withdrawnAt: row.w.withdrawnAt, withdrawnBy: row.w.withdrawnBy! } : null),
  );
}

export interface WithdrawManualQualificationInput {
  orgId: string;
  instantlyCampaignId: string;
  leadEmail: string;
  withdrawnBy: string;
  notes?: string;
}

export type WithdrawManualQualificationResult =
  | { withdrawn: true; qualification: ManualQualificationRow }
  /** Nobody stands behind a kind for this pair — there is nothing to take back. */
  | { withdrawn: false; reason: "no_standing_qualification" };

/**
 * Withdraw the standing human statement for a (campaign, lead) pair.
 *
 * A correction, not an erasure. Nothing is deleted and no "none" value enters
 * the reply-kind vocabulary: the statement row stays exactly as it was written
 * and a withdrawal row is APPENDED beside it, so both what was stated and the
 * fact that it was taken back stay readable.
 *
 * Refuses with `no_standing_qualification` when nothing is standing — including
 * a second withdrawal of the same statement, which finds nothing standing and
 * writes nothing. Idempotent by construction.
 *
 * SCOPE, stated rather than papered over: this withdraws the SENTIMENT — the
 * kind, and the pin that froze it. It does NOT retract the separate assertion
 * that a reply arrived at all. On a sequence-stopping statement the write
 * synthesized a `reply_received` event, which stopped the sequence, cancelled
 * the remaining holds and paused the campaign at Instantly; those are real,
 * already-taken, irreversible actions, and un-asserting the reply while its
 * consequences stand would be incoherent. The resulting state — a reply on
 * record with no kind attached — is exactly the state of an auto-detected reply
 * nobody has qualified, which is the state this endpoint exists to restore.
 */
export async function withdrawManualQualification(
  input: WithdrawManualQualificationInput,
): Promise<WithdrawManualQualificationResult> {
  const standing = await findStandingManualQualification(
    input.orgId,
    input.instantlyCampaignId,
    input.leadEmail,
  );
  if (!standing) return { withdrawn: false, reason: "no_standing_qualification" };

  const [withdrawal] = await db
    .insert(instantlyManualQualificationWithdrawals)
    .values({
      qualificationId: standing.id,
      orgId: standing.orgId,
      campaignId: standing.campaignId,
      instantlyCampaignId: standing.instantlyCampaignId,
      leadEmail: standing.leadEmail,
      withdrawnBy: input.withdrawnBy,
      notes: input.notes ?? null,
    })
    .returning();

  await applyManualQualificationWithdrawalSideEffects({
    bronzeRowId: standing.id,
    instantlyCampaignId: standing.instantlyCampaignId,
    leadEmail: standing.leadEmail,
    withdrawnAt: withdrawal.withdrawnAt,
  });

  return {
    withdrawn: true,
    qualification: {
      ...standing,
      withdrawnAt: withdrawal.withdrawnAt,
      withdrawnBy: withdrawal.withdrawnBy,
    },
  };
}

export interface ApplyManualQualificationWithdrawalSideEffectsInput {
  bronzeRowId: string;
  instantlyCampaignId: string;
  leadEmail: string;
  withdrawnAt: Date;
}

/**
 * Release everything the withdrawn statement moved:
 *
 *  1. Mark its silver mirror event withdrawn. The row is kept (silver records
 *     what was asserted), but the gold current-sentiment projection skips a
 *     withdrawn row, so the counters a manual statement moved stop counting it.
 *  2. Recompute `reply_classification` from what is LEFT — the latest silver
 *     reply-kind event that is not withdrawn, under the same ordering gold
 *     uses — and release the pin by setting the source back to 'auto'. With no
 *     event left the classification is NULL: nothing at all, which is the
 *     honest reading when nobody has said anything. Releasing the pin is what
 *     lets a subsequent webhook classify the reply as it normally would.
 *  3. Refresh the gold status row so the read path converges immediately.
 *
 * Fail loud — a withdrawal that cannot release the pin must not report success.
 */
export async function applyManualQualificationWithdrawalSideEffects(
  input: ApplyManualQualificationWithdrawalSideEffectsInput,
): Promise<void> {
  // 1. The mirror event of THIS statement, identified by the bronze row it was
  //    promoted from. Only manual rows carry a source_row_id, so this can never
  //    touch a webhook event.
  await db
    .update(instantlyEvents)
    .set({ withdrawnAt: input.withdrawnAt })
    .where(
      and(
        eq(instantlyEvents.sourceRowId, input.bronzeRowId),
        eq(instantlyEvents.source, "manual"),
        eq(instantlyEvents.campaignId, input.instantlyCampaignId),
        eq(instantlyEvents.leadEmail, input.leadEmail),
        isNull(instantlyEvents.withdrawnAt),
      ),
    );

  // 2. What the automatic classification says now that the human statement is
  //    gone — the same latest-wins ordering as the gold sentiment projection.
  const kindList = sql.join(
    REPLY_KINDS.map((kind) => sql`${kind}`),
    sql`, `,
  );
  const remaining = await db.execute<{ event_type: string }>(sql`
    SELECT e.event_type
    FROM instantly_events e
    WHERE e.campaign_id = ${input.instantlyCampaignId}
      AND e.lead_email = ${input.leadEmail}
      AND e.event_type IN (${kindList})
      AND e.withdrawn_at IS NULL
    ORDER BY e.timestamp DESC, (e.source = 'manual') DESC, e.created_at DESC, e.id DESC
    LIMIT 1
  `);
  const remainingKind = remaining.rows[0]?.event_type;
  const classification =
    remainingKind && isReplyKind(remainingKind)
      ? REPLY_KIND_CLASSIFICATION[remainingKind]
      : null;

  await db
    .update(instantlyCampaigns)
    .set({
      replyClassification: classification,
      // The pin is released whatever is left: a classification that survives a
      // withdrawal is the automatic one, so 'auto' is the truthful source.
      replyClassificationSource: "auto",
      updatedAt: new Date(),
    })
    .where(eq(instantlyCampaigns.instantlyCampaignId, input.instantlyCampaignId));

  await refreshLeadStatusCurrent(input.instantlyCampaignId, input.leadEmail);

  console.log(
    `[instantly-service] manual qualification withdrawn: campaign=${input.instantlyCampaignId} lead=${input.leadEmail} replyClassification=${classification ?? "null"}`,
  );
}
