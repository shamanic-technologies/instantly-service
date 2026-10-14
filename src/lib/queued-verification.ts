/**
 * A queued sequence is verified before it dispatches (`email-verification-gate.ts`).
 *
 * `POST /orgs/send` verifies every NEW sequence at the door. This sweep covers
 * what was queued before the door existed, and anything that reaches the queue
 * without a door decision (a brand transferred to another org, a send whose
 * verifier answer failed): every active sequence still holding a provisioned
 * step whose address has no ACCEPTED decision for its org is verified here,
 * once per dispatch tick, BEFORE anything is selected (`runDispatch`).
 *
 *  - accepted verdict (valid / catch_all / unknown): recorded, dispatches as before.
 *    The price it was queued at is kept (never re-priced retroactively).
 *  - refused verdict (invalid / risky): the sequence is STOPPED and its queued
 *    holds refunded, same two pipes as `booked-stops-cold.ts`.
 *  - no verdict (verifier down, org cannot pay): recorded, the sequence is HELD
 *    (nothing sent) and asked again next tick.
 *
 * Billed on the sequence's own run (`instantly_campaigns.run_id`, the run of the
 * campaign that sent it), to its org. apollo-service reuses a verdict under 30
 * days old for free, which is most of the backlog (prod 2026-10-14: 1,460 valid,
 * 187 catch_all, 1 invalid, 4 never verified of 1,652 queued addresses).
 *
 * FAILS LOUD when the queue cannot be read: the dispatcher must not send on a
 * guess. One sequence's failure is logged, counted and held; the rest proceed.
 */

import { sql } from "drizzle-orm";

import { db } from "../db";
import { gateAddresses, type GateDecision } from "./email-verification-gate";
import { updateCampaignStatus } from "./instantly-client";
import { resolveInstantlyApiKey, type CallerInfo } from "./key-client";
import { normalizeLeadEmail } from "./recontact-window";
import { isSelfSendCampaignId } from "./self-send/transport";
import { stopSelfSendSequence } from "./self-send/stop-sequence";

/** Sequences verified per tick; the rest stay held until a later tick reaches them. */
export const QUEUED_VERIFY_LIMIT = 2000;
/** Parallel apollo calls (each a single address on its own run). */
export const QUEUED_VERIFY_CONCURRENCY = 8;

export interface UnverifiedQueuedSequence {
  instantlyCampaignId: string;
  campaignId: string | null;
  orgId: string;
  userId: string | null;
  runId: string | null;
  leadEmail: string;
  brandIds: string[];
}

export interface QueuedVerificationSummary {
  /** Queued sequences with no accepted verdict at the start of the tick. */
  unverified: number;
  verified: number;
  accepted: number;
  /** invalid / risky: stopped and refunded. */
  stopped: number;
  /** No verdict (verifier, credits, no identity) or the stop failed: nothing sent, retried next tick. */
  held: number;
  /** Beyond this tick's limit: nothing sent, verified on a later tick. */
  deferred: number;
}

export interface QueuedVerification {
  summary: QueuedVerificationSummary;
  /** `instantly_campaign_id`s that MUST NOT be sent this tick. */
  notCleared: Set<string>;
}

function rowsOf(result: unknown): Record<string, unknown>[] {
  if (Array.isArray(result)) return result as Record<string, unknown>[];
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

/**
 * Every active sequence still holding a provisioned step whose address has no
 * accepted gate decision for its org (latest decision absent or a refusal). Hold
 * match mirrors `matchesHoldCampaign`. Oldest first, so the oldest prospects are
 * cleared first.
 */
async function loadUnverifiedQueuedSequences(): Promise<UnverifiedQueuedSequence[]> {
  const result = await db.execute(sql`
    SELECT c.instantly_campaign_id AS "instantlyCampaignId",
           c.campaign_id           AS "campaignId",
           c.org_id                AS "orgId",
           c.user_id               AS "userId",
           c.run_id                AS "runId",
           c.lead_email            AS "leadEmail",
           to_jsonb(COALESCE(c.brand_ids, ARRAY[]::text[])) AS "brandIds"
    FROM instantly_campaigns c
    LEFT JOIN LATERAL (
      SELECT v.accepted
      FROM email_send_verifications v
      WHERE v.org_id = c.org_id
        AND v.email = lower(trim(c.lead_email))
      ORDER BY v.created_at DESC
      LIMIT 1
    ) g ON true
    WHERE c.status = 'active'
      AND c.lead_email IS NOT NULL
      AND c.org_id IS NOT NULL
      AND c.instantly_campaign_id NOT LIKE 'reserving:%'
      AND g.accepted IS NOT TRUE
      AND EXISTS (
        SELECT 1 FROM sequence_costs sc
        WHERE sc.status = 'provisioned'
          AND sc.lead_email = c.lead_email
          AND (sc.instantly_campaign_id = c.instantly_campaign_id
               OR (sc.instantly_campaign_id IS NULL AND sc.campaign_id = c.campaign_id))
      )
    ORDER BY c.created_at ASC
  `);
  return rowsOf(result).map((r) => ({
    instantlyCampaignId: String(r.instantlyCampaignId),
    campaignId: r.campaignId == null ? null : String(r.campaignId),
    orgId: String(r.orgId),
    userId: r.userId == null ? null : String(r.userId),
    runId: r.runId == null ? null : String(r.runId),
    leadEmail: String(r.leadEmail),
    brandIds: Array.isArray(r.brandIds) ? (r.brandIds as string[]) : [],
  }));
}

async function stopRefusedSequence(
  seq: UnverifiedQueuedSequence,
  decision: GateDecision,
  caller: CallerInfo,
): Promise<void> {
  const reason = `email not verified: ${decision.verdict} (${decision.reason})`;
  if (isSelfSendCampaignId(seq.instantlyCampaignId)) {
    await stopSelfSendSequence(seq, seq.leadEmail, reason);
    return;
  }
  // Instantly: pause first (that is what stops the email), only then refund and mark.
  const { key } = await resolveInstantlyApiKey(seq.orgId, "system", caller);
  await updateCampaignStatus(key, seq.instantlyCampaignId, "paused");
  await stopSelfSendSequence(seq, seq.leadEmail, reason);
}

async function verifyOne(
  seq: UnverifiedQueuedSequence,
  caller: CallerInfo,
  summary: QueuedVerificationSummary,
  notCleared: Set<string>,
): Promise<void> {
  if (!seq.userId || !seq.runId) {
    // No identity to bill a verification to: never sent on a guess.
    summary.held += 1;
    notCleared.add(seq.instantlyCampaignId);
    console.error(
      `[instantly-service] queued-verification: HELD campaign=${seq.instantlyCampaignId} lead=${seq.leadEmail} — no user/run to bill its verification to`,
    );
    return;
  }
  try {
    const decisions = await gateAddresses(
      [seq.leadEmail],
      {
        orgId: seq.orgId,
        userId: seq.userId,
        runId: seq.runId,
        brandIds: seq.brandIds,
        campaignId: seq.campaignId,
      },
      "queued",
    );
    const decision = decisions.get(normalizeLeadEmail(seq.leadEmail));
    if (!decision) throw new Error("no decision returned");
    summary.verified += 1;
    if (decision.accepted) {
      summary.accepted += 1;
      return;
    }
    notCleared.add(seq.instantlyCampaignId);
    if (decision.reason === "verifier_failed" || decision.reason === "insufficient_credits") {
      summary.held += 1;
      return;
    }
    await stopRefusedSequence(seq, decision, caller);
    summary.stopped += 1;
  } catch (error: unknown) {
    summary.held += 1;
    notCleared.add(seq.instantlyCampaignId);
    console.error(
      `[instantly-service] queued-verification: HELD campaign=${seq.instantlyCampaignId} lead=${seq.leadEmail} — ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}

/**
 * Verify every queued sequence without an accepted verdict, stop the refused
 * ones, and return the ids the dispatcher must not send this tick.
 */
export async function verifyQueuedSequences(
  caller: CallerInfo,
  limit: number = QUEUED_VERIFY_LIMIT,
): Promise<QueuedVerification> {
  const queued = await loadUnverifiedQueuedSequences();
  const summary: QueuedVerificationSummary = {
    unverified: queued.length,
    verified: 0,
    accepted: 0,
    stopped: 0,
    held: 0,
    deferred: 0,
  };
  const notCleared = new Set<string>(queued.slice(limit).map((s) => s.instantlyCampaignId));
  summary.deferred = notCleared.size;

  const work = queued.slice(0, limit);
  let next = 0;
  const workers = Array.from({ length: Math.min(QUEUED_VERIFY_CONCURRENCY, work.length) }, async () => {
    while (next < work.length) {
      const seq = work[next++];
      await verifyOne(seq, caller, summary, notCleared);
    }
  });
  await Promise.all(workers);

  if (queued.length > 0) {
    console.log(`[instantly-service] queued-verification: done ${JSON.stringify(summary)}`);
  }
  return { summary, notCleared };
}
