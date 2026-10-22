/**
 * Cutting the queued follow-ups of NAMED leads a campaign emailed IN ERROR.
 *
 * A sourcing bug can hand a campaign people it was never meant to write to
 * (LEAK-1012: a "thanks for signing up" campaign got Apollo prospects). The
 * first email is gone; what is still ours to prevent is the rest of the
 * sequence. A customer stop keeps follow-ups by rule (`stopped-campaigns.ts`),
 * so this is the per-lead tool: the campaign names the leads, every other lead
 * of that campaign keeps its follow-ups.
 *
 * What it is NOT: an opt-out, an unsubscribe, a suppression or a blocklist.
 * Those people never asked us to stop. Nothing here writes a silver event, an
 * opt-out record or anything a serve gate reads; it only stops the sequence the
 * same way every other stop does (`stopSelfSendSequence`: refund the queued
 * holds, then mark the row so the dispatcher no longer sees it; on the Instantly
 * transport, pause at Instantly FIRST).
 *
 * Properties:
 *  - All or nothing on the NAMES: one named email the campaign never wrote to
 *    = `LeadsNotInCampaignError`, nothing cut.
 *  - Idempotent: one audit row per sequence (unique index); a second cut of the
 *    same lead answers `already_cut` and touches nothing.
 *  - Auditable: who (`cut_by` = x-user-id), when, why, holds refunded.
 *  - Fail loud: after the stop, any hold still `provisioned` means a follow-up
 *    could still go out; that lead is `failed` (no audit row, so a retry
 *    re-runs it) and the route answers 502.
 */
import { errorMessage } from "./db-error.js";
import { and, eq, inArray, sql } from "drizzle-orm";

import { db } from "../db";
import { instantlyCampaigns, instantlyFollowupCuts } from "../db/schema";
import { resolveInstantlyApiKey, type CallerInfo } from "./key-client";
import { updateCampaignStatus } from "./instantly-client";
import { isSelfSendCampaignId } from "./self-send/transport";
import { stopSelfSendSequence } from "./self-send/stop-sequence";
import { refreshLeadStatusCurrent } from "./status-gold";
import { normalizeLeadEmail } from "./lead-email";

export class LeadsNotInCampaignError extends Error {
  constructor(public readonly emails: string[]) {
    super(`campaign has no sequence for: ${emails.join(", ")}`);
    this.name = "LeadsNotInCampaignError";
  }
}

export type FollowupCutOutcome = "cut" | "already_cut" | "failed";

export interface FollowupCutResult {
  email: string;
  outcome: FollowupCutOutcome;
  /** Queued steps refunded by THIS call (0 on `already_cut`). */
  holdsCancelled: number;
  /** Set on `already_cut`: the original cut. */
  cutAt: string | null;
  cutBy: string | null;
  error?: string;
}

export interface CutFollowupsInput {
  orgId: string;
  userId: string;
  campaignId: string;
  leadEmails: string[];
  reason: string;
  caller: CallerInfo;
}

interface SequenceRow {
  instantlyCampaignId: string;
  campaignId: string | null;
  orgId: string;
  userId: string | null;
  runId: string | null;
  leadEmail: string;
}

/** Queued (provisioned) holds of one sequence, same match as `matchesHoldCampaign`. */
async function countQueuedHolds(row: SequenceRow): Promise<number> {
  const result = await db.execute(sql`
    SELECT COUNT(*)::int AS n
    FROM sequence_costs s
    WHERE s.status = 'provisioned'
      AND s.lead_email = ${row.leadEmail}
      AND (
        s.instantly_campaign_id = ${row.instantlyCampaignId}
        OR (s.instantly_campaign_id IS NULL AND s.campaign_id = ${row.campaignId})
      )
  `);
  const raw: unknown = result;
  const rows: Array<{ n: number }> = Array.isArray(raw)
    ? (raw as Array<{ n: number }>)
    : ((raw as { rows?: Array<{ n: number }> }).rows ?? []);
  return Number(rows[0]?.n ?? 0);
}

export function normalizeCutEmails(emails: string[]): string[] {
  return [...new Set(emails.map(normalizeLeadEmail).filter((e) => e.length > 0))];
}

export async function cutFollowups(input: CutFollowupsInput): Promise<FollowupCutResult[]> {
  const emails = normalizeCutEmails(input.leadEmails);

  const rows: SequenceRow[] = await db
    .select({
      instantlyCampaignId: instantlyCampaigns.instantlyCampaignId,
      campaignId: instantlyCampaigns.campaignId,
      orgId: instantlyCampaigns.orgId,
      userId: instantlyCampaigns.userId,
      runId: instantlyCampaigns.runId,
      leadEmail: instantlyCampaigns.leadEmail,
    })
    .from(instantlyCampaigns)
    .where(
      and(
        eq(instantlyCampaigns.orgId, input.orgId),
        eq(instantlyCampaigns.campaignId, input.campaignId),
        inArray(sql`lower(${instantlyCampaigns.leadEmail})`, emails),
      ),
    ) as SequenceRow[];

  const byEmail = new Map<string, SequenceRow>();
  for (const row of rows) byEmail.set(normalizeLeadEmail(row.leadEmail), row);
  const missing = emails.filter((e) => !byEmail.has(e));
  if (missing.length > 0) throw new LeadsNotInCampaignError(missing);

  const existing = await db
    .select()
    .from(instantlyFollowupCuts)
    .where(
      inArray(
        instantlyFollowupCuts.instantlyCampaignId,
        rows.map((r) => r.instantlyCampaignId),
      ),
    );
  const cutBySequence = new Map(existing.map((c) => [c.instantlyCampaignId, c]));

  const results: FollowupCutResult[] = [];
  for (const email of emails) {
    const row = byEmail.get(email)!;
    const prior = cutBySequence.get(row.instantlyCampaignId);
    if (prior) {
      results.push({
        email,
        outcome: "already_cut",
        holdsCancelled: 0,
        cutAt: prior.cutAt.toISOString(),
        cutBy: prior.cutBy,
      });
      continue;
    }

    const reason = `follow-ups cut by ${input.userId}: ${input.reason}`;
    try {
      const queuedBefore = await countQueuedHolds(row);
      if (!isSelfSendCampaignId(row.instantlyCampaignId)) {
        const { key } = await resolveInstantlyApiKey(row.orgId, "system", input.caller);
        await updateCampaignStatus(key, row.instantlyCampaignId, "paused");
      }
      await stopSelfSendSequence(row, row.leadEmail, reason);

      const queuedAfter = await countQueuedHolds(row);
      if (queuedAfter > 0) {
        throw new Error(`${queuedAfter} queued step(s) could not be cancelled`);
      }

      const [audit] = await db
        .insert(instantlyFollowupCuts)
        .values({
          orgId: row.orgId,
          campaignId: input.campaignId,
          instantlyCampaignId: row.instantlyCampaignId,
          leadEmail: row.leadEmail,
          reason: input.reason,
          cutBy: input.userId,
          holdsCancelled: queuedBefore,
        })
        .onConflictDoNothing()
        .returning();

      await refreshLeadStatusCurrent(row.instantlyCampaignId, row.leadEmail);

      results.push({
        email,
        outcome: "cut",
        holdsCancelled: queuedBefore,
        cutAt: audit ? audit.cutAt.toISOString() : null,
        cutBy: input.userId,
      });
      console.log(
        `[instantly-service] followup-cuts: cut campaign=${input.campaignId} sequence=${row.instantlyCampaignId} lead=${row.leadEmail} holds=${queuedBefore} by=${input.userId} (${input.reason})`,
      );
    } catch (error: unknown) {
      const message = errorMessage(error);
      console.error(
        `[instantly-service] followup-cuts: FAILED campaign=${input.campaignId} sequence=${row.instantlyCampaignId} lead=${row.leadEmail} — ${message}`,
      );
      results.push({ email, outcome: "failed", holdsCancelled: 0, cutAt: null, cutBy: null, error: message });
    }
  }
  return results;
}

export async function listFollowupCuts(orgId: string, campaignId: string) {
  return db
    .select()
    .from(instantlyFollowupCuts)
    .where(and(eq(instantlyFollowupCuts.orgId, orgId), eq(instantlyFollowupCuts.campaignId, campaignId)))
    .orderBy(instantlyFollowupCuts.cutAt);
}
