/**
 * FOLLOWUP-JIT-1107 (owner 2026-11-07): follow-ups are written and paid on their
 * DUE day by the follow-up path (lead-service), no longer queued and held with
 * the first email. A client who tops up $10 expects most of it to buy new
 * conversations; a follow-up held 3 to 10 days ahead froze about a third of it.
 *
 * This is the ONE-TIME cut of what the old model left queued: every follow-up
 * step (step 2 and later) still waiting on a sequence we dispatch has its hold
 * cancelled, so the money goes back to the org's balance at once. The owner chose
 * to cancel them and give the money back rather than let them drain.
 *
 * What it is NOT:
 *  - not a stop of the lead: the row stays as it is (a first email still queued
 *    leaves as planned), and the lead's next follow-up is written on its due day
 *    by lead-service, through `POST /orgs/send` `followUp: true`. Once nothing of
 *    the sequence is queued, that send is no longer refused `brand_sequence_queued`.
 *  - not a reversal of the 2026-10-06 rule ("stopping a campaign does not pause
 *    the follow-ups") nor of OLIVE-FOLLOWUPS-1103 ("a contacted lead keeps every
 *    follow-up"): the lead still gets follow-ups, written later.
 *  - never a first email: step 1 is never selected, sent or not.
 *
 * Population = a RULE, never a list: every `provisioned` hold of step >= 2 on a
 * self-sent sequence (`self:`). An Instantly-transport hold is reported, never
 * touched (the plan is cancelled: none exist in prod 2026-11-07).
 *
 * Idempotent: a cancelled hold is no longer `provisioned`, so a re-run selects
 * nothing and writes nothing. Audited per lead in `followup_jit_cuts` (steps,
 * holds, dollars released). `restore-cut-followups` never puts these back.
 * Fail loud per sequence: a hold runs-service cannot cancel stays `provisioned`
 * (re-run takes it), the sequence is counted `failed`, no audit row.
 */
import { errorMessage } from "./db-error.js";
import { eq, sql } from "drizzle-orm";

import { db } from "../db";
import { followupJitCuts, sequenceCosts } from "../db/schema";
import { announceEvidenceChanged } from "./evidence-changed";
import { holdCostIds, settleHoldCost } from "./hold-settlement";
import { getRunWithCosts, isRunGoneError, type IdentityContext } from "./runs-client";
import { refreshLeadStatusCurrent } from "./status-gold";

const NIL_USER_UUID = "00000000-0000-0000-0000-000000000000";
const SELF_SEND_PREFIX = "self:";
/** Parallel runs-service calls (dry-run reads, commit sequences). */
export const FOLLOWUP_JIT_CONCURRENCY = 8;

export interface QueuedFollowupHold {
  holdId: string;
  instantlyCampaignId: string;
  campaignId: string | null;
  orgId: string | null;
  userId: string | null;
  brandIds: string[];
  leadEmail: string;
  step: number;
  runId: string;
  costId: string | null;
  domainCostId: string | null;
}

export interface QueuedFollowupSequence {
  instantlyCampaignId: string;
  campaignId: string | null;
  orgId: string | null;
  userId: string | null;
  brandIds: string[];
  leadEmail: string;
  holds: QueuedFollowupHold[];
}

function rowsOf(result: unknown): Record<string, unknown>[] {
  if (Array.isArray(result)) return result as Record<string, unknown>[];
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

/** Every queued follow-up hold (step >= 2), any transport. */
export async function loadQueuedFollowupHolds(
  filter: { orgId?: string; instantlyCampaignIds?: string[] } = {},
): Promise<QueuedFollowupHold[]> {
  const ids = filter.instantlyCampaignIds ?? null;
  if (ids !== null && ids.length === 0) return [];
  const idsParam = ids === null ? null : sql.param(ids);
  const orgParam = filter.orgId ?? null;
  const result = await db.execute(sql`
    SELECT sc.id                    AS "holdId",
           c.instantly_campaign_id  AS "instantlyCampaignId",
           c.campaign_id            AS "campaignId",
           c.org_id                 AS "orgId",
           c.user_id                AS "userId",
           COALESCE(to_jsonb(c.brand_ids), '[]'::jsonb) AS "brandIds",
           c.lead_email             AS "leadEmail",
           sc.step                  AS "step",
           sc.run_id                AS "runId",
           sc.cost_id               AS "costId",
           sc.domain_cost_id        AS "domainCostId"
    FROM sequence_costs sc
    JOIN instantly_campaigns c ON c.instantly_campaign_id = sc.instantly_campaign_id
    WHERE sc.status = 'provisioned'
      AND sc.step >= 2
      AND (${idsParam}::text[] IS NULL OR c.instantly_campaign_id = ANY(${idsParam}::text[]))
      AND (${orgParam}::text IS NULL OR c.org_id = ${orgParam}::text)
    ORDER BY c.org_id, c.instantly_campaign_id, sc.step
  `);
  return rowsOf(result).map((r) => ({
    holdId: String(r.holdId),
    instantlyCampaignId: String(r.instantlyCampaignId),
    campaignId: r.campaignId == null ? null : String(r.campaignId),
    orgId: r.orgId == null ? null : String(r.orgId),
    userId: r.userId == null ? null : String(r.userId),
    brandIds: Array.isArray(r.brandIds) ? (r.brandIds as unknown[]).map(String) : [],
    leadEmail: String(r.leadEmail),
    step: Number(r.step),
    runId: String(r.runId),
    costId: r.costId == null ? null : String(r.costId),
    domainCostId: r.domainCostId == null ? null : String(r.domainCostId),
  }));
}

/** Pure: holds grouped per sequence, first-seen order. Step 1 never passes. */
export function groupBySequence(holds: QueuedFollowupHold[]): QueuedFollowupSequence[] {
  const bySeq = new Map<string, QueuedFollowupSequence>();
  for (const h of holds) {
    if (h.step < 2) continue;
    let seq = bySeq.get(h.instantlyCampaignId);
    if (!seq) {
      seq = {
        instantlyCampaignId: h.instantlyCampaignId,
        campaignId: h.campaignId,
        orgId: h.orgId,
        userId: h.userId,
        brandIds: h.brandIds,
        leadEmail: h.leadEmail,
        holds: [],
      };
      bySeq.set(h.instantlyCampaignId, seq);
    }
    seq.holds.push(h);
  }
  return [...bySeq.values()];
}

export interface BrandCutLine {
  orgId: string | null;
  brandIds: string[];
  leads: number;
  steps: number;
  /** Dry run: held now and would go back. Commit: went back. */
  usdCents: number;
}

/** Pure: per brand (the sorted brand set of the sequence), most money first. */
export function summarizeByBrand(
  sequences: QueuedFollowupSequence[],
  centsBySequence: ReadonlyMap<string, number>,
): BrandCutLine[] {
  const lines = new Map<string, BrandCutLine>();
  for (const s of sequences) {
    const brandIds = [...s.brandIds].sort();
    const key = `${s.orgId ?? ""}|${brandIds.join(",")}`;
    const line = (lines.get(key) ?? { orgId: s.orgId, brandIds, leads: 0, steps: 0, usdCents: 0 });
    line.leads += 1;
    line.steps += s.holds.length;
    line.usdCents += centsBySequence.get(s.instantlyCampaignId) ?? 0;
    lines.set(key, line);
  }
  return [...lines.values()]
    .map((l) => ({ ...l, usdCents: Math.round(l.usdCents * 1e6) / 1e6 }))
    .sort((a, b) => b.usdCents - a.usdCents || b.leads - a.leads);
}

function identityFor(seq: QueuedFollowupSequence, runId: string): IdentityContext {
  return { orgId: seq.orgId || "system", userId: seq.userId || NIL_USER_UUID, runId };
}

/** What a hold has PROVISIONED at runs-service now, in USD cents (read only). */
async function heldCents(seq: QueuedFollowupSequence, hold: QueuedFollowupHold): Promise<number> {
  const ids = new Set(holdCostIds({ id: hold.holdId, runId: hold.runId, costId: hold.costId, domainCostId: hold.domainCostId }));
  if (ids.size === 0) return 0;
  const run = await getRunWithCosts(hold.runId, identityFor(seq, hold.runId));
  if (!run) return 0;
  return run.costs
    .filter((c) => ids.has(c.id) && c.status === "provisioned")
    .reduce((sum, c) => sum + Number(c.totalCostInUsdCents), 0);
}

async function mapPool<T, R>(items: T[], limit: number, fn: (item: T) => Promise<R>): Promise<R[]> {
  const out: R[] = new Array(items.length);
  let next = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (next < items.length) {
      const i = next++;
      out[i] = await fn(items[i]);
    }
  });
  await Promise.all(workers);
  return out;
}

export interface SequenceCutResult {
  instantlyCampaignId: string;
  outcome: "cut" | "failed";
  holdsCancelled: number;
  usdCents: number;
  error?: string;
}

/** Cancel every queued follow-up hold of ONE sequence, then audit it. */
export async function cutSequence(seq: QueuedFollowupSequence): Promise<SequenceCutResult> {
  let cancelled = 0;
  let cents = 0;
  const steps: number[] = [];
  const errors: string[] = [];
  for (const hold of seq.holds) {
    if (hold.step < 2) continue; // never a first email
    const settleable = { id: hold.holdId, runId: hold.runId, costId: hold.costId, domainCostId: hold.domainCostId };
    try {
      const costs = await settleHoldCost(settleable, "cancelled", identityFor(seq, hold.runId));
      cents += costs.reduce((sum, c) => sum + Number(c?.totalCostInUsdCents ?? 0), 0);
      cancelled += 1;
      steps.push(hold.step);
    } catch (error: unknown) {
      if (isRunGoneError(error)) {
        // Run purged: nothing to refund at runs-service; the step must still leave the queue.
        await db
          .update(sequenceCosts)
          .set({ status: "cancelled", updatedAt: new Date() })
          .where(eq(sequenceCosts.id, hold.holdId));
        cancelled += 1;
        steps.push(hold.step);
      } else {
        errors.push(`step ${hold.step}: ${errorMessage(error)}`);
      }
    }
  }

  if (cancelled > 0) {
    await db
      .insert(followupJitCuts)
      .values({
        orgId: seq.orgId,
        campaignId: seq.campaignId,
        brandIds: seq.brandIds,
        instantlyCampaignId: seq.instantlyCampaignId,
        leadEmail: seq.leadEmail,
        steps,
        holdsCancelled: cancelled,
        releasedUsdCents: String(cents),
      })
      .onConflictDoUpdate({
        target: followupJitCuts.instantlyCampaignId,
        set: {
          steps: sql`(SELECT array_agg(DISTINCT s ORDER BY s) FROM unnest(${followupJitCuts.steps} || excluded.steps) s)`,
          holdsCancelled: sql`${followupJitCuts.holdsCancelled} + excluded.holds_cancelled`,
          releasedUsdCents: sql`${followupJitCuts.releasedUsdCents} + excluded.released_usd_cents`,
          updatedAt: sql`now()`,
        },
      });
    await refreshLeadStatusCurrent(seq.instantlyCampaignId, seq.leadEmail);
  }

  if (errors.length > 0) {
    return {
      instantlyCampaignId: seq.instantlyCampaignId,
      outcome: "failed",
      holdsCancelled: cancelled,
      usdCents: cents,
      error: errors.join("; "),
    };
  }
  return { instantlyCampaignId: seq.instantlyCampaignId, outcome: "cut", holdsCancelled: cancelled, usdCents: cents };
}

export interface FollowupJitCutSummary {
  dryRun: boolean;
  leads: number;
  steps: number;
  /** Dry run: held now, would go back. Commit: went back. */
  usdCents: number;
  byBrand: BrandCutLine[];
  /** Instantly-transport sequences holding a follow-up: reported, never touched. */
  skippedNotSelfSend: number;
  cut: number;
  failed: number;
  /** Dry run only: holds whose amount runs-service could not tell (read failed). */
  unreadHolds: number;
}

export async function cutQueuedFollowups(options: {
  dryRun: boolean;
  limit?: number;
  orgId?: string;
  instantlyCampaignIds?: string[];
}): Promise<FollowupJitCutSummary> {
  const all = groupBySequence(
    await loadQueuedFollowupHolds({ orgId: options.orgId, instantlyCampaignIds: options.instantlyCampaignIds }),
  );
  const selfSent = all.filter((s) => s.instantlyCampaignId.startsWith(SELF_SEND_PREFIX));
  const sequences = options.limit ? selfSent.slice(0, options.limit) : selfSent;
  const summary: FollowupJitCutSummary = {
    dryRun: options.dryRun,
    leads: sequences.length,
    steps: sequences.reduce((n, s) => n + s.holds.length, 0),
    usdCents: 0,
    byBrand: [],
    skippedNotSelfSend: all.length - selfSent.length,
    cut: 0,
    failed: 0,
    unreadHolds: 0,
  };
  const cents = new Map<string, number>();

  if (options.dryRun) {
    const flat = sequences.flatMap((s) => s.holds.map((h) => ({ s, h })));
    const amounts = await mapPool(flat, FOLLOWUP_JIT_CONCURRENCY, async ({ s, h }) => {
      try {
        return await heldCents(s, h);
      } catch (error: unknown) {
        summary.unreadHolds += 1;
        console.error(`[instantly-service] followup-jit-cut dry-run: hold=${h.holdId} amount unread: ${errorMessage(error)}`);
        return 0;
      }
    });
    flat.forEach(({ s }, i) => cents.set(s.instantlyCampaignId, (cents.get(s.instantlyCampaignId) ?? 0) + amounts[i]));
  } else {
    const results = await mapPool(sequences, FOLLOWUP_JIT_CONCURRENCY, async (s) => {
      try {
        return await cutSequence(s);
      } catch (error: unknown) {
        return { instantlyCampaignId: s.instantlyCampaignId, outcome: "failed" as const, holdsCancelled: 0, usdCents: 0, error: errorMessage(error) };
      }
    });
    const emailsByOrg = new Map<string, string[]>();
    results.forEach((r, i) => {
      const s = sequences[i];
      cents.set(s.instantlyCampaignId, r.usdCents);
      if (r.outcome === "cut") summary.cut += 1;
      else {
        summary.failed += 1;
        console.error(`[instantly-service] followup-jit-cut: FAILED sequence=${s.instantlyCampaignId} lead=${s.leadEmail}: ${r.error}`);
      }
      if (r.holdsCancelled > 0 && s.orgId) {
        const list = emailsByOrg.get(s.orgId) ?? [];
        list.push(s.leadEmail);
        emailsByOrg.set(s.orgId, list);
      }
    });
    for (const [orgId, emails] of emailsByOrg) {
      for (let i = 0; i < emails.length; i += 1000) {
        await announceEvidenceChanged(orgId, emails.slice(i, i + 1000), "followup-jit-cut");
      }
    }
  }

  summary.byBrand = summarizeByBrand(sequences, cents);
  summary.usdCents = Math.round(summary.byBrand.reduce((n, l) => n + l.usdCents, 0) * 1e6) / 1e6;
  if (!options.dryRun) console.log(`[instantly-service] followup-jit-cut: done ${JSON.stringify({ ...summary, byBrand: summary.byBrand.length })}`);
  return summary;
}
