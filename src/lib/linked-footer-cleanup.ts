/**
 * Sweep: rewrite the OLD linked opt-out footer out of the not-yet-sent steps of
 * every live sequence on the Instantly transport. Logic in `linked-footer.ts`;
 * this module is the IO — the candidate query, the live read, the PATCH.
 *
 * Same shape as the escaped-newline repair, for the same reasons:
 *   - Source of truth = the Instantly API. The local DB only NARROWS the
 *     candidates (live, not self-sent, still holding a step to send, and a
 *     latest bronze config that carries the old footer OR no config at all —
 *     unverifiable from the DB, so it must be checked live rather than skipped).
 *   - Only steps above `lastSentStep` (MAX real `email_sent` step) are rewritten.
 *   - The PATCH sends the FULL step array back: Instantly REPLACES `sequences`.
 *   - Idempotent (the rewrite leaves no old footer) and resumable (each run
 *     re-reads live state). Fail loud per campaign; a partial run never reads
 *     as a clean one. No local DB write, no cost (a campaign PATCH spends
 *     nothing).
 *
 * Kept in `src/lib` (compiled into `dist`) rather than only in `scripts/` so it
 * can run INSIDE the service container, where the database is reachable; the
 * CLI in `scripts/cleanup-linked-footer.ts` is a thin wrapper around it.
 */

import { sql } from "drizzle-orm";
import { db } from "../db";
import { getCampaign, updateCampaign, type InstantlySequenceStep } from "./instantly-client";
import { planFooterFixes, replaceLinkedFooter, type FooterStepBody } from "./linked-footer";

export interface LinkedFooterCleanupOptions {
  commit: boolean;
  limit?: number;
  log?: (line: string) => void;
}

export interface LinkedFooterCleanupSummary {
  candidates: number;
  patched: number;
  wouldPatch: number;
  stepsFixed: number;
  alreadyClean: number;
  noSequence: number;
  skippedAlreadySent: number;
  failed: number;
}

interface CandidateRow {
  instantlyCampaignId: string;
  lastSentStep: number;
}

function rowsOf(result: unknown): Array<Record<string, unknown>> {
  if (Array.isArray(result)) return result as Array<Record<string, unknown>>;
  return ((result as { rows?: unknown[] }).rows ?? []) as Array<Record<string, unknown>>;
}

export async function selectLinkedFooterCandidates(limit?: number): Promise<CandidateRow[]> {
  const limitSql = typeof limit === "number" ? sql`LIMIT ${limit}` : sql``;
  const result = await db.execute(sql`
    WITH latest_config AS (
      SELECT DISTINCT ON (instantly_campaign_id) instantly_campaign_id, payload
      FROM instantly_campaigns_config_raw
      ORDER BY instantly_campaign_id, fetched_at DESC
    )
    SELECT
      c.instantly_campaign_id AS "instantlyCampaignId",
      COALESCE((
        SELECT MAX(e.step) FROM instantly_events e
        WHERE e.campaign_id = c.instantly_campaign_id
          AND e.event_type = 'email_sent'
          AND e.inferred = false
      ), 0) AS "lastSentStep"
    FROM instantly_campaigns c
    LEFT JOIN latest_config lc ON lc.instantly_campaign_id = c.instantly_campaign_id
    WHERE c.status = 'active'
      -- A reservation sentinel and a self-sent sequence are not Instantly
      -- campaigns: getCampaign 400s on both ids.
      AND c.instantly_campaign_id NOT LIKE 'reserving:%'
      AND c.instantly_campaign_id NOT LIKE 'self:%'
      -- Only sequences that still have a step to send can still send the footer.
      AND EXISTS (
        SELECT 1 FROM sequence_costs sc
        WHERE sc.instantly_campaign_id = c.instantly_campaign_id
          AND sc.status = 'provisioned'
      )
      AND (
        lc.instantly_campaign_id IS NULL
        OR position('hear from me again' in lc.payload::text) > 0
      )
    ORDER BY c.created_at DESC
    ${limitSql}
  `);
  return rowsOf(result).map((r) => ({
    instantlyCampaignId: String(r.instantlyCampaignId),
    lastSentStep: Number(r.lastSentStep ?? 0),
  }));
}

function fixStepVariants(step: InstantlySequenceStep): InstantlySequenceStep {
  return {
    ...step,
    variants: (step.variants ?? []).map((v) => ({
      ...v,
      body: v.body === undefined ? v.body : replaceLinkedFooter(v.body),
    })),
  };
}

async function processCampaign(
  apiKey: string,
  row: CandidateRow,
  commit: boolean,
  summary: LinkedFooterCleanupSummary,
  log: (line: string) => void,
): Promise<void> {
  const live = (await getCampaign(apiKey, row.instantlyCampaignId)) as unknown as {
    sequences?: Array<{ steps?: InstantlySequenceStep[] }>;
  };
  const steps = live.sequences?.[0]?.steps;
  if (!steps || steps.length === 0) {
    summary.noSequence++;
    return;
  }

  const stepBodies: FooterStepBody[] = steps.map((s, index) => ({
    index,
    body: s.variants?.[0]?.body ?? "",
  }));
  const plan = planFooterFixes(stepBodies, row.lastSentStep);
  summary.skippedAlreadySent += plan.skippedAlreadySent.length;

  if (plan.fixes.length === 0) {
    summary.alreadyClean++;
    return;
  }

  const fixIndexes = new Set(plan.fixes.map((f) => f.index));
  const nextSteps = steps.map((s, index) => (fixIndexes.has(index) ? fixStepVariants(s) : s));
  summary.stepsFixed += plan.fixes.length;
  log(
    `[cleanup-linked-footer] campaign=${row.instantlyCampaignId} lastSentStep=${row.lastSentStep} ` +
      `fixing steps ${plan.fixes.map((f) => f.index + 1).join(",")}`,
  );

  if (!commit) {
    summary.wouldPatch++;
    return;
  }
  await updateCampaign(apiKey, row.instantlyCampaignId, { sequences: [{ steps: nextSteps }] });
  summary.patched++;
}

export async function runLinkedFooterCleanup(
  apiKey: string,
  options: LinkedFooterCleanupOptions,
): Promise<LinkedFooterCleanupSummary> {
  const log = options.log ?? ((line: string) => console.log(line));
  const rows = await selectLinkedFooterCandidates(options.limit);
  log(`[cleanup-linked-footer] ${rows.length} candidate campaigns (commit=${options.commit})`);

  const summary: LinkedFooterCleanupSummary = {
    candidates: rows.length,
    patched: 0,
    wouldPatch: 0,
    stepsFixed: 0,
    alreadyClean: 0,
    noSequence: 0,
    skippedAlreadySent: 0,
    failed: 0,
  };

  for (const row of rows) {
    try {
      await processCampaign(apiKey, row, options.commit, summary, log);
    } catch (e) {
      summary.failed++;
      log(
        `[cleanup-linked-footer] campaign=${row.instantlyCampaignId} failed: ${
          e instanceof Error ? e.message : String(e)
        }`,
      );
    }
  }
  log(`[cleanup-linked-footer] summary ${JSON.stringify(summary)}`);
  return summary;
}
