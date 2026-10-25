/**
 * COST-ACCOUNTING-1023: the run of ours that judging a reply is done FOR.
 *
 * One `reply-judging` run per (org, UTC day, PIPE), held in `reply_judging_runs`.
 * The pipe is the judged sequence's campaign x workflow (LEDGER-RESIDUAL-1025):
 * the run carries those tags, so a day whose judgments kept no positive reply
 * is NORMAL LOSS of that pipe (runs-service pools a pipe's produced-none on its
 * outputs), never spend with no pipe, which the ledger reads as "undeclared".
 * Every reply-classifier call (`qualifyReply`) for that sequence sends its id to
 * chat-service as `x-run-id`, so chat-service's platform run (the LLM spend,
 * still platform-billed) becomes its child. The same day's positive replies of
 * that pipe declare their `conversation` on it (lib/positive-reply-outcomes.ts),
 * so the judging spend rolls into Positive reply; a reply judged non-positive
 * declares nothing and its cost rides the positive ones (normal loss).
 *
 * The run carries no cost of its own and is closed `completed` at creation: it
 * is a cost anchor, not a job. When it cannot be resolved the judgment is not
 * asked (throws): every judging call states its run (PLATFORM-CALLER-RUN-1024).
 * A sequence sent with no campaign or workflow gets the org's pipe-less run of
 * the day ('' key): no pipe exists to pool it on.
 */
import { sql } from "drizzle-orm";
import { db } from "../db";
import { createRun, updateRun } from "./runs-client";

export const REPLY_JUDGING_TASK = "reply-judging";

/** The pipe a judged sequence was sent on, as recorded on its row. */
export interface ReplyJudgingPipe {
  campaignId: string | null;
  workflowSlug: string | null;
  /** The sequence's brands (run tag only, not part of the key). */
  brandIds: string[];
  /** The sequence's feature (run tag only, not part of the key). */
  featureSlug: string | null;
}

export const NO_PIPE: ReplyJudgingPipe = { campaignId: null, workflowSlug: null, brandIds: [], featureSlug: null };

/** UTC calendar day `YYYY-MM-DD`. */
export function utcDay(asOf: Date): string {
  return asOf.toISOString().slice(0, 10);
}

const memo = new Map<string, string>();

/** Test hook. */
export function resetReplyJudgingRunMemo(): void {
  memo.clear();
}

/** The run tags of a pipe: none at all for a pipe-less run. */
function trackingOf(pipe: ReplyJudgingPipe) {
  if (!pipe.campaignId || !pipe.workflowSlug) return undefined;
  return {
    campaignId: pipe.campaignId,
    workflowSlug: pipe.workflowSlug,
    ...(pipe.brandIds.length > 0 ? { brandId: pipe.brandIds.join(",") } : {}),
    ...(pipe.featureSlug ? { featureSlug: pipe.featureSlug } : {}),
  };
}

/** The org's judging run for the UTC day of `asOf` on this pipe, created once. Throws on failure. */
export async function replyJudgingRunFor(orgId: string, pipe: ReplyJudgingPipe, asOf: Date = new Date()): Promise<string> {
  const day = utcDay(asOf);
  const tracking = trackingOf(pipe);
  // A pipe is a campaign AND a workflow; half of one pools nowhere.
  const campaignId = tracking ? tracking.campaignId : "";
  const workflowSlug = tracking ? tracking.workflowSlug : "";
  const key = `${orgId}|${day}|${campaignId}|${workflowSlug}`;
  const cached = memo.get(key);
  if (cached) return cached;

  const read = async (): Promise<string | null> => {
    const result = await db.execute(sql`
      SELECT run_id FROM reply_judging_runs
      WHERE org_id = ${orgId} AND day = ${day}::date AND campaign_id = ${campaignId} AND workflow_slug = ${workflowSlug}
    `);
    const row = (result.rows as Array<Record<string, unknown>>)[0];
    return row ? String(row.run_id) : null;
  };

  let runId = await read();
  if (!runId) {
    const identity = { orgId, userId: "", ...(tracking ? { tracking } : {}) };
    const run = await createRun({ serviceName: "instantly-service", taskName: REPLY_JUDGING_TASK }, identity);
    await updateRun(run.id, "completed", identity);
    const inserted = await db.execute(sql`
      INSERT INTO reply_judging_runs (org_id, day, campaign_id, workflow_slug, run_id)
      VALUES (${orgId}, ${day}::date, ${campaignId}, ${workflowSlug}, ${run.id})
      ON CONFLICT (org_id, day, campaign_id, workflow_slug) DO NOTHING
      RETURNING run_id
    `);
    // A concurrent caller stored another run first: theirs is the day's run.
    runId = (inserted.rows as unknown[]).length > 0 ? run.id : await read();
    if (!runId) throw new Error(`reply_judging_runs row for ${key} vanished after a conflict`);
  }
  memo.set(key, runId);
  return runId;
}

/** A row's recorded pipe (`instantly_campaigns` columns). */
export function pipeOfRow(row: Record<string, unknown>): ReplyJudgingPipe {
  const text = (v: unknown) => (typeof v === "string" && v.trim() ? v : null);
  return {
    campaignId: text(row.campaign_id),
    workflowSlug: text(row.workflow_slug),
    brandIds: Array.isArray(row.brand_ids) ? (row.brand_ids as unknown[]).filter((b): b is string => typeof b === "string" && !!b) : [],
    featureSlug: text(row.feature_slug),
  };
}

/**
 * The judging run for a reply on this sequence (`instantly_campaign_id`): its
 * org's run of the day on the sequence's pipe. Throws when it cannot be
 * resolved (no org on the sequence nor a fallback): judging a reply is production, it is never sent to chat-service as
 * nobody's work (PLATFORM-CALLER-RUN-1024).
 */
export async function replyJudgingRunForSequence(
  instantlyCampaignId: string,
  opts: { fallbackOrgId?: string | null; asOf?: Date } = {},
): Promise<string> {
  const asOf = opts.asOf ?? new Date();
  const result = await db.execute(sql`
    SELECT org_id, campaign_id, workflow_slug, feature_slug, to_jsonb(brand_ids) AS brand_ids
    FROM instantly_campaigns WHERE instantly_campaign_id = ${instantlyCampaignId} LIMIT 1
  `);
  const row = (result.rows as Array<Record<string, unknown>>)[0];
  const orgId = row?.org_id ? String(row.org_id) : null;
  // The reply row's own org, when its sequence row holds none: that org's
  // pipe-less run (nothing records a pipe for it).
  if (!orgId && opts.fallbackOrgId) return replyJudgingRunFor(opts.fallbackOrgId, NO_PIPE, asOf);
  if (!orgId) {
    throw new Error(`[reply-judging-run] sequence ${instantlyCampaignId} has no org: no reply-judging run to judge it for`);
  }
  return replyJudgingRunFor(orgId, pipeOfRow(row), asOf);
}
