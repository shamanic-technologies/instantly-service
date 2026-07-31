/**
 * One-shot CLI: repair Instantly sequence steps whose body carries LITERAL
 * escape sequences (`\` + `n`) where a line break was meant, so the follow-ups
 * that have NOT been dispatched yet render as real paragraphs.
 *
 * Background — the producer (content-generation-service) converts the LLM's
 * plain-text email body to HTML by splitting on REAL newline characters. When
 * the model's structured output returns newlines OVER-ESCAPED, the body holds
 * `\` + `n` instead of U+000A: nothing splits, the whole email ships as one
 * paragraph, and the prospect sees the escape sequences. Measured against prod
 * 2026-07-31: 53 of 40,979 delivered campaign configs (~0.13%), continuous from
 * 2026-05-27, across every cold-email workflow and both the sales and PR
 * features. The producer-side fix ships separately; this repairs the pending
 * steps of campaigns already pushed to Instantly.
 *
 * Source of truth = the Instantly API. The local DB only NARROWS the candidate
 * set (a live scan of every active campaign is thousands of calls); the dirty
 * FACT and the bodies acted on are always read live via `getCampaign`.
 *
 * Candidate selection (local DB, narrowing only) — `status = 'active'` AND
 * EITHER the latest bronze sequence-config carries a literal escape, OR the
 * campaign has NO bronze config at all (never reconciled — unverifiable from
 * the DB, so it must be checked live rather than silently skipped).
 *
 * Per candidate:
 *   1. `getCampaign` live → `sequences[0].steps`.
 *   2. `lastSentStep` = MAX(step) of REAL (`inferred = false`) `email_sent`
 *      silver events for that Instantly campaign (0 when nothing dispatched).
 *   3. `planStepFixes` — rewrite ONLY steps above `lastSentStep`. An already
 *      dispatched step is left alone (the email is gone; rewriting it would
 *      silently alter the historical record) and reported instead.
 *   4. If anything to fix, PATCH the campaign with the FULL step array (the
 *      PATCH replaces `sequences`, so unmodified steps must be sent back too).
 *
 * Idempotent — `fixEscapedNewlines` leaves no literal escape behind, so a
 * re-run finds nothing. Resumable — each run re-reads live state. Instantly-only:
 * no local DB write, no cost declaration (a campaign PATCH spends nothing).
 *
 * Usage:
 *   npm run cleanup:escaped-newlines                 # dry-run (default)
 *   npm run cleanup:escaped-newlines -- --commit     # actually PATCH Instantly
 *   npm run cleanup:escaped-newlines -- --limit 10   # cap the batch
 *   npm run cleanup:escaped-newlines -- --json       # machine-readable summary
 *
 * Running against prod from a laptop: key-service resolves on internal DNS only,
 * so set the shared cold-email workspace key directly and let Railway inject the
 * (publicly reachable) Neon URL:
 *   railway run -s instantly-service -- bash -lc \
 *     'export INSTANTLY_API_KEY=…; npm run cleanup:escaped-newlines'
 *
 * MUST NOT be wired into boot (port-bind hazard on Railway). Manual CLI only.
 */
import { db, closeDb } from "../src/db";
import { sql } from "drizzle-orm";
import {
  getCampaign,
  updateCampaign,
  type InstantlySequenceStep,
} from "../src/lib/instantly-client";
import {
  fixEscapedNewlines,
  planStepFixes,
  type SequenceStepBody,
} from "../src/lib/escaped-newlines";
import { resolvePlatformInstantlyApiKey } from "../src/lib/key-client";

interface CliArgs {
  commit: boolean;
  limit?: number;
  json: boolean;
}

interface CandidateRow {
  instantlyCampaignId: string;
  leadEmail: string | null;
  lastSentStep: number;
}

interface Summary {
  candidates: number;
  patched: number;
  wouldPatch: number;
  stepsFixed: number;
  alreadyClean: number;
  noSequence: number;
  skippedSentDirty: number;
  failed: number;
}

function parseArgs(): CliArgs {
  const args = process.argv.slice(2);
  const limitIdx = args.indexOf("--limit");
  const limit =
    limitIdx >= 0 && args[limitIdx + 1]
      ? parseInt(args[limitIdx + 1], 10)
      : undefined;
  return {
    commit: args.includes("--commit"),
    limit,
    json: args.includes("--json"),
  };
}

/**
 * Narrow the fleet to the campaigns worth a live read. `lastSentStep` comes
 * from the REAL (non-inferred) `email_sent` silver events, which this service
 * owns — it is the send evidence, not an assumption.
 */
async function selectCandidates(limit?: number): Promise<CandidateRow[]> {
  const limitSql = typeof limit === "number" ? sql`LIMIT ${limit}` : sql``;
  const result = await db.execute(sql`
    WITH latest_config AS (
      SELECT DISTINCT ON (instantly_campaign_id)
        instantly_campaign_id,
        payload
      FROM instantly_campaigns_config_raw
      ORDER BY instantly_campaign_id, fetched_at DESC
    )
    SELECT
      c.instantly_campaign_id AS "instantlyCampaignId",
      c.lead_email            AS "leadEmail",
      COALESCE((
        SELECT MAX(e.step) FROM instantly_events e
        WHERE e.campaign_id = c.instantly_campaign_id
          AND e.event_type = 'email_sent'
          AND e.inferred = false
      ), 0) AS "lastSentStep"
    FROM instantly_campaigns c
    LEFT JOIN latest_config lc ON lc.instantly_campaign_id = c.instantly_campaign_id
    WHERE c.status = 'active'
      -- Exclude DIS-148 reservation sentinels: a 'reserving:<uuid>' row is an
      -- in-flight claim, NOT an Instantly campaign, so it has no bronze config
      -- (it would otherwise fall into the no-config branch below) and getCampaign
      -- 400s on it because the sentinel is not a bare uuid.
      AND c.instantly_campaign_id NOT LIKE 'reserving:%'
      AND (
        lc.instantly_campaign_id IS NULL
        OR position('\\n' in lc.payload::text) > 0
      )
    ORDER BY c.created_at DESC
    ${limitSql}
  `);
  const rows = Array.isArray(result)
    ? result
    : ((result as { rows?: unknown[] }).rows ?? []);
  return (rows as Array<Record<string, unknown>>).map((r) => ({
    instantlyCampaignId: String(r.instantlyCampaignId),
    leadEmail: r.leadEmail === null ? null : String(r.leadEmail),
    lastSentStep: Number(r.lastSentStep ?? 0),
  }));
}

/** Repair every variant body of a step (our sends emit one; be general). */
function fixStepVariants(step: InstantlySequenceStep): InstantlySequenceStep {
  return {
    ...step,
    variants: (step.variants ?? []).map((v) => ({
      ...v,
      body: v.body === undefined ? v.body : fixEscapedNewlines(v.body),
    })),
  };
}

async function processCampaign(
  apiKey: string,
  row: CandidateRow,
  commit: boolean,
  summary: Summary,
): Promise<void> {
  const live = (await getCampaign(apiKey, row.instantlyCampaignId)) as unknown as {
    sequences?: Array<{ steps?: InstantlySequenceStep[] }>;
  };

  const steps = live.sequences?.[0]?.steps;
  if (!steps || steps.length === 0) {
    summary.noSequence++;
    return;
  }

  const stepBodies: SequenceStepBody[] = steps.map((s, index) => ({
    index,
    body: s.variants?.[0]?.body ?? "",
  }));

  const plan = planStepFixes(stepBodies, row.lastSentStep);
  summary.skippedSentDirty += plan.skippedSentDirty.length;

  if (plan.fixes.length === 0) {
    summary.alreadyClean++;
    if (plan.skippedSentDirty.length > 0) {
      console.warn(
        `[cleanup-escaped-newlines] campaign=${row.instantlyCampaignId} ` +
          `lead=${row.leadEmail ?? "?"} already-sent steps carry escapes ` +
          `(${plan.skippedSentDirty.join(",")}) — left alone, email already out`,
      );
    }
    return;
  }

  const fixIndexes = new Set(plan.fixes.map((f) => f.index));
  const nextSteps = steps.map((s, index) =>
    fixIndexes.has(index) ? fixStepVariants(s) : s,
  );

  console.log(
    `[cleanup-escaped-newlines] campaign=${row.instantlyCampaignId} ` +
      `lead=${row.leadEmail ?? "?"} lastSentStep=${row.lastSentStep} ` +
      `fixing steps ${plan.fixes.map((f) => f.index + 1).join(",")}`,
  );

  summary.stepsFixed += plan.fixes.length;

  if (!commit) {
    summary.wouldPatch++;
    return;
  }

  await updateCampaign(apiKey, row.instantlyCampaignId, {
    sequences: [{ steps: nextSteps }],
  });
  summary.patched++;
}

async function resolveApiKey(): Promise<string> {
  const envKey = process.env.INSTANTLY_API_KEY?.trim();
  if (envKey) {
    console.log(
      "[cleanup-escaped-newlines] using INSTANTLY_API_KEY from env (key-service bypassed)",
    );
    return envKey;
  }
  return resolvePlatformInstantlyApiKey({
    method: "POST",
    path: "/internal/cleanup-escaped-newlines",
  });
}

async function main(): Promise<void> {
  const args = parseArgs();
  console.log(
    `[cleanup-escaped-newlines] starting — commit=${args.commit} limit=${args.limit ?? "all"}`,
  );

  const apiKey = await resolveApiKey();
  const rows = await selectCandidates(args.limit);
  console.log(`[cleanup-escaped-newlines] ${rows.length} candidate campaigns`);

  const summary: Summary = {
    candidates: rows.length,
    patched: 0,
    wouldPatch: 0,
    stepsFixed: 0,
    alreadyClean: 0,
    noSequence: 0,
    skippedSentDirty: 0,
    failed: 0,
  };

  for (const row of rows) {
    try {
      await processCampaign(apiKey, row, args.commit, summary);
    } catch (e) {
      summary.failed++;
      const error = e instanceof Error ? e.message : String(e);
      console.error(
        `[cleanup-escaped-newlines] campaign=${row.instantlyCampaignId} failed: ${error}`,
      );
    }
  }

  if (args.json) {
    console.log(JSON.stringify(summary, null, 2));
  } else {
    console.log(
      `[cleanup-escaped-newlines] summary ${JSON.stringify(summary)}` +
        (args.commit ? "" : " (dry-run — pass --commit to PATCH Instantly)"),
    );
  }

  // Fail loud when a campaign could not be processed: a partial repair must not
  // read as a clean run.
  if (summary.failed > 0) {
    throw new Error(
      `[cleanup-escaped-newlines] ${summary.failed} campaign(s) failed`,
    );
  }
}

main()
  .then(() => closeDb())
  .catch(async (e) => {
    console.error("[cleanup-escaped-newlines] fatal:", e);
    await closeDb();
    process.exit(1);
  });
