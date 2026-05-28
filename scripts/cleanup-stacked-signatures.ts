/**
 * One-shot CLI: clean stacked signatures from Instantly campaigns whose lead
 * has been pushed but never actually received any email (yet).
 *
 * Background — historic bug 2026-05-28: `stripAccountSignature` matched only
 * the plain-text marker `\n\n--\n`. Instantly stores `bodyHtml` so the marker
 * never matched HTML bodies, and every retry-stuck re-send appended a fresh
 * signature on top of the existing one. Rows redispatched dozens of times
 * accumulated dozens of stacked signatures inside Instantly's campaign config.
 * PR1 (`hotfix(send-lead): idempotent strip-then-append signature`) fixed the
 * bleeding going forward; this script repairs the legacy state for campaigns
 * that have already been pushed to Instantly but whose lead has NOT yet
 * received any email (no `email_sent` silver event).
 *
 * Selection criteria (lead pushed but Instantly hasn't dispatched yet):
 *   - `delivery_status = 'contacted'`
 *   - `status = 'active'`
 *   - `org_id IS NOT NULL`
 *   - NOT EXISTS any `email_sent` event in `instantly_events` for the campaign
 *
 * Per row:
 *   1. Resolve the org's Instantly API key (cached per org).
 *   2. `getCampaign` to recover sequences + `email_list`.
 *   3. List accounts (cached per key), find the assigned account.
 *   4. For each step body: `stripAccountSignature` → `buildEmailBodyWithSignature`.
 *      Idempotent — clean body in, clean body out.
 *   5. If any step body changed, PATCH the campaign with the cleaned sequences.
 *
 * Idempotent — re-running is a no-op (`buildEmailBodyWithSignature` is
 * idempotent by construction, see PR1).
 *
 * Usage:
 *   npm run cleanup:stacked-sigs                  # dry-run (default — log only)
 *   npm run cleanup:stacked-sigs -- --commit      # actually PATCH Instantly
 *   npm run cleanup:stacked-sigs -- --limit 10    # cap to first 10 rows
 *
 * MUST NOT be wired into boot (port-bind hazard on Railway). Manual CLI only.
 */
import { db, closeDb } from "../src/db";
import { sql } from "drizzle-orm";
import {
  getCampaign,
  listAccounts,
  updateCampaign,
  type Account,
  type InstantlySequenceStep,
} from "../src/lib/instantly-client";
import {
  buildEmailBodyWithSignature,
  stripAccountSignature,
} from "../src/lib/send-lead";
import { resolveInstantlyApiKey } from "../src/lib/key-client";

interface CliArgs {
  commit: boolean;
  limit?: number;
}

interface EligibleRow {
  id: string;
  instantlyCampaignId: string;
  campaignId: string | null;
  leadEmail: string | null;
  orgId: string;
}

type RowOutcome =
  | { kind: "cleaned"; markerCountBefore: number }
  | { kind: "would_clean"; markerCountBefore: number }
  | { kind: "already_clean" }
  | { kind: "no_sequence" }
  | { kind: "no_account" }
  | { kind: "account_not_found" }
  | { kind: "key_unavailable"; error: string }
  | { kind: "error"; error: string };

function parseArgs(): CliArgs {
  const args = process.argv.slice(2);
  const limitIdx = args.indexOf("--limit");
  const limit =
    limitIdx >= 0 && args[limitIdx + 1]
      ? parseInt(args[limitIdx + 1], 10)
      : undefined;
  return { commit: args.includes("--commit"), limit };
}

async function selectEligibleRows(limit?: number): Promise<EligibleRow[]> {
  const limitSql = typeof limit === "number" ? sql`LIMIT ${limit}` : sql``;
  const result = await db.execute(sql`
    SELECT
      id,
      instantly_campaign_id AS "instantlyCampaignId",
      campaign_id           AS "campaignId",
      lead_email            AS "leadEmail",
      org_id                AS "orgId"
    FROM instantly_campaigns c
    WHERE c.delivery_status = 'contacted'
      AND c.status = 'active'
      AND c.org_id IS NOT NULL
      AND NOT EXISTS (
        SELECT 1 FROM instantly_events e
        WHERE e.campaign_id = c.instantly_campaign_id
          AND e.event_type = 'email_sent'
      )
    ORDER BY c.created_at ASC
    ${limitSql}
  `);
  const rows = Array.isArray(result)
    ? result
    : (result as { rows?: unknown[] }).rows ?? [];
  return rows as EligibleRow[];
}

function countMarkers(body: string): number {
  return (body.match(/--/g) ?? []).length;
}

async function processRow(
  row: EligibleRow,
  commit: boolean,
  keyByOrg: Map<string, string>,
  accountsByKey: Map<string, Account[]>,
): Promise<RowOutcome> {
  let apiKey = keyByOrg.get(row.orgId);
  if (!apiKey) {
    try {
      const r = await resolveInstantlyApiKey(row.orgId, "system", {
        method: "POST",
        path: "/internal/cleanup-stacked-sigs",
      });
      apiKey = r.key;
      keyByOrg.set(row.orgId, apiKey);
    } catch (e) {
      const error = e instanceof Error ? e.message : String(e);
      return { kind: "key_unavailable", error };
    }
  }

  const live = (await getCampaign(apiKey, row.instantlyCampaignId)) as unknown as {
    sequences?: Array<{ steps?: InstantlySequenceStep[] }>;
    email_list?: string[];
  };

  const steps = live.sequences?.[0]?.steps;
  if (!steps || steps.length === 0) return { kind: "no_sequence" };

  const emailList = live.email_list ?? [];
  if (emailList.length === 0) return { kind: "no_account" };

  let allAccounts = accountsByKey.get(apiKey);
  if (!allAccounts) {
    allAccounts = await listAccounts(apiKey);
    accountsByKey.set(apiKey, allAccounts);
  }

  const account = allAccounts.find((a) => emailList.includes(a.email));
  if (!account) return { kind: "account_not_found" };

  let changed = false;
  let firstMarkerCountBefore = 0;
  const cleanedSteps: InstantlySequenceStep[] = steps.map((s, i) => {
    const v0 = s.variants?.[0] ?? {};
    const originalBody = v0.body ?? "";
    const cleanedBody = buildEmailBodyWithSignature(
      stripAccountSignature(originalBody),
      account,
    );
    if (i === 0) firstMarkerCountBefore = countMarkers(originalBody);
    if (cleanedBody !== originalBody) changed = true;
    return {
      ...s,
      variants: [
        {
          subject: v0.subject,
          body: cleanedBody,
        },
      ],
    };
  });

  if (!changed) return { kind: "already_clean" };

  console.log(
    `[cleanup-sigs] row=${row.id} instantly=${row.instantlyCampaignId} ` +
      `markers ${firstMarkerCountBefore}→${countMarkers(cleanedSteps[0].variants?.[0]?.body ?? "")}`,
  );

  if (!commit) return { kind: "would_clean", markerCountBefore: firstMarkerCountBefore };

  await updateCampaign(apiKey, row.instantlyCampaignId, {
    sequences: [{ steps: cleanedSteps }],
  });
  return { kind: "cleaned", markerCountBefore: firstMarkerCountBefore };
}

async function main(): Promise<void> {
  const args = parseArgs();
  console.log(
    `[cleanup-sigs] starting — commit=${args.commit} limit=${args.limit ?? "all"}`,
  );

  const rows = await selectEligibleRows(args.limit);
  console.log(`[cleanup-sigs] ${rows.length} eligible rows`);

  const keyByOrg = new Map<string, string>();
  const accountsByKey = new Map<string, Account[]>();
  const counts: Record<RowOutcome["kind"], number> = {
    cleaned: 0,
    would_clean: 0,
    already_clean: 0,
    no_sequence: 0,
    no_account: 0,
    account_not_found: 0,
    key_unavailable: 0,
    error: 0,
  };

  for (const row of rows) {
    try {
      const outcome = await processRow(row, args.commit, keyByOrg, accountsByKey);
      counts[outcome.kind]++;
      if (outcome.kind === "key_unavailable" || outcome.kind === "error") {
        console.warn(`[cleanup-sigs] row=${row.id} ${outcome.kind}: ${outcome.error}`);
      }
    } catch (e) {
      const error = e instanceof Error ? e.message : String(e);
      console.error(`[cleanup-sigs] row=${row.id} threw: ${error}`);
      counts.error++;
    }
  }

  console.log(`[cleanup-sigs] summary ${JSON.stringify(counts)}`);
}

main()
  .then(() => closeDb())
  .catch(async (e) => {
    console.error("[cleanup-sigs] fatal:", e);
    await closeDb();
    process.exit(1);
  });
