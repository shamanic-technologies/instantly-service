/**
 * REPLY-BACKFILL-1119: declare past in-thread answers on the ledger (lib/reply-sends-backfill).
 *
 * In the container:  node dist/scripts/backfill-reply-sends.js --runs <sender-runs.json> [--apply] [--limit N] [--only <dispatchId,...>] [--out <report.json>]
 *
 * Dry run unless `--apply`. `--runs` = workflow-service's script runs that sent one answer
 * (read-only export of `script_runs`, the shape of `SenderRun`).
 */
import { readFileSync, writeFileSync } from "node:fs";
import { backfillReplySends, type SenderRun } from "../lib/reply-sends-backfill";

function arg(name: string): string | undefined {
  const i = process.argv.indexOf(name);
  return i >= 0 ? process.argv[i + 1] : undefined;
}

async function main(): Promise<void> {
  const runsPath = arg("--runs");
  if (!runsPath) throw new Error("--runs <sender-runs.json> is required");
  const senderRuns = JSON.parse(readFileSync(runsPath, "utf8")) as SenderRun[];
  if (!Array.isArray(senderRuns)) throw new Error("--runs must hold a JSON array of sender runs");
  const limitArg = arg("--limit");
  const limit = limitArg === undefined ? undefined : Number(limitArg);
  if (limit !== undefined && (!Number.isInteger(limit) || limit < 1)) throw new Error("--limit must be a positive integer");
  const only = arg("--only")?.split(",").filter(Boolean);
  const apply = process.argv.includes("--apply");
  if (apply && limit === undefined && !only) throw new Error("--apply needs --limit or --only (batches, never the whole set at once)");

  const report = await backfillReplySends({ senderRuns, apply, limit, onlyDispatchIds: only });
  const out = arg("--out");
  if (out) writeFileSync(out, JSON.stringify(report, null, 2));
  const { unmatchedList, declaredDispatchIds, ...summary } = report;
  console.log(JSON.stringify({ ...summary, unmatchedListSize: unmatchedList.length, declaredDispatchIds }, null, 2));
}

main().then(
  () => process.exit(0),
  (error: unknown) => {
    console.error(`[instantly-service] reply-sends-backfill failed: ${error instanceof Error ? error.message : String(error)}`);
    process.exit(1);
  },
);
