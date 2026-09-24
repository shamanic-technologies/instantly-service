/**
 * One-shot CLI: rewrite the OLD linked opt-out footer out of the not-yet-sent
 * steps of live Instantly-transport sequences. All logic lives in
 * `src/lib/linked-footer-cleanup.ts` (so it also runs inside the container,
 * where the database is reachable); see that module for the rules.
 *
 * Usage:
 *   npm run cleanup:linked-footer                 # dry-run (default)
 *   npm run cleanup:linked-footer -- --commit     # actually PATCH Instantly
 *   npm run cleanup:linked-footer -- --limit 10   # cap the batch
 *
 * MUST NOT be wired into boot. Manual only.
 */
import { closeDb } from "../src/db";
import { resolvePlatformInstantlyApiKey } from "../src/lib/key-client";
import { runLinkedFooterCleanup } from "../src/lib/linked-footer-cleanup";

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const limitIdx = args.indexOf("--limit");
  const limit = limitIdx >= 0 && args[limitIdx + 1] ? parseInt(args[limitIdx + 1], 10) : undefined;
  const commit = args.includes("--commit");

  const envKey = process.env.INSTANTLY_API_KEY?.trim();
  const apiKey =
    envKey ||
    (await resolvePlatformInstantlyApiKey({ method: "POST", path: "/internal/cleanup-linked-footer" }));

  const summary = await runLinkedFooterCleanup(apiKey, { commit, limit });
  if (!commit) console.log("[cleanup-linked-footer] dry-run — pass --commit to PATCH Instantly");
  // A partial repair must not read as a clean run.
  if (summary.failed > 0) throw new Error(`[cleanup-linked-footer] ${summary.failed} campaign(s) failed`);
}

main()
  .then(() => closeDb())
  .catch(async (e) => {
    console.error("[cleanup-linked-footer] fatal:", e);
    await closeDb();
    process.exit(1);
  });
