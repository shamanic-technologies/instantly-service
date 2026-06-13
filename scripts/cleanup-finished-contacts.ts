/**
 * One-shot cleanup — delete contacts of FINISHED Instantly campaigns to reclaim
 * the plan's uploaded-contacts quota.
 *
 * WHY: Instantly's plan limit (e.g. 25,000 uploaded contacts) counts contacts
 * CURRENTLY stored across all campaigns. Deleting leads from a campaign frees
 * that quota (~5-10 min sync — see help.instantly.ai/articles/7918680). Each of
 * our sends is its own per-lead campaign, so once a campaign is terminal
 * (Instantly COMPLETED the sequence → status 3, or it was PAUSED → status 2,
 * e.g. the operator paused it after the prospect replied off-Instantly) its
 * contact can be deleted without losing anything: all engagement history already
 * lives in our local silver DB (analytics/status read silver, never Instantly).
 *
 * Rule (locked, option A): finished iff Instantly status is paused (2) or
 * completed (3). ACTIVE (1) is never touched. No pause grace period.
 *
 * SOURCE OF TRUTH = Instantly API. The local DB is NOT consulted (most per-lead
 * campaigns may not be in it, and the duplicate/terminal FACT is derived from
 * Instantly). Selection logic lives in `src/lib/cleanup-finished.ts` (pure,
 * unit-tested); this script does only the Instantly IO.
 *
 * MUTATING. Dry-run by default — prints the delete plan + counts. Pass `--commit`
 * to actually DELETE /leads (campaign-level — the only delete that frees quota;
 * deleting from "Lists" does not). Delete is NOT reversible on Instantly, but the
 * lead's history is preserved in our silver DB and the operation is safe for
 * terminal campaigns (nothing left to send).
 *
 * Idempotent + resumable: each run re-sweeps live state, so already-deleted leads
 * drop out of the sweep and are never re-touched. A killed run resumes by
 * re-running. A delete that 404s (lead already gone) is counted, not fatal.
 *
 * NOT handled here (deliberate, v1): local DB `status` / `delivery_status` write
 * + cost refund. The recurring reconcile path (PR2) marks local rows terminal so
 * reconcile stops polling deleted contacts; from a local CLI shell runs-service
 * lives at *.railway.internal (unreachable).
 *
 * Usage:
 *   railway run -s instantly-service -- bash -lc \
 *     'export INSTANTLY_API_KEY=…; npm run cleanup:finished-contacts -- [--commit] [--limit N] [--json]'
 *
 *   (no --commit)  dry-run: print plan + counts, mutate nothing.
 *   --commit       actually DELETE /leads for every contact of a finished campaign.
 *   --limit N      cap deletions this run (batching; default: all).
 *   --json         machine-readable plan output.
 */

import { resolveInstantlyApiKey } from "../src/lib/key-client";
import {
  selectContactsToDelete,
  countDeletions,
  FINISHED_STATUSES,
  type CleanupCampaign,
  type LeadMembership,
  type DeleteTarget,
} from "../src/lib/cleanup-finished";

const INSTANTLY_API_URL = "https://api.instantly.ai/api/v2";
const PAGE_LIMIT = 100;
const PACE_MS = 110;

interface RawCampaign {
  id: string;
  name?: string;
  status?: number | string;
}
interface RawLead {
  email?: string;
  campaign?: string;
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

class InstantlyHttpError extends Error {
  constructor(
    public readonly status: number,
    message: string,
  ) {
    super(message);
    this.name = "InstantlyHttpError";
  }
}

async function instantly<T>(
  apiKey: string,
  path: string,
  init: { method?: string; body?: unknown } = {},
): Promise<T> {
  const { method = "GET", body } = init;
  for (let attempt = 0; attempt < 4; attempt++) {
    await sleep(PACE_MS);
    const res = await fetch(`${INSTANTLY_API_URL}${path}`, {
      method,
      headers: {
        Authorization: `Bearer ${apiKey}`,
        ...(body !== undefined ? { "Content-Type": "application/json" } : {}),
      },
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });
    if (res.status === 429 || res.status >= 500) {
      const wait = Math.min(1000 * 2 ** attempt, 8000);
      console.warn(`[cleanup-finished] ${method} ${path} → ${res.status}, retry in ${wait}ms`);
      await sleep(wait);
      continue;
    }
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      throw new InstantlyHttpError(res.status, `instantly ${method} ${path} → ${res.status}: ${txt.slice(0, 300)}`);
    }
    return (await res.json()) as T;
  }
  throw new InstantlyHttpError(0, `instantly ${method} ${path} failed after retries`);
}

async function listAllCampaigns(apiKey: string): Promise<RawCampaign[]> {
  const out: RawCampaign[] = [];
  let startingAfter: string | undefined;
  for (;;) {
    const q = new URLSearchParams({ limit: String(PAGE_LIMIT) });
    if (startingAfter) q.set("starting_after", startingAfter);
    const page = await instantly<{ items?: RawCampaign[]; next_starting_after?: string }>(
      apiKey,
      `/campaigns?${q.toString()}`,
    );
    const items = page.items ?? [];
    out.push(...items);
    if (!page.next_starting_after || items.length === 0) break;
    startingAfter = page.next_starting_after;
  }
  return out;
}

async function listAllLeads(apiKey: string): Promise<RawLead[]> {
  const out: RawLead[] = [];
  let startingAfter: string | undefined;
  for (;;) {
    const body: Record<string, unknown> = { limit: PAGE_LIMIT };
    if (startingAfter) body.starting_after = startingAfter;
    const page = await instantly<{ items?: RawLead[]; next_starting_after?: string }>(
      apiKey,
      `/leads/list`,
      { method: "POST", body },
    );
    const items = page.items ?? [];
    out.push(...items);
    if (out.length % 5000 === 0 && out.length > 0) {
      console.log(`[cleanup-finished] swept ${out.length} leads...`);
    }
    if (!page.next_starting_after || items.length === 0) break;
    startingAfter = page.next_starting_after;
  }
  return out;
}

/** DELETE /leads — campaign-level delete (frees quota). Returns true if removed. */
async function deleteLead(apiKey: string, campaignId: string, email: string): Promise<void> {
  await instantly(apiKey, `/leads`, {
    method: "DELETE",
    body: { campaign_id: campaignId, delete_list: [email] },
  });
}

interface Args {
  commit: boolean;
  json: boolean;
  limit: number | null;
}
function parseArgs(argv: string[]): Args {
  const flags = new Set(argv.filter((a) => a.startsWith("--")));
  let limit: number | null = null;
  const i = argv.indexOf("--limit");
  if (i >= 0 && argv[i + 1]) limit = Number(argv[i + 1]) || null;
  const eq = argv.find((a) => a.startsWith("--limit="));
  if (eq) limit = Number(eq.split("=")[1]) || null;
  return { commit: flags.has("--commit"), json: flags.has("--json"), limit };
}

async function resolveKey(): Promise<string> {
  const envKey = process.env.INSTANTLY_API_KEY?.trim();
  if (envKey) {
    console.log(`[cleanup-finished] using INSTANTLY_API_KEY from env; cleaning that key's workspace.`);
    return envKey;
  }
  const orgId = process.argv.slice(2).find((a) => !a.startsWith("--"));
  if (!orgId) {
    console.error(
      "Set INSTANTLY_API_KEY (recommended) or pass <orgId> as the first arg to resolve via key-service.",
    );
    process.exit(1);
  }
  const r = await resolveInstantlyApiKey(orgId, "system", {
    method: "POST",
    path: "/internal/cleanup-finished",
  });
  console.log(`[cleanup-finished] resolved ${r.keySource} key for org ${orgId}`);
  return r.key;
}

/** Flatten targets into a [campaignId, email] work list, capped by --limit. */
function flatten(targets: DeleteTarget[]): Array<{ campaignId: string; email: string }> {
  const out: Array<{ campaignId: string; email: string }> = [];
  for (const t of targets) for (const email of t.emails) out.push({ campaignId: t.campaignId, email });
  return out;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const apiKey = await resolveKey();

  console.log(`[cleanup-finished] listing campaigns...`);
  const campaigns = await listAllCampaigns(apiKey);
  const campaignsById = new Map<string, CleanupCampaign>();
  let finishedCount = 0;
  for (const c of campaigns) {
    const status = Number(c.status);
    if (FINISHED_STATUSES.has(status)) finishedCount++;
    campaignsById.set(c.id, { id: c.id, status });
  }
  console.log(
    `[cleanup-finished] ${campaigns.length} campaigns, ${finishedCount} finished (paused/completed).`,
  );

  console.log(`[cleanup-finished] sweeping leads...`);
  const leads = await listAllLeads(apiKey);
  const memberships: LeadMembership[] = leads
    .filter((l) => l.email && l.campaign)
    .map((l) => ({ email: l.email as string, campaignId: l.campaign as string }));

  const targets = selectContactsToDelete(memberships, campaignsById);
  const totalToDelete = countDeletions(targets);
  let work = flatten(targets);
  if (args.limit && args.limit > 0) work = work.slice(0, args.limit);

  const summary = {
    totalCampaigns: campaigns.length,
    finishedCampaigns: finishedCount,
    campaignsWithContacts: targets.length,
    contactsToDelete: totalToDelete,
    deletingThisRun: work.length,
    mode: args.commit ? "COMMIT" : "DRY-RUN",
  };

  if (args.json) {
    console.log(JSON.stringify({ summary, targets: targets.slice(0, 100) }, null, 2));
  } else {
    const line = "─".repeat(72);
    console.log(`\n${line}`);
    console.log(`CLEANUP — delete contacts of finished campaigns (paused/completed)  [${summary.mode}]`);
    console.log(line);
    console.log(`Total campaigns ................. ${summary.totalCampaigns}`);
    console.log(`Finished (paused/completed) ..... ${summary.finishedCampaigns}`);
    console.log(`Finished w/ contacts ............ ${summary.campaignsWithContacts}`);
    console.log(`Contacts to DELETE .............. ${summary.contactsToDelete}`);
    console.log(`Deleting this run ............... ${summary.deletingThisRun}`);
    console.log(line);
    console.log(`Top campaigns:`);
    for (const t of targets.slice(0, 10)) {
      console.log(`  ${t.campaignId}  contacts=${t.emails.length}`);
    }
    console.log(line);
  }

  if (!args.commit) {
    console.log(`\nDRY-RUN — nothing deleted. Re-run with --commit to delete.\n`);
    return;
  }

  console.log(`\n[cleanup-finished] COMMIT — deleting ${work.length} contacts...`);
  let deleted = 0;
  let alreadyGone = 0;
  let failed = 0;
  for (const { campaignId, email } of work) {
    try {
      await deleteLead(apiKey, campaignId, email);
      deleted++;
    } catch (e) {
      if (e instanceof InstantlyHttpError && e.status === 404) {
        alreadyGone++;
      } else {
        failed++;
        console.warn(
          `[cleanup-finished] delete failed campaign=${campaignId} email=${email}: ${(e as Error).message}`,
        );
      }
    }
    if ((deleted + alreadyGone + failed) % 250 === 0) {
      console.log(
        `[cleanup-finished] progress: ${deleted} deleted, ${alreadyGone} already-gone, ${failed} failed / ${work.length}`,
      );
    }
  }
  console.log(
    `\n[cleanup-finished] DONE — ${deleted} deleted, ${alreadyGone} already-gone, ${failed} failed.`,
  );
  if (totalToDelete > work.length) {
    console.log(
      `[cleanup-finished] ${totalToDelete - work.length} remaining (raise/drop --limit; re-run to resume).`,
    );
  }
}

main()
  .then(() => process.exit(0))
  .catch((e) => {
    console.error("[cleanup-finished]", e);
    process.exit(1);
  });
