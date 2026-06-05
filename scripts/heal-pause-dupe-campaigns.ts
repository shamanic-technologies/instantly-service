/**
 * Heal companion to `audit:dupes` — pause redundant active campaigns.
 *
 * Rule (locked): for every person (email) in ≥2 ACTIVE Instantly campaigns, KEEP
 * the single OLDEST campaign (min created_at) and PAUSE all others. Collapses both
 * retry stacks (DIS-148) and distinct logical campaigns down to one active per
 * person.
 *
 * SOURCE OF TRUTH = Instantly API. The local DB is NOT consulted — most retry
 * campaigns aren't in it. Selection logic lives in `src/lib/heal-duplicates.ts`
 * (pure, unit-tested); this script does only the Instantly IO.
 *
 * MUTATING. Dry-run by default — prints the keep/pause plan. Pass `--commit` to
 * actually pause. Pause is REVERSIBLE (reactivate in Instantly); no delete.
 *
 * Idempotent + resumable: each run re-sweeps live state, so already-paused
 * campaigns drop out of the "active" set and are never re-touched. A killed run
 * is resumed by simply re-running.
 *
 * NOT handled here (deliberate, v1): cost refund + local DB `delivery_status`
 * update. `handleCampaignError` / runs-service live at *.railway.internal,
 * unreachable from a local CLI shell. Cost reconciliation is a follow-up (DIS-148).
 *
 * Usage:
 *   railway run -s instantly-service -- bash -lc \
 *     'export INSTANTLY_API_KEY=…; npm run heal:dupes -- [--commit] [--limit N] [--json]'
 *
 *   (no --commit)  dry-run: print plan + counts, mutate nothing.
 *   --commit       actually POST /campaigns/{id}/pause for every redundant campaign.
 *   --limit N      cap campaigns paused this run (batching; default: all).
 *   --json         machine-readable plan output.
 */

import { resolveInstantlyApiKey } from "../src/lib/key-client";
import {
  selectCampaignsToPause,
  pauseIdSet,
  type HealCampaign,
  type LeadMembership,
} from "../src/lib/heal-duplicates";

const INSTANTLY_API_URL = "https://api.instantly.ai/api/v2";
const PAGE_LIMIT = 100;
const ACTIVE_STATUSES = new Set<number>([1]);
const PACE_MS = 110;

interface RawCampaign {
  id: string;
  name?: string;
  status?: number | string;
  // Instantly's /campaigns list exposes the creation time as `timestamp_created`
  // (ISO 8601). `created_at` is NOT returned by the list endpoint — relying on it
  // makes the oldest-keeper fall back to id sort (wrong). Keep both for safety.
  timestamp_created?: string;
  created_at?: string;
}
interface RawLead {
  email?: string;
  campaign?: string;
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
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
      console.warn(`[heal-dupes] ${method} ${path} → ${res.status}, retry in ${wait}ms`);
      await sleep(wait);
      continue;
    }
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      throw new Error(`instantly ${method} ${path} → ${res.status}: ${txt.slice(0, 300)}`);
    }
    return (await res.json()) as T;
  }
  throw new Error(`instantly ${method} ${path} failed after retries`);
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
      console.log(`[heal-dupes] swept ${out.length} leads...`);
    }
    if (!page.next_starting_after || items.length === 0) break;
    startingAfter = page.next_starting_after;
  }
  return out;
}

/** POST /campaigns/{id}/pause */
async function pauseCampaign(apiKey: string, id: string): Promise<void> {
  await instantly(apiKey, `/campaigns/${id}/pause`, { method: "POST" });
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
    console.log(`[heal-dupes] using INSTANTLY_API_KEY from env; healing that key's workspace.`);
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
    path: "/internal/heal-dupes",
  });
  console.log(`[heal-dupes] resolved ${r.keySource} key for org ${orgId}`);
  return r.key;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const apiKey = await resolveKey();

  console.log(`[heal-dupes] listing campaigns...`);
  const campaigns = await listAllCampaigns(apiKey);
  const campaignsById = new Map<string, HealCampaign>();
  let activeCount = 0;
  for (const c of campaigns) {
    const active = ACTIVE_STATUSES.has(Number(c.status));
    if (active) activeCount++;
    campaignsById.set(c.id, {
      id: c.id,
      active,
      createdAt: c.timestamp_created ?? c.created_at ?? "",
    });
  }
  console.log(`[heal-dupes] ${campaigns.length} campaigns, ${activeCount} active (status 1).`);

  console.log(`[heal-dupes] sweeping leads...`);
  const leads = await listAllLeads(apiKey);
  const memberships: LeadMembership[] = leads
    .filter((l) => l.email && l.campaign)
    .map((l) => ({ email: l.email as string, campaignId: l.campaign as string }));

  const decisions = selectCampaignsToPause(memberships, campaignsById);
  let toPause = pauseIdSet(decisions);
  const totalToPause = toPause.length;
  if (args.limit && args.limit > 0) toPause = toPause.slice(0, args.limit);

  const summary = {
    activeCampaigns: activeCount,
    duplicatedPeople: decisions.length,
    campaignsKept: decisions.length, // one keeper per duplicated email
    campaignsToPause: totalToPause,
    pausingThisRun: toPause.length,
    mode: args.commit ? "COMMIT" : "DRY-RUN",
  };

  if (args.json) {
    console.log(JSON.stringify({ summary, decisions: decisions.slice(0, 100) }, null, 2));
  } else {
    const line = "─".repeat(72);
    console.log(`\n${line}`);
    console.log(`HEAL — pause redundant campaigns (keep oldest per email)  [${summary.mode}]`);
    console.log(line);
    console.log(`Active campaigns ................ ${summary.activeCampaigns}`);
    console.log(`Duplicated people (≥2 active) ... ${summary.duplicatedPeople}`);
    console.log(`Campaigns to KEEP (oldest) ...... ${summary.campaignsKept}`);
    console.log(`Campaigns to PAUSE .............. ${summary.campaignsToPause}`);
    console.log(`Pausing this run ................ ${summary.pausingThisRun}`);
    console.log(line);
    console.log(`Top offenders:`);
    for (const d of decisions.slice(0, 10)) {
      console.log(`  ${d.email}  keep=${d.keepId}  pause=${d.pauseIds.length}`);
    }
    console.log(line);
  }

  if (!args.commit) {
    console.log(`\nDRY-RUN — nothing paused. Re-run with --commit to pause.\n`);
    return;
  }

  console.log(`\n[heal-dupes] COMMIT — pausing ${toPause.length} campaigns...`);
  let paused = 0;
  let failed = 0;
  for (const id of toPause) {
    try {
      await pauseCampaign(apiKey, id);
      paused++;
    } catch (e) {
      failed++;
      console.warn(`[heal-dupes] pause failed id=${id}: ${(e as Error).message}`);
    }
    if ((paused + failed) % 250 === 0) {
      console.log(`[heal-dupes] progress: ${paused} paused, ${failed} failed / ${toPause.length}`);
    }
  }
  console.log(`\n[heal-dupes] DONE — ${paused} paused, ${failed} failed.`);
  if (totalToPause > toPause.length) {
    console.log(`[heal-dupes] ${totalToPause - toPause.length} remaining (raise/drop --limit; re-run to resume).`);
  }
}

main()
  .then(() => process.exit(0))
  .catch((e) => {
    console.error("[heal-dupes]", e);
    process.exit(1);
  });
