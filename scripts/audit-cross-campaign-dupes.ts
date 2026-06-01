/**
 * Read-only audit: cross-campaign duplicate contacts.
 *
 * Answers "is the same person sitting in ≥2 ACTIVE campaigns right now?" — the
 * duplicate-outreach bug DIS-77 healed once (57 active dups cancelled) but whose
 * root-cause prevention in POST /send was never shipped, so it can re-accumulate.
 *
 * SOURCE OF TRUTH = Instantly API (the local DB can be stale / wrong, so the
 * duplicate FACT is derived entirely from Instantly):
 *   1. List all campaigns      → resolve which are ACTIVE (status === 1).
 *   2. Sweep all leads          → group lead rows by email, collect campaigns.
 *   3. Flag emails in ≥2 active campaigns.
 *
 * BRAND/ORG LABEL = local DB (the ONLY place brand lives — Instantly has no
 * brand field). Used purely to CLASSIFY each collision (same-brand vs cross-
 * brand), never to decide the duplicate fact. Rows with no DB match are still
 * reported (severity `unknown-brand`).
 *
 * READ-ONLY: no PATCH / activate / pause / cancel. This audits; it does not heal.
 * For healing, see DIS-77's (uncommitted) dedup-active-duplicates pass.
 *
 * Usage:
 *   npm run audit:dupes -- <orgId> [--json] [--limit N] [--severe-only]
 *
 *   <orgId>        internal org UUID used to resolve the Instantly API key via
 *                  key-service (mirrors scripts/inspect-body.ts). Pass an org on
 *                  the PLATFORM key to audit the shared platform workspace.
 *   --json         emit machine-readable JSON instead of the text report.
 *   --limit N      max detail rows to print (default 50; ignored with --json).
 *   --severe-only  only print same-brand collisions (true redundant outreach).
 *
 * Required env (prod): DATABASE_URL, KEY_SERVICE_URL, KEY_SERVICE_API_KEY.
 */

import { db, closeDb } from "../src/db";
import { inArray } from "drizzle-orm";
import { instantlyCampaigns } from "../src/db/schema";
import { resolveInstantlyApiKey } from "../src/lib/key-client";
import {
  findDuplicateContacts,
  summarizeDuplicates,
  type AuditCampaign,
  type LeadMembership,
} from "../src/lib/audit-duplicates";

const INSTANTLY_API_URL = "https://api.instantly.ai/api/v2";
const PAGE_LIMIT = 100;
/** Instantly campaign status codes considered "actively contacting". 1 = Active. */
const ACTIVE_STATUSES = new Set<number>([1]);
/** Pacing between calls — 110ms ≈ 545 req/min, under Instantly's ~600/min general cap. */
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
      console.warn(`[audit-dupes] ${method} ${path} → ${res.status}, retry in ${wait}ms`);
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

/** GET /campaigns — paginated via next_starting_after (limit 100; >100 returns empty). */
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

/** POST /leads/list with no campaign filter — sweep every lead in the workspace. */
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
    if (out.length % 1000 === 0 && out.length > 0) {
      console.log(`[audit-dupes] swept ${out.length} leads...`);
    }
    if (!page.next_starting_after || items.length === 0) break;
    startingAfter = page.next_starting_after;
  }
  return out;
}

/** DB query is chunked: drizzle expands `ANY(${jsArray})` into a ROW expression,
 * which Postgres caps at 1664 entries (error 54011). Batch well under that. */
const BRAND_LABEL_CHUNK = 1000;

/** Local DB brand/org label per Instantly campaign id. Instantly has no brand field. */
async function loadBrandLabels(
  instantlyCampaignIds: string[],
): Promise<Map<string, { brandIds: string[]; orgId: string | null }>> {
  const map = new Map<string, { brandIds: string[]; orgId: string | null }>();
  for (let i = 0; i < instantlyCampaignIds.length; i += BRAND_LABEL_CHUNK) {
    const chunk = instantlyCampaignIds.slice(i, i + BRAND_LABEL_CHUNK);
    const rows = await db
      .select({
        id: instantlyCampaigns.instantlyCampaignId,
        brandIds: instantlyCampaigns.brandIds,
        orgId: instantlyCampaigns.orgId,
      })
      .from(instantlyCampaigns)
      .where(inArray(instantlyCampaigns.instantlyCampaignId, chunk));
    for (const r of rows) {
      map.set(r.id, {
        brandIds: r.brandIds ?? [],
        orgId: r.orgId,
      });
    }
  }
  return map;
}

interface Args {
  orgId: string;
  json: boolean;
  limit: number;
  severeOnly: boolean;
}

function parseArgs(argv: string[]): Args {
  const positional = argv.filter((a) => !a.startsWith("--"));
  const flags = new Set(argv.filter((a) => a.startsWith("--") && !a.includes("=")));
  const limitArg = argv.find((a) => a.startsWith("--limit="));
  // also support "--limit N"
  let limit = 50;
  if (limitArg) limit = Number(limitArg.split("=")[1]) || 50;
  else {
    const i = argv.indexOf("--limit");
    if (i >= 0 && argv[i + 1]) limit = Number(argv[i + 1]) || 50;
  }
  return {
    orgId: positional[0] ?? "",
    json: flags.has("--json"),
    limit,
    severeOnly: flags.has("--severe-only"),
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));

  // Key resolution. Prefer an explicit INSTANTLY_API_KEY from env: key-service
  // lives at *.railway.internal, which only resolves INSIDE Railway's network —
  // a local `railway run` shell cannot reach it (ENOTFOUND). Setting
  // INSTANTLY_API_KEY directly lets the audit run from any laptop against the
  // workspace that key belongs to (e.g. the shared platform workspace). When the
  // env key is absent, fall back to key-service resolution keyed on <orgId>.
  let apiKey: string;
  let keySource: string;
  const envKey = process.env.INSTANTLY_API_KEY?.trim();
  if (envKey) {
    apiKey = envKey;
    keySource = "env";
    console.log(
      `[audit-dupes] using INSTANTLY_API_KEY from env (key-service bypassed); auditing that key's workspace.`,
    );
  } else {
    if (!args.orgId) {
      console.error(
        "Usage: npm run audit:dupes -- <orgId> [--json] [--limit N] [--severe-only]\n" +
          "       (or set INSTANTLY_API_KEY to audit that key's workspace directly)",
      );
      process.exit(1);
    }
    const resolved = await resolveInstantlyApiKey(args.orgId, "system", {
      method: "GET",
      path: "/internal/audit-dupes",
    });
    apiKey = resolved.key;
    keySource = resolved.keySource;
    console.log(`[audit-dupes] resolved ${keySource} Instantly key for org ${args.orgId}`);
  }

  console.log(`[audit-dupes] listing campaigns...`);
  const campaigns = await listAllCampaigns(apiKey);
  const activeIds = campaigns
    .filter((c) => ACTIVE_STATUSES.has(Number(c.status)))
    .map((c) => c.id);
  console.log(
    `[audit-dupes] ${campaigns.length} campaigns, ${activeIds.length} active (status 1).`,
  );

  // Build the campaign map with active flag but NO brand yet. Brand labels are
  // loaded from the DB only for campaigns that actually collide (computed after
  // the lead sweep) — querying brand for all 10k+ active campaigns up front is
  // both wasteful and trips Postgres' ROW-expression limit.
  const campaignsById = new Map<string, AuditCampaign>();
  for (const c of campaigns) {
    campaignsById.set(c.id, {
      id: c.id,
      name: c.name ?? c.id,
      active: ACTIVE_STATUSES.has(Number(c.status)),
      brandIds: [],
      orgId: null,
    });
  }

  console.log(`[audit-dupes] sweeping leads from Instantly...`);
  const leads = await listAllLeads(apiKey);
  const memberships: LeadMembership[] = leads
    .filter((l) => l.email && l.campaign)
    .map((l) => ({ email: l.email as string, campaignId: l.campaign as string }));

  // distinct emails that sit in at least one ACTIVE campaign (denominator)
  const activeSet = new Set(activeIds);
  const distinctActiveEmails = new Set(
    memberships.filter((m) => activeSet.has(m.campaignId)).map((m) => m.email.toLowerCase()),
  );

  // First pass (no brand) → identifies which campaigns collide.
  const firstPass = findDuplicateContacts(memberships, campaignsById);
  const dupCampaignIds = [
    ...new Set(firstPass.flatMap((d) => d.activeCampaigns.map((c) => c.id))),
  ];
  console.log(
    `[audit-dupes] ${firstPass.length} duplicate emails; loading brand labels for ${dupCampaignIds.length} colliding campaigns...`,
  );

  // Patch only the colliding campaigns with their DB brand/org, then re-classify.
  const brandLabels = await loadBrandLabels(dupCampaignIds);
  for (const id of dupCampaignIds) {
    const label = brandLabels.get(id);
    const c = campaignsById.get(id);
    if (c && label) {
      c.brandIds = label.brandIds;
      c.orgId = label.orgId;
    }
  }

  let duplicates = findDuplicateContacts(memberships, campaignsById);
  if (args.severeOnly) duplicates = duplicates.filter((d) => d.severity === "same-brand");

  const summary = summarizeDuplicates(duplicates, activeIds.length, distinctActiveEmails.size);

  if (args.json) {
    console.log(JSON.stringify({ summary, duplicates }, null, 2));
    return;
  }

  printReport(summary, duplicates, args.limit);
}

function printReport(
  summary: ReturnType<typeof summarizeDuplicates>,
  duplicates: ReturnType<typeof findDuplicateContacts>,
  limit: number,
) {
  const line = "─".repeat(72);
  console.log(`\n${line}`);
  console.log(`CROSS-CAMPAIGN DUPLICATE AUDIT  (source: Instantly API)`);
  console.log(line);
  console.log(`Active campaigns scanned ........ ${summary.activeCampaignsScanned}`);
  console.log(`Distinct emails in active ....... ${summary.distinctEmailsInActive}`);
  console.log(`Emails in ≥2 active campaigns ... ${summary.duplicateEmails}`);
  console.log(`  ├─ same-brand (SEVERE) ........ ${summary.sameBrand}`);
  console.log(`  ├─ cross-brand ................ ${summary.crossBrand}`);
  console.log(`  └─ unknown-brand (DB gap) ..... ${summary.unknownBrand}`);
  console.log(`Redundant active campaigns ...... ${summary.redundantActiveCampaigns}`);
  if (summary.worstOffender) {
    console.log(
      `Worst offender .................. ${summary.worstOffender.email} (${summary.worstOffender.totalActive} active campaigns)`,
    );
  }
  console.log(line);
  console.log(`Note: brand/org labels come from the LOCAL DB (only source of brand).`);
  console.log(`      The duplicate fact itself is from Instantly and is authoritative.`);
  console.log(line);

  if (duplicates.length === 0) {
    console.log(`\n✅ No cross-campaign duplicates found.\n`);
    return;
  }

  const shown = duplicates.slice(0, limit);
  console.log(`\nDetail (showing ${shown.length} of ${duplicates.length}):\n`);
  for (const d of shown) {
    const tag =
      d.severity === "same-brand" ? "🔴 SAME-BRAND" : d.severity === "cross-brand" ? "🟡 cross-brand" : "⚪ unknown-brand";
    console.log(`${tag}  ${d.email}  (${d.totalActive} active)`);
    if (d.sameBrandIds.length > 0) {
      console.log(`    shared brand(s): ${d.sameBrandIds.join(", ")}`);
    }
    for (const c of d.activeCampaigns) {
      const brand = c.brandIds.length > 0 ? c.brandIds.join(",") : "?";
      const org = c.orgId ?? "?";
      console.log(`    - ${c.id}  "${c.name}"  brand=${brand}  org=${org}`);
    }
    console.log("");
  }
  if (duplicates.length > shown.length) {
    console.log(`… ${duplicates.length - shown.length} more (raise --limit or use --json).\n`);
  }
}

main()
  .then(() => closeDb())
  .catch(async (e) => {
    console.error("[audit-dupes]", e);
    await closeDb();
    process.exit(1);
  });
