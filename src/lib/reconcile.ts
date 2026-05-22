/**
 * Reconciler — pulls Instantly state for every campaign and promotes any
 * webhook events we missed.
 *
 * Architecture: bronze/silver/gold layering.
 *   Phase 1 — Poll /campaigns/analytics, insert into instantly_analytics_raw,
 *             compare aggregate counts to local. If no drift, skip phases 2-3.
 *   Phase 2 — Poll /leads/list, insert into instantly_leads_raw, promote silver
 *             delivery_status + synthetic opens/clicks events.
 *   Phase 3 — Poll /emails (rate-limited 20/min), insert into instantly_emails_raw,
 *             promote silver email_sent + reply_received events with step.
 *
 * Concurrency: phase 1 & 2 = 5 concurrent calls per org (well under Instantly's
 * 100 req/sec ceiling). Phase 3 = sequential per org (20 req/min ceiling).
 *
 * Limitations: open/click discrete events have no /emails-style endpoint at
 * Instantly. Best-effort synthetic recovery via /leads/list aggregate counts.
 */
import { db } from "../db";
import { instantlyCampaigns } from "../db/schema";
import { sql } from "drizzle-orm";
import {
  getCampaign,
  getCampaignAnalytics,
  listLeadsFull,
  listEmails,
  type CampaignAnalytics,
  type LeadFull,
  type EmailRecord,
} from "./instantly-client";
import { resolveInstantlyApiKey, KeyServiceError } from "./key-client";
import {
  insertAnalyticsSnapshot,
  insertCampaignConfigSnapshot,
  insertEmailsBatch,
  insertLeadsSnapshot,
} from "./bronze";
import {
  promoteFromCampaignConfig,
  promoteFromEmailRecord,
  promoteFromLead,
  promoteSyntheticOpensFromLead,
  promoteSyntheticClicksFromLead,
} from "./silver-promote";

const RECONCILE_CONCURRENCY = 5;

export interface DriftReport {
  emailsSent: number;
  replies: number;
  bounces: number;
  unsubs: number;
  opensBackfilled: number;
  clicksBackfilled: number;
  leadStatusUpdates: number;
}

export interface ReconcileSummary {
  campaignsScanned: number;
  campaignsWithDrift: number;
  campaignsSkippedNoKey: number;
  campaignsFailed: number;
  drift: DriftReport;
  durationMs: number;
}

interface CampaignRow {
  id: string;
  instantlyCampaignId: string;
  orgId: string | null;
}

interface LocalCounts {
  sent: number;
  replies: number;
  bounces: number;
  unsubs: number;
}

async function getLocalCounts(instantlyCampaignId: string): Promise<LocalCounts> {
  const result = await db.execute(sql`
    SELECT
      COALESCE(SUM(CASE WHEN event_type = 'email_sent' THEN 1 ELSE 0 END), 0)::int AS "sent",
      COALESCE(SUM(CASE WHEN event_type = 'reply_received' THEN 1 ELSE 0 END), 0)::int AS "replies",
      COALESCE(SUM(CASE WHEN event_type = 'email_bounced' THEN 1 ELSE 0 END), 0)::int AS "bounces",
      COALESCE(SUM(CASE WHEN event_type = 'lead_unsubscribed' THEN 1 ELSE 0 END), 0)::int AS "unsubs"
    FROM instantly_events
    WHERE campaign_id = ${instantlyCampaignId}
  `);
  const rows = Array.isArray(result) ? result : (result as { rows?: unknown[] }).rows ?? [];
  const row = (rows[0] as LocalCounts | undefined) ?? { sent: 0, replies: 0, bounces: 0, unsubs: 0 };
  return row;
}

function detectDrift(local: LocalCounts, remote: CampaignAnalytics): boolean {
  return (
    remote.emails_sent_count > local.sent ||
    remote.reply_count > local.replies ||
    remote.bounced_count > local.bounces ||
    remote.unsubscribed_count > local.unsubs
  );
}

async function getEmailsCursor(instantlyCampaignId: string): Promise<string | undefined> {
  const result = await db.execute(sql`
    SELECT MAX((payload->>'timestamp_email')::timestamp) AS "maxTs"
    FROM instantly_emails_raw
    WHERE instantly_campaign_id = ${instantlyCampaignId}
  `);
  const rows = Array.isArray(result) ? result : (result as { rows?: unknown[] }).rows ?? [];
  const maxTs = (rows[0] as { maxTs?: string | null })?.maxTs;
  return maxTs ? new Date(maxTs).toISOString() : undefined;
}

interface CampaignReconcileResult {
  drifted: boolean;
  drift: DriftReport;
}

async function reconcileOneCampaign(
  campaign: CampaignRow,
  apiKey: string,
): Promise<CampaignReconcileResult> {
  const drift: DriftReport = {
    emailsSent: 0,
    replies: 0,
    bounces: 0,
    unsubs: 0,
    opensBackfilled: 0,
    clicksBackfilled: 0,
    leadStatusUpdates: 0,
  };

  // ─── Phase 0: campaign config → not_sending_status observability ───────────
  // Pull full /campaigns/{id} payload into bronze, promote `not_sending_status`
  // to silver. Failures here MUST NOT abort the rest of reconcile — observability
  // is best-effort.
  try {
    const config = (await getCampaign(
      apiKey,
      campaign.instantlyCampaignId,
    )) as unknown as Record<string, unknown> | null;
    if (config) {
      const bronzeRef = await insertCampaignConfigSnapshot(
        campaign.instantlyCampaignId,
        campaign.orgId,
        config,
      );
      const rawValue = config["not_sending_status"];
      const notSendingStatus =
        typeof rawValue === "number" ? rawValue : null;
      await promoteFromCampaignConfig({
        bronzeRowId: bronzeRef.id,
        instantlyCampaignId: campaign.instantlyCampaignId,
        notSendingStatus,
      });
    }
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(
      `[instantly-service] reconcile: /campaigns/${campaign.instantlyCampaignId} failed (phase 0): ${message}`,
    );
  }

  // ─── Phase 1: aggregate sanity check ───────────────────────────────────────
  const analytics = await getCampaignAnalytics(apiKey, campaign.instantlyCampaignId);
  if (!analytics) {
    return { drifted: false, drift };
  }

  const analyticsRef = await insertAnalyticsSnapshot(
    campaign.instantlyCampaignId,
    campaign.orgId,
    analytics,
  );

  const localCounts = await getLocalCounts(campaign.instantlyCampaignId);
  const drifted = detectDrift(localCounts, analytics);
  if (!drifted) {
    return { drifted: false, drift };
  }

  console.log(
    `[instantly-service] reconcile: drift campaign=${campaign.instantlyCampaignId} ` +
      `remote(sent=${analytics.emails_sent_count}, replies=${analytics.reply_count}, ` +
      `bounces=${analytics.bounced_count}, unsubs=${analytics.unsubscribed_count}) ` +
      `local(sent=${localCounts.sent}, replies=${localCounts.replies}, ` +
      `bounces=${localCounts.bounces}, unsubs=${localCounts.unsubs}) ` +
      `snapshot=${analyticsRef.id}`,
  );

  // ─── Phase 2: /leads/list — per-lead status ────────────────────────────────
  let leads: LeadFull[];
  try {
    leads = await listLeadsFull(apiKey, campaign.instantlyCampaignId);
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(
      `[instantly-service] reconcile: /leads/list failed campaign=${campaign.instantlyCampaignId}: ${message}`,
    );
    leads = [];
  }

  const leadsRefs = await insertLeadsSnapshot(
    campaign.instantlyCampaignId,
    campaign.orgId,
    leads,
  );

  for (let i = 0; i < leads.length; i++) {
    const lead = leads[i];
    const ref = leadsRefs[i];
    if (!ref) continue;

    const statusResult = await promoteFromLead({
      bronzeRowId: ref.id,
      instantlyCampaignId: campaign.instantlyCampaignId,
      lead,
    });
    if (statusResult.promoted) {
      drift.leadStatusUpdates++;
      if (lead.status === -1) drift.bounces++;
      else if (lead.status === -2) drift.unsubs++;
      else if (lead.timestamp_last_reply) drift.replies++;
    }

    const opensResult = await promoteSyntheticOpensFromLead({
      bronzeRowId: ref.id,
      instantlyCampaignId: campaign.instantlyCampaignId,
      lead,
    });
    if (opensResult.promoted) drift.opensBackfilled++;

    const clicksResult = await promoteSyntheticClicksFromLead({
      bronzeRowId: ref.id,
      instantlyCampaignId: campaign.instantlyCampaignId,
      lead,
    });
    if (clicksResult.promoted) drift.clicksBackfilled++;
  }

  // ─── Phase 3: /emails — per-step event backfill ────────────────────────────
  const cursor = await getEmailsCursor(campaign.instantlyCampaignId);
  let emails: EmailRecord[];
  try {
    emails = await listEmails(apiKey, {
      campaignId: campaign.instantlyCampaignId,
      minTimestampCreated: cursor,
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(
      `[instantly-service] reconcile: /emails failed campaign=${campaign.instantlyCampaignId}: ${message}`,
    );
    emails = [];
  }

  // insertEmailsBatch is idempotent (UNIQUE on instantly_email_id). Only NEW
  // rows are returned. To promote silver for every email (new + already-stored),
  // re-query the bronze table by Instantly email IDs to map ID→bronzeRowId.
  await insertEmailsBatch(campaign.instantlyCampaignId, campaign.orgId, emails);

  if (emails.length === 0) {
    return { drifted: true, drift };
  }

  const bronzeLookup = await db.execute(sql`
    SELECT id, instantly_email_id
    FROM instantly_emails_raw
    WHERE instantly_email_id IN (${sql.join(
      emails.map((e) => sql`${e.id}`),
      sql`, `,
    )})
  `);
  const lookupRows = Array.isArray(bronzeLookup)
    ? bronzeLookup
    : (bronzeLookup as { rows?: unknown[] }).rows ?? [];
  const emailIdToBronzeId = new Map<string, string>();
  for (const r of lookupRows) {
    const row = r as { id: string; instantly_email_id: string };
    emailIdToBronzeId.set(row.instantly_email_id, row.id);
  }

  for (const email of emails) {
    const bronzeId = emailIdToBronzeId.get(email.id);
    if (!bronzeId) continue;

    const result = await promoteFromEmailRecord({ bronzeRowId: bronzeId, email });
    if (result.promoted) {
      if (email.ue_type === 1) drift.emailsSent++;
      else if (email.ue_type === 2) drift.replies++;
    }
  }

  return { drifted: true, drift };
}

/** Limited-concurrency map. */
async function mapWithConcurrency<T, R>(
  items: T[],
  limit: number,
  fn: (item: T) => Promise<R>,
): Promise<R[]> {
  const results: R[] = new Array(items.length);
  let next = 0;
  async function worker() {
    for (;;) {
      const i = next++;
      if (i >= items.length) return;
      results[i] = await fn(items[i]);
    }
  }
  const workers = Array.from({ length: Math.min(limit, items.length) }, () => worker());
  await Promise.all(workers);
  return results;
}

function emptyDrift(): DriftReport {
  return {
    emailsSent: 0,
    replies: 0,
    bounces: 0,
    unsubs: 0,
    opensBackfilled: 0,
    clicksBackfilled: 0,
    leadStatusUpdates: 0,
  };
}

function mergeDrift(a: DriftReport, b: DriftReport): DriftReport {
  return {
    emailsSent: a.emailsSent + b.emailsSent,
    replies: a.replies + b.replies,
    bounces: a.bounces + b.bounces,
    unsubs: a.unsubs + b.unsubs,
    opensBackfilled: a.opensBackfilled + b.opensBackfilled,
    clicksBackfilled: a.clicksBackfilled + b.clicksBackfilled,
    leadStatusUpdates: a.leadStatusUpdates + b.leadStatusUpdates,
  };
}

/**
 * Reconcile all campaigns: scan instantly_campaigns, group by orgId, fetch
 * Instantly state per campaign, write bronze, promote silver. Returns summary.
 */
export async function reconcileAll(): Promise<ReconcileSummary> {
  const startedAt = Date.now();

  const campaigns: CampaignRow[] = await db
    .select({
      id: instantlyCampaigns.id,
      instantlyCampaignId: instantlyCampaigns.instantlyCampaignId,
      orgId: instantlyCampaigns.orgId,
    })
    .from(instantlyCampaigns);

  console.log(`[instantly-service] reconcile: starting, total=${campaigns.length}`);

  const byOrg = new Map<string | null, CampaignRow[]>();
  for (const c of campaigns) {
    const key = c.orgId ?? null;
    if (!byOrg.has(key)) byOrg.set(key, []);
    byOrg.get(key)!.push(c);
  }

  let scanned = 0;
  let withDrift = 0;
  let skippedNoKey = 0;
  let failed = 0;
  let totalDrift = emptyDrift();

  for (const [orgId, orgCampaigns] of byOrg) {
    let apiKey: string;
    try {
      if (!orgId) throw new Error("Campaign missing orgId");
      const keyResult = await resolveInstantlyApiKey(orgId, "system", {
        method: "POST",
        path: "/internal/campaigns/reconcile",
      });
      apiKey = keyResult.key;
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : String(error);
      const isKeyMissing = error instanceof KeyServiceError && error.statusCode === 404;
      const logLevel = isKeyMissing ? console.warn : console.error;
      logLevel(
        `[instantly-service] reconcile: skipping org=${orgId} (${orgCampaigns.length} campaigns) — ${message}`,
      );
      skippedNoKey += orgCampaigns.length;
      continue;
    }

    const orgResults = await mapWithConcurrency(
      orgCampaigns,
      RECONCILE_CONCURRENCY,
      async (campaign) => {
        try {
          return await reconcileOneCampaign(campaign, apiKey);
        } catch (error: unknown) {
          const message = error instanceof Error ? error.message : String(error);
          console.error(
            `[instantly-service] reconcile: campaign=${campaign.instantlyCampaignId} failed: ${message}`,
          );
          return null;
        }
      },
    );

    for (const result of orgResults) {
      scanned++;
      if (result === null) {
        failed++;
        continue;
      }
      if (result.drifted) {
        withDrift++;
        totalDrift = mergeDrift(totalDrift, result.drift);
      }
    }
  }

  const durationMs = Date.now() - startedAt;
  console.log(
    `[instantly-service] reconcile: done scanned=${scanned} drift=${withDrift} ` +
      `skippedNoKey=${skippedNoKey} failed=${failed} duration=${durationMs}ms ` +
      `drift=${JSON.stringify(totalDrift)}`,
  );

  return {
    campaignsScanned: scanned,
    campaignsWithDrift: withDrift,
    campaignsSkippedNoKey: skippedNoKey,
    campaignsFailed: failed,
    drift: totalDrift,
    durationMs,
  };
}

// Re-export for tests
export { reconcileOneCampaign };
