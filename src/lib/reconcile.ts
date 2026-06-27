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
import { sql, eq } from "drizzle-orm";
import {
  getCampaign,
  getCampaignAnalytics,
  listLeadsFull,
  listEmails,
  deleteLeads,
  type CampaignAnalytics,
  type LeadFull,
  type EmailRecord,
} from "./instantly-client";
import {
  isDeleteFinishedEnabled,
  parseInstantlyStatus,
  isFinishedInstantlyStatus,
  localTerminalStatus,
  isLocallyTerminal,
  isLeadAlreadyGone,
} from "./finished-contacts";
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
  promoteSyntheticInterestFromLead,
  cancelRemainingProvisions,
} from "./silver-promote";

const RECONCILE_CONCURRENCY = 5;

export interface DriftReport {
  emailsSent: number;
  replies: number;
  bounces: number;
  unsubs: number;
  opensBackfilled: number;
  clicksBackfilled: number;
  interestBackfilled: number;
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
  /** Logical campaign id (sequence_costs key); null for platform sends. */
  campaignId: string | null;
  /** Run owner; passed through to the cost-cancel identity. */
  userId: string | null;
  /** Our known recipient for this per-lead campaign (used to delete the contact). */
  leadEmail: string | null;
  /** Local funnel/lifecycle status; "paused"/"completed" = already terminal. */
  status: string;
}

/**
 * Delete a finished campaign's contact on Instantly to reclaim plan quota, then
 * mark the local row terminal so reconcile stops re-polling it. Called at the
 * END of reconcileOneCampaign (AFTER the read phases backfilled all current
 * state) so a late reply/bounce is never lost to the delete.
 *
 * Campaign-level `DELETE /leads` (the only delete that frees quota). A 404 (lead
 * already gone) is tolerated — the op is idempotent; everything else fails loud
 * (propagates to the per-campaign wrapper → counted failed, retried next run).
 */
async function deleteFinishedContact(
  campaign: CampaignRow,
  apiKey: string,
  instantlyStatus: number,
): Promise<void> {
  const local = localTerminalStatus(instantlyStatus);

  if (campaign.leadEmail) {
    try {
      await deleteLeads(apiKey, campaign.instantlyCampaignId, [campaign.leadEmail]);
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : String(error);
      if (!isLeadAlreadyGone(message)) throw error;
      console.warn(
        `[instantly-service] reconcile: lead already gone campaign=${campaign.instantlyCampaignId} — ${message}`,
      );
    }
  }

  await db
    .update(instantlyCampaigns)
    .set({ status: local, updatedAt: new Date() })
    .where(eq(instantlyCampaigns.instantlyCampaignId, campaign.instantlyCampaignId));

  console.log(
    `[instantly-service] reconcile: deleted finished contact campaign=${campaign.instantlyCampaignId} ` +
      `instantlyStatus=${instantlyStatus} localStatus=${local}`,
  );
}

interface LocalCounts {
  sent: number;
  replies: number;
  bounces: number;
  unsubs: number;
  /** Distinct leads observed with at least one `email_opened` silver row. */
  opensUnique: number;
}

async function getLocalCounts(instantlyCampaignId: string): Promise<LocalCounts> {
  const result = await db.execute(sql`
    SELECT
      COALESCE(SUM(CASE WHEN event_type = 'email_sent' THEN 1 ELSE 0 END), 0)::int AS "sent",
      COALESCE(SUM(CASE WHEN event_type = 'reply_received' THEN 1 ELSE 0 END), 0)::int AS "replies",
      COALESCE(SUM(CASE WHEN event_type = 'email_bounced' THEN 1 ELSE 0 END), 0)::int AS "bounces",
      COALESCE(SUM(CASE WHEN event_type = 'lead_unsubscribed' THEN 1 ELSE 0 END), 0)::int AS "unsubs",
      COALESCE(COUNT(DISTINCT CASE WHEN event_type = 'email_opened' THEN lead_email END), 0)::int AS "opensUnique"
    FROM instantly_events
    WHERE campaign_id = ${instantlyCampaignId}
  `);
  const rows = Array.isArray(result) ? result : (result as { rows?: unknown[] }).rows ?? [];
  const row = (rows[0] as LocalCounts | undefined) ?? {
    sent: 0,
    replies: 0,
    bounces: 0,
    unsubs: 0,
    opensUnique: 0,
  };
  return row;
}

function detectDrift(local: LocalCounts, remote: CampaignAnalytics): boolean {
  return (
    remote.emails_sent_count > local.sent ||
    remote.reply_count > local.replies ||
    remote.bounced_count > local.bounces ||
    remote.unsubscribed_count > local.unsubs ||
    remote.open_count_unique > local.opensUnique
  );
}

/**
 * One-shot bypass of the per-campaign drift gate in Phase 1. When the env var
 * `RECONCILE_FORCE_PHASE_2` is "true", every campaign skips the drift check
 * and proceeds straight to Phase 2 (/leads/list backfill). Use only for
 * historical backfill runs where new silver event types (e.g. interest
 * status) were added — the drift gate sees no change on the metrics it
 * tracks, so naturally-converging reconcile would never visit those leads.
 *
 * Cost: one `/leads/list` + `/emails` call per campaign per reconcile run
 * (12k+ Instantly API calls per cycle on prod). Leave OFF in steady state.
 */
function isForcePhase2Enabled(): boolean {
  return process.env.RECONCILE_FORCE_PHASE_2 === "true";
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
    interestBackfilled: 0,
    leadStatusUpdates: 0,
  };

  // Instantly campaign status read in Phase 0 (1 active / 2 paused / 3 completed),
  // acted on only at the very end via `finish` — AFTER every read phase has
  // backfilled current state, so deleting a finished contact never drops a late
  // reply/bounce. Gated by the DELETE_FINISHED_CONTACTS_ENABLED kill-switch.
  let instantlyStatus: number | null = null;
  const finish = async (
    result: CampaignReconcileResult,
  ): Promise<CampaignReconcileResult> => {
    if (isFinishedInstantlyStatus(instantlyStatus)) {
      // CREDIT-LEAK FIX: a finished (paused/completed) campaign will not send
      // its remaining steps, and no SEQUENCE_STOP_EVENT will ever fire for the
      // un-sent steps — so cancel any still-provisioned holds here (refund the
      // org). Runs AFTER the read phases (a real reply/bounce backfilled above
      // already cancelled/actualized the relevant steps) and BEFORE the contact
      // is deleted. Independent of DELETE_FINISHED_CONTACTS_ENABLED: the credit
      // leak exists whether or not we reclaim the Instantly upload quota.
      // Idempotent — cancelRemainingProvisions only touches status='provisioned'
      // rows, so re-runs (delete OFF) no-op once cancelled.
      if (campaign.leadEmail) {
        await cancelRemainingProvisions(
          {
            campaignId: campaign.campaignId,
            instantlyCampaignId: campaign.instantlyCampaignId,
            orgId: campaign.orgId,
            userId: campaign.userId,
            runId: null,
          },
          campaign.leadEmail,
        );
      }
      if (isDeleteFinishedEnabled()) {
        await deleteFinishedContact(campaign, apiKey, instantlyStatus as number);
      }
    }
    return result;
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
      // Capture the campaign lifecycle status (acted on at the end via `finish`).
      instantlyStatus = parseInstantlyStatus(config["status"]);
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
    return finish({ drifted: false, drift });
  }

  const analyticsRef = await insertAnalyticsSnapshot(
    campaign.instantlyCampaignId,
    campaign.orgId,
    analytics,
  );

  const localCounts = await getLocalCounts(campaign.instantlyCampaignId);
  const drifted = detectDrift(localCounts, analytics);
  const forcePhase2 = isForcePhase2Enabled();
  if (!drifted && !forcePhase2) {
    return finish({ drifted: false, drift });
  }

  console.log(
    `[instantly-service] reconcile: ${forcePhase2 && !drifted ? "force-phase-2" : "drift"} ` +
      `campaign=${campaign.instantlyCampaignId} ` +
      `remote(sent=${analytics.emails_sent_count}, replies=${analytics.reply_count}, ` +
      `bounces=${analytics.bounced_count}, unsubs=${analytics.unsubscribed_count}, ` +
      `opensUnique=${analytics.open_count_unique}) ` +
      `local(sent=${localCounts.sent}, replies=${localCounts.replies}, ` +
      `bounces=${localCounts.bounces}, unsubs=${localCounts.unsubs}, ` +
      `opensUnique=${localCounts.opensUnique}) ` +
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

    const interestResult = await promoteSyntheticInterestFromLead({
      bronzeRowId: ref.id,
      instantlyCampaignId: campaign.instantlyCampaignId,
      lead,
    });
    if (interestResult.promoted) drift.interestBackfilled++;
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
    return finish({ drifted: true, drift });
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

  return finish({ drifted: true, drift });
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
    interestBackfilled: 0,
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
    interestBackfilled: a.interestBackfilled + b.interestBackfilled,
    leadStatusUpdates: a.leadStatusUpdates + b.leadStatusUpdates,
  };
}

/**
 * Reconcile all campaigns: scan instantly_campaigns, group by orgId, fetch
 * Instantly state per campaign, write bronze, promote silver. Returns summary.
 */
export async function reconcileAll(): Promise<ReconcileSummary> {
  const startedAt = Date.now();

  const allRows: CampaignRow[] = await db
    .select({
      id: instantlyCampaigns.id,
      instantlyCampaignId: instantlyCampaigns.instantlyCampaignId,
      orgId: instantlyCampaigns.orgId,
      campaignId: instantlyCampaigns.campaignId,
      userId: instantlyCampaigns.userId,
      leadEmail: instantlyCampaigns.leadEmail,
      status: instantlyCampaigns.status,
    })
    .from(instantlyCampaigns);

  // When finished-contact deletion is enabled, skip rows already marked terminal
  // locally (lead deleted on a prior run) — no point re-polling a gone contact.
  // When disabled (default), behaviour is unchanged: scan every row.
  const deleteEnabled = isDeleteFinishedEnabled();
  const campaigns = deleteEnabled
    ? allRows.filter((c) => !isLocallyTerminal(c.status))
    : allRows;

  console.log(
    `[instantly-service] reconcile: starting, total=${campaigns.length}` +
      (deleteEnabled ? ` (skipped ${allRows.length - campaigns.length} locally-terminal)` : ""),
  );

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
