/**
 * Put back the follow-ups the stopped-campaign sweep cut on a customer stop.
 *
 * From 2026-10-02 to 2026-10-06 `stopped-campaigns.ts` cut every queued step of
 * every stopped campaign, contacted lead or not: holds cancelled, row marked
 * `paused`, Instantly campaign paused. Owner rule 2026-10-06: "stopping a
 * campaign SHOULD NOT pause the followups!!!" The sweep now leaves contacted
 * leads alone; this re-queues the ones it already cut, for an OWNER-APPROVED
 * list of campaign ids (the caller names them; nothing here picks campaigns).
 *
 * A candidate is a lead this sweep cut and nothing else stopped:
 *   - row `paused`, last touched since the sweep shipped (2026-10-02);
 *   - a REAL email already sent on it (a never-contacted lead's first email is a
 *     NEW first email: not restored);
 *   - step(s) > the last sent one whose hold was cancelled since 2026-10-02 and
 *     never sent, with no live (provisioned/actual) hold on that step;
 *   - no stop evidence: no reply / bounce / unsubscribe / click / reply
 *     classification event on the sequence, no standing opt-out, no manual
 *     qualification, for the org.
 *
 * Restoring one goes through the normal cost path, billed to the org as before:
 * per step a fresh step run + PROVISIONED `instantly-account-email-sent` +
 * `instantly-domain-email-sent` (`provisionStepEmailCosts`), then ONE
 * `authorizeCreditSpend` for the steps (platform key only; refused = holds
 * cancelled, lead skipped), then the Instantly campaign is RESUMED (Instantly
 * transport), then the row goes back to `active`. The dispatcher (self-send)
 * and Instantly then send at their normal per-mailbox pace; a self-send step's
 * schedule is computed off the last sent step, so the original schedule holds
 * and anything overdue just becomes due.
 *
 * Idempotent: a restored row is `active` and its steps hold a provisioned row,
 * so it is no longer a candidate. Dry-run by default.
 */

import { sql } from "drizzle-orm";

import { db } from "../db";
import { sequenceCosts } from "../db/schema";
import { authorizeCreditSpend } from "./billing-client";
import { updateCampaignStatus } from "./instantly-client";
import { resolveInstantlyApiKey, type CallerInfo } from "./key-client";
import { createRun, updateCostStatus, updateRun, type IdentityContext } from "./runs-client";
import { provisionStepEmailCosts, sendAuthorizeItems } from "./send-costs";
import { isSelfSendCampaignId } from "./self-send/transport";

/** The day the cutting sweep shipped (commit 065b431). Nothing before it is ours to undo. */
export const SWEEP_SHIPPED_AT = "2026-10-02T00:00:00Z";

const SYSTEM_USER_ID = "00000000-0000-0000-0000-000000000000";

export interface RestoreCandidate {
  rowId: string;
  instantlyCampaignId: string;
  campaignId: string;
  orgId: string;
  userId: string | null;
  runId: string | null;
  brandIds: string[] | null;
  leadEmail: string;
  /** The cancelled, never-sent steps to queue again, ascending. */
  steps: number[];
}

function rowsOf(result: unknown): Record<string, unknown>[] {
  if (Array.isArray(result)) return result as Record<string, unknown>[];
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

export async function loadRestoreCandidates(campaignIds: string[]): Promise<RestoreCandidate[]> {
  if (campaignIds.length === 0) return [];
  const result = await db.execute(sql`
    WITH paused AS (
      SELECT c.*
      FROM instantly_campaigns c
      WHERE c.campaign_id = ANY(${sql.param(campaignIds)}::text[])
        AND c.status = 'paused'
        AND c.updated_at >= ${SWEEP_SHIPPED_AT}::timestamptz
        AND c.org_id IS NOT NULL
        AND c.lead_email IS NOT NULL
        AND c.instantly_campaign_id NOT LIKE 'reserving:%'
    ),
    contacted AS (
      SELECT p.id, MAX(e.step) AS last_sent_step
      FROM paused p
      JOIN instantly_events e
        ON e.campaign_id = p.instantly_campaign_id
       AND e.event_type = 'email_sent'
       AND e.inferred = false
      GROUP BY p.id
    ),
    cut AS (
      SELECT p.id,
             COALESCE(jsonb_agg(DISTINCT sc.step ORDER BY sc.step), '[]'::jsonb) AS steps
      FROM paused p
      JOIN contacted k ON k.id = p.id
      JOIN sequence_costs sc
        ON sc.instantly_campaign_id = p.instantly_campaign_id
       AND sc.lead_email = p.lead_email
      WHERE sc.status = 'cancelled'
        AND sc.step > COALESCE(k.last_sent_step, 1)
        AND sc.updated_at >= ${SWEEP_SHIPPED_AT}::timestamptz
        AND NOT EXISTS (
          SELECT 1 FROM sequence_costs live
          WHERE live.instantly_campaign_id = p.instantly_campaign_id
            AND live.step = sc.step
            AND live.status IN ('provisioned', 'actual')
        )
        AND NOT EXISTS (
          SELECT 1 FROM instantly_events s
          WHERE s.campaign_id = p.instantly_campaign_id
            AND s.event_type = 'email_sent'
            AND s.step = sc.step
            AND s.inferred = false
        )
      GROUP BY p.id
    )
    SELECT p.id                    AS "rowId",
           p.instantly_campaign_id AS "instantlyCampaignId",
           p.campaign_id           AS "campaignId",
           p.org_id                AS "orgId",
           p.user_id               AS "userId",
           p.run_id                AS "runId",
           p.brand_ids             AS "brandIds",
           p.lead_email            AS "leadEmail",
           cut.steps               AS "steps"
    FROM paused p
    JOIN cut ON cut.id = p.id
    WHERE NOT EXISTS (
        SELECT 1 FROM instantly_events x
        WHERE x.campaign_id = p.instantly_campaign_id
          AND (x.event_type IN ('reply_received', 'auto_reply_received', 'email_bounced',
                                'lead_unsubscribed', 'email_link_clicked')
               OR x.event_type LIKE 'lead\\_%')
      )
      AND NOT EXISTS (
        SELECT 1 FROM instantly_lead_optouts_raw o
        WHERE o.org_id = p.org_id
          AND lower(o.lead_email) = lower(p.lead_email)
          AND NOT EXISTS (
            SELECT 1 FROM instantly_lead_optout_withdrawals w WHERE w.optout_id = o.id
          )
      )
      AND NOT EXISTS (
        SELECT 1 FROM instantly_manual_qualifications_raw q
        WHERE q.org_id = p.org_id
          AND lower(q.lead_email) = lower(p.lead_email)
      )
    ORDER BY p.campaign_id, p.lead_email
  `);
  return rowsOf(result).map((r) => ({
    rowId: String(r.rowId),
    instantlyCampaignId: String(r.instantlyCampaignId),
    campaignId: String(r.campaignId),
    orgId: String(r.orgId),
    userId: r.userId === null || r.userId === undefined ? null : String(r.userId),
    runId: r.runId === null || r.runId === undefined ? null : String(r.runId),
    brandIds: Array.isArray(r.brandIds) ? (r.brandIds as string[]) : null,
    leadEmail: String(r.leadEmail),
    steps: Array.isArray(r.steps) ? (r.steps as unknown[]).map(Number) : [],
  }));
}

export interface RestoreSummary {
  dryRun: boolean;
  candidates: number;
  steps: number;
  byCampaign: Record<string, { leads: number; steps: number }>;
  restored: number;
  restoredSteps: number;
  refusedInsufficientCredits: number;
  failed: number;
}

function summarize(candidates: RestoreCandidate[], dryRun: boolean): RestoreSummary {
  const byCampaign: RestoreSummary["byCampaign"] = {};
  let steps = 0;
  for (const c of candidates) {
    const b = (byCampaign[c.campaignId] ??= { leads: 0, steps: 0 });
    b.leads += 1;
    b.steps += c.steps.length;
    steps += c.steps.length;
  }
  return {
    dryRun,
    candidates: candidates.length,
    steps,
    byCampaign,
    restored: 0,
    restoredSteps: 0,
    refusedInsufficientCredits: 0,
    failed: 0,
  };
}

interface NewHold {
  step: number;
  runId: string;
  costId: string;
  domainCostId: string;
}

/**
 * Give back holds provisioned for a lead whose restore did not complete. No
 * local row exists yet (rows are written only once the restore succeeded), so
 * only runs-service is told.
 */
async function abandon(holds: NewHold[], identity: IdentityContext): Promise<void> {
  for (const h of holds) {
    const stepIdentity = { ...identity, runId: h.runId };
    for (const costId of [h.costId, h.domainCostId].filter(Boolean)) {
      await updateCostStatus(h.runId, costId, "cancelled", stepIdentity);
    }
    await updateRun(h.runId, "failed", stepIdentity, "restore abandoned");
  }
}

/**
 * Re-queue ONE lead's cut follow-ups. Returns "restored" or "insufficient_credits".
 * Throws on any other failure, after giving back what it provisioned.
 */
export async function restoreOne(
  c: RestoreCandidate,
  caller: CallerInfo,
): Promise<"restored" | "insufficient_credits"> {
  const userId = c.userId ?? SYSTEM_USER_ID;
  const brandId = c.brandIds?.join(",") || undefined;
  const tracking = { campaignId: c.campaignId, ...(brandId ? { brandId } : {}) };
  const parent: IdentityContext = { orgId: c.orgId, userId, runId: c.runId ?? undefined, tracking };
  const { key, keySource } = await resolveInstantlyApiKey(c.orgId, userId, caller);

  const holds: NewHold[] = [];
  try {
    for (const step of c.steps) {
      const stepRun = await createRun(
        {
          serviceName: "instantly-service",
          taskName: `email-send-step-${step}`,
          brandId,
          campaignId: c.campaignId,
        },
        parent,
      );
      const hold: NewHold = { step, runId: stepRun.id, costId: "", domainCostId: "" };
      holds.push(hold);
      const ids = await provisionStepEmailCosts(stepRun.id, keySource, {
        ...parent,
        runId: stepRun.id,
      });
      hold.costId = ids.costId;
      hold.domainCostId = ids.domainCostId;
    }

    if (keySource === "platform") {
      const auth = await authorizeCreditSpend(
        sendAuthorizeItems(c.steps.length),
        "instantly-send",
        parent,
      );
      if (!auth.sufficient) {
        await abandon(holds, parent);
        return "insufficient_credits";
      }
    }

    // Instantly transport: the campaign was paused on Instantly; resume it so
    // Instantly sends the rest of the sequence.
    if (!isSelfSendCampaignId(c.instantlyCampaignId)) {
      await updateCampaignStatus(key, c.instantlyCampaignId, "active");
    }
  } catch (error) {
    await abandon(holds, parent).catch((e: unknown) =>
      console.error(
        `[instantly-service] restore-stopped-followups: could not give back holds of ${c.instantlyCampaignId}: ${e instanceof Error ? e.message : String(e)}`,
      ),
    );
    throw error;
  }

  // Holds first, THEN the row goes active: an active row is what the
  // dispatcher reads, and it must find its holds.
  for (const h of holds) {
    await db.insert(sequenceCosts).values({
      campaignId: c.campaignId,
      instantlyCampaignId: c.instantlyCampaignId,
      leadEmail: c.leadEmail,
      step: h.step,
      runId: h.runId,
      costId: h.costId,
      domainCostId: h.domainCostId,
      status: "provisioned",
    });
    await updateRun(h.runId, "completed", { ...parent, runId: h.runId });
  }
  await db.execute(sql`
    UPDATE instantly_campaigns SET status = 'active', updated_at = now()
    WHERE id = ${c.rowId} AND status = 'paused'
  `);
  console.log(
    `[instantly-service] restore-stopped-followups: restored campaign=${c.instantlyCampaignId} lead=${c.leadEmail} steps=${c.steps.join(",")} (campaign ${c.campaignId})`,
  );
  return "restored";
}

export async function restoreStoppedFollowups(options: {
  campaignIds: string[];
  dryRun: boolean;
  limit?: number;
  caller: CallerInfo;
}): Promise<RestoreSummary> {
  const all = await loadRestoreCandidates(options.campaignIds);
  const candidates = options.limit ? all.slice(0, options.limit) : all;
  const summary = summarize(candidates, options.dryRun);
  if (options.dryRun) return summary;

  for (const c of candidates) {
    try {
      const outcome = await restoreOne(c, options.caller);
      if (outcome === "restored") {
        summary.restored += 1;
        summary.restoredSteps += c.steps.length;
      } else {
        summary.refusedInsufficientCredits += 1;
        console.warn(
          `[instantly-service] restore-stopped-followups: refused, insufficient credits org=${c.orgId} lead=${c.leadEmail}`,
        );
      }
    } catch (error: unknown) {
      summary.failed += 1;
      console.error(
        `[instantly-service] restore-stopped-followups: failed campaign=${c.instantlyCampaignId} lead=${c.leadEmail}: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
  }
  console.log(`[instantly-service] restore-stopped-followups: done ${JSON.stringify(summary)}`);
  return summary;
}
