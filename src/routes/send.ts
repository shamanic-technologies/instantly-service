import { Router, Request, Response } from "express";
import { db } from "../db";
import {
  instantlyCampaigns,
  instantlyLeads,
  sequenceCosts,
} from "../db/schema";
import { eq, and, ne, isNotNull } from "drizzle-orm";
import {
  Lead,
} from "../lib/instantly-client";
import { sendLeadToInstantly } from "../lib/send-lead";
import {
  createRun,
  updateRun,
  addCosts,
  type TrackingHeaders,
} from "../lib/runs-client";
import { resolveInstantlyApiKey, KeyServiceError } from "../lib/key-client";
import { authorizeCreditSpend } from "../lib/billing-client";
import { SendRequestSchema } from "../schemas";
import { traceEvent } from "../lib/trace-event";

/** Extract tracking headers from res.locals (set by requireOrgId middleware) */
function getTracking(res: Response): TrackingHeaders {
  const t: TrackingHeaders = {};
  if (res.locals.headerCampaignId) t.campaignId = res.locals.headerCampaignId;
  if (res.locals.headerBrandId) t.brandId = res.locals.headerBrandId;
  if (res.locals.headerWorkflowSlug) t.workflowSlug = res.locals.headerWorkflowSlug;
  if (res.locals.headerFeatureSlug) t.featureSlug = res.locals.headerFeatureSlug;
  return t;
}

const router = Router();

/**
 * Check if a campaign already exists for this (campaignId, leadEmail) pair.
 */
async function findExistingCampaign(campaignId: string, leadEmail: string) {
  const [existing] = await db
    .select()
    .from(instantlyCampaigns)
    .where(
      and(
        eq(instantlyCampaigns.campaignId, campaignId),
        eq(instantlyCampaigns.leadEmail, leadEmail),
      ),
    );
  return existing ?? null;
}

/**
 * POST /send
 * Add a lead to a multi-step sequence campaign via Instantly.
 *
 * Creates one run per sequence step:
 * - Step 1: run completed immediately, cost = actual
 * - Steps 2-N: runs stay ongoing, costs = provisioned
 *
 * Follow-up runs are completed when webhook email_sent arrives,
 * or failed on reply/bounce/unsub/not_interested/campaign error.
 *
 * Dispatch (find healthy account + create campaign + add lead + activate)
 * is delegated to `sendLeadToInstantly()` in `lib/send-lead.ts`. One-shot —
 * NSS post-activate is logged but never causes a retry (retry-stuck owns
 * the eventual catch-up 72h later if the campaign never dispatches).
 */
router.post("/", async (req: Request, res: Response) => {
  const parsed = SendRequestSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({
      error: "Invalid request",
      details: parsed.error.flatten(),
    });
  }
  const body = parsed.data;
  const orgId = res.locals.orgId as string;
  const userId = res.locals.userId as string;
  const tracking = getTracking(res);

  // Read from headers only (no body duplication)
  const brandIds: string[] = (res.locals.headerBrandIds as string[] | undefined) ?? [];
  const campaignId = tracking.campaignId ?? null;
  const campaignName = campaignId ? `Campaign ${campaignId}` : `Platform send ${body.to}`;
  const brandId = brandIds.join(",") || undefined;
  const workflowSlug = tracking.workflowSlug;

  console.log(`[send] POST /send to=${body.to} campaignId=${campaignId ?? "none"} brandIds=${brandIds.join(",")} subject="${body.subject}" steps=${body.sequence.length}`);
  traceEvent(res.locals.runId as string, { service: "instantly-service", event: "send-start", detail: `to=${body.to}, campaignId=${campaignId ?? "none"}, steps=${body.sequence.length}` }, req.headers).catch(() => {});

  try {
    // 0. Resolve Instantly API key (auto-resolves org vs platform key)
    const { key: apiKey, keySource } = await resolveInstantlyApiKey(orgId, userId, {
      method: "POST",
      path: "/send",
    });
    traceEvent(res.locals.runId as string, { service: "instantly-service", event: "send-key-resolved", detail: `keySource=${keySource}` }, req.headers).catch(() => {});

    // 1. Credit authorization (platform keys only)
    if (keySource === "platform") {
      const auth = await authorizeCreditSpend(
        [
          { costName: "instantly-contact-uploaded", quantity: 1 },
          { costName: "instantly-account-email-sent", quantity: body.sequence.length },
          { costName: "instantly-domain-email-sent", quantity: body.sequence.length },
        ],
        "instantly-send",
        {
          orgId,
          userId,
          runId: res.locals.runId as string,
          campaignId: campaignId ?? undefined,
          brandId,
          workflowSlug,
          featureSlug: tracking.featureSlug,
        },
      );
      if (!auth.sufficient) {
        return res.status(402).json({
          error: "Insufficient credits",
          balance_cents: auth.balance_cents,
          required_cents: auth.required_cents,
        });
      }
    }

    // 2. Per-step runs are created AFTER successful campaign activation
    const stepRuns: { step: number; runId: string }[] = [];

    try {
      const sortedSequence = [...body.sequence].sort((a, b) => a.step - b.step);

      // 3. Lead ID conflict check: if this email already exists with a different lead_id, reject
      if (body.leadId) {
        const [conflict] = await db
          .select({ leadId: instantlyCampaigns.leadId })
          .from(instantlyCampaigns)
          .where(
            and(
              eq(instantlyCampaigns.leadEmail, body.to),
              isNotNull(instantlyCampaigns.leadId),
              ne(instantlyCampaigns.leadId, body.leadId),
            ),
          )
          .limit(1);

        if (conflict) {
          console.error(`[send] Lead ID conflict: email=${body.to} existing=${conflict.leadId} received=${body.leadId}`);
          return res.status(409).json({
            error: "Lead ID conflict",
            details: `Email ${body.to} already exists with lead_id ${conflict.leadId}, received ${body.leadId}`,
          });
        }
      }

      // 4. Dedup: check if this (campaignId, leadEmail) pair already has a campaign
      const existing = campaignId ? await findExistingCampaign(campaignId, body.to) : null;

      let savedLead: { id: string } | undefined;
      let added = 0;

      if (existing) {
        // Already processed — return early, no new step runs or costs
        console.log(`[send] Lead ${body.to} already processed for campaign ${campaignId}, skipping`);
        return res.status(200).json({
          success: true,
          campaignId,
          added: 0,
          duplicate: true,
        });
      } else {
        // 5. Dispatch lead to a healthy Instantly account (retries internally).
        const lead: Lead = {
          email: body.to,
          first_name: body.firstName,
          last_name: body.lastName,
          company_name: body.company,
          variables: body.variables,
        };

        const sendResult = await sendLeadToInstantly({
          apiKey,
          campaignName,
          subject: body.subject,
          sortedSequence,
          lead,
        });

        if (!sendResult.ok) {
          const detail = "No active Instantly accounts available for this organization";
          console.error(`[send] ${detail} for ${campaignId ?? "none"}/${body.to}`);
          return res.status(500).json({
            error: "Failed to send lead",
            details: detail,
          });
        }

        traceEvent(
          res.locals.runId as string,
          {
            service: "instantly-service",
            event: "send-campaign-created",
            detail: `instantlyCampaignId=${sendResult.value.instantlyCampaignId}, added=${sendResult.value.added}, account=${sendResult.value.account.email}`,
          },
          req.headers,
        ).catch(() => {});

        added = sendResult.value.added;

        // Success — save campaign to DB (atomic dedup via unique index)
        const [insertedCampaign] = await db
          .insert(instantlyCampaigns)
          .values({
            campaignId,
            leadEmail: body.to,
            leadId: body.leadId,
            instantlyCampaignId: sendResult.value.instantlyCampaignId,
            name: campaignName,
            status: "active",
            deliveryStatus: "contacted",
            orgId,
            userId,
            brandIds,
            workflowSlug,
            featureSlug: tracking.featureSlug,
            runId: res.locals.runId as string,
          })
          .onConflictDoNothing()
          .returning();

        if (!insertedCampaign) {
          // A concurrent request already claimed this (campaignId, leadEmail) pair
          console.warn(`[send] Race condition: lead ${body.to} for campaign ${campaignId} was already claimed by a concurrent request`);
          return res.status(409).json({
            error: "Lead was not added to campaign (possibly duplicate)",
            details: `Lead ${body.to} is already being processed for campaign ${campaignId}`,
          });
        }

        // Save lead to DB
        const [createdLead] = await db
          .insert(instantlyLeads)
          .values({
            instantlyCampaignId: sendResult.value.instantlyCampaignId,
            email: body.to,
            firstName: body.firstName,
            lastName: body.lastName,
            companyName: body.company,
            customVariables: body.variables,
            orgId,
            runId: null,
          })
          .onConflictDoNothing()
          .returning();

        if (createdLead) savedLead = createdLead;
      }

      // 4. Create per-step runs. Every step's email costs are inserted as
      //    `provisioned` and flipped to `actual` when the `email_sent` webhook
      //    arrives (silver-promote.ts:handleFollowUpSent). Instantly's daily
      //    quota slot per sender is only consumed at actual dispatch, so we
      //    must not charge customers at /send time for step 1.
      //
      //    Cost model:
      //    - instantly-contact-uploaded: 1× on first step, actual at /send (the
      //      lead IS uploaded to Instantly, regardless of subsequent dispatch).
      //    - instantly-account-email-sent: 1× per step, provisioned at /send,
      //      promoted to actual on webhook email_sent.
      //    - instantly-domain-email-sent: 1× per step, provisioned at /send,
      //      promoted to actual on webhook email_sent.
      const parentIdentity = { orgId, userId, runId: res.locals.runId as string, tracking };
      for (const s of sortedSequence) {
        const isFirstStep = s.step === sortedSequence[0].step;
        const stepRun = await createRun({
          serviceName: "instantly-service",
          taskName: `email-send-step-${s.step}`,
          brandId,
          campaignId: campaignId ?? undefined,
        }, parentIdentity);

        const stepIdentity = { orgId, userId, runId: stepRun.id, tracking };

        const costItems: { costName: string; quantity: number; costSource: "platform" | "org"; status: "actual" | "provisioned" }[] = [
          { costName: "instantly-account-email-sent", quantity: 1, costSource: keySource, status: "provisioned" },
          { costName: "instantly-domain-email-sent", quantity: 1, costSource: keySource, status: "provisioned" },
        ];
        // Contact upload cost: once per send, always actual, not tracked in sequence_costs
        if (isFirstStep) {
          costItems.push({ costName: "instantly-contact-uploaded", quantity: 1, costSource: keySource, status: "actual" });
        }

        const costResult = await addCosts(stepRun.id, costItems, stepIdentity);

        // Store email costs in sequence_costs for webhook lifecycle management
        // Contact upload cost is NOT stored here — it is never cancelled
        for (const cost of costResult.costs) {
          if (cost.costName === "instantly-contact-uploaded") continue;
          await db.insert(sequenceCosts).values({
            campaignId,
            leadEmail: body.to,
            step: s.step,
            runId: stepRun.id,
            costId: cost.id,
            status: "provisioned",
          });
        }

        await updateRun(stepRun.id, "completed", stepIdentity);

        stepRuns.push({ step: s.step, runId: stepRun.id });
      }

      traceEvent(res.locals.runId as string, { service: "instantly-service", event: "send-done", detail: `to=${body.to}, campaignId=${campaignId ?? "none"}, added=${added}, stepRuns=${stepRuns.length}` }, req.headers).catch(() => {});
      console.log(`[send] Done — to=${body.to} campaignId=${campaignId ?? "none"} added=${added} stepRuns=${stepRuns.length}`);
      res.status(200).json({
        success: true,
        campaignId,
        leadId: savedLead?.id,
        added,
        stepRuns: stepRuns.length > 0 ? stepRuns : undefined,
      });
    } catch (error: any) {
      // Fail any step runs that were already created
      for (const sr of stepRuns) {
        try {
          await updateRun(sr.runId, "failed", { orgId, userId, runId: sr.runId }, error.message);
        } catch {
          // Run may already be completed (step 1) — ignore
        }
      }
      throw error;
    }
  } catch (error: any) {
    if (error instanceof KeyServiceError && error.statusCode === 404) {
      return res.status(422).json({
        error: "API key not configured for this organization",
        details: "Please configure your Instantly API key before sending emails.",
      });
    }
    traceEvent(res.locals.runId as string, { service: "instantly-service", event: "send-error", detail: error.message, level: "error" }, req.headers).catch(() => {});
    console.error(`[send] Failed to send — to=${body.to} error="${error.message}"`);
    res.status(500).json({
      error: "Failed to send email",
      details: error.message,
    });
  }
});

export default router;
