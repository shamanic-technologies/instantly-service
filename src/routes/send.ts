import { Router, Request, Response } from "express";
import { db } from "../db";
import {
  instantlyCampaigns,
  instantlyLeads,
  sequenceCosts,
} from "../db/schema";
import { eq, and } from "drizzle-orm";
import {
  createCampaign as createInstantlyCampaign,
  updateCampaign as updateInstantlyCampaign,
  getCampaign as getInstantlyCampaign,
  addLeads as addInstantlyLeads,
  updateCampaignStatus,
  listAccounts,
  Lead,
  Account,
  SequenceStep,
} from "../lib/instantly-client";
import {
  createRun,
  updateRun,
  addCosts,
  type TrackingHeaders,
} from "../lib/runs-client";
import { resolveInstantlyApiKey, KeyServiceError } from "../lib/key-client";
import { authorizeCreditSpend } from "../lib/billing-client";
import { SendRequestSchema } from "../schemas";

/** Extract tracking headers from res.locals (set by identityHeaders middleware) */
function getTracking(res: Response): TrackingHeaders {
  const t: TrackingHeaders = {};
  if (res.locals.headerCampaignId) t.campaignId = res.locals.headerCampaignId;
  if (res.locals.headerBrandId) t.brandId = res.locals.headerBrandId;
  if (res.locals.headerWorkflowSlug) t.workflowSlug = res.locals.headerWorkflowSlug;
  if (res.locals.headerFeatureSlug) t.featureSlug = res.locals.headerFeatureSlug;
  return t;
}

const router = Router();

const MAX_SEND_RETRIES = 3;

/**
 * Pick an account from the list with priority order:
 * 1. kevin@growthagency.dev (if available)
 * 2. kevin@distribute.you (if available)
 * 3. Random from the remaining accounts
 *
 * Each per-lead campaign is assigned a single account so the
 * signature in the email body always matches the actual sender.
 */
const PRIORITY_ACCOUNTS = [
  "kevin@growthagency.dev",
  "kevin@distribute.you",
];

export function pickRandomAccount(accounts: Account[]): Account {
  for (const preferred of PRIORITY_ACCOUNTS) {
    const match = accounts.find((a) => a.email === preferred);
    if (match) return match;
  }
  return accounts[Math.floor(Math.random() * accounts.length)];
}

/**
 * Inject the selected account's signature into the email body.
 *
 * {{accountSignature}} only resolves in the Instantly UI — campaigns created
 * via the API send it as literal text.  Instead we use the assigned account's
 * `signature` field and splice it in directly.
 */
export function buildEmailBodyWithSignature(
  body: string,
  account: Account,
): string {
  const signature = account.signature?.trim() || "";

  if (!signature) {
    console.warn(
      `[send] Account ${account.email} has no signature configured — email will be sent without signature`,
    );
    return body.replace(/\n*\{\{accountSignature\}\}/g, "");
  }

  if (body.includes("{{accountSignature}}")) {
    return body.replace("{{accountSignature}}", `--\n${signature}`);
  }

  return `${body}\n\n--\n${signature}`;
}

/**
 * Build Instantly sequence steps from the request sequence.
 * Injects the account signature into every step's bodyHtml.
 * All steps share the same subject (Instantly handles Re: threading for follow-ups).
 */
export function buildSequenceSteps(
  subject: string,
  sequence: { step: number; bodyHtml: string; daysSinceLastStep: number }[],
  account: Account,
): SequenceStep[] {
  return sequence
    .sort((a, b) => a.step - b.step)
    .map((s) => ({
      subject,
      bodyHtml: buildEmailBodyWithSignature(s.bodyHtml, account),
      daysSinceLastStep: s.daysSinceLastStep,
    }));
}

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
 * Create an Instantly campaign, assign an account, add a lead, and activate it.
 * Returns the instantlyCampaignId on success, or null if not_sending_status detected.
 */
async function tryCreateAndActivateCampaign(
  apiKey: string,
  campaignId: string,
  account: Account,
  steps: SequenceStep[],
  lead: Lead,
): Promise<{ instantlyCampaignId: string; added: number } | null> {
  console.log(`[send] Creating new Instantly campaign for ${campaignId} with account ${account.email}`);
  const instantlyCampaign = await createInstantlyCampaign(apiKey, {
    name: `Campaign ${campaignId}`,
    steps,
  });
  console.log(`[send] Created instantly campaign id=${instantlyCampaign.id} status=${instantlyCampaign.status}`);

  // Assign the selected account via PATCH
  console.log(`[send] Assigning account ${account.email} to campaign ${instantlyCampaign.id}`);
  await updateInstantlyCampaign(apiKey, instantlyCampaign.id, {
    email_list: [account.email],
    open_tracking: true,
    link_tracking: true,
    insert_unsubscribe_header: true,
    stop_on_reply: true,
  });

  // Verify accounts were assigned
  const verified = await getInstantlyCampaign(apiKey, instantlyCampaign.id) as unknown as Record<string, unknown>;
  console.log(`[send] Verify after PATCH — email_list=${JSON.stringify(verified.email_list)} not_sending_status=${JSON.stringify(verified.not_sending_status)}`);

  // Add lead
  console.log(`[send] Adding lead ${lead.email} to instantly campaign ${instantlyCampaign.id}`);
  const result = await addInstantlyLeads(apiKey, {
    campaign_id: instantlyCampaign.id,
    leads: [lead],
  });
  console.log(`[send] addLeads result: added=${result.added}`);

  // Activate
  console.log(`[send] Activating campaign ${instantlyCampaign.id}`);
  await updateCampaignStatus(apiKey, instantlyCampaign.id, "active");

  // Verify post-activation
  const postActivate = await getInstantlyCampaign(apiKey, instantlyCampaign.id) as unknown as Record<string, unknown>;
  console.log(`[send] Post-activate — status=${postActivate.status} email_list=${JSON.stringify(postActivate.email_list)} not_sending_status=${JSON.stringify(postActivate.not_sending_status)}`);

  if (postActivate.not_sending_status) {
    const reason = `not_sending_status: ${JSON.stringify(postActivate.not_sending_status)}`;
    console.warn(`[send] Campaign ${instantlyCampaign.id} has ${reason}`);
    return null;
  }

  return { instantlyCampaignId: instantlyCampaign.id, added: result.added };
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
 * If Instantly reports not_sending_status after activation, retries up to
 * MAX_SEND_RETRIES times with a different random account before giving up.
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

  // Use header values as fallback when body fields are missing
  const brandIds: string[] = body.brandIds.length > 0
    ? body.brandIds
    : (res.locals.headerBrandIds as string[] | undefined) ?? [];
  const campaignId = body.campaignId || tracking.campaignId || "";
  const workflowSlug = body.workflowSlug || tracking.workflowSlug;

  console.log(`[send] POST /send to=${body.to} campaignId=${campaignId} brandIds=${brandIds.join(",")} subject="${body.subject}" steps=${body.sequence.length}`);

  try {
    // 0. Resolve Instantly API key (auto-resolves org vs platform key)
    const { key: apiKey, keySource } = await resolveInstantlyApiKey(orgId, userId, {
      method: "POST",
      path: "/send",
    });

    // 1. Credit authorization (platform keys only)
    if (keySource === "platform") {
      const auth = await authorizeCreditSpend(
        [{ costName: "instantly-email-send", quantity: body.sequence.length }],
        "instantly-email-send",
        {
          orgId,
          userId,
          runId: res.locals.runId as string,
          campaignId,
          brandId: brandIds.join(","),
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
      // 3. Check available accounts — status > 0 means active (1 = active, 2 = active+warming)
      const allAccounts = await listAccounts(apiKey);
      console.log(`[send] Account statuses: ${JSON.stringify(allAccounts.map((a) => ({ email: a.email, status: a.status, warmup_status: a.warmup_status })))}`);
      const accounts = allAccounts.filter((a) => a.status > 0);
      if (accounts.length === 0) {
        const total = allAccounts.length;
        const msg = total === 0
          ? "No email accounts found in Instantly"
          : `Found ${total} email account(s) but none are active — please check your Instantly subscriptions`;
        throw new Error(msg);
      }
      console.log(`[send] ${accounts.length}/${allAccounts.length} accounts are active`);

      const sortedSequence = [...body.sequence].sort((a, b) => a.step - b.step);

      // 3. Dedup: check if this (campaignId, leadEmail) pair already has a campaign
      const existing = await findExistingCampaign(campaignId, body.to);

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
        // New campaign — create with retry loop
        const lead: Lead = {
          email: body.to,
          first_name: body.firstName,
          last_name: body.lastName,
          company_name: body.company,
          variables: body.variables,
        };

        let result: { instantlyCampaignId: string; added: number } | null = null;
        let lastReason: string | undefined;

        for (let attempt = 1; attempt <= MAX_SEND_RETRIES; attempt++) {
          const account = pickRandomAccount(accounts);
          const steps = buildSequenceSteps(body.subject, sortedSequence, account);

          console.log(`[send] Attempt ${attempt}/${MAX_SEND_RETRIES} for ${campaignId}/${body.to} with account ${account.email}`);
          result = await tryCreateAndActivateCampaign(apiKey, campaignId, account, steps, lead);

          if (result) {
            break;
          }

          lastReason = `Attempt ${attempt}/${MAX_SEND_RETRIES} failed with not_sending_status`;
          console.warn(`[send] ${lastReason}`);
        }

        if (!result) {
          // All retries exhausted — no step runs were created yet
          const errorMsg = `Campaign failed after ${MAX_SEND_RETRIES} retry attempts`;
          console.error(`[send] ${errorMsg} for ${campaignId}/${body.to}`);
          return res.status(500).json({
            error: errorMsg,
            details: lastReason,
          });
        }

        added = result.added;

        // Success — save campaign to DB (atomic dedup via unique index)
        const [insertedCampaign] = await db
          .insert(instantlyCampaigns)
          .values({
            campaignId,
            leadEmail: body.to,
            leadId: body.leadId,
            instantlyCampaignId: result.instantlyCampaignId,
            name: `Campaign ${campaignId}`,
            status: "active",
            deliveryStatus: "sent",
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
            instantlyCampaignId: result.instantlyCampaignId,
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

      // 4. Create per-step runs: 1 actual+completed (step 1) + N-1 provisioned+ongoing
      const parentIdentity = { orgId, userId, runId: res.locals.runId as string, tracking };
      for (const s of sortedSequence) {
        const isFirstStep = s.step === sortedSequence[0].step;
        const stepRun = await createRun({
          serviceName: "instantly-service",
          taskName: `email-send-step-${s.step}`,
          brandId: brandIds.join(","),
          campaignId,
        }, parentIdentity);

        const stepIdentity = { orgId, userId, runId: stepRun.id, tracking };
        const costResult = await addCosts(stepRun.id, [{
          costName: "instantly-email-send",
          quantity: 1,
          costSource: keySource,
          status: isFirstStep ? "actual" as const : "provisioned" as const,
        }], stepIdentity);

        const costId = costResult.costs[0]?.id;
        if (costId) {
          await db.insert(sequenceCosts).values({
            campaignId,
            leadEmail: body.to,
            step: s.step,
            runId: stepRun.id,
            costId,
            status: isFirstStep ? "actual" : "provisioned",
          });
        }

        if (isFirstStep) {
          await updateRun(stepRun.id, "completed", stepIdentity);
        }

        stepRuns.push({ step: s.step, runId: stepRun.id });
      }

      console.log(`[send] Done — to=${body.to} campaignId=${campaignId} added=${added} stepRuns=${stepRuns.length}`);
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
    console.error(`[send] Failed to send — to=${body.to} error="${error.message}"`);
    res.status(500).json({
      error: "Failed to send email",
      details: error.message,
    });
  }
});

export default router;
