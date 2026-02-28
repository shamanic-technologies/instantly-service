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
} from "../lib/runs-client";
import { resolveInstantlyApiKey, KeyServiceError } from "../lib/key-client";
import { SendRequestSchema } from "../schemas";

const router = Router();

const MAX_SEND_RETRIES = 3;

/**
 * Pick a random account from the list.
 *
 * Each per-lead campaign is assigned a single random account so the
 * signature in the email body always matches the actual sender.
 */
export function pickRandomAccount(accounts: Account[]): Account {
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
  console.log(`[send] POST /send to=${body.to} campaignId=${body.campaignId} subject="${body.subject}" steps=${body.sequence.length}`);

  try {
    // 0. Resolve Instantly API key (BYOK per-org or shared app key)
    const apiKey = await resolveInstantlyApiKey(body.orgId, {
      method: "POST",
      path: "/send",
    });

    // 1. Per-step runs are created AFTER successful campaign activation
    const stepRuns: { step: number; runId: string }[] = [];

    try {
      // 3. Check available accounts
      const accounts = await listAccounts(apiKey);
      if (accounts.length === 0) {
        throw new Error("No email accounts available — cannot create campaign");
      }

      const sortedSequence = [...body.sequence].sort((a, b) => a.step - b.step);

      // 3. Dedup: check if this (campaignId, leadEmail) pair already has a campaign
      const existing = await findExistingCampaign(body.campaignId, body.to);

      let savedLead: { id: string } | undefined;
      let added = 0;

      if (existing) {
        // Already processed — skip Instantly API calls
        console.log(`[send] Lead ${body.to} already processed for campaign ${body.campaignId}, skipping`);
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

          console.log(`[send] Attempt ${attempt}/${MAX_SEND_RETRIES} for ${body.campaignId}/${body.to} with account ${account.email}`);
          result = await tryCreateAndActivateCampaign(apiKey, body.campaignId, account, steps, lead);

          if (result) {
            break;
          }

          lastReason = `Attempt ${attempt}/${MAX_SEND_RETRIES} failed with not_sending_status`;
          console.warn(`[send] ${lastReason}`);
        }

        if (!result) {
          // All retries exhausted — no step runs were created yet
          const errorMsg = `Campaign failed after ${MAX_SEND_RETRIES} retry attempts`;
          console.error(`[send] ${errorMsg} for ${body.campaignId}/${body.to}`);
          return res.status(500).json({
            error: errorMsg,
            details: lastReason,
          });
        }

        added = result.added;

        // Success — save campaign to DB
        await db
          .insert(instantlyCampaigns)
          .values({
            campaignId: body.campaignId,
            leadEmail: body.to,
            leadId: body.leadId,
            instantlyCampaignId: result.instantlyCampaignId,
            name: `Campaign ${body.campaignId}`,
            status: "active",
            deliveryStatus: "pending",
            orgId: body.orgId,
            brandId: body.brandId,
            appId: body.appId,
            runId: body.runId,
          })
          .returning();

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
            orgId: body.orgId,
            runId: null,
          })
          .onConflictDoNothing()
          .returning();

        if (createdLead) savedLead = createdLead;
      }

      // 4. Create per-step runs: 1 actual+completed (step 1) + N-1 provisioned+ongoing
      if (body.orgId) {
        for (const s of sortedSequence) {
          const isFirstStep = s.step === sortedSequence[0].step;
          const stepRun = await createRun({
            orgId: body.orgId,
            appId: body.appId,
            serviceName: "instantly-service",
            taskName: `email-send-step-${s.step}`,
            brandId: body.brandId,
            campaignId: body.campaignId,
            parentRunId: body.runId,
          });

          const costResult = await addCosts(stepRun.id, [{
            costName: "instantly-email-send",
            quantity: 1,
            status: isFirstStep ? "actual" as const : "provisioned" as const,
          }]);

          const costId = costResult.costs[0]?.id;
          if (costId) {
            await db.insert(sequenceCosts).values({
              campaignId: body.campaignId,
              leadEmail: body.to,
              step: s.step,
              runId: stepRun.id,
              costId,
              status: isFirstStep ? "actual" : "provisioned",
            });
          }

          if (isFirstStep) {
            await updateRun(stepRun.id, "completed");
          }

          stepRuns.push({ step: s.step, runId: stepRun.id });
        }
      }

      console.log(`[send] Done — to=${body.to} campaignId=${body.campaignId} added=${added} stepRuns=${stepRuns.length}`);
      res.status(200).json({
        success: true,
        campaignId: body.campaignId,
        leadId: savedLead?.id,
        added,
        stepRuns: stepRuns.length > 0 ? stepRuns : undefined,
      });
    } catch (error: any) {
      // Fail any step runs that were already created
      for (const sr of stepRuns) {
        try {
          await updateRun(sr.runId, "failed", error.message);
        } catch {
          // Run may already be completed (step 1) — ignore
        }
      }
      throw error;
    }
  } catch (error: any) {
    if (error instanceof KeyServiceError && error.statusCode === 404) {
      return res.status(422).json({
        error: "BYOK key not configured for this organization",
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
