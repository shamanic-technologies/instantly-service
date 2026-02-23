import { Router, Request, Response } from "express";
import { db } from "../db";
import {
  organizations,
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

async function getOrCreateOrganization(clerkOrgId: string): Promise<string> {
  const [existing] = await db
    .select()
    .from(organizations)
    .where(eq(organizations.clerkOrgId, clerkOrgId));

  if (existing) {
    return existing.id;
  }

  const [created] = await db
    .insert(organizations)
    .values({ clerkOrgId })
    .returning();

  return created.id;
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
  campaignId: string,
  account: Account,
  steps: SequenceStep[],
  lead: Lead,
): Promise<{ instantlyCampaignId: string; added: number } | null> {
  console.log(`[send] Creating new Instantly campaign for ${campaignId} with account ${account.email}`);
  const instantlyCampaign = await createInstantlyCampaign({
    name: `Campaign ${campaignId}`,
    steps,
  });
  console.log(`[send] Created instantly campaign id=${instantlyCampaign.id} status=${instantlyCampaign.status}`);

  // Assign the selected account via PATCH
  console.log(`[send] Assigning account ${account.email} to campaign ${instantlyCampaign.id}`);
  await updateInstantlyCampaign(instantlyCampaign.id, {
    email_list: [account.email],
    bcc_list: ["kevin@mcpfactory.org"],
    open_tracking: true,
    link_tracking: true,
    insert_unsubscribe_header: true,
    stop_on_reply: true,
  });

  // Verify accounts were assigned
  const verified = await getInstantlyCampaign(instantlyCampaign.id) as unknown as Record<string, unknown>;
  console.log(`[send] Verify after PATCH — email_list=${JSON.stringify(verified.email_list)} not_sending_status=${JSON.stringify(verified.not_sending_status)}`);

  // Add lead
  console.log(`[send] Adding lead ${lead.email} to instantly campaign ${instantlyCampaign.id}`);
  const result = await addInstantlyLeads({
    campaign_id: instantlyCampaign.id,
    leads: [lead],
  });
  console.log(`[send] addLeads result: added=${result.added}`);

  // Activate
  console.log(`[send] Activating campaign ${instantlyCampaign.id}`);
  await updateCampaignStatus(instantlyCampaign.id, "active");

  // Verify post-activation
  const postActivate = await getInstantlyCampaign(instantlyCampaign.id) as unknown as Record<string, unknown>;
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
 * Creates 1 actual cost (step 1 sent immediately) + N-1 provisioned costs
 * (follow-up steps). Provisioned costs are converted to actual on webhook
 * email_sent, or cancelled on reply/bounce/unsub/not_interested.
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
    // 1. Get or create organization (only if orgId provided)
    let organizationId: string | null = null;
    if (body.orgId) {
      organizationId = await getOrCreateOrganization(body.orgId);
    }

    // 2. Create run in runs-service (only if orgId provided)
    let sendRun: { id: string } | null = null;
    if (body.orgId) {
      sendRun = await createRun({
        clerkOrgId: body.orgId,
        appId: body.appId,
        serviceName: "instantly-service",
        taskName: "email-send",
        brandId: body.brandId,
        campaignId: body.campaignId,
        parentRunId: body.runId,
      });
    }

    try {
      // 3. Check available accounts
      const accounts = await listAccounts();
      if (accounts.length === 0) {
        throw new Error("No email accounts available — cannot create campaign");
      }

      const sortedSequence = [...body.sequence].sort((a, b) => a.step - b.step);

      // 4. Dedup: check if this (campaignId, leadEmail) pair already has a campaign
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
          result = await tryCreateAndActivateCampaign(body.campaignId, account, steps, lead);

          if (result) {
            break;
          }

          lastReason = `Attempt ${attempt}/${MAX_SEND_RETRIES} failed with not_sending_status`;
          console.warn(`[send] ${lastReason}`);
        }

        if (!result) {
          // All retries exhausted — fail without adding costs
          const errorMsg = `Campaign failed after ${MAX_SEND_RETRIES} retry attempts`;
          console.error(`[send] ${errorMsg} for ${body.campaignId}/${body.to}`);
          if (sendRun) {
            await updateRun(sendRun.id, "failed", errorMsg);
          }
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
            instantlyCampaignId: result.instantlyCampaignId,
            name: `Campaign ${body.campaignId}`,
            status: "active",
            orgId: organizationId,
            clerkOrgId: body.orgId,
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
            orgId: organizationId,
            runId: sendRun?.id,
          })
          .onConflictDoNothing()
          .returning();

        if (createdLead) savedLead = createdLead;
      }

      // 5. Log costs: 1 actual (step 1) + N-1 provisioned (follow-ups)
      if (sendRun) {
        const costItems = sortedSequence.map((s, i) => ({
          costName: "instantly-email-send",
          quantity: 1,
          status: i === 0 ? ("actual" as const) : ("provisioned" as const),
        }));

        const costResult = await addCosts(sendRun.id, costItems);

        // Store ALL cost IDs (including step 1) so handleCampaignError
        // can cancel them if the campaign fails later
        for (let i = 0; i < costResult.costs.length; i++) {
          const cost = costResult.costs[i];
          if (!cost.id) continue;
          await db.insert(sequenceCosts).values({
            campaignId: body.campaignId,
            leadEmail: body.to,
            step: i + 1,
            runId: sendRun.id,
            costId: cost.id,
            status: i === 0 ? "actual" : "provisioned",
          });
        }

        await updateRun(sendRun.id, "completed");
      }

      console.log(`[send] Done — to=${body.to} campaignId=${body.campaignId} added=${added}`);
      res.status(200).json({
        success: true,
        campaignId: body.campaignId,
        leadId: savedLead?.id,
        added,
      });
    } catch (error: any) {
      if (sendRun) {
        await updateRun(sendRun.id, "failed", error.message);
      }
      throw error;
    }
  } catch (error: any) {
    console.error(`[send] Failed to send — to=${body.to} error="${error.message}"`);
    res.status(500).json({
      error: "Failed to send email",
      details: error.message,
    });
  }
});

export default router;
