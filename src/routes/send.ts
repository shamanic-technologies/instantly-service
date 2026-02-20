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
 * Create one Instantly campaign per lead with multi-step sequence.
 *
 * Each (campaignId, leadEmail) pair gets its own Instantly campaign so that
 * every lead receives its own personalised sequence.  The caller's
 * `campaignId` is stored as a grouping key — it is NOT a 1:1 mapping to a
 * single Instantly campaign.
 */
async function getOrCreateCampaignForLead(
  campaignId: string,
  leadEmail: string,
  organizationId: string | null,
  steps: SequenceStep[],
  account: Account,
  runId: string,
  clerkOrgId: string | undefined,
  brandId: string,
  appId: string,
): Promise<{ id: string; instantlyCampaignId: string; campaignId: string; isNew: boolean }> {
  // Dedup: if we already created an Instantly campaign for this exact lead
  // in this logical campaign, reuse it.
  const [existing] = await db
    .select()
    .from(instantlyCampaigns)
    .where(
      and(
        eq(instantlyCampaigns.campaignId, campaignId),
        eq(instantlyCampaigns.leadEmail, leadEmail),
      ),
    );

  if (existing) {
    console.log(`[send] Reusing existing campaign for ${campaignId}/${leadEmail} → instantly=${existing.instantlyCampaignId}`);
    return {
      id: existing.id,
      instantlyCampaignId: existing.instantlyCampaignId,
      campaignId,
      isNew: false,
    };
  }

  console.log(`[send] Picked account ${account.email} for ${campaignId}/${leadEmail}`);
  console.log(`[send] Creating new Instantly campaign for ${campaignId}/${leadEmail} subject="${steps[0]?.subject}" steps=${steps.length}`);
  const instantlyCampaign = await createInstantlyCampaign({
    name: `Campaign ${campaignId}`,
    steps,
  });
  console.log(`[send] Created instantly campaign id=${instantlyCampaign.id} status=${instantlyCampaign.status}`);

  // Assign the single selected account via PATCH (V2 ignores account_ids in create body)
  // Also enable stop_on_reply so Instantly stops the sequence when a lead replies
  {
    console.log(`[send] Assigning account ${account.email} to campaign ${instantlyCampaign.id}`);
    await updateInstantlyCampaign(instantlyCampaign.id, {
      email_list: [account.email],
      bcc_list: ["kevin@mcpfactory.org"],
      open_tracking: true,
      link_tracking: true,
      insert_unsubscribe_header: true,
      stop_on_reply: true,
    });

    // Verify accounts were actually assigned
    const verified = await getInstantlyCampaign(instantlyCampaign.id) as unknown as Record<string, unknown>;
    console.log(`[send] Verify after PATCH — email_list=${JSON.stringify(verified.email_list)} bcc_list=${JSON.stringify(verified.bcc_list)} not_sending_status=${JSON.stringify(verified.not_sending_status)}`);
  }

  const [created] = await db
    .insert(instantlyCampaigns)
    .values({
      campaignId,
      leadEmail,
      instantlyCampaignId: instantlyCampaign.id,
      name: `Campaign ${campaignId}`,
      status: instantlyCampaign.status,
      orgId: organizationId,
      clerkOrgId,
      brandId,
      appId,
      runId,
    })
    .returning();

  return {
    id: created.id,
    instantlyCampaignId: created.instantlyCampaignId,
    campaignId,
    isNew: true,
  };
}

/**
 * POST /send
 * Add a lead to a multi-step sequence campaign via Instantly.
 *
 * Creates 1 actual cost (step 1 sent immediately) + N-1 provisioned costs
 * (follow-up steps). Provisioned costs are converted to actual on webhook
 * email_sent, or cancelled on reply/bounce/unsub/not_interested.
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
      // 3. Pick account and build sequence steps with signature injected
      const accounts = await listAccounts();
      if (accounts.length === 0) {
        throw new Error("No email accounts available — cannot create campaign");
      }
      const account = pickRandomAccount(accounts);
      const sortedSequence = [...body.sequence].sort((a, b) => a.step - b.step);
      const steps = buildSequenceSteps(body.subject, sortedSequence, account);

      const campaign = await getOrCreateCampaignForLead(
        body.campaignId,
        body.to,
        organizationId,
        steps,
        account,
        body.runId,
        body.orgId,
        body.brandId,
        body.appId,
      );

      let savedLead: { id: string } | undefined;
      let added = 0;

      if (campaign.isNew) {
        // 5. Add lead to the new Instantly campaign
        const lead: Lead = {
          email: body.to,
          first_name: body.firstName,
          last_name: body.lastName,
          company_name: body.company,
          variables: body.variables,
        };

        console.log(`[send] Adding lead ${body.to} to instantly campaign ${campaign.instantlyCampaignId}`);
        const result = await addInstantlyLeads({
          campaign_id: campaign.instantlyCampaignId,
          leads: [lead],
        });
        added = result.added;
        console.log(`[send] addLeads result: added=${added}`);

        // 6. Save lead to database
        const [created] = await db
          .insert(instantlyLeads)
          .values({
            instantlyCampaignId: campaign.instantlyCampaignId,
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

        if (created) savedLead = created;

        // 7. Activate the new campaign
        console.log(`[send] Activating new campaign ${campaign.instantlyCampaignId}`);
        await updateCampaignStatus(campaign.instantlyCampaignId, "active");

        // Verify campaign state after activation
        const postActivate = await getInstantlyCampaign(campaign.instantlyCampaignId) as unknown as Record<string, unknown>;
        console.log(`[send] Post-activate — status=${postActivate.status} email_list=${JSON.stringify(postActivate.email_list)} not_sending_status=${JSON.stringify(postActivate.not_sending_status)}`);

        await db
          .update(instantlyCampaigns)
          .set({ status: "active", updatedAt: new Date() })
          .where(eq(instantlyCampaigns.id, campaign.id));
      } else {
        console.log(`[send] Lead ${body.to} already processed for campaign ${body.campaignId}, skipping`);
      }

      // 8. Log costs: 1 actual (step 1) + N-1 provisioned (follow-ups)
      if (sendRun) {
        const costItems = sortedSequence.map((s, i) => ({
          costName: "instantly-email-send",
          quantity: 1,
          status: i === 0 ? ("actual" as const) : ("provisioned" as const),
        }));

        const costResult = await addCosts(sendRun.id, costItems);

        // Store provisioned cost IDs for follow-up steps so webhooks can
        // convert them to actual or cancel them later
        if (costResult.costs.length > 1) {
          const provisionedCosts = costResult.costs
            .filter((c) => c.id)
            .slice(1); // skip first (actual)

          for (let i = 0; i < provisionedCosts.length; i++) {
            const stepNumber = i + 2; // steps 2, 3, ...
            await db.insert(sequenceCosts).values({
              campaignId: body.campaignId,
              leadEmail: body.to,
              step: stepNumber,
              runId: sendRun.id,
              costId: provisionedCosts[i].id,
              status: "provisioned",
            });
          }
        }

        await updateRun(sendRun.id, "completed");
      }

      console.log(`[send] Done — to=${body.to} campaignId=${body.campaignId} isNew=${campaign.isNew} added=${added}`);
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
