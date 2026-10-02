/**
 * What one email sent to a lead costs the org, and how it is declared.
 *
 * Owner decision 2026-10-02 (reverses 2026-08-24, PR #622): every outreach email
 * is billed to the org again, on BOTH transports, at the catalogue price of two
 * cost names that costs-service sets to the measured real cost of sending one
 * email (infrastructure paid / emails sent, recomputed daily) ×2, split 50/50.
 *
 * Discipline per step: PROVISION both costs on the step's run when the sequence
 * is queued (`/orgs/send`, retry-stuck redispatch) → AUTHORIZE the whole
 * sequence once (platform key only) → the email leaves later (Instantly or our
 * dispatch worker) → ACTUALIZE on the real `email_sent`, CANCEL when the step
 * can no longer send (`settleHoldCost`).
 *
 * NOT declared here, on purpose: `instantly-contact-uploaded` (included at the
 * vendor; costs-service does not price it), warmup, warmup replies, placement /
 * seed tests and manual replies (our own mail, never a lead send).
 */
import { addCosts, type IdentityContext, type RunCost } from "./runs-client";

export const ACCOUNT_EMAIL_SENT_COST = "instantly-account-email-sent";
export const DOMAIN_EMAIL_SENT_COST = "instantly-domain-email-sent";

/** The two cost ids one queued step carries. */
export interface StepCostIds {
  costId: string;
  domainCostId: string;
}

/**
 * Provision the step's two email costs on its own run. Fail loud: a runs-service
 * error (422 unknown/unpriced name included) or an answer missing either cost
 * throws, so the step is never queued without its charge.
 */
export async function provisionStepEmailCosts(
  stepRunId: string,
  costSource: "platform" | "org",
  identity: IdentityContext,
): Promise<StepCostIds> {
  const { costs } = await addCosts(
    stepRunId,
    [
      { costName: ACCOUNT_EMAIL_SENT_COST, quantity: 1, costSource, status: "provisioned" },
      { costName: DOMAIN_EMAIL_SENT_COST, quantity: 1, costSource, status: "provisioned" },
    ],
    identity,
  );
  const byName = (name: string): RunCost => {
    const found = (costs ?? []).find((c) => c.costName === name);
    if (!found?.id) {
      throw new Error(
        `runs-service did not return a ${name} cost for run ${stepRunId}: ${JSON.stringify(costs)}`,
      );
    }
    return found;
  };
  return {
    costId: byName(ACCOUNT_EMAIL_SENT_COST).id,
    domainCostId: byName(DOMAIN_EMAIL_SENT_COST).id,
  };
}

/** Authorize basket for a sequence of `stepCount` emails. */
export function sendAuthorizeItems(stepCount: number) {
  return [
    { costName: ACCOUNT_EMAIL_SENT_COST, quantity: stepCount },
    { costName: DOMAIN_EMAIL_SENT_COST, quantity: stepCount },
  ];
}
