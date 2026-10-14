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
 * The PRICE FOLLOWS THE RECIPIENT'S VERDICT from our verifier (owner 2026-10-14,
 * « le prix ... doit correspondre aux valid… si catch all on x2 ce prix…
 * unknown x3 »): `valid` = the two base names, `catch_all` = their `-catch-all`
 * twins (×2), `unknown` = their `-unknown` twins (×3), each pair recomputed by
 * costs-service from the same measured cost. invalid / risky / no verdict have
 * no name: they are never sent (`email-verification-gate.ts`). A sequence keeps
 * the names it was queued with (never re-priced).
 *
 * NOT declared here, on purpose: `instantly-contact-uploaded` (included at the
 * vendor; costs-service does not price it), warmup, warmup replies, placement /
 * seed tests and manual replies (our own mail, never a lead send).
 */
import { addCosts, type IdentityContext, type RunCost } from "./runs-client";

export const ACCOUNT_EMAIL_SENT_COST = "instantly-account-email-sent";
export const DOMAIN_EMAIL_SENT_COST = "instantly-domain-email-sent";

/** A verdict mail may leave for, and therefore a price tier. */
export type SendableVerdict = "valid" | "catch_all" | "unknown";

/** The two cost names one email to an address with this verdict carries. */
export interface EmailSentCostNames {
  account: string;
  domain: string;
}

const COST_NAMES_BY_VERDICT: Record<SendableVerdict, EmailSentCostNames> = {
  valid: { account: ACCOUNT_EMAIL_SENT_COST, domain: DOMAIN_EMAIL_SENT_COST },
  catch_all: {
    account: `${ACCOUNT_EMAIL_SENT_COST}-catch-all`,
    domain: `${DOMAIN_EMAIL_SENT_COST}-catch-all`,
  },
  unknown: {
    account: `${ACCOUNT_EMAIL_SENT_COST}-unknown`,
    domain: `${DOMAIN_EMAIL_SENT_COST}-unknown`,
  },
};

/** Pure. Throws on a verdict that is never sent: there is no price to declare for it. */
export function emailSentCostNames(verdict: string): EmailSentCostNames {
  const names = COST_NAMES_BY_VERDICT[verdict as SendableVerdict];
  if (!names) {
    throw new Error(`no email price for verdict "${verdict}": an address with it is never sent`);
  }
  return names;
}

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
  verdict: SendableVerdict,
): Promise<StepCostIds> {
  const names = emailSentCostNames(verdict);
  const { costs } = await addCosts(
    stepRunId,
    [
      { costName: names.account, quantity: 1, costSource, status: "provisioned" },
      { costName: names.domain, quantity: 1, costSource, status: "provisioned" },
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
    costId: byName(names.account).id,
    domainCostId: byName(names.domain).id,
  };
}

/** Authorize basket for a sequence of `stepCount` emails to an address with this verdict. */
export function sendAuthorizeItems(stepCount: number, verdict: SendableVerdict) {
  const names = emailSentCostNames(verdict);
  return [
    { costName: names.account, quantity: stepCount },
    { costName: names.domain, quantity: stepCount },
  ];
}
