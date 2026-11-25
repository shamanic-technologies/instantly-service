/**
 * The send door's checks on the PERSON, asked before anything is written
 * (SEND-CHECK-BEFORE-WRITE-1120).
 *
 * Why: a sender script used to ask its writer (a paid LLM call) and only then
 * learn from `POST /orgs/send` that the address may not be mailed (125 written
 * then refused in 7 days, 30% of follow-up sends). `POST /orgs/send-checks`
 * answers the same refusals the send would, before the writer is asked.
 *
 * ONE rule set, two callers: the send route (`routes/send.ts`) and the check
 * route run these very functions, in the send's order, with the send's bodies.
 * - opt-out (`lead_opted_out`, org-wide, never lapses)
 * - not a prospect (`lead_not_a_prospect`, per brand)
 * - stated not to the brand (`stated_not_to_brand`, crm-service; 502 when unreadable)
 * - address verdict (`email_not_verified`; 402 when the org cannot pay to
 *   verify; 409 `email_not_verified` when the verifier answered it has no
 *   verdict; 502 only when the verifier stayed unreachable through the gate's
 *   retries, SEND-CHECK-502-1125). A verdict under 30 days old is
 *   reused for free by apollo-service; otherwise verified now and billed on the
 *   caller's run. The decision is recorded (`email_send_verifications`), so the
 *   send right after reuses the verdict at no cost.
 *
 * Not here: the follow-up race (`brand_sequence_queued`) and the signature
 * conflict, which are about OUR queued work, not about the person.
 */
import { messageOf } from "./db-error.js";
import { findStandingOptOut, optOutRefusal } from "./lead-optouts";
import { findNotAProspect, notAProspectRefusal } from "./not-a-prospect";
import { findStatedNotToBrand, statedNotToBrandRefusal } from "./stated-not-to-brand";
import {
  emailNotVerifiedRefusal,
  gateAddresses,
  type GateContext,
  type GateDecision,
} from "./email-verification-gate";
import type { VerificationIdentity } from "./apollo-client";
import { creditRefusalBody, fundingRefusalError } from "./funding-refusal";
import { normalizeLeadEmail } from "./lead-email";

/** A refusal the send route answers: its status and body, verbatim. */
export interface SendCheckRefusal {
  status: number;
  body: Record<string, unknown>;
  /** For the route's log / trace line. */
  event: string;
  detail: string;
}

/**
 * The answer for an address whose verification the door refuses, or null when
 * mail may leave for it. Shared by `POST /orgs/send` and `POST /orgs/send-checks`.
 */
export function verificationRefusal(to: string, orgId: string, verification: GateDecision): SendCheckRefusal | null {
  if (verification.accepted) return null;
  const detail = `to=${to}, verdict=${verification.verdict ?? "none"}, reason=${verification.reason}`;
  if (verification.reason === "insufficient_credits") {
    const cause = verification.fundingCause ?? null;
    console.warn(
      cause
        ? `[send] Refused — ${fundingRefusalError(cause)} (${cause.reason}) for org ${orgId}, cannot verify ${to}`
        : `[send] Refused — org ${orgId} cannot pay to verify ${to}`,
    );
    return {
      status: 402,
      event: "send-refused-email-not-verified",
      detail,
      body: creditRefusalBody(
        {
          error: "Insufficient credits",
          code: "insufficient_credits",
          details: `the address ${to} must be verified before it is emailed and the org's balance cannot pay for it`,
        },
        cause,
        `the address ${to} must be verified before it is emailed`,
      ) as unknown as Record<string, unknown>,
    };
  }
  // The verifier ANSWERED it has no verdict for this address (after the gate's
  // retries): a refusal about the address, like a bad verdict, never an outage
  // 502 that kills the caller's run (SEND-CHECK-502-1125). No verdict is never
  // sent (the gate's rule); the next send asks again.
  if (verification.reason === "verifier_failed" && verification.vendorNoVerdict === true) {
    const refusal = emailNotVerifiedRefusal(to, verification);
    console.warn(`[send] Refused — ${refusal.details}`);
    return { status: 409, event: "send-refused-email-not-verified", detail, body: refusal };
  }
  if (verification.reason === "verifier_failed") {
    console.error(`[send] NOT SENT — no verdict could be obtained for ${to} (verifier unavailable)`);
    return {
      status: 502,
      event: "send-refused-email-not-verified",
      detail,
      body: {
        error: "Email verification unavailable",
        code: "email_verification_unavailable",
        details: `no verdict could be obtained for ${to}; nothing was sent or billed for the email`,
      },
    };
  }
  const refusal = emailNotVerifiedRefusal(to, verification);
  console.warn(`[send] Refused — ${refusal.details}`);
  return { status: 409, event: "send-refused-email-not-verified", detail, body: refusal };
}

/** The answer when crm-service cannot say whether the brand stated the person is not theirs. */
export function statedNotToBrandUnreadable(to: string, brandIds: string[], error: unknown): SendCheckRefusal {
  const message = messageOf(error);
  console.error(`[send] Refused — cannot read whether ${to} was stated not to brand(s) ${brandIds.join(",")}: ${message}`);
  return {
    status: 502,
    event: "send-refused-stated-not-to-brand-unreadable",
    detail: `to=${to}`,
    body: {
      error: "Could not read whether the brand stated this person is not theirs",
      code: "stated_not_to_brand_unreadable",
      details: `${message}. No email was sent and nothing was billed.`,
    },
  };
}

export interface PersonCheckInput {
  orgId: string;
  to: string;
  brandIds: string[];
  /** The caller's run: the stated-not-to-brand read and the verification bill on it. */
  runId: string | null;
  identity: VerificationIdentity;
  context: GateContext;
}

export type PersonCheckAnswer =
  | { sendable: true; verification: GateDecision }
  | { sendable: false; refusal: SendCheckRefusal };

/**
 * Every check on the person, in the send's order, first refusal wins. Fail
 * loud: a DB error propagates (an unreadable refusal is never "none").
 */
export async function checkPersonForSend(input: PersonCheckInput): Promise<PersonCheckAnswer> {
  const { orgId, to, brandIds } = input;

  const optOut = await findStandingOptOut(orgId, to);
  if (optOut) {
    const body = optOutRefusal(to, optOut);
    return { sendable: false, refusal: { status: 409, event: "send-refused-opted-out", detail: `to=${to}, channel=${optOut.channel}`, body } };
  }

  const notAProspect = await findNotAProspect(to, brandIds);
  if (notAProspect) {
    const body = notAProspectRefusal(to, notAProspect);
    return { sendable: false, refusal: { status: 409, event: "send-refused-not-a-prospect", detail: `to=${to}`, body } };
  }

  let stated;
  try {
    stated = await findStatedNotToBrand(orgId, to, brandIds, input.runId);
  } catch (error: unknown) {
    return { sendable: false, refusal: statedNotToBrandUnreadable(to, brandIds, error) };
  }
  if (stated) {
    const body = statedNotToBrandRefusal(to, stated);
    return {
      sendable: false,
      refusal: { status: 409, event: "send-refused-stated-not-to-brand", detail: `to=${to}, brandId=${stated.brandId ?? "-"}`, body },
    };
  }

  const decisions = await gateAddresses([to], input.identity, input.context);
  const verification = decisions.get(normalizeLeadEmail(to));
  if (!verification) throw new Error(`send-checks: no verification decision for ${to}`);
  const refused = verificationRefusal(to, orgId, verification);
  if (refused) return { sendable: false, refusal: refused };
  return { sendable: true, verification };
}
