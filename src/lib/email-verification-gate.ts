/**
 * The send door's verification gate: NO email leaves our inboxes for an address
 * that has no verdict from our verifier (BounceVerify, through apollo-service).
 *
 * Why (owner 2026-10-14, « il faut bloquer ça dans notre infra »): our sending
 * domains and mailboxes are shared by every client, so one client's bad list
 * burns everybody's reputation. Measured in prod (first-step sends, latest
 * verdict before the send): valid 1.1% bounced, catch_all 1.9%, unknown 2.9%,
 * invalid 6.3%, a verification that returned NO verdict 24.6%, never verified
 * at all 9.2% (33,430 sends: CRM contacts, waterfall sources, pushed emails).
 *
 * The rule, one for every source and every channel, no branch per caller:
 *  - an address is verified before mail leaves for it. apollo-service reuses a
 *    decisive verdict under 30 days old for free; otherwise it verifies now and
 *    bills the org on a child run of the run that sends (the campaign's run).
 *  - `valid`, `catch_all`, `unknown` are SENT (owner 2026-10-14: the email price
 *    follows the verdict, valid ×1, catch_all ×2, unknown ×3).
 *  - `invalid` and `risky` (a spam trap) are NEVER sent.
 *  - no verdict (verifier down, empty answer, the org cannot pay for it) is never
 *    sent either: there is nothing to read as "fine".
 *
 * Which verdicts a campaign WANTS (valid_only / valid_and_catch_all) is decided
 * upstream at serve time (human-service, lead-service pools); this door does not
 * re-decide it. It guarantees the floor: a real verdict, never a bad one.
 *
 * Every decision is written to `email_send_verifications` with its reason, so a
 * refusal is never silent: the send route answers 409 `email_not_verified`, the
 * status read states it as `sendRefusal`, and the queued-sequence sweep stops
 * (and refunds) the sequence.
 */

import { messageOf } from "./db-error.js";
import { sql } from "drizzle-orm";

import { db } from "../db";
import { emailSendVerifications } from "../db/schema";
import {
  EmailVerifierError,
  verifyEmailAddresses,
  type EmailVerdict,
  type EmailVerification,
  type VerificationIdentity,
} from "./apollo-client";
import { normalizeLeadEmail } from "./lead-email";
import type { FundingRefusalCause } from "./funding-refusal";

/** The 409 `code` the send route answers, and the status read states. */
export const EMAIL_NOT_VERIFIED_REFUSAL_CODE = "email_not_verified";

/** Verdicts mail may leave for. Everything else, and no verdict, is refused. */
export const SENDABLE_VERDICTS: ReadonlySet<string> = new Set<EmailVerdict>([
  "valid",
  "catch_all",
  "unknown",
]);

/**
 * How long a refusal on a verdict stands: apollo-service re-verifies an address
 * once its verdict is older than this, so the person may be asked about again.
 */
export const VERDICT_REFUSAL_DAYS = 30;

/** Label stored by apollo-service on its bronze row (`verify:<source>`). */
export const VERIFY_SOURCE = "instantly-send-gate";

/** `check` = `POST /orgs/send-checks`, asked before a message is written (SEND-CHECK-BEFORE-WRITE-1120). */
export type GateContext = "send" | "queued" | "check";

export type GateReason =
  | "verdict_valid"
  | "verdict_catch_all"
  | "verdict_unknown"
  | "verdict_invalid"
  | "verdict_risky"
  | "verdict_unrecognised"
  | "verifier_failed"
  | "insufficient_credits";

export interface GateDecision {
  email: string;
  verdict: string | null;
  accepted: boolean;
  reason: GateReason;
  verificationId: string | null;
  verifiedAt: string | null;
  reused: boolean | null;
  /**
   * `verifier_failed` only: the verifier ANSWERED that it could give no verdict
   * for this address (BounceBan still verifying past its wait, every step-1
   * verifier unable to answer), as opposed to an outage on our side (apollo,
   * runs or billing unreachable). Not persisted: the ledger keeps
   * `verifier_failed`, a fact about no person, asked again on the next send.
   */
  vendorNoVerdict?: boolean;
  /**
   * `insufficient_credits` only: billing's NAMED cause when the refusal was a
   * sales funnel's max budget, not the balance (relayed verbatim; not persisted,
   * the ledger keeps `insufficient_credits`). Absent otherwise.
   */
  fundingCause?: FundingRefusalCause | null;
}

/** Pure: what the door does with one verdict. */
export function judgeVerdict(verdict: string): { accepted: boolean; reason: GateReason } {
  switch (verdict) {
    case "valid":
    case "catch_all":
    case "unknown":
    case "invalid":
    case "risky":
      return { accepted: SENDABLE_VERDICTS.has(verdict), reason: `verdict_${verdict}` as GateReason };
    default:
      // A verdict this code does not know is not a verdict it may send on.
      return { accepted: false, reason: "verdict_unrecognised" };
  }
}

/** Pure: when a refusal on this decision lapses (null = no lapse stated). */
export function refusalUntil(decision: Pick<GateDecision, "verifiedAt">): Date | null {
  if (!decision.verifiedAt) return null;
  const at = new Date(decision.verifiedAt);
  if (Number.isNaN(at.getTime())) return null;
  return new Date(at.getTime() + VERDICT_REFUSAL_DAYS * 24 * 60 * 60 * 1000);
}

function decisionFromVerification(v: EmailVerification): GateDecision {
  const { accepted, reason } = judgeVerdict(v.verdict);
  return {
    email: normalizeLeadEmail(v.email),
    verdict: v.verdict,
    accepted,
    reason,
    verificationId: v.verificationId ?? null,
    verifiedAt: v.verifiedAt ?? null,
    reused: typeof v.reused === "boolean" ? v.reused : null,
  };
}

/**
 * apollo-service's words for "the verifier answered, and it has no verdict for
 * this address" (lib/email-verification.ts there): BounceBan still verifying
 * past its wait (`the next ask resumes it`, a free poll), every step-1 verifier
 * unable to answer, BounceBan ending without a result. Anything else (apollo,
 * runs, billing, treg unreachable or 5xx) is an outage, retried below.
 */
const VENDOR_NO_VERDICT =
  /the next ask resumes it|every step-1 verifier failed|bounceban task ended with status|returned no known (verdict|result)/i;

/** Pure: the verifier answered that it could give no verdict (never an outage, never a credit refusal). */
export function isVendorNoVerdict(error: EmailVerifierError): boolean {
  return !error.insufficientCredits && VENDOR_NO_VERDICT.test(error.message);
}

/**
 * Pure: a failure worth asking again. A credit refusal is a decision and a 4xx
 * is our own request: never retried. An unreachable apollo (no status) or any
 * 5xx is (an outage, or a vendor still working: a re-ask resumes it).
 */
export function isRetryableVerifierError(error: EmailVerifierError): boolean {
  if (error.insufficientCredits) return false;
  return error.status === null || error.status >= 500;
}

/**
 * SEND-CHECK-502-1125: a check or a send waiting on a verdict asks again, with
 * backoff, when the verifier fails transiently (22 follow-up runs died on a 502
 * in 3 days: BounceBan still verifying, billing "Failed to compute balance",
 * treg's pool saturated). A re-ask never pays twice: apollo-service hands back
 * any decisive verdict it already holds for free and RESUMES a BounceBan task
 * still running (a free poll), and the next ask starts only once the previous
 * one has answered. No new attempt starts past VERIFY_RETRY_START_BUDGET_MS, so
 * the whole check stays within the gateway's wait (email-gateway
 * VERIFYING_TIMEOUT_MS). The dispatch sweep (`queued`) never waits: its next
 * tick asks again.
 */
export const VERIFY_RETRY_WAITS_MS: readonly number[] = [5_000, 15_000];
export const VERIFY_RETRY_START_BUDGET_MS = 120_000;
const RETRYING_CONTEXTS: ReadonlySet<GateContext> = new Set<GateContext>(["send", "check"]);

let sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
/** Tests only. */
export function setVerifyRetrySleepForTests(fn: ((ms: number) => Promise<void>) | null): void {
  sleep = fn ?? ((ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)));
}

async function verifyWithRetry(
  chunk: string[],
  identity: VerificationIdentity,
  context: GateContext,
): Promise<EmailVerification[]> {
  const started = Date.now();
  for (let attempt = 0; ; attempt++) {
    try {
      return await verifyEmailAddresses(chunk, identity, VERIFY_SOURCE);
    } catch (error: unknown) {
      if (!(error instanceof EmailVerifierError)) throw error;
      const wait = VERIFY_RETRY_WAITS_MS[attempt];
      if (
        !RETRYING_CONTEXTS.has(context) ||
        !isRetryableVerifierError(error) ||
        wait === undefined ||
        Date.now() - started + wait > VERIFY_RETRY_START_BUDGET_MS
      ) {
        throw error;
      }
      console.warn(
        `[instantly-service] email-verification-gate: no verdict yet for ${chunk.length} address(es) of org ${identity.orgId} (run ${identity.runId}), asking again in ${wait / 1000}s (attempt ${attempt + 2}) — ${messageOf(error)}`,
      );
      await sleep(wait);
    }
  }
}

function failureDecision(email: string, error: EmailVerifierError): GateDecision {
  return {
    email: normalizeLeadEmail(email),
    verdict: null,
    accepted: false,
    reason: error.insufficientCredits ? "insufficient_credits" : "verifier_failed",
    verificationId: null,
    verifiedAt: null,
    reused: null,
    ...(!error.insufficientCredits && isVendorNoVerdict(error) ? { vendorNoVerdict: true } : {}),
    ...(error.fundingCause ? { fundingCause: error.fundingCause } : {}),
  };
}

async function recordDecisions(
  orgId: string,
  decisions: GateDecision[],
  context: GateContext,
  runId: string,
): Promise<void> {
  if (decisions.length === 0) return;
  await db.insert(emailSendVerifications).values(
    decisions.map((d) => ({
      orgId,
      email: d.email,
      verdict: d.verdict,
      verificationId: d.verificationId,
      verifiedAt: d.verifiedAt ? new Date(d.verifiedAt) : null,
      reused: d.reused,
      accepted: d.accepted,
      reason: d.reason,
      context,
      runId,
    })),
  );
}

/**
 * Verify addresses for one org and record the door's decision for each.
 *
 * One apollo call per 50 addresses, billed to `identity` (the run of the
 * campaign that sends). A verifier failure is NOT thrown: it comes back as a
 * refused decision (`verifier_failed` / `insufficient_credits`), recorded like
 * any other, so the caller decides how loud to be while nothing gets sent. A
 * ledger write failure DOES throw: a decision we cannot record is not taken.
 */
export async function gateAddresses(
  emails: string[],
  identity: VerificationIdentity,
  context: GateContext,
): Promise<Map<string, GateDecision>> {
  const distinct = [...new Set(emails.map(normalizeLeadEmail))].filter((e) => e.length > 0);
  const out = new Map<string, GateDecision>();

  for (let i = 0; i < distinct.length; i += 50) {
    const chunk = distinct.slice(i, i + 50);
    let decisions: GateDecision[];
    try {
      const results = await verifyWithRetry(chunk, identity, context);
      const byEmail = new Map(results.map((r) => [normalizeLeadEmail(r.email), r]));
      decisions = chunk.map((email) => {
        const found = byEmail.get(email);
        if (!found) {
          // apollo answers one result per distinct address; a missing one is no verdict.
          return failureDecision(email, new EmailVerifierError("no result for this address", 200, false));
        }
        return decisionFromVerification(found);
      });
    } catch (error: unknown) {
      if (!(error instanceof EmailVerifierError)) throw error;
      console.error(
        `[instantly-service] email-verification-gate: NO VERDICT for ${chunk.length} address(es) of org ${identity.orgId} (run ${identity.runId}) — nothing will be sent to them: ${messageOf(error)}`,
      );
      decisions = chunk.map((email) => failureDecision(email, error));
    }
    await recordDecisions(identity.orgId, decisions, context, identity.runId);
    for (const d of decisions) out.set(d.email, d);
  }
  return out;
}

/** Gate one address for `POST /orgs/send`. */
export async function gateAddressForSend(
  email: string,
  identity: VerificationIdentity,
): Promise<GateDecision> {
  const decisions = await gateAddresses([email], identity, "send");
  const decision = decisions.get(normalizeLeadEmail(email));
  if (!decision) throw new Error(`email-verification-gate: no decision for ${email}`);
  return decision;
}

/**
 * For a path that RE-QUEUES an existing sequence (restore, redispatch): the
 * address's verdict, verified now if needed (context `queued`), so the new
 * holds carry the price of that verdict. Throws when mail may not leave for
 * it: the caller must not queue anything.
 */
export async function requireSendableVerdict(
  email: string,
  identity: VerificationIdentity,
): Promise<"valid" | "catch_all" | "unknown"> {
  const decisions = await gateAddresses([email], identity, "queued");
  const decision = decisions.get(normalizeLeadEmail(email));
  if (!decision || !decision.accepted || !decision.verdict) {
    throw new Error(
      `email-verification-gate: ${email} may not be emailed (${decision?.reason ?? "no decision"}, verdict ${decision?.verdict ?? "none"})`,
    );
  }
  return decision.verdict as "valid" | "catch_all" | "unknown";
}

/** The 409 body for an address whose verdict the door refuses. */
export function emailNotVerifiedRefusal(email: string, decision: GateDecision) {
  const until = refusalUntil(decision);
  return {
    error: "Email not verified",
    code: EMAIL_NOT_VERIFIED_REFUSAL_CODE,
    email,
    verdict: decision.verdict,
    reason: decision.reason,
    verifiedAt: decision.verifiedAt,
    until: until ? until.toISOString() : null,
    details:
      decision.verdict === null
        ? `${email} was not sent: our verifier could give no verdict for it (${decision.reason}); only valid, catch_all and unknown addresses are ever emailed, and the next send asks again`
        : `${email} was not sent: our verifier's verdict is ${decision.verdict} (${decision.reason}); only valid, catch_all and unknown addresses are ever emailed`,
  };
}

function rowsOf(result: unknown): Record<string, unknown>[] {
  if (Array.isArray(result)) return result as Record<string, unknown>[];
  const rows = (result as { rows?: unknown }).rows;
  return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

/**
 * Batch read for the status route: the addresses of this org whose LATEST gate
 * decision is a refusal on a verdict (invalid, risky, unrecognised) that has not
 * lapsed. Keyed by normalized address, value = when it lapses. A verifier outage
 * is not stated here: it is no fact about the person, the next send asks again.
 * Fail loud.
 */
export async function findEmailNotVerifiedRefusals(
  orgId: string,
  emails: string[],
): Promise<Map<string, Date>> {
  const keys = [...new Set(emails.map(normalizeLeadEmail))].filter((e) => e.length > 0);
  const out = new Map<string, Date>();
  if (keys.length === 0) return out;
  const result = await db.execute(sql`
    SELECT DISTINCT ON (v.email)
           v.email AS "email",
           v.accepted AS "accepted",
           v.reason AS "reason",
           v.verified_at + make_interval(days => ${VERDICT_REFUSAL_DAYS}) AS "until"
    FROM email_send_verifications v
    WHERE v.org_id = ${orgId}
      AND v.email = ANY(${sql.param(keys)}::text[])
      AND v.reason <> 'verifier_failed'
      AND v.reason <> 'insufficient_credits'
    ORDER BY v.email, v.created_at DESC
  `);
  const now = Date.now();
  for (const row of rowsOf(result)) {
    if (row.accepted === true) continue;
    if (!row.until) continue;
    const until = new Date(row.until as string);
    if (Number.isNaN(until.getTime()) || until.getTime() <= now) continue;
    out.set(String(row.email), until);
  }
  return out;
}
