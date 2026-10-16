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
 *  - `valid`, `catch_all` are SENT (owner 2026-10-14: the email price follows the
 *    verdict, valid ×1, catch_all ×2, unknown ×3).
 *  - `unknown` is NOT sent on a first answer (CATCHALL-BOUNCE-1016): the verifier
 *    could not tell, so we ask again after `UNKNOWN_RECHECK_HOURS` (apollo never
 *    reuses an `unknown`, a re-ask is a fresh check, and BounceVerify does not
 *    bill it). It is sent (priced ×3, rationed by the domain's bounce budget at
 *    dispatch) only when it is CONFIRMED: an earlier `unknown` for the address at
 *    least `UNKNOWN_CONFIRM_MIN_HOURS` old (the retry answered the same), or the
 *    address accepted a real email of ours in the last `PROOF_FRESH_DAYS` and
 *    never bounced one.
 *
 *    Why, market consensus (cited in the PR):
 *      - ZeroBounce (status codes doc): an `unknown` is an address it could not
 *        validate (anti-spam gateway, server offline); "in most cases 80 % of
 *        unknowns are invalid/bad"; re-validate them, do not mail on them.
 *      - LeadMagic ("Catch-all vs risky vs unknown", "Unknown email status"):
 *        cold outbound cannot take a maybe; exclude unknowns from sends, retry
 *        after 24-48 h, and only then treat a persistent unknown like a
 *        catch-all (small, measured share).
 *      - Security gateways (Mimecast, Proofpoint, Barracuda) answer every probe
 *        the same way, so their mailboxes come back `unknown`; the gateway then
 *        judges the real email on the SENDER's reputation and content.
 *    Ours, to illustrate: 2026-10-14 four follow-ups to unknown Forbes /
 *    TechCrunch addresses from one mailbox were refused within six minutes by
 *    Mimecast ("554 Email rejected due to security policies"); fleet first
 *    emails 2026-09-16..10-16 by verdict at send: valid 87 / 3,975 bounced
 *    (2.2 %), catch_all 32 / 298 (10.7 %), unknown 7 / 19 (36.8 %).
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
import { PROOF_FRESH_DAYS } from "./bounce-budget";
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

/**
 * An `unknown` is re-asked after this many hours (LeadMagic: retry after 24-48 h).
 * It is the `until` of the refusal, so lead-service parks the person until then.
 */
export const UNKNOWN_RECHECK_HOURS = 48;

/** An earlier `unknown` at least this old confirms a new one (a real retry, not the same call). */
export const UNKNOWN_CONFIRM_MIN_HOURS = 24;

/** Only an earlier unknown this recent counts as the retry's first answer. */
export const UNKNOWN_CONFIRM_MAX_DAYS = 30;

/** Label stored by apollo-service on its bronze row (`verify:<source>`). */
export const VERIFY_SOURCE = "instantly-send-gate";

export type GateContext = "send" | "queued";

export type GateReason =
  | "verdict_valid"
  | "verdict_catch_all"
  | "verdict_unknown"
  | "verdict_unknown_recheck"
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
   * `insufficient_credits` only: billing's NAMED cause when the refusal was a
   * sales funnel's max budget, not the balance (relayed verbatim; not persisted,
   * the ledger keeps `insufficient_credits`). Absent otherwise.
   */
  fundingCause?: FundingRefusalCause | null;
}

/**
 * Pure: what the door does with one verdict. `unknownConfirmed` = the address has
 * an earlier `unknown` at least a day old, or accepted our email recently
 * (`findConfirmedUnknowns`); a first `unknown` is re-asked, never sent.
 */
export function judgeVerdict(
  verdict: string,
  unknownConfirmed = false,
): { accepted: boolean; reason: GateReason } {
  switch (verdict) {
    case "unknown":
      return unknownConfirmed
        ? { accepted: true, reason: "verdict_unknown" }
        : { accepted: false, reason: "verdict_unknown_recheck" };
    case "valid":
    case "catch_all":
    case "invalid":
    case "risky":
      return { accepted: SENDABLE_VERDICTS.has(verdict), reason: `verdict_${verdict}` as GateReason };
    default:
      // A verdict this code does not know is not a verdict it may send on.
      return { accepted: false, reason: "verdict_unrecognised" };
  }
}

/** Pure: when a refusal on this decision lapses (null = no lapse stated). */
export function refusalUntil(decision: Pick<GateDecision, "verifiedAt" | "reason">): Date | null {
  if (!decision.verifiedAt) return null;
  const at = new Date(decision.verifiedAt);
  if (Number.isNaN(at.getTime())) return null;
  const ms =
    decision.reason === "verdict_unknown_recheck"
      ? UNKNOWN_RECHECK_HOURS * 60 * 60 * 1000
      : VERDICT_REFUSAL_DAYS * 24 * 60 * 60 * 1000;
  return new Date(at.getTime() + ms);
}

function decisionFromVerification(v: EmailVerification, unknownConfirmed: boolean): GateDecision {
  const { accepted, reason } = judgeVerdict(v.verdict, unknownConfirmed);
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

function failureDecision(email: string, error: EmailVerifierError): GateDecision {
  return {
    email: normalizeLeadEmail(email),
    verdict: null,
    accepted: false,
    reason: error.insufficientCredits ? "insufficient_credits" : "verifier_failed",
    verificationId: null,
    verifiedAt: null,
    reused: null,
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
      const results = await verifyEmailAddresses(chunk, identity, VERIFY_SOURCE);
      const byEmail = new Map(results.map((r) => [normalizeLeadEmail(r.email), r]));
      const unknowns = chunk.filter((email) => byEmail.get(email)?.verdict === "unknown");
      const confirmed = await findConfirmedUnknowns(unknowns);
      decisions = chunk.map((email) => {
        const found = byEmail.get(email);
        if (!found) {
          // apollo answers one result per distinct address; a missing one is no verdict.
          return failureDecision(email, new EmailVerifierError("no result for this address", 200, false));
        }
        return decisionFromVerification(found, confirmed.has(email));
      });
    } catch (error: unknown) {
      if (!(error instanceof EmailVerifierError)) throw error;
      console.error(
        `[instantly-service] email-verification-gate: NO VERDICT for ${chunk.length} address(es) of org ${identity.orgId} (run ${identity.runId}) — nothing will be sent to them: ${error.message}`,
      );
      decisions = chunk.map((email) => failureDecision(email, error));
    }
    await recordDecisions(identity.orgId, decisions, context, identity.runId);
    for (const d of decisions) out.set(d.email, d);
  }
  return out;
}

/**
 * Of these addresses (each just answered `unknown`), the ones whose unknown is
 * CONFIRMED and may be sent. Never an address that ever bounced one of our
 * emails (2026-10-14: the Forbes / TechCrunch unknowns bounced; their retry must
 * not read as confirmation). Otherwise: an earlier `unknown` in our mirror of
 * apollo's verification history between `UNKNOWN_CONFIRM_MIN_HOURS` and
 * `UNKNOWN_CONFIRM_MAX_DAYS` old (the 48 h retry answered the same), or a real
 * email of ours accepted within `PROOF_FRESH_DAYS`. Fail loud: an unreadable
 * history must not read as "confirmed".
 */
export async function findConfirmedUnknowns(emails: string[]): Promise<Set<string>> {
  const keys = [...new Set(emails.map(normalizeLeadEmail))].filter((e) => e.length > 0);
  const out = new Set<string>();
  if (keys.length === 0) return out;
  const result = await db.execute(sql`
    SELECT k.email AS "email"
    FROM unnest(${sql.param(keys)}::text[]) AS k(email)
    WHERE NOT EXISTS (
            SELECT 1 FROM instantly_events e
            WHERE e.lead_email = k.email AND e.event_type = 'email_bounced'
              AND e.inferred = false AND e.withdrawn_at IS NULL
          )
      AND (
            EXISTS (
              SELECT 1 FROM email_verification_history h
              WHERE h.email = k.email
                AND h.verdict = 'unknown'
                AND h.verified_at <= now() - make_interval(hours => ${UNKNOWN_CONFIRM_MIN_HOURS})
                AND h.verified_at > now() - make_interval(days => ${UNKNOWN_CONFIRM_MAX_DAYS})
            )
         OR EXISTS (
              SELECT 1 FROM instantly_events e
              WHERE e.lead_email = k.email AND e.event_type = 'email_sent'
                AND e.inferred = false AND e.withdrawn_at IS NULL
                AND e."timestamp" > (now() AT TIME ZONE 'UTC') - make_interval(days => ${PROOF_FRESH_DAYS})
            )
          )
  `);
  for (const row of rowsOf(result)) out.add(String(row.email));
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
      decision.reason === "verdict_unknown_recheck"
        ? `${email} was not sent: our verifier could not tell whether it exists (unknown); it is checked again after ${until ? until.toISOString() : `${UNKNOWN_RECHECK_HOURS} h`} and sent only if that check is valid, catch_all, or unknown again`
        : `${email} was not sent: our verifier's verdict is ${decision.verdict} (${decision.reason}); only valid, catch_all and confirmed unknown addresses are ever emailed`,
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
           v.verified_at + CASE WHEN v.reason = 'verdict_unknown_recheck'
                                THEN make_interval(hours => ${UNKNOWN_RECHECK_HOURS})
                                ELSE make_interval(days => ${VERDICT_REFUSAL_DAYS}) END AS "until"
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
