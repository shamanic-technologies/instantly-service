/**
 * apollo-service client: email verification (the send door's gate) and the
 * verification history feed. FAILS LOUD on any non-2xx, status and body intact.
 */

import { fundingRefusalCauseFromText, type FundingRefusalCause } from "./funding-refusal";

const APOLLO_SERVICE_URL = process.env.APOLLO_SERVICE_URL;
const APOLLO_SERVICE_API_KEY = process.env.APOLLO_SERVICE_API_KEY;

// ─── Email verification (the send door's gate) ──────────────────────────────

/** BounceVerify's verdict, as apollo-service states it. */
export type EmailVerdict = "valid" | "invalid" | "catch_all" | "risky" | "unknown";

/** One address's verdict from `POST /email-verifications`. */
export interface EmailVerification {
  email: string;
  verdict: EmailVerdict;
  verificationId: string;
  verifiedAt: string;
  /**
   * true = apollo-service already held a decisive verdict under 30 days old and
   * handed it back (nothing billed); false = verified now, billed to the org as a
   * child run of x-run-id.
   */
  reused: boolean;
}

/**
 * Why a verification could not be obtained. `insufficientCredits` = the org's
 * balance cannot pay for it (apollo's authorize refused); anything else is the
 * verifier, runs or billing being unreachable — no verdict either way.
 */
export class EmailVerifierError extends Error {
  constructor(
    message: string,
    readonly status: number | null,
    readonly insufficientCredits: boolean,
    /**
     * apollo relayed billing's NAMED cause (a sales funnel max budget, not a
     * balance shortfall): `reason` + `sales_funnel_cap` verbatim. Null otherwise.
     */
    readonly fundingCause: FundingRefusalCause | null = null,
  ) {
    super(message);
    this.name = "EmailVerifierError";
  }
}

/** The identity a verification is billed to: the run of the campaign that sends. */
export interface VerificationIdentity {
  orgId: string;
  userId: string;
  runId: string;
  brandIds?: string[];
  campaignId?: string | null;
  featureSlug?: string | null;
  workflowSlug?: string | null;
}

/**
 * Verdicts for up to 50 addresses (apollo-service `POST /email-verifications`).
 *
 * ALL OR NOTHING by the producer's contract: one address it cannot verify fails
 * the whole call (502), so a caller never holds a partial answer. FAILS LOUD:
 * any non-2xx throws an {@link EmailVerifierError}; nothing here reads a missing
 * verdict as a good one.
 */
export async function verifyEmailAddresses(
  emails: string[],
  identity: VerificationIdentity,
  source: string,
): Promise<EmailVerification[]> {
  if (!APOLLO_SERVICE_URL || !APOLLO_SERVICE_API_KEY) {
    throw new EmailVerifierError("APOLLO_SERVICE_URL or APOLLO_SERVICE_API_KEY is not set", null, false);
  }
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    "x-api-key": APOLLO_SERVICE_API_KEY,
    "x-org-id": identity.orgId,
    "x-user-id": identity.userId,
    "x-run-id": identity.runId,
  };
  if (identity.brandIds && identity.brandIds.length > 0) headers["x-brand-id"] = identity.brandIds.join(",");
  if (identity.campaignId) headers["x-campaign-id"] = identity.campaignId;
  if (identity.featureSlug) headers["x-feature-slug"] = identity.featureSlug;
  if (identity.workflowSlug) headers["x-workflow-slug"] = identity.workflowSlug;

  let response: Response;
  try {
    response = await fetch(`${APOLLO_SERVICE_URL}/email-verifications`, {
      method: "POST",
      headers,
      body: JSON.stringify({ emails, source }),
    });
  } catch (error: unknown) {
    throw new EmailVerifierError(
      `apollo-service POST /email-verifications unreachable: ${error instanceof Error ? error.message : String(error)}`,
      null,
      false,
    );
  }

  if (!response.ok) {
    const detail = await response.text();
    // The 502 names an insufficient balance in its message; the 400 says it by type.
    // A refusal for a sales funnel's max budget carries billing's `reason` (and
    // `sales_funnel_cap`): still a refusal to pay, but named, never "add credit".
    const fundingCause = fundingRefusalCauseFromText(detail);
    const insufficientCredits =
      fundingCause !== null || /credit_insufficient|insufficient balance/i.test(detail);
    throw new EmailVerifierError(
      `apollo-service POST /email-verifications failed: ${response.status} - ${detail.slice(0, 300)}`,
      response.status,
      insufficientCredits,
      fundingCause,
    );
  }

  const body = (await response.json()) as { results?: unknown };
  if (!Array.isArray(body.results)) {
    throw new EmailVerifierError("apollo-service POST /email-verifications returned no results array", response.status, false);
  }
  return body.results as EmailVerification[];
}

/** One verification of apollo-service's history, as our mirror stores it. */
export interface VerificationHistoryRow {
  id: string;
  email: string;
  /** apollo's verdict; null when the call returned none (never invented). */
  verdict: string | null;
  verifiedAt: string;
}

export interface VerificationHistoryPage {
  rows: VerificationHistoryRow[];
  nextCursor: string | null;
  hasMore: boolean;
}

/** Rows per page asked of the feed (its max is 5000). */
export const VERIFICATION_HISTORY_PAGE_SIZE = 5000;

/**
 * One page of apollo-service's verification history
 * (`GET /internal/email-verifications/history?after=<cursor>&limit=`): every
 * verification ever made, every org, in insertion order. `cursor` null = from the
 * very first. The feed holds back rows younger than its settle window (60 s), so
 * a consumer that passed a cursor never has a row appear behind it. FAILS LOUD on
 * any non-2xx or a body that is not the documented page.
 */
export async function fetchVerificationHistoryPage(cursor: string | null): Promise<VerificationHistoryPage> {
  if (!APOLLO_SERVICE_URL || !APOLLO_SERVICE_API_KEY) {
    throw new Error("APOLLO_SERVICE_URL or APOLLO_SERVICE_API_KEY is not set");
  }
  const params = new URLSearchParams({ limit: String(VERIFICATION_HISTORY_PAGE_SIZE) });
  if (cursor) params.set("after", cursor);
  const url = `${APOLLO_SERVICE_URL}/internal/email-verifications/history?${params.toString()}`;
  const response = await fetch(url, { headers: { "x-api-key": APOLLO_SERVICE_API_KEY } });
  if (!response.ok) {
    throw new Error(`apollo-service GET /internal/email-verifications/history ${response.status}: ${await response.text()}`);
  }
  const body = (await response.json()) as {
    verifications?: Array<{ id: string; email: string; verdict: string | null; verifiedAt: string }>;
    nextCursor?: string;
    hasMore?: boolean;
  };
  if (!Array.isArray(body.verifications) || typeof body.nextCursor !== "string" || typeof body.hasMore !== "boolean") {
    throw new Error("apollo-service verification history: unexpected page shape");
  }
  return {
    rows: body.verifications.map((v) => ({ id: v.id, email: v.email, verdict: v.verdict ?? null, verifiedAt: v.verifiedAt })),
    nextCursor: body.nextCursor,
    hasMore: body.hasMore,
  };
}
