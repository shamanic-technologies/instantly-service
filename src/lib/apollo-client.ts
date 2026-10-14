/**
 * apollo-service client — the prospect's phone number, when Apollo has one.
 *
 * ⚠️ THE REVEAL IS ASYNCHRONOUS BY APOLLO'S OWN DESIGN, so this is two calls and
 * not one. The POST asks; Apollo answers immediately WITHOUT the number and
 * delivers it minutes later to apollo-service's callback, so the POST returns 202
 * with `status: "pending"` and the number arrives on a later GET. A client that
 * read the POST's own body as the answer would report "no number" for every
 * prospect Apollo was about to hand us one for.
 *
 * Nothing is charged when Apollo finds nothing: apollo-service provisions the
 * worst case before calling and CANCELS the hold when the callback brings
 * nothing back, so a fruitless reveal is free. A reveal already made (or already
 * in flight) is served back rather than spent again.
 *
 * ⚠️ `status` IS THE ANSWER, NEVER A NULL NUMBER. `pending` (not here yet),
 * `not_found` (Apollo has none — a real answer), and `failed` (the reveal broke)
 * are three different facts that all present as `mobilePhone: null`, and only the
 * first is worth waiting for. Read the status.
 *
 * FAILS LOUD. Both calls throw on any non-2xx, with the status and body intact —
 * the caller is a fail-soft side effect and swallows it, but it swallows it
 * knowing whether Apollo said "nobody" or whether we never got to ask.
 */

const APOLLO_SERVICE_URL = process.env.APOLLO_SERVICE_URL;
const APOLLO_SERVICE_API_KEY = process.env.APOLLO_SERVICE_API_KEY;

/** One number Apollo returned, with its own do-not-call verdict attached. */
export interface RevealedPhone {
  rawNumber: string | null;
  sanitizedNumber: string | null;
  type: string | null;
  status: string | null;
  dncStatus: string | null;
  position: number | null;
  confidence: string | null;
  /** True means this number must NEVER be dialled. Unknown DNC values are true. */
  doNotCall: boolean;
}

/** apollo-service's reveal record, identical on the POST and the GET. */
export interface PhoneReveal {
  revealId: string;
  apolloPersonId: string;
  /**
   * `pending` = Apollo has not delivered yet; `found` = a number arrived;
   * `not_found` = Apollo has none for this person; `failed` = the reveal broke.
   */
  status: "pending" | "found" | "not_found" | "failed";
  /** The number to connect a rep on, when Apollo returned a mobile. */
  mobilePhone: string | null;
  dncStatus: string | null;
  /** Do-not-call for the PRIMARY number. True means never dial it. */
  doNotCall: boolean;
  phoneNumbers: RevealedPhone[];
  failureReason: string | null;
  creditsConsumed: number | null;
  requestedAt: string | null;
  completedAt: string | null;
  reused?: boolean;
}

/** The identity apollo-service requires on a reveal — every field is mandatory. */
export interface RevealIdentity {
  orgId: string;
  userId: string;
  /**
   * ⚠️ A REAL run id. apollo-service declares the reveal's cost against it and
   * traces the request onto it, so a fabricated uuid is rejected downstream and
   * surfaces here as a 400 that reads like a broken endpoint.
   */
  runId: string;
  brandId?: string | null;
  campaignId?: string | null;
  /** The feature the reveal is spent for: its cost is read under this slug. */
  featureSlug?: string | null;
}

function revealHeaders(identity: RevealIdentity): Record<string, string> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    "x-api-key": APOLLO_SERVICE_API_KEY as string,
    "x-org-id": identity.orgId,
    "x-user-id": identity.userId,
    "x-run-id": identity.runId,
  };
  if (identity.brandId) headers["x-brand-id"] = identity.brandId;
  if (identity.campaignId) headers["x-campaign-id"] = identity.campaignId;
  if (identity.featureSlug) headers["x-feature-slug"] = identity.featureSlug;
  return headers;
}

function assertConfigured(): void {
  if (!APOLLO_SERVICE_URL || !APOLLO_SERVICE_API_KEY) {
    throw new Error("APOLLO_SERVICE_URL or APOLLO_SERVICE_API_KEY is not set");
  }
}

/**
 * Ask Apollo to reveal this person's phone number.
 *
 * 202 (`pending`) is the ordinary answer — the number is delivered to
 * apollo-service's callback minutes later and read back with
 * {@link readPhoneReveal}. 200 means a number was already held, or arrived
 * synchronously, and no wait is needed at all.
 */
export async function requestPhoneReveal(
  apolloPersonId: string,
  identity: RevealIdentity,
): Promise<PhoneReveal> {
  assertConfigured();

  const response = await fetch(
    `${APOLLO_SERVICE_URL}/people/${encodeURIComponent(apolloPersonId)}/phone-reveal`,
    { method: "POST", headers: revealHeaders(identity), body: "{}" },
  );

  if (!response.ok) {
    const detail = await response.text();
    throw new Error(
      `apollo-service POST /people/{apolloPersonId}/phone-reveal failed: ${response.status} - ${detail.slice(0, 200)}`,
    );
  }

  return (await response.json()) as PhoneReveal;
}

/** Has the revealed number arrived yet? Same record shape as the request. */
export async function readPhoneReveal(
  apolloPersonId: string,
  identity: RevealIdentity,
): Promise<PhoneReveal> {
  assertConfigured();

  const response = await fetch(
    `${APOLLO_SERVICE_URL}/people/${encodeURIComponent(apolloPersonId)}/phone-reveal`,
    { headers: revealHeaders(identity) },
  );

  if (!response.ok) {
    const detail = await response.text();
    throw new Error(
      `apollo-service GET /people/{apolloPersonId}/phone-reveal failed: ${response.status} - ${detail.slice(0, 200)}`,
    );
  }

  return (await response.json()) as PhoneReveal;
}

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
    const insufficientCredits =
      /credit_insufficient|insufficient balance/i.test(detail);
    throw new EmailVerifierError(
      `apollo-service POST /email-verifications failed: ${response.status} - ${detail.slice(0, 300)}`,
      response.status,
      insufficientCredits,
    );
  }

  const body = (await response.json()) as { results?: unknown };
  if (!Array.isArray(body.results)) {
    throw new EmailVerifierError("apollo-service POST /email-verifications returned no results array", response.status, false);
  }
  return body.results as EmailVerification[];
}
