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
