/**
 * Tell lead-service that an address's delivery evidence just changed.
 *
 * lead-service answers the customer's Leads page (tab counts, board standing
 * counts, filtered pages) from a READ MODEL it keeps per scope, and holds each
 * address's delivery evidence (contacted, clicked, replied + its classification,
 * opted out, bounced) for at most five minutes. That bound is fine for what a
 * PROVIDER observes. It is not fine for something a PERSON just did in the
 * product — classifying a reply, recording or withdrawing an opt-out — which
 * they expect to see on the very next read. This service is where those
 * statements land, so this is where the change is announced.
 *
 * Contract (lead-service v0.81.11): `POST /orgs/leads/evidence-changed`,
 * `{ emails: [...] }`, 1..1000 addresses, case irrelevant, answers 202. It only
 * RECORDS the change; lead-service asks email-gateway again on its next read.
 * Idempotent — announcing twice is harmless, which is why the manual paths may
 * announce an address that `promoteEvent` also announces.
 *
 * ⚠️ A FRESHNESS HINT, NEVER A CONDITION. The statement is the fact; this is a
 * notification about it. So `announceEvidenceChanged` never throws and never
 * rolls anything back — but it LOGS every failure loudly, because a failed
 * notification is a real failure (the customer waits up to five minutes) and a
 * silent one would be indistinguishable from a working one.
 */

/** Producer ceiling on one call. */
export const EVIDENCE_CHANGED_MAX_EMAILS = 1000;

/** A freshness hint must not hold a person's request (or a webhook) hostage. */
const EVIDENCE_CHANGED_TIMEOUT_MS = 3_000;

/** Distinct, trimmed, lower-cased, non-empty — case is irrelevant to the producer. */
export function normalizeEvidenceEmails(emails: ReadonlyArray<string | null | undefined>): string[] {
  const out = new Set<string>();
  for (const email of emails) {
    const normalized = (email ?? "").trim().toLowerCase();
    if (normalized) out.add(normalized);
  }
  return [...out];
}

/**
 * POST the change. FAILS LOUD — the fail-soft wrapper below is the only caller
 * the write paths use, and it needs the reason intact to log it.
 */
export async function notifyEvidenceChanged(orgId: string, emails: string[]): Promise<void> {
  // Read at USE, never captured at load: a service deployed without the vars
  // still boots, and every failure names what is missing.
  const url = process.env.LEAD_SERVICE_URL;
  const apiKey = process.env.LEAD_SERVICE_API_KEY;
  if (!url || !apiKey) {
    throw new Error("LEAD_SERVICE_URL or LEAD_SERVICE_API_KEY is not set");
  }

  for (let i = 0; i < emails.length; i += EVIDENCE_CHANGED_MAX_EMAILS) {
    const chunk = emails.slice(i, i + EVIDENCE_CHANGED_MAX_EMAILS);
    const response = await fetch(`${url}/orgs/leads/evidence-changed`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": apiKey,
        "x-org-id": orgId,
      },
      body: JSON.stringify({ emails: chunk }),
      signal: AbortSignal.timeout(EVIDENCE_CHANGED_TIMEOUT_MS),
    });

    if (!response.ok) {
      const detail = await response.text();
      throw new Error(
        `lead-service POST /orgs/leads/evidence-changed failed: ${response.status} - ${detail.slice(0, 200)}`,
      );
    }
  }
}

/**
 * Announce that `emails` changed for `orgId`. Never throws.
 *
 * A null org (a platform send) has no customer Leads page to refresh, so it is
 * an ordinary absence, not a failure. Everything else that goes wrong is
 * warned, naming the org, the addresses and why.
 */
export async function announceEvidenceChanged(
  orgId: string | null | undefined,
  emails: ReadonlyArray<string | null | undefined>,
  reason: string,
): Promise<void> {
  const normalized = normalizeEvidenceEmails(emails);
  if (!orgId || normalized.length === 0) return;

  try {
    await notifyEvidenceChanged(orgId, normalized);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.warn(
      `[instantly-service] evidence-changed NOT delivered to lead-service (Leads page stale up to 5 min): org=${orgId} reason=${reason} emails=${normalized.join(",")} — ${message}`,
    );
  }
}
