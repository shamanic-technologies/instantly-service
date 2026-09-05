/**
 * lead-service client — states that we OWE an answer to a person.
 *
 * lead-service owns the follow-up queue: a due date per (campaign, person) that a
 * scheduled worker claims, oldest-due first. It has always held the queue and the
 * sweep that drains it; what nobody ever did was ENTER anybody into it. Measured in
 * production before this shipped: zero rows fleet-wide carried a due date, so every
 * run of the worker that answers interested prospects came back empty.
 *
 * The door used here is the by-EMAIL one, deliberately: this service holds the
 * campaign and the person's address and nothing else — it does not hold, and must
 * not guess at, lead-service's own `leads_campaigns` row id. Identification on that
 * side is exact (case-folded equality on the registered address, scoped to the org
 * and the campaign named), and it REFUSES rather than guesses: an unknown address is
 * a 404, an address matching two rows is a 409. Both are named refusals, which is
 * what makes a failure here readable instead of a silent no-op.
 */

const LEAD_SERVICE_URL = process.env.LEAD_SERVICE_URL;
const LEAD_SERVICE_API_KEY = process.env.LEAD_SERVICE_API_KEY;

/** What lead-service reports back: the debt as it now stands, and who it is owed to. */
export interface ScheduledFollowup {
  followup: {
    id: string;
    leadId: string;
    campaignId: string;
    dueAt: string | null;
    claimedAt: string | null;
    followupCount: number;
    lastActionAt: string | null;
    stoppedReason: string | null;
  };
  leadId: string;
  email: string;
}

/**
 * State that an answer is owed to `email` on `campaignId`, at `dueAt`.
 *
 * ⚠️ `campaignId` is the CALLER campaign — campaign-service's own row id, the one
 * this service persists on `instantly_campaigns.campaign_id`. lead-service scopes
 * the match to the id NAMED, not to its identity family, matching the claim's scope
 * exactly: enqueueing onto a sibling row would write a debt nothing ever claims.
 *
 * FAILS LOUD. The caller is a fail-soft side effect and swallows this, but the
 * failure must reach it with its reason intact — a client that degraded to silence
 * would reproduce the exact bug this closes (a queue nobody ever wrote to, with
 * nothing anywhere saying so).
 */
export async function scheduleFollowupByEmail(params: {
  orgId: string;
  campaignId: string;
  email: string;
  dueAt: string;
}): Promise<ScheduledFollowup> {
  if (!LEAD_SERVICE_URL || !LEAD_SERVICE_API_KEY) {
    throw new Error("LEAD_SERVICE_URL or LEAD_SERVICE_API_KEY is not set");
  }

  const { orgId, campaignId, ...body } = params;
  const response = await fetch(
    `${LEAD_SERVICE_URL}/orgs/campaigns/${encodeURIComponent(campaignId)}/followups/schedule-by-email`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": LEAD_SERVICE_API_KEY,
        "x-org-id": orgId,
      },
      body: JSON.stringify(body),
    },
  );

  if (!response.ok) {
    const detail = await response.text();
    throw new Error(
      `lead-service POST /orgs/campaigns/{campaignId}/followups/schedule-by-email failed: ${response.status} - ${detail.slice(0, 200)}`,
    );
  }

  return (await response.json()) as ScheduledFollowup;
}

/**
 * What this service needs to KNOW about a person before it rings a rep about
 * them: how to name them out loud, and Apollo's id for them so a phone number
 * can be asked for.
 */
export interface LeadForCall {
  /** lead-service's `leads_campaigns` row id. */
  id: string;
  /** The registered address, as lead-service holds it. */
  email: string;
  /** Apollo's person id — the key a phone reveal is asked on. Null when unknown. */
  apolloPersonId: string | null;
  /** The person's name, when lead-service holds one. */
  name: string | null;
  /** Their company's name, when known. */
  company: string | null;
}

/** Case-folded address equality — the same normalization the send path applies. */
function sameAddress(a: string | null | undefined, b: string): boolean {
  return (a ?? "").trim().toLowerCase() === b.trim().toLowerCase();
}

/**
 * The one person on this campaign holding this email address, or null.
 *
 * ⚠️ THE SEARCH IS A NARROWING, NOT THE MATCH. lead-service exposes no exact
 * by-address lead READ — its only exact by-email door is the follow-up write,
 * which answers with a person id this service cannot then look a lead up by. So
 * the campaign-scoped free-text search narrows the population and the EXACT
 * match is made here, case-folded, against the address lead-service returns:
 * anything other than exactly one row matching outright resolves to null. A
 * substring search resolving two people would otherwise ring a rep about the
 * wrong one, and being told nothing about the right one is the far cheaper
 * mistake. A cleaner exact read on lead-service would let this collapse to one
 * call and one comparison; until then this is the available door, not a guess.
 *
 * `view=basic` is deliberate: the slim projection already carries every field
 * needed here (`apolloPersonId`, the name, the organization) and skips the full
 * lead graph, ~90% of which would be discarded.
 *
 * FAILS LOUD on a non-2xx. The caller is fail-soft and rings the rep anyway,
 * without a number — but "lead-service was unreachable" and "this campaign holds
 * nobody at that address" must not arrive as the same silence.
 */
export async function findLeadOnCampaignByEmail(params: {
  orgId: string;
  campaignId: string;
  email: string;
}): Promise<LeadForCall | null> {
  if (!LEAD_SERVICE_URL || !LEAD_SERVICE_API_KEY) {
    throw new Error("LEAD_SERVICE_URL or LEAD_SERVICE_API_KEY is not set");
  }

  const query = new URLSearchParams({
    view: "basic",
    campaignId: params.campaignId,
    q: params.email,
    // The person replied, so they were served — but a read that quietly excluded
    // a lifecycle state would report "nobody at that address" about somebody
    // plainly there.
    status: "all",
    limit: "25",
  });

  const response = await fetch(`${LEAD_SERVICE_URL}/orgs/leads?${query}`, {
    headers: {
      "x-api-key": LEAD_SERVICE_API_KEY,
      "x-org-id": params.orgId,
    },
  });

  if (!response.ok) {
    const detail = await response.text();
    throw new Error(
      `lead-service GET /orgs/leads failed: ${response.status} - ${detail.slice(0, 200)}`,
    );
  }

  const body = (await response.json()) as {
    leads?: Array<{
      id?: string;
      email?: string;
      apolloPersonId?: string | null;
      lead?: {
        name?: string | null;
        firstName?: string | null;
        lastName?: string | null;
        organization?: { name?: string | null } | null;
      } | null;
    }>;
  };

  const rows = Array.isArray(body.leads) ? body.leads : [];
  const exact = rows.filter((r) => sameAddress(r.email, params.email));
  if (exact.length !== 1) return null;

  const row = exact[0];
  const lead = row.lead ?? null;
  const fromParts = [lead?.firstName, lead?.lastName]
    .filter((p): p is string => typeof p === "string" && p.trim().length > 0)
    .join(" ")
    .trim();

  return {
    id: String(row.id ?? ""),
    email: String(row.email ?? params.email),
    apolloPersonId: row.apolloPersonId ?? null,
    name: lead?.name?.trim() || fromParts || null,
    company: lead?.organization?.name?.trim() || null,
  };
}
