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
