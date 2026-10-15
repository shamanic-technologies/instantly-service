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
 * Tell lead-service this person ALREADY buys from the brand: a won sale our
 * outreach did not cause (source `reply`, no value, no cost — nothing is
 * estimated). Idempotent on lead-service's side (`recorded` / `already_recorded`
 * / `already_won`). Throws on any non-2xx with the status and body, so a caller
 * logs WHY (404 `lead_not_found`, 409 `ambiguous_lead`, 409 `stated_never`).
 */
export async function recordExistingCustomerByEmail(params: {
  orgId: string;
  campaignId: string;
  email: string;
  replyRef?: string;
}): Promise<{ status: string }> {
  if (!LEAD_SERVICE_URL || !LEAD_SERVICE_API_KEY) {
    throw new Error("LEAD_SERVICE_URL or LEAD_SERVICE_API_KEY is not set");
  }
  const { orgId, campaignId, ...body } = params;
  const response = await fetch(
    `${LEAD_SERVICE_URL}/orgs/campaigns/${encodeURIComponent(campaignId)}/existing-customers/by-email`,
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
      `lead-service POST /orgs/campaigns/{campaignId}/existing-customers/by-email failed: ${response.status} - ${detail.slice(0, 200)}`,
    );
  }
  return (await response.json()) as { status: string };
}

/**
 * What lead-service holds about a person on one campaign: how to name them, and
 * Apollo's id for them.
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
  /**
   * Who they are, spelled out. Every one of these already rides the `?view=basic`
   * projection lead-service serves — they are read here because a call that
   * restates a prospect's identity before asking the rep to commit to a live
   * conversation needs more than a display name. Null when lead-service holds
   * nothing; NEVER derived from another field, because a guessed first name is
   * spoken aloud to a customer.
   */
  firstName: string | null;
  lastName: string | null;
  /** Their job title at the current employer (`currentTitle` on the wire). */
  title: string | null;
  city: string | null;
  state: string | null;
  country: string | null;
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
        currentTitle?: string | null;
        city?: string | null;
        state?: string | null;
        country?: string | null;
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
    firstName: lead?.firstName?.trim() || null,
    lastName: lead?.lastName?.trim() || null,
    title: lead?.currentTitle?.trim() || null,
    city: lead?.city?.trim() || null,
    state: lead?.state?.trim() || null,
    country: lead?.country?.trim() || null,
  };
}

/**
 * Empty a person's follow-up schedule, with a reason.
 *
 * ⚠️ NOBODY IN THE FLEET HAD EVER CALLED THIS. lead-service's queue documents
 * three stops — opted out, meeting booked, and "they answered again, the
 * observer of that reply says so" — and only the first two were ever posted by
 * anything. So the third was a documented behaviour that did not exist, and a
 * thread a human took over stayed claimable forever.
 *
 * ⚠️ A STOP IS NOT A TOMBSTONE, by lead-service's own model: a later `scheduled`
 * re-enters the person. That is right for a decline or a fresh reply. A thread
 * handed to a PERSON is the other case: use `handFollowupsToHuman`, never a stop.
 *
 * `reason` is REQUIRED by the producer and must be non-empty (it 400s
 * `reason_required` otherwise) — a schedule emptied for a reason nobody recorded
 * is not auditable later.
 *
 * FAILS LOUD, same as its sibling above. The caller decides whether a failure to
 * stop the ladder should fail the whole escalation.
 */
export async function stopFollowups(params: {
  orgId: string;
  /** lead-service's `leads_campaigns` row id — from `findLeadOnCampaignByEmail`. */
  leadRowId: string;
  reason: string;
}): Promise<void> {
  if (!LEAD_SERVICE_URL || !LEAD_SERVICE_API_KEY) {
    throw new Error("LEAD_SERVICE_URL or LEAD_SERVICE_API_KEY is not set");
  }

  const response = await fetch(
    `${LEAD_SERVICE_URL}/orgs/leads/${encodeURIComponent(params.leadRowId)}/followups`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": LEAD_SERVICE_API_KEY,
        "x-org-id": params.orgId,
      },
      body: JSON.stringify({ kind: "stopped", reason: params.reason }),
    },
  );

  if (!response.ok) {
    const detail = await response.text();
    throw new Error(
      `lead-service POST /orgs/leads/{id}/followups failed: ${response.status} - ${detail.slice(0, 200)}`,
    );
  }
}

/**
 * One lead row's follow-up state, as lead-service holds it — or null when the
 * row does not exist for this org.
 *
 * Read BEFORE a stop that is not certain to be needed: lead-service records a
 * stop reason on the row whatever it held, and the customer's lead timeline
 * renders that reason as "follow-ups stopped". Stopping a schedule that never
 * existed would put a false event on every declining lead's page.
 *
 * FAILS LOUD on anything but 200/404, same as the siblings above.
 */
export async function readFollowupState(params: {
  orgId: string;
  leadRowId: string;
}): Promise<ScheduledFollowup["followup"] | null> {
  if (!LEAD_SERVICE_URL || !LEAD_SERVICE_API_KEY) {
    throw new Error("LEAD_SERVICE_URL or LEAD_SERVICE_API_KEY is not set");
  }

  const response = await fetch(
    `${LEAD_SERVICE_URL}/orgs/leads/${encodeURIComponent(params.leadRowId)}/followups`,
    {
      headers: {
        "x-api-key": LEAD_SERVICE_API_KEY,
        "x-org-id": params.orgId,
      },
    },
  );

  if (response.status === 404) return null;
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(
      `lead-service GET /orgs/leads/{id}/followups failed: ${response.status} - ${detail.slice(0, 200)}`,
    );
  }

  const body = (await response.json()) as { followup?: ScheduledFollowup["followup"] };
  return body.followup ?? null;
}

/**
 * Outcomes that mean a meeting is booked with the person: the booking itself, and the two steps
 * that cannot happen without one. The SAME set lead-service's own follow-up queue excludes on
 * (`FOLLOWUP_BOOKED_OUTCOMES` there), read here verbatim rather than re-decided.
 */
export const BOOKED_OUTCOME_EVENTS = ["meeting_booked", "meeting_attended", "sale"] as const;

/** Who has a live booked outcome at one brand, keyed both ways a local row can be matched. */
export interface BookedPeople {
  leadIds: Set<string>;
  emails: Set<string>;
}

const BOOKED_READ_TIMEOUT_MS = 15_000;

/**
 * Every person with a LIVE booked outcome at `brandId`, read from lead-service's conversion ledger
 * (`GET /internal/brands/:brandId/converted-leads?event=…`: attributed, not withdrawn, any source —
 * CRM, tracker, a person's statement). One read per event in {@link BOOKED_OUTCOME_EVENTS}.
 *
 * FAILS LOUD on any non-2xx or malformed body: an unreadable ledger is NOT "nobody booked", and the
 * caller must not keep emailing on a guess.
 */
export async function listBookedPeople(brandId: string): Promise<BookedPeople> {
  if (!LEAD_SERVICE_URL || !LEAD_SERVICE_API_KEY) {
    throw new Error("LEAD_SERVICE_URL or LEAD_SERVICE_API_KEY is not set");
  }
  const booked: BookedPeople = { leadIds: new Set(), emails: new Set() };
  await Promise.all(
    BOOKED_OUTCOME_EVENTS.map(async (event) => {
      const response = await fetch(
        `${LEAD_SERVICE_URL}/internal/brands/${encodeURIComponent(brandId)}/converted-leads?event=${event}`,
        {
          headers: { "x-api-key": LEAD_SERVICE_API_KEY },
          signal: AbortSignal.timeout(BOOKED_READ_TIMEOUT_MS),
        },
      );
      if (!response.ok) {
        const detail = await response.text();
        throw new Error(
          `lead-service GET /internal/brands/{brandId}/converted-leads?event=${event} failed: ${response.status} - ${detail.slice(0, 200)}`,
        );
      }
      const body = (await response.json()) as { outcomes?: unknown };
      if (!Array.isArray(body.outcomes)) {
        throw new Error(
          `lead-service GET /internal/brands/{brandId}/converted-leads?event=${event} returned no outcomes array`,
        );
      }
      for (const raw of body.outcomes as Array<{ leadId?: unknown; email?: unknown }>) {
        if (typeof raw.leadId === "string" && raw.leadId) booked.leadIds.add(raw.leadId);
        if (typeof raw.email === "string" && raw.email) booked.emails.add(raw.email.trim().toLowerCase());
      }
    }),
  );
  return booked;
}

/**
 * Withdraw a hand-over WE stated, because the verdict behind it was wrong
 * (lib/lift-wrong-handoff). lead-service lifts the row only when every
 * hand-over on it carries `handedReason` EXACTLY; a hand-over anyone else
 * stated is never lifted. Nothing is due afterwards until a `scheduled`.
 *
 * Returns true when the row was lifted, false when there was nothing to lift
 * (not handed, or handed for another reason). FAILS LOUD on anything else.
 */
export async function withdrawHandoff(params: {
  orgId: string;
  /** lead-service's `leads_campaigns` row id — from `findLeadOnCampaignByEmail`. */
  leadRowId: string;
  /** The reason the hand-over was stated with, verbatim. */
  handedReason: string;
  /** Why it is withdrawn (kept as history). */
  reason: string;
}): Promise<boolean> {
  if (!LEAD_SERVICE_URL || !LEAD_SERVICE_API_KEY) {
    throw new Error("LEAD_SERVICE_URL or LEAD_SERVICE_API_KEY is not set");
  }

  const response = await fetch(
    `${LEAD_SERVICE_URL}/orgs/leads/${encodeURIComponent(params.leadRowId)}/followups/handover/withdraw`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": LEAD_SERVICE_API_KEY,
        "x-org-id": params.orgId,
      },
      body: JSON.stringify({ handedReason: params.handedReason, reason: params.reason }),
    },
  );

  if (!response.ok) {
    const detail = await response.text();
    throw new Error(
      `lead-service POST /orgs/leads/{id}/followups/handover/withdraw failed: ${response.status} - ${detail.slice(0, 200)}`,
    );
  }
  const body = (await response.json()) as { outcome?: unknown };
  if (body.outcome === "lifted") return true;
  if (body.outcome === "nothing_to_lift") return false;
  throw new Error(`lead-service handover/withdraw answered an unknown outcome: ${JSON.stringify(body.outcome)}`);
}

/**
 * State that we ACTED on this person and when the next answer is owed:
 * lead-service `kind: "acted"` (count + 1, due date set, claim released).
 *
 * Stated by this service as the OBSERVER of a message the brand's own person
 * typed by hand on the thread (owner 2026-10-15, REPLY-ANY-LEAD-1015): it counts
 * as one of ours, the follow-ups go on. No `x-campaign-id` (an observer: anyone
 * may claim the next date), so no campaign's worker is credited with it.
 *
 * FAILS LOUD; the caller is fail-soft.
 */
export async function recordFollowupActed(params: {
  orgId: string;
  /** lead-service's `leads_campaigns` row id — from `findLeadOnCampaignByEmail`. */
  leadRowId: string;
  nextDueAt: string;
}): Promise<ScheduledFollowup["followup"]> {
  if (!LEAD_SERVICE_URL || !LEAD_SERVICE_API_KEY) {
    throw new Error("LEAD_SERVICE_URL or LEAD_SERVICE_API_KEY is not set");
  }

  const response = await fetch(
    `${LEAD_SERVICE_URL}/orgs/leads/${encodeURIComponent(params.leadRowId)}/followups`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": LEAD_SERVICE_API_KEY,
        "x-org-id": params.orgId,
      },
      body: JSON.stringify({ kind: "acted", nextDueAt: params.nextDueAt }),
    },
  );

  if (!response.ok) {
    const detail = await response.text();
    throw new Error(
      `lead-service POST /orgs/leads/{id}/followups (acted) failed: ${response.status} - ${detail.slice(0, 200)}`,
    );
  }
  const body = (await response.json()) as { followup?: ScheduledFollowup["followup"] };
  if (!body.followup) throw new Error("lead-service POST /orgs/leads/{id}/followups (acted) answered no followup");
  return body.followup;
}

/**
 * Give a person's thread to a HUMAN for good: lead-service `kind: "handed"`
 * (owner 2026-10-11). No automated responder claims the row again, and unlike a
 * stop a later `scheduled` (the prospect writes again) does NOT re-enter it, so
 * switching a responder on can never answer a thread a person owns
 * (Shockwavecenters: the client had been asked to answer, staff had promised to
 * follow up, and the responder answered both).
 *
 * `reason` is REQUIRED (to whom / why). FAILS LOUD; callers are fail-soft.
 */
export async function handFollowupsToHuman(params: {
  orgId: string;
  /** lead-service's `leads_campaigns` row id — from `findLeadOnCampaignByEmail`. */
  leadRowId: string;
  reason: string;
}): Promise<void> {
  if (!LEAD_SERVICE_URL || !LEAD_SERVICE_API_KEY) {
    throw new Error("LEAD_SERVICE_URL or LEAD_SERVICE_API_KEY is not set");
  }

  const response = await fetch(
    `${LEAD_SERVICE_URL}/orgs/leads/${encodeURIComponent(params.leadRowId)}/followups`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-api-key": LEAD_SERVICE_API_KEY,
        "x-org-id": params.orgId,
      },
      body: JSON.stringify({ kind: "handed", reason: params.reason }),
    },
  );

  if (!response.ok) {
    const detail = await response.text();
    throw new Error(
      `lead-service POST /orgs/leads/{id}/followups (handed) failed: ${response.status} - ${detail.slice(0, 200)}`,
    );
  }
}

/**
 * The timezone lead-service holds for the one person on this campaign at this address, or
 * null when it holds none (or the address does not resolve to exactly one row).
 *
 * The person's zone is lead-service's fact (stated by the client, enriched, or inferred from
 * their phone / location / domain). A sender that forgot to pass it on `POST /orgs/send` left
 * the sequence on the fleet default zone; this read is how the queue heals that
 * (lib/queued-sequence-timezone). Same campaign-scoped narrowing + exact case-folded match as
 * `findLeadOnCampaignByEmail`, but on the FULL view: the slim `view=basic` projection carries
 * no timezone.
 *
 * FAILS LOUD on a non-2xx.
 */
export async function findLeadTimezoneOnCampaignByEmail(params: {
  orgId: string;
  campaignId: string;
  email: string;
}): Promise<string | null> {
  if (!LEAD_SERVICE_URL || !LEAD_SERVICE_API_KEY) {
    throw new Error("LEAD_SERVICE_URL or LEAD_SERVICE_API_KEY is not set");
  }

  const query = new URLSearchParams({
    campaignId: params.campaignId,
    q: params.email,
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
    leads?: Array<{ email?: string; lead?: { timezone?: string | null } | null }>;
  };
  const rows = Array.isArray(body.leads) ? body.leads : [];
  const exact = rows.filter((r) => sameAddress(r.email, params.email));
  if (exact.length !== 1) return null;
  const zone = exact[0].lead?.timezone;
  return typeof zone === "string" && zone.trim() ? zone.trim() : null;
}
