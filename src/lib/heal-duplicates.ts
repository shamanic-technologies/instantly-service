/**
 * Pure selection for the cross-campaign duplicate HEAL.
 *
 * Rule (locked by the requester): for each person (email) sitting in ≥2 ACTIVE
 * Instantly campaigns, KEEP the single OLDEST campaign (min created_at) and PAUSE
 * every other active campaign. Collapses both retry stacks and distinct logical
 * campaigns down to exactly one active campaign per person.
 *
 * Network/DB-free so it can be unit-tested deterministically — the script
 * (`scripts/heal-pause-dupe-campaigns.ts`) does the Instantly IO and feeds
 * resolved inputs in here. Source of truth for "active" + created_at is Instantly.
 */

export interface HealCampaign {
  /** Instantly campaign id. */
  id: string;
  /** Resolved by the caller from Instantly status (active === status 1). */
  active: boolean;
  /** Instantly `created_at` (ISO 8601). Lexical sort === chronological. */
  createdAt: string;
}

export interface LeadMembership {
  email: string;
  campaignId: string;
}

export interface HealDecision {
  email: string;
  /** The single oldest active campaign — left untouched. */
  keepId: string;
  /** All other active campaigns for this email — to be paused. */
  pauseIds: string[];
}

/**
 * Group memberships by email, and for every email with ≥2 active campaigns,
 * keep the oldest (min createdAt, tiebroken by id for determinism) and mark the
 * rest for pausing. Memberships are deduped by (email, campaignId).
 *
 * Emails with 0 or 1 active campaign produce no decision (nothing to pause).
 */
export function selectCampaignsToPause(
  memberships: LeadMembership[],
  campaignsById: Map<string, HealCampaign>,
): HealDecision[] {
  const byEmail = new Map<string, Set<string>>();
  for (const m of memberships) {
    if (!m.email || !m.campaignId) continue;
    const email = m.email.toLowerCase();
    let set = byEmail.get(email);
    if (!set) {
      set = new Set<string>();
      byEmail.set(email, set);
    }
    set.add(m.campaignId);
  }

  const decisions: HealDecision[] = [];

  for (const [email, campaignIds] of byEmail) {
    const active: HealCampaign[] = [];
    for (const id of campaignIds) {
      const c = campaignsById.get(id);
      if (c && c.active) active.push(c);
    }
    if (active.length < 2) continue;

    // Oldest first: createdAt asc, then id asc as a stable tiebreak.
    active.sort(
      (a, b) => a.createdAt.localeCompare(b.createdAt) || a.id.localeCompare(b.id),
    );

    const [keep, ...rest] = active;
    decisions.push({
      email,
      keepId: keep.id,
      pauseIds: rest.map((c) => c.id),
    });
  }

  // Stable output: emails with the most campaigns to pause first.
  decisions.sort(
    (a, b) => b.pauseIds.length - a.pauseIds.length || a.email.localeCompare(b.email),
  );

  return decisions;
}

/** Flatten decisions into the de-duplicated set of campaign ids to pause. */
export function pauseIdSet(decisions: HealDecision[]): string[] {
  const set = new Set<string>();
  for (const d of decisions) for (const id of d.pauseIds) set.add(id);
  return [...set];
}
