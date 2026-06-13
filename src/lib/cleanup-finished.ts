/**
 * Pure selection for the FINISHED-CONTACT cleanup.
 *
 * Goal (cost control): Instantly's plan limit counts contacts CURRENTLY stored
 * across all campaigns (active, paused, completed, draft). Deleting leads from a
 * campaign frees that quota (~5-10 min sync). So once a per-lead campaign is
 * terminal — Instantly COMPLETED its sequence (status 3) or it was PAUSED
 * (status 2, e.g. the operator paused it manually after the prospect replied
 * off-Instantly) — its contact can be deleted to reclaim a slot.
 *
 * Rule (locked by the requester, option A): a campaign is "finished" iff its
 * Instantly status is PAUSED (2) or COMPLETED (3). ACTIVE (1) campaigns are
 * never touched. There is no pause grace period — a pause means "done".
 *
 * Network/DB-free so it can be unit-tested deterministically — the script
 * (`scripts/cleanup-finished-contacts.ts`) does the Instantly IO and feeds
 * resolved inputs in here. Source of truth for status + membership is Instantly.
 */

/** Instantly campaign status codes that mean "finished" → contacts deletable. */
export const FINISHED_STATUSES = new Set<number>([2, 3]); // 2 = paused, 3 = completed

export interface CleanupCampaign {
  /** Instantly campaign id. */
  id: string;
  /** Instantly numeric status (1 active, 2 paused, 3 completed). */
  status: number;
}

export interface LeadMembership {
  email: string;
  campaignId: string;
}

export interface DeleteTarget {
  /** Instantly campaign id the leads must be deleted FROM (campaign-level delete). */
  campaignId: string;
  /** Distinct lower-cased emails to delete from this campaign. */
  emails: string[];
}

/**
 * Group lead memberships by campaign, keeping only campaigns whose Instantly
 * status is finished (paused or completed). Memberships are deduped by
 * (email, campaignId); emails are lower-cased. Campaigns that are active,
 * unknown (no status row), or carry no leads produce no target.
 *
 * Output is stable: targets with the most emails first, then campaignId asc.
 */
export function selectContactsToDelete(
  memberships: LeadMembership[],
  campaignsById: Map<string, CleanupCampaign>,
): DeleteTarget[] {
  const byCampaign = new Map<string, Set<string>>();

  for (const m of memberships) {
    if (!m.email || !m.campaignId) continue;
    const campaign = campaignsById.get(m.campaignId);
    if (!campaign || !FINISHED_STATUSES.has(campaign.status)) continue;

    let set = byCampaign.get(m.campaignId);
    if (!set) {
      set = new Set<string>();
      byCampaign.set(m.campaignId, set);
    }
    set.add(m.email.toLowerCase());
  }

  const targets: DeleteTarget[] = [];
  for (const [campaignId, emails] of byCampaign) {
    targets.push({ campaignId, emails: [...emails].sort() });
  }

  targets.sort(
    (a, b) => b.emails.length - a.emails.length || a.campaignId.localeCompare(b.campaignId),
  );

  return targets;
}

/** Total number of distinct (campaign, email) deletions across all targets. */
export function countDeletions(targets: DeleteTarget[]): number {
  let n = 0;
  for (const t of targets) n += t.emails.length;
  return n;
}
