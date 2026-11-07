/**
 * Move a person's next follow-up date LATER at every brand of a send, in
 * lead-service (`PUT /orgs/leads/{row}/followup-date`, LEAD-FOLLOWUP-DATE-1016).
 * Never earlier than what lead-service holds, never over a stop.
 *
 * ONE writer for every "this person waits" fact of this service (an
 * out-of-office return date, OOO-RETURN-DATE-1101).
 */

import {
  findLeadOnCampaignByEmail,
  findLeadRowOfBrandByEmail,
  readLeadFollowupDate,
  stateLeadFollowupDate,
} from "./lead-client";

export interface FollowupWait {
  orgId: string | null;
  /** The caller's campaign: lead-service's lead row is found on it. */
  campaignId: string | null;
  brandIds: readonly string[];
  leadEmail: string;
  until: Date;
  /** Stored as the date's reason: what the person waits for, in plain words. */
  reason: string;
}

/** Returns what happened per brand (`<brand>:stated|already_later|kept_stopped`). Throws on a lead-service failure. */
export async function moveFollowupDateLater(wait: FollowupWait): Promise<string> {
  if (!wait.orgId) return "no_org";
  const orgId = wait.orgId;
  if (wait.brandIds.length === 0) return "no_brand";
  // The caller campaign's row first; else ANY row of the person at the brand
  // (the date is the person x brand's, lead-service accepts any of their rows):
  // a wait that finds no row would leave the date due and the person refused
  // again on every serve (mike@rxwellness.com, 2026-11-07).
  const campaignRow = wait.campaignId
    ? await findLeadOnCampaignByEmail({ orgId, campaignId: wait.campaignId, email: wait.leadEmail })
    : null;
  const outcomes: string[] = [];
  for (const brandId of wait.brandIds) {
    const rowId =
      campaignRow?.id || (await findLeadRowOfBrandByEmail({ orgId, brandId, email: wait.leadEmail }))?.id;
    if (!rowId) {
      outcomes.push(`${brandId}:no_lead_row`);
      continue;
    }
    const row = { id: rowId };
    const current = await readLeadFollowupDate({ orgId, leadRowId: row.id, brandId });
    if (current.nextFollowupAt === null) {
      outcomes.push(`${brandId}:kept_stopped`);
      continue;
    }
    if (new Date(current.nextFollowupAt).getTime() >= wait.until.getTime()) {
      outcomes.push(`${brandId}:already_later`);
      continue;
    }
    await stateLeadFollowupDate({
      orgId,
      leadRowId: row.id,
      brandId,
      nextFollowupAt: wait.until.toISOString(),
      reason: wait.reason,
    });
    outcomes.push(`${brandId}:stated`);
  }
  return outcomes.join(",");
}
