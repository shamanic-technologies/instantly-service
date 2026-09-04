/**
 * Which stored campaign rows are ONE campaign, as the customer knows it.
 *
 * campaign-service mints a fresh campaign row every time the campaign's workflow
 * changes and keeps the ancestors, so what a customer calls "my campaign" is
 * routinely dozens of stored rows — 46 for one production brand, 45 of them
 * stopped. Each row carries its own runs and its own costs and none of them is
 * wrong; they are simply slices of one history.
 *
 * The IDENTITY is campaign-service's OWN uniqueness key —
 * `uniq_campaigns_org_brand_funnel_channel`: (org, brand, funnel, leg, channel),
 * with `coalesce(..., '')` on the two nullable parts so a row that states no
 * funnel or no leg pools with its like rather than becoming distinct from
 * everything. Reading it any other way would disagree with the owner about what
 * one campaign is.
 *
 * ⚠️ EVERY PART IS READ FROM campaign-service, NEVER RE-DERIVED. In particular
 * the funnel is never inferred from a goal: two funnels answer to the same goal
 * (`sales_meetings_from_conversation` and `sales_meetings_from_website` are both
 * a booked meeting), so an inference prints a funnel the campaign never stated.
 *
 * NOTE — this mirrors features-service's `campaign-identity.ts`, one notch
 * FINER: that module's key predates campaign-service's leg widening and omits
 * `legKey`. Including it can only ever REFUSE to pool two rows the owner itself
 * considers distinct, which is the safe direction, and today it is a no-op (1
 * of 705 production rows states a leg).
 *
 * Pure. The network read lives in `campaign-client.ts`.
 */

/** A campaign row as campaign-service serves it, trimmed to the identity. */
export interface CampaignIdentityRow {
  id: string;
  orgId?: string | null;
  /** The identity's brand. Null on a row predating campaign-service's brand column. */
  brandId?: string | null;
  /** Legacy array the brand used to live in — read ONLY as a fallback for `brandId`. */
  brandIds?: string[] | null;
  /** The sales funnel the campaign states. NULL is a real state, not a gap to fill. */
  funnelKey?: string | null;
  /** The funnel leg. NULL is a real state; part of the owner's own key. */
  legKey?: string | null;
  acquisitionChannel?: string | null;
}

function brandOf(row: CampaignIdentityRow): string | null {
  return row.brandId ?? row.brandIds?.[0] ?? null;
}

/**
 * The identity key, or null when the row does not state enough of it to pool.
 *
 * Null is deliberate: a row with no brand or no channel is one campaign-service
 * could not police either (its unique index skips exactly those), so pooling it
 * here would invent an identity the owner never asserted. Such a row is its own
 * campaign of one — today's behaviour, never a guess.
 */
export function identityKeyOf(row: CampaignIdentityRow): string | null {
  const brandId = brandOf(row);
  const channel = row.acquisitionChannel ?? null;
  if (!brandId || !channel) return null;
  const orgId = row.orgId ?? "";
  return [orgId, brandId, row.funnelKey ?? "", row.legKey ?? "", channel].join("|");
}

/**
 * Every campaign id that answers to the same campaign as `campaignId`, ascending,
 * the asked id always included.
 *
 * `[campaignId]` when the asked row is absent from `rows` or states too little to
 * be pooled — an unplaceable campaign is a family of one, which is exactly what
 * this read did before it knew about families.
 */
export function familyOf(
  rows: CampaignIdentityRow[],
  campaignId: string,
): string[] {
  const self = rows.find((r) => r.id === campaignId);
  const key = self ? identityKeyOf(self) : null;
  if (key === null) return [campaignId];

  const members = new Set<string>([campaignId]);
  for (const row of rows) {
    if (row.id && identityKeyOf(row) === key) members.add(row.id);
  }
  return [...members].sort();
}
