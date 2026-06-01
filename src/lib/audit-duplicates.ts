/**
 * Pure duplicate-contact detection for the cross-campaign audit.
 *
 * Source of truth for the *duplicate fact* is Instantly: an email sitting in
 * ≥2 ACTIVE campaigns of one workspace is being double-contacted, period.
 * This module is network- and DB-free so it can be unit-tested deterministically
 * — the script (`scripts/audit-cross-campaign-dupes.ts`) does the Instantly +
 * local-DB IO and feeds resolved inputs in here.
 *
 * Brand attribution is a SECONDARY enrichment that exists ONLY in the local DB
 * (`instantly_campaigns.brand_ids`). Instantly has no brand/org field. The
 * caller stamps each campaign's `brandIds` from the DB; this module classifies
 * each collision but never decides the duplicate fact from brand data.
 *
 * Severity (mirrors DIS-77's (lead, brand) keying):
 *   - same-brand    → ≥2 active campaigns share a brand. True redundant outreach
 *                     (the dangerous case DIS-77's Phase A/B cancelled).
 *   - cross-brand   → ≥2 active campaigns, every campaign's brand known, no brand
 *                     repeated. Same prospect hit by multiple distinct brands.
 *   - unknown-brand → ≥2 active campaigns but at least one has no DB brand row and
 *                     no same-brand match. Duplicate is real; brand label can't be
 *                     trusted (DB gap — the very reason this audit reads Instantly).
 */

export interface AuditCampaign {
  /** Instantly campaign id. */
  id: string;
  name: string;
  /** Resolved by the caller from Instantly status (active === status 1). */
  active: boolean;
  /** From local DB `instantly_campaigns.brand_ids`; `[]` when no DB row / unknown. */
  brandIds: string[];
  /** From local DB; `null` when no DB row. */
  orgId: string | null;
}

export interface LeadMembership {
  email: string;
  /** Instantly campaign id this lead row belongs to. */
  campaignId: string;
}

export type DuplicateSeverity = "same-brand" | "cross-brand" | "unknown-brand";

export interface DuplicateContact {
  email: string;
  activeCampaigns: AuditCampaign[];
  totalActive: number;
  severity: DuplicateSeverity;
  /** Brand ids hit by ≥2 active campaigns (drives the same-brand verdict). */
  sameBrandIds: string[];
}

const SEVERITY_RANK: Record<DuplicateSeverity, number> = {
  "same-brand": 0,
  "unknown-brand": 1,
  "cross-brand": 2,
};

/**
 * Group lead memberships by email and flag every email sitting in ≥2 ACTIVE
 * campaigns. Memberships are deduped by (email, campaignId) so a lead row that
 * appears twice for the same campaign counts once.
 *
 * Output is sorted: same-brand first (most actionable), then unknown-brand,
 * then cross-brand; within a severity, by `totalActive` desc.
 */
export function findDuplicateContacts(
  memberships: LeadMembership[],
  campaignsById: Map<string, AuditCampaign>,
): DuplicateContact[] {
  // email -> set of distinct campaign ids
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

  const duplicates: DuplicateContact[] = [];

  for (const [email, campaignIds] of byEmail) {
    const activeCampaigns: AuditCampaign[] = [];
    for (const id of campaignIds) {
      const c = campaignsById.get(id);
      if (c && c.active) activeCampaigns.push(c);
    }
    if (activeCampaigns.length < 2) continue;

    // brand -> count of active campaigns carrying it
    const brandCount = new Map<string, number>();
    let anyUnknownBrand = false;
    for (const c of activeCampaigns) {
      if (c.brandIds.length === 0) {
        anyUnknownBrand = true;
        continue;
      }
      for (const b of c.brandIds) {
        brandCount.set(b, (brandCount.get(b) ?? 0) + 1);
      }
    }

    const sameBrandIds = [...brandCount.entries()]
      .filter(([, n]) => n >= 2)
      .map(([b]) => b);

    let severity: DuplicateSeverity;
    if (sameBrandIds.length > 0) {
      severity = "same-brand";
    } else if (anyUnknownBrand) {
      severity = "unknown-brand";
    } else {
      severity = "cross-brand";
    }

    duplicates.push({
      email,
      activeCampaigns,
      totalActive: activeCampaigns.length,
      severity,
      sameBrandIds,
    });
  }

  duplicates.sort(
    (a, b) =>
      SEVERITY_RANK[a.severity] - SEVERITY_RANK[b.severity] ||
      b.totalActive - a.totalActive ||
      a.email.localeCompare(b.email),
  );

  return duplicates;
}

export interface DuplicateSummary {
  activeCampaignsScanned: number;
  distinctEmailsInActive: number;
  duplicateEmails: number;
  sameBrand: number;
  crossBrand: number;
  unknownBrand: number;
  redundantActiveCampaigns: number;
  worstOffender: { email: string; totalActive: number } | null;
}

/** Roll up a duplicate list into headline numbers for the report. */
export function summarizeDuplicates(
  duplicates: DuplicateContact[],
  activeCampaignsScanned: number,
  distinctEmailsInActive: number,
): DuplicateSummary {
  let sameBrand = 0;
  let crossBrand = 0;
  let unknownBrand = 0;
  let redundantActiveCampaigns = 0;
  let worstOffender: { email: string; totalActive: number } | null = null;

  for (const d of duplicates) {
    if (d.severity === "same-brand") sameBrand++;
    else if (d.severity === "cross-brand") crossBrand++;
    else unknownBrand++;
    // every active campaign beyond the first for an email is redundant
    redundantActiveCampaigns += d.totalActive - 1;
    if (!worstOffender || d.totalActive > worstOffender.totalActive) {
      worstOffender = { email: d.email, totalActive: d.totalActive };
    }
  }

  return {
    activeCampaignsScanned,
    distinctEmailsInActive,
    duplicateEmails: duplicates.length,
    sameBrand,
    crossBrand,
    unknownBrand,
    redundantActiveCampaigns,
    worstOffender,
  };
}
