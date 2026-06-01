import { describe, it, expect } from "vitest";
import {
  findDuplicateContacts,
  summarizeDuplicates,
  type AuditCampaign,
  type LeadMembership,
} from "../../src/lib/audit-duplicates";

/** Build an AuditCampaign with sensible defaults. */
function campaign(
  id: string,
  over: Partial<AuditCampaign> = {},
): AuditCampaign {
  return {
    id,
    name: `Campaign ${id}`,
    active: true,
    brandIds: [],
    orgId: "org-1",
    ...over,
  };
}

function mapOf(...campaigns: AuditCampaign[]): Map<string, AuditCampaign> {
  return new Map(campaigns.map((c) => [c.id, c]));
}

describe("findDuplicateContacts", () => {
  it("1. returns [] for no memberships", () => {
    expect(findDuplicateContacts([], mapOf())).toEqual([]);
  });

  it("2. does not flag an email in a single active campaign", () => {
    const memberships: LeadMembership[] = [{ email: "a@x.com", campaignId: "c1" }];
    const result = findDuplicateContacts(memberships, mapOf(campaign("c1")));
    expect(result).toEqual([]);
  });

  it("3. flags an email in 2 active campaigns sharing a brand as same-brand", () => {
    const memberships: LeadMembership[] = [
      { email: "a@x.com", campaignId: "c1" },
      { email: "a@x.com", campaignId: "c2" },
    ];
    const result = findDuplicateContacts(
      memberships,
      mapOf(
        campaign("c1", { brandIds: ["brand-A"] }),
        campaign("c2", { brandIds: ["brand-A"] }),
      ),
    );
    expect(result).toHaveLength(1);
    expect(result[0].email).toBe("a@x.com");
    expect(result[0].severity).toBe("same-brand");
    expect(result[0].totalActive).toBe(2);
    expect(result[0].sameBrandIds).toEqual(["brand-A"]);
  });

  it("4. flags an email in 2 active campaigns of different brands as cross-brand", () => {
    const memberships: LeadMembership[] = [
      { email: "a@x.com", campaignId: "c1" },
      { email: "a@x.com", campaignId: "c2" },
    ];
    const result = findDuplicateContacts(
      memberships,
      mapOf(
        campaign("c1", { brandIds: ["brand-A"] }),
        campaign("c2", { brandIds: ["brand-B"] }),
      ),
    );
    expect(result).toHaveLength(1);
    expect(result[0].severity).toBe("cross-brand");
    expect(result[0].sameBrandIds).toEqual([]);
  });

  it("5. does not flag when only one of the two campaigns is active", () => {
    const memberships: LeadMembership[] = [
      { email: "a@x.com", campaignId: "c1" },
      { email: "a@x.com", campaignId: "c2" },
    ];
    const result = findDuplicateContacts(
      memberships,
      mapOf(
        campaign("c1", { active: true, brandIds: ["brand-A"] }),
        campaign("c2", { active: false, brandIds: ["brand-A"] }),
      ),
    );
    expect(result).toEqual([]);
  });

  it("6. flags as unknown-brand when both active campaigns lack brand data", () => {
    const memberships: LeadMembership[] = [
      { email: "a@x.com", campaignId: "c1" },
      { email: "a@x.com", campaignId: "c2" },
    ];
    const result = findDuplicateContacts(
      memberships,
      mapOf(
        campaign("c1", { brandIds: [] }),
        campaign("c2", { brandIds: [] }),
      ),
    );
    expect(result).toHaveLength(1);
    expect(result[0].severity).toBe("unknown-brand");
  });

  it("7. dedupes a repeated (email, campaign) row → single campaign not flagged", () => {
    const memberships: LeadMembership[] = [
      { email: "a@x.com", campaignId: "c1" },
      { email: "a@x.com", campaignId: "c1" },
    ];
    const result = findDuplicateContacts(memberships, mapOf(campaign("c1")));
    expect(result).toEqual([]);
  });

  it("8. flags same-brand when 2 of 3 active campaigns share brand A", () => {
    const memberships: LeadMembership[] = [
      { email: "a@x.com", campaignId: "c1" },
      { email: "a@x.com", campaignId: "c2" },
      { email: "a@x.com", campaignId: "c3" },
    ];
    const result = findDuplicateContacts(
      memberships,
      mapOf(
        campaign("c1", { brandIds: ["brand-A"] }),
        campaign("c2", { brandIds: ["brand-A"] }),
        campaign("c3", { brandIds: ["brand-B"] }),
      ),
    );
    expect(result).toHaveLength(1);
    expect(result[0].severity).toBe("same-brand");
    expect(result[0].sameBrandIds).toEqual(["brand-A"]);
    expect(result[0].totalActive).toBe(3);
  });

  it("normalizes email case when grouping", () => {
    const memberships: LeadMembership[] = [
      { email: "A@X.com", campaignId: "c1" },
      { email: "a@x.com", campaignId: "c2" },
    ];
    const result = findDuplicateContacts(
      memberships,
      mapOf(campaign("c1", { brandIds: ["b"] }), campaign("c2", { brandIds: ["b"] })),
    );
    expect(result).toHaveLength(1);
    expect(result[0].email).toBe("a@x.com");
  });

  it("sorts same-brand before cross-brand", () => {
    const memberships: LeadMembership[] = [
      { email: "cross@x.com", campaignId: "c1" },
      { email: "cross@x.com", campaignId: "c2" },
      { email: "same@x.com", campaignId: "c3" },
      { email: "same@x.com", campaignId: "c4" },
    ];
    const result = findDuplicateContacts(
      memberships,
      mapOf(
        campaign("c1", { brandIds: ["brand-A"] }),
        campaign("c2", { brandIds: ["brand-B"] }),
        campaign("c3", { brandIds: ["brand-C"] }),
        campaign("c4", { brandIds: ["brand-C"] }),
      ),
    );
    expect(result.map((d) => d.severity)).toEqual(["same-brand", "cross-brand"]);
  });
});

describe("summarizeDuplicates", () => {
  it("rolls up counts, redundant campaigns, and worst offender", () => {
    const dups = findDuplicateContacts(
      [
        { email: "a@x.com", campaignId: "c1" },
        { email: "a@x.com", campaignId: "c2" },
        { email: "a@x.com", campaignId: "c3" },
        { email: "b@x.com", campaignId: "c4" },
        { email: "b@x.com", campaignId: "c5" },
      ],
      mapOf(
        campaign("c1", { brandIds: ["A"] }),
        campaign("c2", { brandIds: ["A"] }),
        campaign("c3", { brandIds: ["B"] }),
        campaign("c4", { brandIds: ["C"] }),
        campaign("c5", { brandIds: ["D"] }),
      ),
    );
    const summary = summarizeDuplicates(dups, 5, 2);
    expect(summary.duplicateEmails).toBe(2);
    expect(summary.sameBrand).toBe(1);
    expect(summary.crossBrand).toBe(1);
    expect(summary.unknownBrand).toBe(0);
    // a@x.com: 3 active → 2 redundant; b@x.com: 2 active → 1 redundant
    expect(summary.redundantActiveCampaigns).toBe(3);
    expect(summary.worstOffender).toEqual({ email: "a@x.com", totalActive: 3 });
  });

  it("returns null worst offender for an empty duplicate list", () => {
    const summary = summarizeDuplicates([], 0, 0);
    expect(summary.worstOffender).toBeNull();
    expect(summary.duplicateEmails).toBe(0);
  });
});
