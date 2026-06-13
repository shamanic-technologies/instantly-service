import { describe, it, expect } from "vitest";
import {
  selectContactsToDelete,
  countDeletions,
  FINISHED_STATUSES,
  type CleanupCampaign,
  type LeadMembership,
} from "../../src/lib/cleanup-finished";

function campaigns(
  entries: Array<[id: string, status: number]>,
): Map<string, CleanupCampaign> {
  const m = new Map<string, CleanupCampaign>();
  for (const [id, status] of entries) m.set(id, { id, status });
  return m;
}

describe("selectContactsToDelete", () => {
  it("selects leads of COMPLETED (status 3) campaigns", () => {
    const m: LeadMembership[] = [{ email: "a@x.com", campaignId: "c1" }];
    const targets = selectContactsToDelete(m, campaigns([["c1", 3]]));
    expect(targets).toEqual([{ campaignId: "c1", emails: ["a@x.com"] }]);
  });

  it("selects leads of PAUSED (status 2) campaigns", () => {
    const m: LeadMembership[] = [{ email: "a@x.com", campaignId: "c1" }];
    const targets = selectContactsToDelete(m, campaigns([["c1", 2]]));
    expect(targets).toEqual([{ campaignId: "c1", emails: ["a@x.com"] }]);
  });

  it("never selects ACTIVE (status 1) campaigns", () => {
    const m: LeadMembership[] = [{ email: "a@x.com", campaignId: "c1" }];
    const targets = selectContactsToDelete(m, campaigns([["c1", 1]]));
    expect(targets).toEqual([]);
  });

  it("skips leads whose campaign is unknown (no status row)", () => {
    const m: LeadMembership[] = [{ email: "a@x.com", campaignId: "ghost" }];
    const targets = selectContactsToDelete(m, campaigns([["c1", 3]]));
    expect(targets).toEqual([]);
  });

  it("dedups (email, campaignId) and lower-cases the email", () => {
    const m: LeadMembership[] = [
      { email: "A@X.com", campaignId: "c1" },
      { email: "a@x.com", campaignId: "c1" },
    ];
    const targets = selectContactsToDelete(m, campaigns([["c1", 2]]));
    expect(targets).toEqual([{ campaignId: "c1", emails: ["a@x.com"] }]);
  });

  it("keeps the same email separately per campaign (campaign-level delete)", () => {
    const m: LeadMembership[] = [
      { email: "a@x.com", campaignId: "c1" },
      { email: "a@x.com", campaignId: "c2" },
    ];
    const targets = selectContactsToDelete(m, campaigns([["c1", 3], ["c2", 2]]));
    expect(targets.map((t) => t.campaignId).sort()).toEqual(["c1", "c2"]);
    expect(countDeletions(targets)).toBe(2);
  });

  it("ignores empty email / campaignId rows", () => {
    const m: LeadMembership[] = [
      { email: "", campaignId: "c1" },
      { email: "a@x.com", campaignId: "" },
    ];
    expect(selectContactsToDelete(m, campaigns([["c1", 3]]))).toEqual([]);
  });

  it("orders targets by email count desc, then campaignId asc", () => {
    const m: LeadMembership[] = [
      { email: "a@x.com", campaignId: "big" },
      { email: "b@x.com", campaignId: "big" },
      { email: "c@x.com", campaignId: "small" },
    ];
    const targets = selectContactsToDelete(m, campaigns([["big", 3], ["small", 2]]));
    expect(targets.map((t) => t.campaignId)).toEqual(["big", "small"]);
  });

  it("countDeletions sums all emails across targets", () => {
    const targets = [
      { campaignId: "c1", emails: ["a@x.com", "b@x.com"] },
      { campaignId: "c2", emails: ["c@x.com"] },
    ];
    expect(countDeletions(targets)).toBe(3);
  });

  it("FINISHED_STATUSES is exactly {2,3}", () => {
    expect([...FINISHED_STATUSES].sort()).toEqual([2, 3]);
  });
});
