import { describe, it, expect } from "vitest";
import {
  selectCampaignsToPause,
  pauseIdSet,
  type HealCampaign,
  type LeadMembership,
} from "../../src/lib/heal-duplicates";

function campaign(id: string, createdAt: string, active = true): HealCampaign {
  return { id, createdAt, active };
}

function mapOf(...cs: HealCampaign[]): Map<string, HealCampaign> {
  return new Map(cs.map((c) => [c.id, c]));
}

describe("selectCampaignsToPause", () => {
  it("no memberships → no decisions", () => {
    expect(selectCampaignsToPause([], mapOf())).toEqual([]);
  });

  it("single active campaign → nothing to pause", () => {
    const m: LeadMembership[] = [{ email: "a@x.com", campaignId: "c1" }];
    expect(selectCampaignsToPause(m, mapOf(campaign("c1", "2026-01-01")))).toEqual([]);
  });

  it("keeps the OLDEST and pauses the rest", () => {
    const m: LeadMembership[] = [
      { email: "a@x.com", campaignId: "new" },
      { email: "a@x.com", campaignId: "old" },
      { email: "a@x.com", campaignId: "mid" },
    ];
    const result = selectCampaignsToPause(
      m,
      mapOf(
        campaign("new", "2026-03-01"),
        campaign("old", "2026-01-01"),
        campaign("mid", "2026-02-01"),
      ),
    );
    expect(result).toHaveLength(1);
    expect(result[0].keepId).toBe("old");
    expect(result[0].pauseIds.sort()).toEqual(["mid", "new"]);
  });

  it("ignores inactive campaigns when choosing keep/pause", () => {
    const m: LeadMembership[] = [
      { email: "a@x.com", campaignId: "paused-old" },
      { email: "a@x.com", campaignId: "active-new" },
    ];
    // oldest is inactive → only one ACTIVE campaign → nothing to pause
    const result = selectCampaignsToPause(
      m,
      mapOf(
        campaign("paused-old", "2026-01-01", false),
        campaign("active-new", "2026-02-01", true),
      ),
    );
    expect(result).toEqual([]);
  });

  it("keeps oldest ACTIVE even when an older inactive exists", () => {
    const m: LeadMembership[] = [
      { email: "a@x.com", campaignId: "inactive-oldest" },
      { email: "a@x.com", campaignId: "active-older" },
      { email: "a@x.com", campaignId: "active-newer" },
    ];
    const result = selectCampaignsToPause(
      m,
      mapOf(
        campaign("inactive-oldest", "2025-12-01", false),
        campaign("active-older", "2026-01-01", true),
        campaign("active-newer", "2026-02-01", true),
      ),
    );
    expect(result).toHaveLength(1);
    expect(result[0].keepId).toBe("active-older");
    expect(result[0].pauseIds).toEqual(["active-newer"]);
  });

  it("dedupes repeated (email, campaign) rows", () => {
    const m: LeadMembership[] = [
      { email: "a@x.com", campaignId: "c1" },
      { email: "a@x.com", campaignId: "c1" },
      { email: "a@x.com", campaignId: "c2" },
    ];
    const result = selectCampaignsToPause(
      m,
      mapOf(campaign("c1", "2026-01-01"), campaign("c2", "2026-02-01")),
    );
    expect(result[0].keepId).toBe("c1");
    expect(result[0].pauseIds).toEqual(["c2"]);
  });

  it("normalizes email case when grouping", () => {
    const m: LeadMembership[] = [
      { email: "A@X.com", campaignId: "c1" },
      { email: "a@x.com", campaignId: "c2" },
    ];
    const result = selectCampaignsToPause(
      m,
      mapOf(campaign("c1", "2026-01-01"), campaign("c2", "2026-02-01")),
    );
    expect(result).toHaveLength(1);
    expect(result[0].email).toBe("a@x.com");
    expect(result[0].keepId).toBe("c1");
  });

  it("tiebreaks equal createdAt by id for determinism", () => {
    const m: LeadMembership[] = [
      { email: "a@x.com", campaignId: "bbb" },
      { email: "a@x.com", campaignId: "aaa" },
    ];
    const result = selectCampaignsToPause(
      m,
      mapOf(campaign("bbb", "2026-01-01"), campaign("aaa", "2026-01-01")),
    );
    expect(result[0].keepId).toBe("aaa");
    expect(result[0].pauseIds).toEqual(["bbb"]);
  });

  it("handles multiple emails and sorts by pause count desc", () => {
    const m: LeadMembership[] = [
      { email: "small@x.com", campaignId: "s1" },
      { email: "small@x.com", campaignId: "s2" },
      { email: "big@x.com", campaignId: "b1" },
      { email: "big@x.com", campaignId: "b2" },
      { email: "big@x.com", campaignId: "b3" },
    ];
    const result = selectCampaignsToPause(
      m,
      mapOf(
        campaign("s1", "2026-01-01"),
        campaign("s2", "2026-02-01"),
        campaign("b1", "2026-01-01"),
        campaign("b2", "2026-02-01"),
        campaign("b3", "2026-03-01"),
      ),
    );
    expect(result.map((d) => d.email)).toEqual(["big@x.com", "small@x.com"]);
    expect(pauseIdSet(result).sort()).toEqual(["b2", "b3", "s2"]);
  });
});
