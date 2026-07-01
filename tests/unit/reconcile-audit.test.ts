import { describe, it, expect } from "vitest";
import type { CampaignAnalytics, CampaignSequenceInfo } from "../../src/lib/instantly-client";
import {
  summarizeInstantlyCounts,
  computeInstantlyPendingSends,
  buildReconciliation,
  type LocalReconcileCounts,
  type InstantlyReconcileCounts,
} from "../../src/lib/reconcile-audit";

function analytics(overrides: Partial<CampaignAnalytics>): CampaignAnalytics {
  return {
    campaign_id: "c",
    campaign_name: "n",
    campaign_status: 1,
    leads_count: 0,
    contacted_count: 0,
    emails_sent_count: 0,
    new_leads_contacted_count: 0,
    open_count: 0,
    open_count_unique: 0,
    reply_count: 0,
    link_click_count: 0,
    bounced_count: 0,
    unsubscribed_count: 0,
    completed_count: 0,
    ...overrides,
  };
}

function seq(overrides: Partial<CampaignSequenceInfo>): CampaignSequenceInfo {
  return { id: "c", status: 1, stepCount: 0, ...overrides };
}

describe("computeInstantlyPendingSends", () => {
  it("sums (stepCount − sent) over ACTIVE campaigns only", () => {
    const campaigns = [
      seq({ id: "a", status: 1, stepCount: 4 }), // sent 1 → remaining 3
      seq({ id: "b", status: 1, stepCount: 3 }), // sent 3 → remaining 0
      seq({ id: "p", status: 2, stepCount: 5 }), // paused → excluded
    ];
    const sent = new Map([
      ["a", 1],
      ["b", 3],
      ["p", 0],
    ]);
    expect(computeInstantlyPendingSends(campaigns, sent)).toBe(3); // 3 + 0, paused excluded
  });

  it("a campaign missing from analytics contributes its full stepCount (nothing sent)", () => {
    const campaigns = [seq({ id: "fresh", status: 1, stepCount: 4 })];
    expect(computeInstantlyPendingSends(campaigns, new Map())).toBe(4);
  });

  it("floors at 0 when sent exceeds stepCount (never negative)", () => {
    const campaigns = [seq({ id: "a", status: 1, stepCount: 2 })];
    expect(computeInstantlyPendingSends(campaigns, new Map([["a", 5]]))).toBe(0);
  });
});

describe("summarizeInstantlyCounts", () => {
  it("counts ACTIVE campaigns, sums cumulative counters, derives pendingSends", () => {
    const rows: CampaignAnalytics[] = [
      analytics({ campaign_id: "a", campaign_status: 1, leads_count: 1, contacted_count: 1, emails_sent_count: 1 }),
      analytics({ campaign_id: "b", campaign_status: 2, leads_count: 1, contacted_count: 1, emails_sent_count: 2 }), // paused
      analytics({ campaign_id: "c", campaign_status: 3, leads_count: 1, contacted_count: 0, emails_sent_count: 1 }), // completed
    ];
    const campaigns = [
      seq({ id: "a", status: 1, stepCount: 4 }), // active: remaining 4 − 1 = 3
      seq({ id: "b", status: 2, stepCount: 4 }), // paused: excluded
      seq({ id: "c", status: 3, stepCount: 4 }), // completed: excluded
    ];
    expect(summarizeInstantlyCounts(rows, campaigns)).toEqual({
      activeCampaigns: 1,
      emailsSent: 4, // 1 + 2 + 1
      contactedDispatched: 2, // 1 + 1 + 0
      contactsStored: 3, // 1 + 1 + 1
      pendingSends: 3, // only campaign a (active): 4 − 1
    });
  });

  it("empty fleet → all zeros", () => {
    expect(summarizeInstantlyCounts([], [])).toEqual({
      activeCampaigns: 0,
      emailsSent: 0,
      contactedDispatched: 0,
      contactsStored: 0,
      pendingSends: 0,
    });
  });

  it("treats only status===1 as active (2/3/other excluded)", () => {
    const rows = [
      analytics({ campaign_status: 1 }),
      analytics({ campaign_status: 1 }),
      analytics({ campaign_status: 0 }), // draft
      analytics({ campaign_status: 4 }), // some other lifecycle code
    ];
    expect(summarizeInstantlyCounts(rows, []).activeCampaigns).toBe(2);
  });
});

describe("buildReconciliation", () => {
  const local: LocalReconcileCounts = {
    activeCampaigns: 812,
    emailsSent: 41230,
    contactedDispatched: 9001,
    contactsStored: 812,
    pendingSends: 1540,
  };
  const instantly: InstantlyReconcileCounts = {
    activeCampaigns: 809,
    emailsSent: 41198,
    contactedDispatched: 9000,
    contactsStored: 800,
    pendingSends: 1502,
  };

  it("emits five metrics in stable order with local, instantly, and delta = local - instantly", () => {
    const metrics = buildReconciliation(local, instantly);
    expect(metrics.map((m) => m.key)).toEqual([
      "activeCampaigns",
      "emailsSent",
      "contactedDispatched",
      "contactsStored",
      "pendingSends",
    ]);
    expect(metrics[0]).toEqual({
      key: "activeCampaigns",
      label: "Active campaigns",
      local: 812,
      instantly: 809,
      delta: 3,
      sourceOfTruth: "instantly",
    });
    expect(metrics[1].delta).toBe(32); // 41230 - 41198
    expect(metrics[3].delta).toBe(12); // 812 - 800 (cleanup drift)
  });

  it("pendingSends now reconciles against Instantly's derived count (real delta)", () => {
    const pending = buildReconciliation(local, instantly).find((m) => m.key === "pendingSends");
    expect(pending).toEqual({
      key: "pendingSends",
      label: "Pending sends (forecast volume)",
      local: 1540,
      instantly: 1502,
      delta: 38, // 1540 − 1502
      sourceOfTruth: "instantly",
    });
  });

  it("a negative delta (local behind Instantly) is preserved, not clamped", () => {
    const metrics = buildReconciliation(
      { ...local, emailsSent: 41000 },
      instantly,
    );
    expect(metrics.find((m) => m.key === "emailsSent")!.delta).toBe(-198);
  });

  it("zero delta when the two sides agree", () => {
    const metrics = buildReconciliation(
      { activeCampaigns: 5, emailsSent: 5, contactedDispatched: 5, contactsStored: 5, pendingSends: 7 },
      { activeCampaigns: 5, emailsSent: 5, contactedDispatched: 5, contactsStored: 5, pendingSends: 7 },
    );
    for (const m of metrics) {
      expect(m.delta).toBe(0);
    }
  });
});
