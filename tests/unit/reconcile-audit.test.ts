import { describe, it, expect } from "vitest";
import type { CampaignAnalytics } from "../../src/lib/instantly-client";
import {
  summarizeInstantlyCounts,
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

describe("summarizeInstantlyCounts", () => {
  it("counts ACTIVE (status 1) campaigns; sums the cumulative counters across ALL", () => {
    const rows: CampaignAnalytics[] = [
      analytics({ campaign_status: 1, leads_count: 1, contacted_count: 1, emails_sent_count: 3 }),
      analytics({ campaign_status: 2, leads_count: 1, contacted_count: 1, emails_sent_count: 2 }), // paused
      analytics({ campaign_status: 3, leads_count: 1, contacted_count: 0, emails_sent_count: 1 }), // completed
    ];
    expect(summarizeInstantlyCounts(rows)).toEqual({
      activeCampaigns: 1, // only the status-1 row
      emailsSent: 6, // 3 + 2 + 1 across all
      contactedDispatched: 2, // 1 + 1 + 0
      contactsStored: 3, // 1 + 1 + 1
    });
  });

  it("empty fleet → all zeros", () => {
    expect(summarizeInstantlyCounts([])).toEqual({
      activeCampaigns: 0,
      emailsSent: 0,
      contactedDispatched: 0,
      contactsStored: 0,
    });
  });

  it("treats only status===1 as active (2/3/other excluded)", () => {
    const rows = [
      analytics({ campaign_status: 1 }),
      analytics({ campaign_status: 1 }),
      analytics({ campaign_status: 0 }), // draft
      analytics({ campaign_status: 4 }), // some other lifecycle code
    ];
    expect(summarizeInstantlyCounts(rows).activeCampaigns).toBe(2);
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

  it("pendingSends is local-only: instantly + delta null, sourceOfTruth 'local'", () => {
    const pending = buildReconciliation(local, instantly).find((m) => m.key === "pendingSends");
    expect(pending).toEqual({
      key: "pendingSends",
      label: "Pending sends (forecast volume)",
      local: 1540,
      instantly: null,
      delta: null,
      sourceOfTruth: "local",
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
      { activeCampaigns: 5, emailsSent: 5, contactedDispatched: 5, contactsStored: 5, pendingSends: 0 },
      { activeCampaigns: 5, emailsSent: 5, contactedDispatched: 5, contactsStored: 5 },
    );
    for (const m of metrics.filter((x) => x.instantly !== null)) {
      expect(m.delta).toBe(0);
    }
  });
});
