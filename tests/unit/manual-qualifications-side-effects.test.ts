import { describe, it, expect, vi, beforeEach } from "vitest";

// ─── Mocks ───────────────────────────────────────────────────────────────────

const mockDbInsertValues = vi.fn();
const mockDbUpdateSet = vi.fn();
const mockRefreshLeadStatusCurrent = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    insert: () => ({
      values: (v: unknown) => {
        mockDbInsertValues(v);
        return Promise.resolve([{}]);
      },
    }),
    update: () => ({
      set: (v: unknown) => {
        mockDbUpdateSet(v);
        return { where: () => Promise.resolve([{}]) };
      },
    }),
  },
}));

vi.mock("../../src/db/schema", () => ({
  instantlyCampaigns: { instantlyCampaignId: "instantly_campaign_id" },
  instantlyEvents: { campaignId: "campaign_id", eventType: "event_type" },
  instantlyManualQualificationsRaw: {
    orgId: "org_id",
    instantlyCampaignId: "instantly_campaign_id",
    leadEmail: "lead_email",
    qualifiedAt: "qualified_at",
  },
}));

const mockPromoteEvent = vi.fn();

vi.mock("../../src/lib/silver-promote", () => ({
  promoteEvent: (...args: unknown[]) => mockPromoteEvent(...args),
}));

vi.mock("../../src/lib/status-gold", () => ({
  refreshLeadStatusCurrent: (...args: unknown[]) => mockRefreshLeadStatusCurrent(...args),
}));

import { applyManualQualificationSideEffects } from "../../src/lib/manual-qualifications";

beforeEach(() => {
  vi.resetAllMocks();
  mockPromoteEvent.mockResolvedValue({ promoted: true, silverEventId: "ev-1" });
  mockRefreshLeadStatusCurrent.mockResolvedValue(undefined);
});

describe("applyManualQualificationSideEffects", () => {
  const baseInput = {
    bronzeRowId: "bronze-1",
    instantlyCampaignId: "inst-camp-1",
    leadEmail: "lead@test.com",
    qualifiedAt: new Date("2026-05-24T10:00:00.000Z"),
    rawPayload: { campaign_id: "camp-1", email: "lead@test.com" },
  };

  it("synthesizes a `reply_received` silver event via promoteEvent (source='manual', inferred=false)", async () => {
    await applyManualQualificationSideEffects({
      ...baseInput,
      status: "lead_interested",
    });

    expect(mockPromoteEvent).toHaveBeenCalledTimes(1);
    const call = mockPromoteEvent.mock.calls[0][0];
    expect(call).toEqual(
      expect.objectContaining({
        eventType: "reply_received",
        instantlyCampaignId: "inst-camp-1",
        leadEmail: "lead@test.com",
        source: "manual",
        inferred: false,
        sourceRowId: "bronze-1",
      }),
    );
  });

  it("also writes the lead-status event in silver (direct insert)", async () => {
    await applyManualQualificationSideEffects({
      ...baseInput,
      status: "lead_meeting_booked",
    });

    const leadStatusInsert = mockDbInsertValues.mock.calls.find((c) => {
      const v = c[0] as Record<string, unknown>;
      return v.eventType === "lead_meeting_booked";
    });
    expect(leadStatusInsert).toBeDefined();
    const v = leadStatusInsert![0] as Record<string, unknown>;
    expect(v.source).toBe("manual");
    expect(v.inferred).toBe(false);
    expect(v.campaignId).toBe("inst-camp-1");
    expect(v.leadEmail).toBe("lead@test.com");
  });

  it("pins reply_classification + source='manual' on instantly_campaigns", async () => {
    await applyManualQualificationSideEffects({
      ...baseInput,
      status: "lead_interested",
    });

    const updateCall = mockDbUpdateSet.mock.calls.find((c) => {
      const v = c[0] as Record<string, unknown>;
      return "replyClassification" in v;
    });
    expect(updateCall).toBeDefined();
    const v = updateCall![0] as Record<string, unknown>;
    expect(v.replyClassification).toBe("positive");
    expect(v.replyClassificationSource).toBe("manual");
  });

  it("refreshes the Gold status row after pinning the manual classification", async () => {
    await applyManualQualificationSideEffects({
      ...baseInput,
      status: "lead_not_interested",
    });

    expect(mockRefreshLeadStatusCurrent).toHaveBeenCalledWith(
      "inst-camp-1",
      "lead@test.com",
    );
  });

  it("maps lead_not_interested to negative", async () => {
    await applyManualQualificationSideEffects({
      ...baseInput,
      status: "lead_not_interested",
    });

    const updateCall = mockDbUpdateSet.mock.calls.find((c) => {
      const v = c[0] as Record<string, unknown>;
      return "replyClassification" in v;
    });
    expect(updateCall).toBeDefined();
    expect((updateCall![0] as Record<string, unknown>).replyClassification).toBe("negative");
  });

  it("maps lead_neutral / lead_out_of_office / auto_reply_received to neutral", async () => {
    for (const status of ["lead_neutral", "lead_out_of_office", "auto_reply_received"] as const) {
      vi.resetAllMocks();
      mockPromoteEvent.mockResolvedValue({ promoted: true, silverEventId: "ev-1" });

      await applyManualQualificationSideEffects({ ...baseInput, status });

      const updateCall = mockDbUpdateSet.mock.calls.find((c) => {
        const v = c[0] as Record<string, unknown>;
        return "replyClassification" in v;
      });
      expect(updateCall).toBeDefined();
      expect((updateCall![0] as Record<string, unknown>).replyClassification).toBe("neutral");
    }
  });
});
