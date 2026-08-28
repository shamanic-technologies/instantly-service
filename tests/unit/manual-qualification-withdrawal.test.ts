import { describe, it, expect, vi, beforeEach } from "vitest";

// ─── Mocks ───────────────────────────────────────────────────────────────────

const mockUpdateSet = vi.fn();
const mockUpdateWhere = vi.fn();
const mockExecute = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    update: () => ({
      set: (v: unknown) => {
        mockUpdateSet(v);
        return {
          where: (w: unknown) => {
            mockUpdateWhere(w);
            return Promise.resolve([{}]);
          },
        };
      },
    }),
    execute: (q: unknown) => mockExecute(q),
  },
}));

vi.mock("../../src/db/schema", () => ({
  instantlyCampaigns: { instantlyCampaignId: "instantly_campaign_id" },
  instantlyEvents: {
    sourceRowId: "source_row_id",
    source: "source",
    campaignId: "campaign_id",
    leadEmail: "lead_email",
    withdrawnAt: "withdrawn_at",
  },
  instantlyManualQualificationsRaw: {
    orgId: "org_id",
    instantlyCampaignId: "instantly_campaign_id",
    leadEmail: "lead_email",
    qualifiedAt: "qualified_at",
    id: "id",
  },
  instantlyManualQualificationWithdrawals: { qualificationId: "qualification_id", id: "id" },
}));

vi.mock("../../src/lib/silver-promote", () => ({ promoteEvent: vi.fn() }));

const mockRefreshLeadStatusCurrent = vi.fn();
vi.mock("../../src/lib/status-gold", () => ({
  refreshLeadStatusCurrent: (...args: unknown[]) => mockRefreshLeadStatusCurrent(...args),
}));

vi.mock("../../src/lib/key-client", () => ({ resolveInstantlyApiKey: vi.fn() }));
vi.mock("../../src/lib/instantly-client", () => ({ updateCampaignStatus: vi.fn() }));

import { applyManualQualificationWithdrawalSideEffects } from "../../src/lib/manual-qualifications";

const INPUT = {
  bronzeRowId: "bronze-row-1",
  instantlyCampaignId: "inst-camp-1",
  leadEmail: "lead@test.com",
  withdrawnAt: new Date("2026-08-28T10:00:00.000Z"),
};

beforeEach(() => {
  vi.resetAllMocks();
  mockRefreshLeadStatusCurrent.mockResolvedValue(undefined);
  mockExecute.mockResolvedValue({ rows: [] });
});

describe("applyManualQualificationWithdrawalSideEffects", () => {
  it("marks the statement's own silver mirror event withdrawn — it never deletes it", async () => {
    await applyManualQualificationWithdrawalSideEffects(INPUT);

    // The row survives: the only write to it sets withdrawn_at.
    expect(mockUpdateSet).toHaveBeenCalledWith({ withdrawnAt: INPUT.withdrawnAt });
  });

  it("releases the manual pin so the automatic classification takes over again", async () => {
    await applyManualQualificationWithdrawalSideEffects(INPUT);

    const campaignPatch = mockUpdateSet.mock.calls
      .map((call) => call[0] as Record<string, unknown>)
      .find((patch) => "replyClassificationSource" in patch);

    expect(campaignPatch?.replyClassificationSource).toBe("auto");
  });

  it("falls back to NOTHING AT ALL when no automatic classification is left", async () => {
    mockExecute.mockResolvedValue({ rows: [] });

    await applyManualQualificationWithdrawalSideEffects(INPUT);

    const campaignPatch = mockUpdateSet.mock.calls
      .map((call) => call[0] as Record<string, unknown>)
      .find((patch) => "replyClassificationSource" in patch);

    // Null, not a fabricated neutral — nobody has said anything about this reply.
    expect(campaignPatch?.replyClassification).toBeNull();
  });

  it("restores the surviving automatic classification when one is left", async () => {
    mockExecute.mockResolvedValue({ rows: [{ event_type: "lead_not_interested" }] });

    await applyManualQualificationWithdrawalSideEffects(INPUT);

    const campaignPatch = mockUpdateSet.mock.calls
      .map((call) => call[0] as Record<string, unknown>)
      .find((patch) => "replyClassificationSource" in patch);

    expect(campaignPatch?.replyClassification).toBe("negative");
  });

  it("reads the surviving classification with the SAME latest-wins ordering as gold, skipping withdrawn rows", async () => {
    await applyManualQualificationWithdrawalSideEffects(INPUT);

    const query = mockExecute.mock.calls[0]?.[0] as { queryChunks: unknown[] };
    const text = JSON.stringify(query);
    // A withdrawn event must not be able to come back as "the automatic answer".
    expect(text).toContain("withdrawn_at IS NULL");
    expect(text).toContain("e.timestamp DESC");
    expect(text).toContain("(e.source = 'manual') DESC");
  });

  it("refreshes the gold status row so the read path converges immediately", async () => {
    await applyManualQualificationWithdrawalSideEffects(INPUT);

    expect(mockRefreshLeadStatusCurrent).toHaveBeenCalledWith("inst-camp-1", "lead@test.com");
  });

  it("fails loud — a withdrawal that cannot release the pin does not report success", async () => {
    mockExecute.mockRejectedValue(new Error("db down"));

    await expect(applyManualQualificationWithdrawalSideEffects(INPUT)).rejects.toThrow("db down");
  });
});
