import { describe, it, expect, vi, beforeEach } from "vitest";

// ─── Mocks ───────────────────────────────────────────────────────────────────

const mockDbExecute = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    execute: (...args: unknown[]) => mockDbExecute(...args),
  },
}));

const mockCancelRemainingProvisions = vi.fn();

vi.mock("../../src/lib/silver-promote", () => ({
  cancelRemainingProvisions: (...args: unknown[]) => mockCancelRemainingProvisions(...args),
}));

import { refundStrandedHolds } from "../../src/lib/refund-stranded-holds";

function strandedRow(overrides: Record<string, unknown> = {}) {
  return {
    campaignId: "camp-1",
    instantlyCampaignId: "inst-1",
    orgId: "org-1",
    userId: "user-1",
    leadEmail: "lead@example.com",
    ...overrides,
  };
}

describe("refundStrandedHolds", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockCancelRemainingProvisions.mockResolvedValue(undefined);
  });

  it("cancels remaining provisions for every stranded terminal campaign", async () => {
    mockDbExecute.mockResolvedValue({
      rows: [
        strandedRow({ campaignId: "camp-1", leadEmail: "a@x.com" }),
        strandedRow({ campaignId: "camp-2", leadEmail: "b@x.com", instantlyCampaignId: "inst-2" }),
      ],
    });

    const summary = await refundStrandedHolds();

    expect(summary).toEqual({ campaignsProcessed: 2, campaignsFailed: 0 });
    expect(mockCancelRemainingProvisions).toHaveBeenCalledTimes(2);
    expect(mockCancelRemainingProvisions).toHaveBeenCalledWith(
      expect.objectContaining({ campaignId: "camp-1", instantlyCampaignId: "inst-1", runId: null }),
      "a@x.com",
    );
    expect(mockCancelRemainingProvisions).toHaveBeenCalledWith(
      expect.objectContaining({ campaignId: "camp-2", instantlyCampaignId: "inst-2" }),
      "b@x.com",
    );
  });

  it("isolates a per-campaign failure (counted, sweep continues)", async () => {
    mockDbExecute.mockResolvedValue({
      rows: [strandedRow({ campaignId: "camp-1" }), strandedRow({ campaignId: "camp-2" })],
    });
    mockCancelRemainingProvisions
      .mockRejectedValueOnce(new Error("runs-service 500"))
      .mockResolvedValueOnce(undefined);

    const summary = await refundStrandedHolds();

    expect(summary).toEqual({ campaignsProcessed: 1, campaignsFailed: 1 });
    expect(mockCancelRemainingProvisions).toHaveBeenCalledTimes(2);
  });

  it("no-ops cleanly when nothing is stranded", async () => {
    mockDbExecute.mockResolvedValue({ rows: [] });

    const summary = await refundStrandedHolds();

    expect(summary).toEqual({ campaignsProcessed: 0, campaignsFailed: 0 });
    expect(mockCancelRemainingProvisions).not.toHaveBeenCalled();
  });

  it("handles array-shaped db.execute result (no .rows wrapper)", async () => {
    mockDbExecute.mockResolvedValue([strandedRow()]);

    const summary = await refundStrandedHolds({ limit: 100 });

    expect(summary.campaignsProcessed).toBe(1);
  });
});
