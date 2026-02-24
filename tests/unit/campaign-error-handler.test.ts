import { describe, it, expect, vi, beforeEach } from "vitest";

// Mock DB
const mockDbWhere = vi.fn();
const mockDbSetWhere = vi.fn();
const mockDbInsertValues = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    select: () => ({
      from: () => ({ where: mockDbWhere }),
    }),
    update: () => ({
      set: (v: unknown) => ({
        where: () => {
          mockDbSetWhere(v);
          return Promise.resolve([{}]);
        },
      }),
    }),
    insert: () => ({
      values: (v: unknown) => {
        mockDbInsertValues(v);
        return Promise.resolve();
      },
    }),
  },
}));

vi.mock("../../src/db/schema", () => ({
  instantlyCampaigns: {
    id: "id",
    instantlyCampaignId: "instantly_campaign_id",
    status: "status",
  },
  sequenceCosts: {
    id: "id",
    campaignId: "campaign_id",
    leadEmail: "lead_email",
    status: "status",
  },
}));

// Mock runs-client
const mockUpdateRun = vi.fn();
const mockUpdateCostStatus = vi.fn();

vi.mock("../../src/lib/runs-client", () => ({
  updateRun: (...args: unknown[]) => mockUpdateRun(...args),
  updateCostStatus: (...args: unknown[]) => mockUpdateCostStatus(...args),
}));

// Mock email-client
const mockSendEmail = vi.fn();

vi.mock("../../src/lib/email-client", () => ({
  sendEmail: (...args: unknown[]) => mockSendEmail(...args),
}));

import { handleCampaignError } from "../../src/lib/campaign-error-handler";

const baseCampaign = {
  id: "db-1",
  campaignId: "camp-1",
  leadEmail: "lead@test.com",
  instantlyCampaignId: "inst-camp-1",
  name: "Campaign camp-1",
  status: "active",
  runId: "run-1",
  metadata: null,
};

describe("handleCampaignError", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockUpdateRun.mockResolvedValue({});
    mockUpdateCostStatus.mockResolvedValue({});
    mockSendEmail.mockResolvedValue({});
  });

  it("should update DB status to error and store reason in metadata", async () => {
    mockDbWhere.mockResolvedValueOnce([baseCampaign]); // campaign lookup
    mockDbWhere.mockResolvedValueOnce([]); // no provisioned costs

    await handleCampaignError("inst-camp-1", "account disconnected");

    expect(mockDbSetWhere).toHaveBeenCalledWith(
      expect.objectContaining({
        status: "error",
        metadata: { errorReason: "account disconnected" },
      }),
    );
  });

  it("should cancel all remaining provisioned costs and fail step runs", async () => {
    mockDbWhere.mockResolvedValueOnce([baseCampaign]); // campaign lookup
    mockDbWhere.mockResolvedValueOnce([
      { id: "sc-2", step: 2, runId: "step-run-2", costId: "cost-2", status: "provisioned" },
      { id: "sc-3", step: 3, runId: "step-run-3", costId: "cost-3", status: "provisioned" },
    ]); // provisioned costs

    await handleCampaignError("inst-camp-1", "account disconnected");

    expect(mockUpdateCostStatus).toHaveBeenCalledTimes(2);
    expect(mockUpdateCostStatus).toHaveBeenCalledWith("step-run-2", "cost-2", "cancelled");
    expect(mockUpdateCostStatus).toHaveBeenCalledWith("step-run-3", "cost-3", "cancelled");
    // Step runs should also be failed
    expect(mockUpdateRun).toHaveBeenCalledWith("step-run-2", "failed", "account disconnected");
    expect(mockUpdateRun).toHaveBeenCalledWith("step-run-3", "failed", "account disconnected");
  });

  it("should mark the parent run as failed", async () => {
    mockDbWhere.mockResolvedValueOnce([baseCampaign]);
    mockDbWhere.mockResolvedValueOnce([]); // no costs

    await handleCampaignError("inst-camp-1", "account disconnected");

    expect(mockUpdateRun).toHaveBeenCalledWith("run-1", "failed", "account disconnected");
  });

  it("should throw if parent updateRun fails", async () => {
    mockDbWhere.mockResolvedValueOnce([baseCampaign]);
    mockDbWhere.mockResolvedValueOnce([]); // no costs
    mockUpdateRun.mockRejectedValue(new Error("runs-service down"));

    await expect(
      handleCampaignError("inst-camp-1", "account disconnected"),
    ).rejects.toThrow("runs-service down");
  });

  it("should send admin notification email", async () => {
    mockDbWhere.mockResolvedValueOnce([baseCampaign]);
    mockDbWhere.mockResolvedValueOnce([]);

    await handleCampaignError("inst-camp-1", "account disconnected");

    expect(mockSendEmail).toHaveBeenCalledWith(
      expect.objectContaining({
        appId: "instantly-service",
        eventType: "campaign-error",
        metadata: expect.objectContaining({
          campaignId: "camp-1",
          leadEmail: "lead@test.com",
          instantlyCampaignId: "inst-camp-1",
          errorReason: "account disconnected",
        }),
      }),
    );
  });

  it("should NOT throw if notification email fails", async () => {
    mockDbWhere.mockResolvedValueOnce([baseCampaign]);
    mockDbWhere.mockResolvedValueOnce([]);
    mockSendEmail.mockRejectedValue(new Error("email service down"));

    // Should not throw despite email failure
    await handleCampaignError("inst-camp-1", "account disconnected");

    // Run should still be marked as failed
    expect(mockUpdateRun).toHaveBeenCalledWith("run-1", "failed", "account disconnected");
  });

  it("should skip if campaign already in error state", async () => {
    mockDbWhere.mockResolvedValueOnce([{ ...baseCampaign, status: "error" }]);

    await handleCampaignError("inst-camp-1", "account disconnected");

    expect(mockUpdateRun).not.toHaveBeenCalled();
    expect(mockUpdateCostStatus).not.toHaveBeenCalled();
    expect(mockSendEmail).not.toHaveBeenCalled();
  });

  it("should skip if campaign not found", async () => {
    mockDbWhere.mockResolvedValueOnce([]);

    await handleCampaignError("inst-camp-unknown", "account disconnected");

    expect(mockUpdateRun).not.toHaveBeenCalled();
    expect(mockUpdateCostStatus).not.toHaveBeenCalled();
  });

  it("should cancel both actual and provisioned costs, and fail all step runs", async () => {
    // Step 1 run is already completed — updateRun may reject, should not throw
    mockUpdateRun
      .mockResolvedValueOnce({}) // step-run-1 fail attempt (may fail in prod, but mock accepts)
      .mockResolvedValueOnce({}) // step-run-2
      .mockResolvedValueOnce({}) // step-run-3
      .mockResolvedValueOnce({}); // parent run

    mockDbWhere.mockResolvedValueOnce([baseCampaign]); // campaign lookup
    mockDbWhere.mockResolvedValueOnce([
      { id: "sc-1", step: 1, runId: "step-run-1", costId: "cost-1", status: "actual" },
      { id: "sc-2", step: 2, runId: "step-run-2", costId: "cost-2", status: "provisioned" },
      { id: "sc-3", step: 3, runId: "step-run-3", costId: "cost-3", status: "provisioned" },
    ]); // mixed costs

    await handleCampaignError("inst-camp-1", "email gateway fail");

    expect(mockUpdateCostStatus).toHaveBeenCalledTimes(3);
    expect(mockUpdateCostStatus).toHaveBeenCalledWith("step-run-1", "cost-1", "cancelled");
    expect(mockUpdateCostStatus).toHaveBeenCalledWith("step-run-2", "cost-2", "cancelled");
    expect(mockUpdateCostStatus).toHaveBeenCalledWith("step-run-3", "cost-3", "cancelled");
    // All step runs should be failed (including step 1)
    expect(mockUpdateRun).toHaveBeenCalledWith("step-run-1", "failed", "email gateway fail");
    expect(mockUpdateRun).toHaveBeenCalledWith("step-run-2", "failed", "email gateway fail");
    expect(mockUpdateRun).toHaveBeenCalledWith("step-run-3", "failed", "email gateway fail");
    // Plus the parent run
    expect(mockUpdateRun).toHaveBeenCalledWith("run-1", "failed", "email gateway fail");
  });

  it("should not throw when step 1 run fail is rejected (already completed)", async () => {
    mockUpdateRun
      .mockRejectedValueOnce(new Error("cannot transition completed to failed")) // step-run-1
      .mockResolvedValueOnce({}) // parent run

    mockDbWhere.mockResolvedValueOnce([baseCampaign]); // campaign lookup
    mockDbWhere.mockResolvedValueOnce([
      { id: "sc-1", step: 1, runId: "step-run-1", costId: "cost-1", status: "actual" },
    ]);

    // Should not throw despite step run fail rejection
    await handleCampaignError("inst-camp-1", "email gateway fail");

    // Cost should still be cancelled
    expect(mockUpdateCostStatus).toHaveBeenCalledWith("step-run-1", "cost-1", "cancelled");
    // Parent run should still be failed
    expect(mockUpdateRun).toHaveBeenCalledWith("run-1", "failed", "email gateway fail");
  });

  it("should handle campaign with no runId", async () => {
    mockDbWhere.mockResolvedValueOnce([{ ...baseCampaign, runId: null }]);
    mockDbWhere.mockResolvedValueOnce([]);

    await handleCampaignError("inst-camp-1", "account disconnected");

    // DB status should still be updated
    expect(mockDbSetWhere).toHaveBeenCalledWith(
      expect.objectContaining({ status: "error" }),
    );
    // But no run update
    expect(mockUpdateRun).not.toHaveBeenCalled();
    // Notification still sent
    expect(mockSendEmail).toHaveBeenCalled();
  });
});
