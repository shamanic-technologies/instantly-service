import { describe, it, expect, vi, beforeEach } from "vitest";

// ─── Mocks ───────────────────────────────────────────────────────────────────

const mockDbUpdate = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    update: () => ({
      set: (v: unknown) => ({
        where: () => {
          mockDbUpdate(v);
          return Promise.resolve([{}]);
        },
      }),
    }),
  },
}));

vi.mock("../../src/db/schema", () => ({
  sequenceCosts: { id: "id", status: "status", updatedAt: "updated_at" },
}));

const mockUpdateCostStatus = vi.fn();
vi.mock("../../src/lib/runs-client", () => ({
  updateCostStatus: (...args: unknown[]) => mockUpdateCostStatus(...args),
}));

const { settleHoldCost } = await import("../../src/lib/hold-settlement");

const identity = { orgId: "org-1", userId: "user-1", runId: "run-1" };

beforeEach(() => {
  vi.resetAllMocks();
  mockUpdateCostStatus.mockResolvedValue(undefined);
});

describe("settleHoldCost", () => {
  // A hold written after the Instantly spend became a fixed cost carries no
  // cost id. It is still a queue entry — the local flip is what removes the step
  // from the due set — but there is nothing to declare to runs-service.
  it("flips an UNBILLED hold locally without touching runs-service", async () => {
    await settleHoldCost({ id: "hold-1", runId: "run-1", costId: null }, "actual", identity);

    expect(mockUpdateCostStatus).not.toHaveBeenCalled();
    expect(mockDbUpdate).toHaveBeenCalledTimes(1);
    expect(mockDbUpdate.mock.calls[0][0]).toMatchObject({ status: "actual" });
  });

  it("cancels an UNBILLED hold locally without touching runs-service", async () => {
    await settleHoldCost({ id: "hold-1", runId: "run-1", costId: null }, "cancelled", identity);

    expect(mockUpdateCostStatus).not.toHaveBeenCalled();
    expect(mockDbUpdate.mock.calls[0][0]).toMatchObject({ status: "cancelled" });
  });

  // Historical rows declared before the cutover keep their cost id and must keep
  // resolving against runs-service, or spend already reserved never settles.
  it("declares a BILLED hold to runs-service, then flips it locally", async () => {
    await settleHoldCost({ id: "hold-2", runId: "run-9", costId: "cost-9" }, "actual", identity);

    expect(mockUpdateCostStatus).toHaveBeenCalledWith("run-9", "cost-9", "actual", identity);
    expect(mockDbUpdate.mock.calls[0][0]).toMatchObject({ status: "actual" });
  });

  // Fail loud: the throw must reach the caller so its own error handling runs
  // (a terminal run-gone 404 cancels locally, a transient error leaves the hold
  // provisioned for the next sweep). Flipping the row first would mark a hold
  // settled that runs-service never accepted.
  it("propagates a runs-service failure and leaves the local row untouched", async () => {
    mockUpdateCostStatus.mockRejectedValue(new Error("runs-service PATCH failed: 500"));

    await expect(
      settleHoldCost({ id: "hold-3", runId: "run-9", costId: "cost-9" }, "actual", identity),
    ).rejects.toThrow("runs-service PATCH failed: 500");

    expect(mockDbUpdate).not.toHaveBeenCalled();
  });
});
