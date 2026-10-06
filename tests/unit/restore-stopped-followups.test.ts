import { describe, it, expect, vi, beforeEach } from "vitest";

const mockExecute = vi.fn();
const mockInsertValues = vi.fn();
const mockCreateRun = vi.fn();
const mockUpdateRun = vi.fn();
const mockUpdateCostStatus = vi.fn();
const mockProvision = vi.fn();
const mockAuthorize = vi.fn();
const mockResolveKey = vi.fn();
const mockUpdateCampaignStatus = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    execute: (...args: unknown[]) => mockExecute(...args),
    insert: () => ({ values: (v: unknown) => Promise.resolve(mockInsertValues(v)) }),
  },
}));
vi.mock("../../src/db/schema", () => ({ sequenceCosts: {} }));
vi.mock("../../src/lib/runs-client", () => ({
  createRun: (...a: unknown[]) => mockCreateRun(...a),
  updateRun: (...a: unknown[]) => mockUpdateRun(...a),
  updateCostStatus: (...a: unknown[]) => mockUpdateCostStatus(...a),
}));
vi.mock("../../src/lib/send-costs", () => ({
  provisionStepEmailCosts: (...a: unknown[]) => mockProvision(...a),
  sendAuthorizeItems: (n: number) => [{ costName: "x", quantity: n }],
}));
vi.mock("../../src/lib/billing-client", () => ({
  authorizeCreditSpend: (...a: unknown[]) => mockAuthorize(...a),
}));
vi.mock("../../src/lib/key-client", () => ({
  resolveInstantlyApiKey: (...a: unknown[]) => mockResolveKey(...a),
}));
vi.mock("../../src/lib/instantly-client", () => ({
  updateCampaignStatus: (...a: unknown[]) => mockUpdateCampaignStatus(...a),
}));

const { restoreOne, restoreStoppedFollowups } = await import(
  "../../src/lib/restore-stopped-followups"
);

const CALLER = { method: "POST", path: "/restore-stopped-followups" };

function candidate(instantlyCampaignId: string, steps = [2, 3]) {
  return {
    rowId: "row-1",
    instantlyCampaignId,
    campaignId: "cb528e24",
    orgId: "org-1",
    userId: "user-1",
    runId: "parent-run",
    brandIds: ["brand-1"],
    leadEmail: "p@x.com",
    steps,
  };
}

let runSeq = 0;
beforeEach(() => {
  vi.resetAllMocks();
  runSeq = 0;
  mockResolveKey.mockResolvedValue({ key: "k", keySource: "platform" });
  mockCreateRun.mockImplementation(async () => ({ id: `run-${++runSeq}` }));
  mockProvision.mockImplementation(async (runId: string) => ({
    costId: `acc-${runId}`,
    domainCostId: `dom-${runId}`,
  }));
  mockAuthorize.mockResolvedValue({ sufficient: true, balance_cents: 1000, required_cents: 4 });
  mockUpdateRun.mockResolvedValue({});
  mockUpdateCostStatus.mockResolvedValue({});
  mockUpdateCampaignStatus.mockResolvedValue({});
  mockExecute.mockResolvedValue({ rows: [] });
});

describe("restoreOne — the cut follow-ups go back through the normal cost path", () => {
  it("self-send: one provisioned hold per cut step, one authorize, row back to active; no Instantly call", async () => {
    const outcome = await restoreOne(candidate("self:aaa"), CALLER);

    expect(outcome).toBe("restored");
    expect(mockProvision).toHaveBeenCalledTimes(2);
    expect(mockAuthorize).toHaveBeenCalledTimes(1);
    expect(mockAuthorize.mock.calls[0]![0]).toEqual([{ costName: "x", quantity: 2 }]);
    expect(mockInsertValues).toHaveBeenCalledTimes(2);
    expect(mockInsertValues).toHaveBeenCalledWith(
      expect.objectContaining({
        instantlyCampaignId: "self:aaa",
        step: 2,
        status: "provisioned",
        costId: "acc-run-1",
        domainCostId: "dom-run-1",
      }),
    );
    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
    // Holds written BEFORE the row turns active (the dispatcher reads active rows).
    expect(mockInsertValues.mock.invocationCallOrder[1]!).toBeLessThan(
      mockExecute.mock.invocationCallOrder[0]!,
    );
  });

  it("Instantly transport: the paused Instantly campaign is resumed", async () => {
    await restoreOne(candidate("019f9856-0000-4000-8000-000000000000"), CALLER);
    expect(mockUpdateCampaignStatus).toHaveBeenCalledWith(
      "k",
      "019f9856-0000-4000-8000-000000000000",
      "active",
    );
  });

  it("an org out of credit: holds given back, nothing queued, row stays paused", async () => {
    mockAuthorize.mockResolvedValue({ sufficient: false, balance_cents: 0, required_cents: 4 });

    const outcome = await restoreOne(candidate("self:aaa"), CALLER);

    expect(outcome).toBe("insufficient_credits");
    expect(mockUpdateCostStatus).toHaveBeenCalledTimes(4);
    expect(mockUpdateCostStatus).toHaveBeenCalledWith("run-1", "acc-run-1", "cancelled", expect.anything());
    expect(mockInsertValues).not.toHaveBeenCalled();
    expect(mockExecute).not.toHaveBeenCalled();
  });

  it("BYOK: no authorize (the org pays its vendor), still provisioned", async () => {
    mockResolveKey.mockResolvedValue({ key: "k", keySource: "org" });
    await restoreOne(candidate("self:aaa"), CALLER);
    expect(mockAuthorize).not.toHaveBeenCalled();
    expect(mockProvision).toHaveBeenCalledWith("run-1", "org", expect.anything());
  });

  it("a failed Instantly resume gives the holds back and throws", async () => {
    mockUpdateCampaignStatus.mockRejectedValue(new Error("Instantly 500"));
    await expect(restoreOne(candidate("019f9856-0000-4000-8000-000000000000"), CALLER)).rejects.toThrow(
      /Instantly 500/,
    );
    expect(mockUpdateCostStatus).toHaveBeenCalledTimes(4);
    expect(mockInsertValues).not.toHaveBeenCalled();
  });
});

describe("restoreStoppedFollowups", () => {
  it("dry-run reports the plan per campaign and writes nothing", async () => {
    mockExecute.mockResolvedValue({
      rows: [
        { ...candidate("self:a"), steps: [2, 3] },
        { ...candidate("self:b"), leadEmail: "q@x.com", steps: [3] },
      ],
    });

    const summary = await restoreStoppedFollowups({ campaignIds: ["cb528e24"], dryRun: true, caller: CALLER });

    expect(summary).toMatchObject({ dryRun: true, candidates: 2, steps: 3, byCampaign: { cb528e24: { leads: 2, steps: 3 } } });
    expect(mockCreateRun).not.toHaveBeenCalled();
    expect(mockInsertValues).not.toHaveBeenCalled();
  });
});
