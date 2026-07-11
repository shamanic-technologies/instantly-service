import { describe, it, expect, vi, beforeEach } from "vitest";

// ─── Mocks ───────────────────────────────────────────────────────────────────

const mockDbExecute = vi.fn();
const mockDbUpdateWhere = vi.fn();
const mockDbUpdateSet = vi.fn(() => ({ where: mockDbUpdateWhere }));

vi.mock("../../src/db", () => ({
  db: {
    execute: (...args: unknown[]) => mockDbExecute(...args),
    update: () => ({ set: mockDbUpdateSet }),
  },
}));

const mockUpdateCostStatus = vi.fn();
const mockIsRunGoneError = vi.fn();

vi.mock("../../src/lib/runs-client", () => ({
  updateCostStatus: (...args: unknown[]) => mockUpdateCostStatus(...args),
  isRunGoneError: (...args: unknown[]) => mockIsRunGoneError(...args),
}));

import {
  classifyHold,
  reconcileProvisionedHolds,
  selectHoldActions,
} from "../../src/lib/reconcile-provisioned-holds";

// SQL evidence row shape returned by selectHoldActions' db.execute.
function evidenceRow(overrides: Record<string, unknown> = {}) {
  return {
    id: "sc-1",
    runId: "run-1",
    costId: "cost-1",
    orgId: "org-1",
    userId: "user-1",
    has_sent: false,
    lead_stopped: false,
    camp_terminal: false,
    ...overrides,
  };
}

// ─── Pure classifier ─────────────────────────────────────────────────────────

describe("classifyHold", () => {
  it("actualizes when the step has a real email_sent (even if lead later stopped)", () => {
    expect(classifyHold({ hasSent: true, leadStopped: false, campTerminal: false })).toBe("actualize");
    // hasSent wins — the email already dispatched, it is billable.
    expect(classifyHold({ hasSent: true, leadStopped: true, campTerminal: true })).toBe("actualize");
  });

  it("cancels a terminal send that never sent this step", () => {
    expect(classifyHold({ hasSent: false, leadStopped: true, campTerminal: false })).toBe("cancel");
    expect(classifyHold({ hasSent: false, leadStopped: false, campTerminal: true })).toBe("cancel");
  });

  it("skips an in-flight hold (no send, not terminal) — never cancels on assumption", () => {
    expect(classifyHold({ hasSent: false, leadStopped: false, campTerminal: false })).toBe("skip");
  });
});

// ─── selectHoldActions ───────────────────────────────────────────────────────

describe("selectHoldActions", () => {
  beforeEach(() => vi.resetAllMocks());

  it("maps each evidence row to its action", async () => {
    mockDbExecute.mockResolvedValue({
      rows: [
        evidenceRow({ id: "a", has_sent: true }),
        evidenceRow({ id: "b", lead_stopped: true }),
        evidenceRow({ id: "c", camp_terminal: true }),
        evidenceRow({ id: "d" }),
      ],
    });

    const rows = await selectHoldActions();

    expect(rows.map((r) => [r.id, r.action])).toEqual([
      ["a", "actualize"],
      ["b", "cancel"],
      ["c", "cancel"],
      ["d", "skip"],
    ]);
  });

  it("gates on real (inferred=false) send/stop evidence in the SQL", async () => {
    mockDbExecute.mockResolvedValue({ rows: [] });
    await selectHoldActions();
    const text = JSON.stringify(mockDbExecute.mock.calls[0][0]);
    expect(text).toContain("email_sent");
    expect(text).toContain("inferred");
    expect(text).toContain("provisioned");
    expect(text).toContain("reply_received");
  });

  it("handles array-shaped db.execute result (no .rows wrapper)", async () => {
    mockDbExecute.mockResolvedValue([evidenceRow({ has_sent: true })]);
    const rows = await selectHoldActions(50);
    expect(rows).toHaveLength(1);
    expect(rows[0].action).toBe("actualize");
  });
});

// ─── reconcileProvisionedHolds ───────────────────────────────────────────────

describe("reconcileProvisionedHolds", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockUpdateCostStatus.mockResolvedValue({});
    mockIsRunGoneError.mockReturnValue(false);
  });

  it("dry-run (default) reports the plan and mutates nothing", async () => {
    mockDbExecute.mockResolvedValue({
      rows: [
        evidenceRow({ id: "a", has_sent: true }),
        evidenceRow({ id: "b", lead_stopped: true }),
        evidenceRow({ id: "c" }),
      ],
    });

    const summary = await reconcileProvisionedHolds();

    expect(summary).toMatchObject({
      holdsClassified: 3,
      planActualize: 1,
      planCancel: 1,
      planSkip: 1,
      actualized: 0,
      cancelled: 0,
      transient: 0,
      dryRun: true,
    });
    expect(mockUpdateCostStatus).not.toHaveBeenCalled();
    expect(mockDbUpdateWhere).not.toHaveBeenCalled();
  });

  it("commit actualizes sent holds and cancels terminal holds via each hold's own run/cost id", async () => {
    mockDbExecute.mockResolvedValue({
      rows: [
        evidenceRow({ id: "a", runId: "run-a", costId: "cost-a", has_sent: true }),
        evidenceRow({ id: "b", runId: "run-b", costId: "cost-b", lead_stopped: true }),
        evidenceRow({ id: "c" }), // skip
      ],
    });

    const summary = await reconcileProvisionedHolds({ dryRun: false });

    expect(summary).toMatchObject({ actualized: 1, cancelled: 1, transient: 0, failed: 0, dryRun: false });
    expect(mockUpdateCostStatus).toHaveBeenCalledWith("run-a", "cost-a", "actual", expect.any(Object));
    expect(mockUpdateCostStatus).toHaveBeenCalledWith("run-b", "cost-b", "cancelled", expect.any(Object));
    // skip → no PATCH for hold c
    expect(mockUpdateCostStatus).toHaveBeenCalledTimes(2);
    expect(mockDbUpdateSet).toHaveBeenCalledWith(expect.objectContaining({ status: "actual" }));
    expect(mockDbUpdateSet).toHaveBeenCalledWith(expect.objectContaining({ status: "cancelled" }));
  });

  it("a gone-run 404 on an actualize flips the hold to cancelled locally (unbillable), not transient", async () => {
    mockDbExecute.mockResolvedValue({
      rows: [evidenceRow({ id: "a", has_sent: true })],
    });
    mockUpdateCostStatus.mockRejectedValueOnce(new Error("runs-service PATCH failed: 404 - not found"));
    mockIsRunGoneError.mockReturnValue(true);

    const summary = await reconcileProvisionedHolds({ dryRun: false });

    expect(summary).toMatchObject({ actualized: 0, cancelled: 1, transient: 0 });
    expect(mockDbUpdateSet).toHaveBeenCalledWith(expect.objectContaining({ status: "cancelled" }));
  });

  it("a transient error leaves the hold provisioned (retried next run)", async () => {
    mockDbExecute.mockResolvedValue({
      rows: [evidenceRow({ id: "a", has_sent: true })],
    });
    mockUpdateCostStatus.mockRejectedValueOnce(new Error("runs-service PATCH failed: 503 - unavailable"));
    mockIsRunGoneError.mockReturnValue(false);

    const summary = await reconcileProvisionedHolds({ dryRun: false });

    expect(summary).toMatchObject({ actualized: 0, cancelled: 0, transient: 1 });
    // No local flip on transient.
    expect(mockDbUpdateWhere).not.toHaveBeenCalled();
  });

  it("no-ops cleanly when nothing is provisioned", async () => {
    mockDbExecute.mockResolvedValue({ rows: [] });
    const summary = await reconcileProvisionedHolds({ dryRun: false });
    expect(summary).toMatchObject({ holdsClassified: 0, actualized: 0, cancelled: 0 });
    expect(mockUpdateCostStatus).not.toHaveBeenCalled();
  });
});
