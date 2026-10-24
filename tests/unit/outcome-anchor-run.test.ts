import { describe, it, expect, vi, beforeEach } from "vitest";

const execute = vi.fn();
vi.mock("../../src/db", () => ({ db: { execute: (...a: unknown[]) => execute(...a) } }));
const createRun = vi.fn();
const updateRun = vi.fn();
vi.mock("../../src/lib/runs-client", () => ({
  createRun: (...a: unknown[]) => createRun(...a),
  updateRun: (...a: unknown[]) => updateRun(...a),
}));

import { outcomeAnchorRun, resetOutcomeAnchorRunMemo } from "../../src/lib/outcome-anchor-run";

const pgResult = (rows: unknown[]) => ({ rows, rowCount: rows.length, command: "SELECT", fields: [] });
const ORG = "06ffd332-0000-4000-8000-000000000001";
const RUN = "4c8b7a1e-1111-4222-8333-444455556666";

describe("outcomeAnchorRun: one spend-free run per (task, org, UTC day) (LEDGER-HISTORY-1024)", () => {
  beforeEach(() => {
    execute.mockReset();
    createRun.mockReset().mockResolvedValue({ id: RUN });
    updateRun.mockReset().mockResolvedValue({});
    resetOutcomeAnchorRunMemo();
  });

  it("creates the day's run once, closes it at creation (no cost, no child) and stores it", async () => {
    execute.mockResolvedValueOnce(pgResult([])).mockResolvedValueOnce(pgResult([{ run_id: RUN }]));
    expect(await outcomeAnchorRun("website-visit", ORG, "2026-06-12")).toBe(RUN);
    expect(createRun.mock.calls[0][0]).toEqual({ serviceName: "instantly-service", taskName: "website-visit" });
    expect(updateRun).toHaveBeenCalledWith(RUN, "completed", expect.objectContaining({ orgId: ORG }));
    expect(await outcomeAnchorRun("website-visit", ORG, "2026-06-12")).toBe(RUN);
    expect(execute).toHaveBeenCalledTimes(2);
  });

  it("reuses a stored run", async () => {
    execute.mockResolvedValueOnce(pgResult([{ run_id: RUN }]));
    expect(await outcomeAnchorRun("website-visit", ORG, "2026-06-13")).toBe(RUN);
    expect(createRun).not.toHaveBeenCalled();
  });

  it("a concurrent writer's run wins the conflict", async () => {
    execute
      .mockResolvedValueOnce(pgResult([]))
      .mockResolvedValueOnce(pgResult([]))
      .mockResolvedValueOnce(pgResult([{ run_id: "theirs" }]));
    expect(await outcomeAnchorRun("website-visit", ORG, "2026-06-14")).toBe("theirs");
  });
});
