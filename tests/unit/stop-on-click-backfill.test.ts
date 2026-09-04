import { describe, it, expect, vi, beforeEach } from "vitest";

const mockDbExecute = vi.fn();
const mockMaybeStopOnClickForFunnel = vi.fn();

vi.mock("../../src/db", () => ({ db: { execute: (...a: unknown[]) => mockDbExecute(...a) } }));
vi.mock("../../src/lib/stop-on-click", () => ({
  maybeStopOnClickForFunnel: (...a: unknown[]) => mockMaybeStopOnClickForFunnel(...a),
}));

const { backfillStopOnClick, selectClickedActiveCampaigns } = await import(
  "../../src/lib/stop-on-click-backfill"
);

/**
 * node-postgres resolves `db.execute` to a QueryResult OBJECT, never a bare array — mocking the
 * postgres.js shape would let a `rows is not iterable` bug pass the whole suite.
 */
function pgResult(rows: unknown[]) {
  return { command: "SELECT", rowCount: rows.length, oid: 0, fields: [], rows };
}

const ROW = {
  campaignId: "camp-1",
  instantlyCampaignId: "inst-1",
  orgId: "org-1",
  userId: "user-1",
  runId: "run-1",
  leadEmail: "lead@x.com",
};

beforeEach(() => {
  vi.resetAllMocks();
  mockMaybeStopOnClickForFunnel.mockResolvedValue(undefined);
});

describe("selectClickedActiveCampaigns", () => {
  it("selects live, org-scoped sequences with a REAL click", async () => {
    mockDbExecute.mockResolvedValue(pgResult([ROW]));

    await selectClickedActiveCampaigns();

    const sqlText = JSON.stringify(mockDbExecute.mock.calls[0]![0]);
    // An inferred click is a synthetic predecessor projected from a downstream event — pausing on
    // one would act on a click nobody made.
    expect(sqlText).toContain("e.inferred = false");
    expect(sqlText).toContain("email_link_clicked");
    // Only a live sequence can still be sending, and only a caller campaign runs a funnel.
    expect(sqlText).toContain("c.status = 'active'");
    expect(sqlText).toContain("c.campaign_id IS NOT NULL");
  });

  it("unwraps the QueryResult rather than iterating the result object", async () => {
    mockDbExecute.mockResolvedValue(pgResult([ROW, { ...ROW, instantlyCampaignId: "inst-2" }]));

    await expect(selectClickedActiveCampaigns()).resolves.toHaveLength(2);
  });
});

describe("backfillStopOnClick", () => {
  // The gate lives in ONE place. The sweep re-asks the same question through the same helper — it
  // must not re-implement a funnel test that could drift from the live path.
  it("re-asks the live stop-on-click helper per lead", async () => {
    mockDbExecute.mockResolvedValue(pgResult([ROW]));

    const summary = await backfillStopOnClick();

    expect(mockMaybeStopOnClickForFunnel).toHaveBeenCalledWith(
      {
        instantlyCampaignId: "inst-1",
        campaignId: "camp-1",
        orgId: "org-1",
        userId: "user-1",
        runId: "run-1",
      },
      "lead@x.com",
    );
    expect(summary).toEqual({ candidates: 1, processed: 1, failed: 0 });
  });

  it("keeps going when one lead throws", async () => {
    mockDbExecute.mockResolvedValue(
      pgResult([ROW, { ...ROW, instantlyCampaignId: "inst-2", leadEmail: "b@x.com" }]),
    );
    mockMaybeStopOnClickForFunnel.mockRejectedValueOnce(new Error("boom"));
    const err = vi.spyOn(console, "error").mockImplementation(() => {});

    const summary = await backfillStopOnClick();

    expect(summary).toEqual({ candidates: 2, processed: 1, failed: 1 });
    err.mockRestore();
  });

  it("bounds the batch when a limit is given", async () => {
    mockDbExecute.mockResolvedValue(pgResult([]));

    await backfillStopOnClick({ limit: 10 });

    expect(JSON.stringify(mockDbExecute.mock.calls[0]![0])).toContain("LIMIT");
  });

  it("no-ops on an empty backlog", async () => {
    mockDbExecute.mockResolvedValue(pgResult([]));

    await expect(backfillStopOnClick()).resolves.toEqual({
      candidates: 0,
      processed: 0,
      failed: 0,
    });
    expect(mockMaybeStopOnClickForFunnel).not.toHaveBeenCalled();
  });
});
