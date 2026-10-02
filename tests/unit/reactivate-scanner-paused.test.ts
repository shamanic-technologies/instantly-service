import { describe, it, expect, vi, beforeEach } from "vitest";

const mockDbExecute = vi.fn();
const mockTxExecute = vi.fn();
const mockRefreshGold = vi.fn();
const mockAnnounce = vi.fn();
const mockFindOptOut = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    execute: (...args: unknown[]) => mockDbExecute(...args),
    transaction: async (fn: (tx: unknown) => Promise<unknown>) =>
      fn({ execute: (...args: unknown[]) => mockTxExecute(...args) }),
  },
}));
vi.mock("../../src/lib/status-gold", () => ({
  refreshLeadStatusCurrent: (...args: unknown[]) => mockRefreshGold(...args),
}));
vi.mock("../../src/lib/evidence-changed", () => ({
  announceEvidenceChanged: (...args: unknown[]) => mockAnnounce(...args),
}));
vi.mock("../../src/lib/lead-optouts", () => ({
  findStandingOptOut: (...args: unknown[]) => mockFindOptOut(...args),
}));

const { reactivateScannerPausedSequences } = await import(
  "../../src/lib/self-send/reactivate-scanner-paused"
);

function pgResult(rows: Record<string, unknown>[]) {
  return { command: "SELECT", rowCount: rows.length, oid: 0, fields: [], rows };
}

function sqlText(node: unknown): string {
  if (typeof node === "string") return node;
  if (Array.isArray(node)) return node.map(sqlText).join("");
  if (node && typeof node === "object") {
    const obj = node as Record<string, unknown>;
    if (Array.isArray(obj.queryChunks)) return sqlText(obj.queryChunks);
    if (typeof obj.value === "string") return obj.value;
    if (Array.isArray(obj.value)) return sqlText(obj.value);
  }
  return "";
}

function candidate(over: Record<string, unknown> = {}) {
  return {
    instantly_campaign_id: "self:olive-1",
    org_id: "org-olive",
    lead_email: "bhemelaar@flowtraders.com",
    created_at: "2026-10-01T06:14:00.000Z",
    reopen_steps: [2, 3],
    billed: false,
    ...over,
  };
}

beforeEach(() => {
  vi.resetAllMocks();
  mockFindOptOut.mockResolvedValue(null);
  mockAnnounce.mockResolvedValue(undefined);
  mockRefreshGold.mockResolvedValue(undefined);
});

describe("reactivateScannerPausedSequences — candidate selection", () => {
  it("only reads self-send rows a scanner stopped, with every other stop reason excluded", async () => {
    mockDbExecute.mockResolvedValue(pgResult([]));
    await reactivateScannerPausedSequences();

    const text = sqlText(mockDbExecute.mock.calls[0][0]);
    expect(text).toContain("c.status = 'paused'");
    expect(text).toContain("h.classification = 'scanner'");
    // A click still standing as a person's keeps its stop.
    expect(text).toContain("o.classification IN ('human', 'legacy')");
    // Anyone who answered, was hand-qualified, or is held by a newer sequence.
    expect(text).toContain("'reply_received', 'auto_reply_received', 'email_bounced', 'lead_unsubscribed'");
    expect(text).toContain("instantly_manual_qualifications_raw");
    expect(text).toContain("o.created_at > c.created_at");
    expect(text).toContain("stalledFirstEmailClosed");
    // Only the holds the stop took away: cancelled after the scanner click, never sent.
    expect(text).toContain("sc.updated_at >= k.first_at");
    expect(text).toContain("e.event_type = 'email_sent'");
  });
});

describe("reactivateScannerPausedSequences — dry run", () => {
  it("counts and writes NOTHING", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([candidate(), candidate({ instantly_campaign_id: "self:olive-2", created_at: "2026-09-12T00:00:00Z" })]));

    const summary = await reactivateScannerPausedSequences();

    expect(summary).toMatchObject({ dryRun: true, candidates: 2, reactivated: 2, stepsReopened: 4 });
    expect(summary.byMonth).toEqual({ "2026-10": 1, "2026-09": 1 });
    expect(mockTxExecute).not.toHaveBeenCalled();
    expect(mockRefreshGold).not.toHaveBeenCalled();
    expect(mockAnnounce).not.toHaveBeenCalled();
  });
});

describe("reactivateScannerPausedSequences — commit", () => {
  it("re-activates the row and puts its unbilled stolen holds back in the queue", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([candidate()]));
    mockTxExecute.mockResolvedValueOnce(pgResult([{ id: "row-1" }]));
    mockTxExecute.mockResolvedValue(pgResult([]));

    const summary = await reactivateScannerPausedSequences({ dryRun: false });

    expect(summary).toMatchObject({ reactivated: 1, stepsReopened: 2, failed: 0 });
    const campaignUpdate = sqlText(mockTxExecute.mock.calls[0][0]);
    expect(campaignUpdate).toContain("SET status = 'active'");
    expect(campaignUpdate).toContain("reactivatedAfterScannerClick");
    expect(campaignUpdate).toContain("AND status = 'paused'");
    const holdUpdate = sqlText(mockTxExecute.mock.calls[1][0]);
    expect(holdUpdate).toContain("SET status = 'provisioned'");
    expect(holdUpdate).toContain("cost_id IS NULL");
    expect(holdUpdate).toContain("domain_cost_id IS NULL");
    expect(mockRefreshGold).toHaveBeenCalledWith("self:olive-1", "bhemelaar@flowtraders.com");
    expect(mockAnnounce).toHaveBeenCalledWith("org-olive", ["bhemelaar@flowtraders.com"], "scanner_click_stop_reverted");
  });

  it("leaves a sequence with a BILLED stolen hold paused: its charge was cancelled at runs-service too", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([candidate({ billed: true })]));

    const summary = await reactivateScannerPausedSequences({ dryRun: false });

    expect(summary).toMatchObject({ reactivated: 0, skippedBilled: 1 });
    expect(mockTxExecute).not.toHaveBeenCalled();
  });

  it("never resumes someone with a standing opt-out", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([candidate()]));
    mockFindOptOut.mockResolvedValueOnce({ id: "opt-1" });

    const summary = await reactivateScannerPausedSequences({ dryRun: false });

    expect(summary).toMatchObject({ reactivated: 0, skippedOptedOut: 1 });
    expect(mockTxExecute).not.toHaveBeenCalled();
  });

  it("does not reopen holds when the row left `paused` between the read and the write", async () => {
    mockDbExecute.mockResolvedValueOnce(pgResult([candidate()]));
    mockTxExecute.mockResolvedValueOnce(pgResult([]));

    const summary = await reactivateScannerPausedSequences({ dryRun: false });

    expect(summary).toMatchObject({ reactivated: 0, skippedRaced: 1 });
    expect(mockTxExecute).toHaveBeenCalledTimes(1);
    expect(mockRefreshGold).not.toHaveBeenCalled();
  });
});
