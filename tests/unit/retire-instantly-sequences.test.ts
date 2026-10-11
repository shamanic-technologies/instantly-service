import { describe, it, expect, vi, beforeEach } from "vitest";

const mockExecute = vi.fn();
vi.mock("../../src/db", () => ({
  db: { execute: (...a: unknown[]) => mockExecute(...a) },
}));

const mockUpdateCampaignStatus = vi.fn();
vi.mock("../../src/lib/instantly-client", () => ({
  updateCampaignStatus: (...a: unknown[]) => mockUpdateCampaignStatus(...a),
}));

vi.mock("../../src/lib/key-client", () => ({
  resolveInstantlyApiKey: vi.fn(async () => ({ key: "org-key" })),
  resolvePlatformInstantlyApiKey: vi.fn(async () => "platform-key"),
}));

const mockCancelRemainingProvisions = vi.fn();
vi.mock("../../src/lib/silver-promote", () => ({
  cancelRemainingProvisions: (...a: unknown[]) => mockCancelRemainingProvisions(...a),
}));

const mockRefresh = vi.fn();
vi.mock("../../src/lib/status-gold", () => ({
  refreshLeadStatusCurrent: (...a: unknown[]) => mockRefresh(...a),
}));

vi.mock("../../src/lib/evidence-changed", () => ({
  announceEvidenceChanged: vi.fn(async () => undefined),
}));

import { retireInstantlySequences } from "../../src/lib/self-send/retire-instantly-sequences";

const ROWS = [
  {
    instantlyCampaignId: "inst-1",
    campaignId: "camp-1",
    orgId: "org-1",
    userId: "user-1",
    leadEmail: "ana@acme.com",
    pendingSteps: 2,
  },
  {
    instantlyCampaignId: "inst-2",
    campaignId: null,
    orgId: null,
    userId: null,
    leadEmail: "bo@acme.com",
    pendingSteps: 0,
  },
];

/** SQL text of a drizzle `sql` template, for asserting which statement ran. */
function sqlText(arg: unknown): string {
  const chunks = (arg as { queryChunks?: unknown[] }).queryChunks ?? [];
  return chunks
    .map((c) => (c && typeof c === "object" && "value" in c ? (c as { value: string[] }).value.join("") : ""))
    .join("?");
}

function updates(): string[] {
  return mockExecute.mock.calls.map((c) => sqlText(c[0])).filter((t) => t.includes("UPDATE instantly_campaigns"));
}

beforeEach(() => {
  vi.clearAllMocks();
  mockExecute.mockImplementation(async (q: unknown) =>
    sqlText(q).includes("FROM instantly_campaigns c") ? { rows: ROWS } : { rows: [] },
  );
  mockUpdateCampaignStatus.mockResolvedValue(undefined);
  mockCancelRemainingProvisions.mockResolvedValue(undefined);
  mockRefresh.mockResolvedValue(undefined);
});

describe("retireInstantlySequences — the Instantly plan is cancelled", () => {
  it("dry-runs by default: counts, never calls Instantly, never writes", async () => {
    const summary = await retireInstantlySequences();

    expect(summary).toMatchObject({
      dryRun: true,
      open: 2,
      withPendingSteps: 1,
      pendingSteps: 2,
      closed: 0,
    });
    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
    expect(mockCancelRemainingProvisions).not.toHaveBeenCalled();
    expect(updates()).toHaveLength(0);
  });

  it("pauses on Instantly, refunds the unsent steps, then marks the row paused", async () => {
    const order: string[] = [];
    mockUpdateCampaignStatus.mockImplementation(async (_k: string, id: string) => {
      order.push(`pause:${id}`);
    });
    mockCancelRemainingProvisions.mockImplementation(async (c: { instantlyCampaignId: string }) => {
      order.push(`refund:${c.instantlyCampaignId}`);
    });

    const summary = await retireInstantlySequences({ dryRun: false });

    expect(summary).toMatchObject({ closed: 2, refundedSteps: 2, failed: 0 });
    expect(order).toEqual(["pause:inst-1", "refund:inst-1", "pause:inst-2", "refund:inst-2"]);
    expect(mockUpdateCampaignStatus).toHaveBeenCalledWith("org-key", "inst-1", "paused");
    // A platform send (no org) pauses with the platform key.
    expect(mockUpdateCampaignStatus).toHaveBeenCalledWith("platform-key", "inst-2", "paused");
    expect(updates()).toHaveLength(2);
    expect(updates()[0]).toContain("status = 'paused'");
    expect(mockRefresh).toHaveBeenCalledWith("inst-1", "ana@acme.com");
  });

  it("treats an Instantly 404 as already gone and still closes the row", async () => {
    mockUpdateCampaignStatus.mockRejectedValueOnce(new Error("Instantly PATCH failed: 404 not found"));

    const summary = await retireInstantlySequences({ dryRun: false });

    expect(summary).toMatchObject({ closed: 2, failed: 0 });
  });

  // Refunding a step Instantly could still send would give the email away AND
  // the money back. A row whose pause failed is left exactly as it was.
  it("leaves a row untouched when the Instantly pause fails", async () => {
    mockUpdateCampaignStatus.mockRejectedValueOnce(new Error("Instantly PATCH failed: 500 boom"));

    const summary = await retireInstantlySequences({ dryRun: false });

    expect(summary).toMatchObject({ closed: 1, failed: 1, refundedSteps: 0 });
    expect(mockCancelRemainingProvisions).toHaveBeenCalledTimes(1);
    expect(mockCancelRemainingProvisions.mock.calls[0]![0]).toMatchObject({ instantlyCampaignId: "inst-2" });
  });

  it("honours the limit", async () => {
    const summary = await retireInstantlySequences({ dryRun: false, limit: 1 });

    expect(summary.closed).toBe(1);
    expect(mockUpdateCampaignStatus).toHaveBeenCalledTimes(1);
  });
});
