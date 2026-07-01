import { describe, it, expect, vi, beforeEach } from "vitest";

// ─── Mocks ───────────────────────────────────────────────────────────────────

const mockDbExecute = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    execute: (...args: unknown[]) => mockDbExecute(...args),
  },
}));

const mockHandleEmailSent = vi.fn();

vi.mock("../../src/lib/silver-promote", () => ({
  handleEmailSent: (...args: unknown[]) => mockHandleEmailSent(...args),
}));

import {
  actualizeOrphanedSends,
  selectOrphanedSends,
} from "../../src/lib/actualize-orphaned-sends";

function orphanRow(overrides: Record<string, unknown> = {}) {
  return {
    campaignId: "camp-1",
    instantlyCampaignId: "inst-1",
    orgId: "org-1",
    userId: "user-1",
    leadEmail: "lead@example.com",
    step: 2,
    ...overrides,
  };
}

describe("actualizeOrphanedSends", () => {
  beforeEach(() => {
    vi.resetAllMocks();
    mockHandleEmailSent.mockResolvedValue(undefined);
  });

  it("actualizes each orphaned (campaign, lead, step) via handleEmailSent", async () => {
    mockDbExecute.mockResolvedValue({
      rows: [
        orphanRow({ campaignId: "camp-1", leadEmail: "a@x.com", step: 1 }),
        orphanRow({ campaignId: "camp-2", leadEmail: "b@x.com", instantlyCampaignId: "inst-2", step: 3 }),
      ],
    });

    const summary = await actualizeOrphanedSends();

    expect(summary).toEqual({ stepsProcessed: 2, stepsFailed: 0 });
    expect(mockHandleEmailSent).toHaveBeenCalledTimes(2);
    expect(mockHandleEmailSent).toHaveBeenCalledWith(
      expect.objectContaining({ campaignId: "camp-1", instantlyCampaignId: "inst-1", runId: null }),
      "a@x.com",
      1,
    );
    expect(mockHandleEmailSent).toHaveBeenCalledWith(
      expect.objectContaining({ campaignId: "camp-2", instantlyCampaignId: "inst-2" }),
      "b@x.com",
      3,
    );
  });

  it("isolates a per-step failure (counted, sweep continues) — e.g. runs-service 404 on a gone run", async () => {
    mockDbExecute.mockResolvedValue({
      rows: [orphanRow({ step: 1 }), orphanRow({ step: 2 })],
    });
    mockHandleEmailSent
      .mockRejectedValueOnce(new Error("runs-service 404 - run gone"))
      .mockResolvedValueOnce(undefined);

    const summary = await actualizeOrphanedSends();

    expect(summary).toEqual({ stepsProcessed: 1, stepsFailed: 1 });
    expect(mockHandleEmailSent).toHaveBeenCalledTimes(2);
  });

  it("no-ops cleanly when nothing is orphaned", async () => {
    mockDbExecute.mockResolvedValue({ rows: [] });

    const summary = await actualizeOrphanedSends();

    expect(summary).toEqual({ stepsProcessed: 0, stepsFailed: 0 });
    expect(mockHandleEmailSent).not.toHaveBeenCalled();
  });

  it("handles array-shaped db.execute result (no .rows wrapper)", async () => {
    mockDbExecute.mockResolvedValue([orphanRow()]);

    const summary = await actualizeOrphanedSends({ limit: 100 });

    expect(summary.stepsProcessed).toBe(1);
  });

  it("selectOrphanedSends gates on a real (inferred=false) email_sent for the same step", async () => {
    mockDbExecute.mockResolvedValue({ rows: [orphanRow()] });

    await selectOrphanedSends();

    const sqlArg = mockDbExecute.mock.calls[0][0] as { queryChunks?: unknown[] };
    // The query text lives across drizzle's queryChunks; join their string parts.
    const text = JSON.stringify(sqlArg);
    expect(text).toContain("email_sent");
    expect(text).toContain("inferred");
    expect(text).toContain("provisioned");
  });
});
