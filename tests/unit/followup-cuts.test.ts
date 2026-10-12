import { describe, it, expect, vi, beforeEach } from "vitest";

// LEAK-1012: a campaign emailed people it was never meant to write to. The cut
// stops the rest of THOSE sequences only, and records nothing that reads as an
// opt-out.

const selectQueue: unknown[][] = [];
const executeQueue: number[] = [];
const inserted: unknown[] = [];
const mockStop = vi.fn();
const mockPause = vi.fn();
const mockResolveKey = vi.fn();
const mockRefreshGold = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    select: () => ({
      from: () => ({
        where: () => Promise.resolve(selectQueue.shift() ?? []),
      }),
    }),
    execute: () => Promise.resolve({ rows: [{ n: executeQueue.shift() ?? 0 }] }),
    insert: () => ({
      values: (v: unknown) => {
        inserted.push(v);
        return {
          onConflictDoNothing: () => ({
            returning: () => Promise.resolve([{ cutAt: new Date("2026-10-13T08:00:00.000Z") }]),
          }),
        };
      },
    }),
  },
}));

vi.mock("../../src/lib/self-send/stop-sequence", () => ({
  stopSelfSendSequence: (...a: unknown[]) => mockStop(...a),
}));
vi.mock("../../src/lib/instantly-client", () => ({
  updateCampaignStatus: (...a: unknown[]) => mockPause(...a),
}));
vi.mock("../../src/lib/key-client", () => ({
  resolveInstantlyApiKey: (...a: unknown[]) => mockResolveKey(...a),
}));
vi.mock("../../src/lib/status-gold", () => ({
  refreshLeadStatusCurrent: (...a: unknown[]) => mockRefreshGold(...a),
}));

const { cutFollowups, LeadsNotInCampaignError } = await import("../../src/lib/followup-cuts");

const CAMPAIGN = "fb201499-4f90-40da-9cc3-f612392664f2";
function seq(email: string, id: string) {
  return { instantlyCampaignId: id, campaignId: CAMPAIGN, orgId: "org-1", userId: null, runId: null, leadEmail: email };
}
const BASE = {
  orgId: "org-1",
  userId: "user-1",
  campaignId: CAMPAIGN,
  reason: "sourcing bug",
  caller: { method: "POST", path: "/orgs/followup-cuts" },
};

beforeEach(() => {
  vi.resetAllMocks();
  selectQueue.length = 0;
  executeQueue.length = 0;
  inserted.length = 0;
  mockResolveKey.mockResolvedValue({ key: "k" });
});

describe("cutFollowups", () => {
  it("stops only the named self-send sequence, refunds its queued steps and audits who/why", async () => {
    selectQueue.push([seq("Karolin@Tolacapital.com", "self:a")], []);
    executeQueue.push(2, 0); // 2 queued before, 0 after

    const results = await cutFollowups({ ...BASE, leadEmails: [" karolin@tolacapital.com "] });

    expect(results).toEqual([
      { email: "karolin@tolacapital.com", outcome: "cut", holdsCancelled: 2, cutAt: "2026-10-13T08:00:00.000Z", cutBy: "user-1" },
    ]);
    expect(mockStop).toHaveBeenCalledTimes(1);
    expect(mockStop.mock.calls[0][0]).toMatchObject({ instantlyCampaignId: "self:a" });
    expect(mockPause).not.toHaveBeenCalled(); // no Instantly campaign behind a self: sequence
    expect(inserted).toEqual([
      expect.objectContaining({ instantlyCampaignId: "self:a", cutBy: "user-1", reason: "sourcing bug", holdsCancelled: 2 }),
    ]);
    expect(mockRefreshGold).toHaveBeenCalledWith("self:a", "Karolin@Tolacapital.com");
  });

  it("pauses an Instantly-transport sequence at Instantly BEFORE the local stop", async () => {
    selectQueue.push([seq("a@x.com", "019f-inst")], []);
    executeQueue.push(1, 0);
    const order: string[] = [];
    mockPause.mockImplementation(async () => order.push("pause"));
    mockStop.mockImplementation(async () => order.push("stop"));

    const [r] = await cutFollowups({ ...BASE, leadEmails: ["a@x.com"] });

    expect(r.outcome).toBe("cut");
    expect(order).toEqual(["pause", "stop"]);
    expect(mockPause).toHaveBeenCalledWith("k", "019f-inst", "paused");
  });

  it("refuses the whole call when a named email has no sequence in the campaign (nothing cut)", async () => {
    selectQueue.push([seq("a@x.com", "self:a")]);

    await expect(cutFollowups({ ...BASE, leadEmails: ["a@x.com", "stranger@y.com"] })).rejects.toBeInstanceOf(
      LeadsNotInCampaignError,
    );
    expect(mockStop).not.toHaveBeenCalled();
    expect(inserted).toEqual([]);
  });

  it("is idempotent: a lead already cut is reported, never stopped twice", async () => {
    selectQueue.push(
      [seq("a@x.com", "self:a")],
      [{ instantlyCampaignId: "self:a", cutAt: new Date("2026-10-12T21:00:00.000Z"), cutBy: "user-0" }],
    );

    const [r] = await cutFollowups({ ...BASE, leadEmails: ["a@x.com"] });

    expect(r).toEqual({ email: "a@x.com", outcome: "already_cut", holdsCancelled: 0, cutAt: "2026-10-12T21:00:00.000Z", cutBy: "user-0" });
    expect(mockStop).not.toHaveBeenCalled();
    expect(inserted).toEqual([]);
  });

  it("fails loud when a queued step survives the stop: no audit row, so a retry re-runs it", async () => {
    selectQueue.push([seq("a@x.com", "self:a")], []);
    executeQueue.push(2, 1);

    const [r] = await cutFollowups({ ...BASE, leadEmails: ["a@x.com"] });

    expect(r.outcome).toBe("failed");
    expect(r.error).toMatch(/1 queued step/);
    expect(inserted).toEqual([]);
  });
});
