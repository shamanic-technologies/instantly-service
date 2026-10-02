import { describe, it, expect, vi, beforeEach } from "vitest";

const mockExecute = vi.fn();
const mockUpdateSet = vi.fn();
const mockListCampaignStatuses = vi.fn();
const mockCancelRemainingProvisions = vi.fn();
const mockResolveInstantlyApiKey = vi.fn();
const mockUpdateCampaignStatus = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    execute: (...args: unknown[]) => mockExecute(...args),
    update: () => ({
      set: (v: unknown) => ({ where: () => Promise.resolve(mockUpdateSet(v)) }),
    }),
  },
}));

vi.mock("../../src/db/schema", () => ({
  instantlyCampaigns: { instantlyCampaignId: "instantly_campaign_id" },
}));

vi.mock("../../src/lib/campaign-client", () => ({
  listCampaignStatuses: (...args: unknown[]) => mockListCampaignStatuses(...args),
}));

vi.mock("../../src/lib/silver-promote", () => ({
  cancelRemainingProvisions: (...args: unknown[]) => mockCancelRemainingProvisions(...args),
}));

vi.mock("../../src/lib/key-client", () => ({
  resolveInstantlyApiKey: (...args: unknown[]) => mockResolveInstantlyApiKey(...args),
}));

vi.mock("../../src/lib/instantly-client", () => ({
  updateCampaignStatus: (...args: unknown[]) => mockUpdateCampaignStatus(...args),
}));

const { stoppedCampaignIds, stopQueuedSequencesOfStoppedCampaigns } = await import(
  "../../src/lib/stopped-campaigns"
);

const CALLER = { method: "POST", path: "/internal/self-send/dispatch" };

function campaign(id: string, status: string, over: Record<string, unknown> = {}) {
  return {
    id,
    status,
    orgId: "org-1",
    brandId: "brand-1",
    offerId: "offer-1",
    legKey: "start_to_conversation",
    acquisitionChannel: "cold_email",
    ...over,
  };
}

function queued(instantlyCampaignId: string, campaignId: string, leadEmail: string, orgId = "org-1") {
  return { instantlyCampaignId, campaignId, orgId, userId: null, runId: "run-1", leadEmail };
}

beforeEach(() => {
  vi.resetAllMocks();
  mockResolveInstantlyApiKey.mockResolvedValue({ key: "k", keySource: "platform" });
  mockUpdateCampaignStatus.mockResolvedValue({});
  mockCancelRemainingProvisions.mockResolvedValue(undefined);
  mockUpdateSet.mockReturnValue([{}]);
});

describe("stoppedCampaignIds — campaign-service's status, family-aware", () => {
  it("a stopped campaign with no live sibling is stopped", () => {
    expect(stoppedCampaignIds([campaign("a", "stopped")])).toEqual(new Set(["a"]));
  });

  it("a running campaign is never stopped", () => {
    expect(stoppedCampaignIds([campaign("a", "ongoing")]).size).toBe(0);
  });

  // campaign-service keeps an ancestor row per workflow change: a sequence
  // enrolled under the ancestor belongs to a campaign the customer still runs.
  it("a stopped ANCESTOR of a running campaign is NOT stopped", () => {
    const ids = stoppedCampaignIds([campaign("old", "stopped"), campaign("new", "ongoing")]);
    expect(ids.has("old")).toBe(false);
  });

  it("a live campaign of ANOTHER identity does not keep a stopped one alive", () => {
    const ids = stoppedCampaignIds([
      campaign("a", "stopped"),
      campaign("b", "ongoing", { legKey: "start_to_website_visit" }),
    ]);
    expect(ids).toEqual(new Set(["a"]));
  });

  it("a row stating too little to pool is judged on its own status", () => {
    const ids = stoppedCampaignIds([
      campaign("a", "stopped", { acquisitionChannel: null }),
      campaign("b", "ongoing", { acquisitionChannel: null }),
    ]);
    expect(ids).toEqual(new Set(["a"]));
  });
});

describe("stopQueuedSequencesOfStoppedCampaigns", () => {
  it("customer stop: a queued SELF-SEND sequence is refunded and taken out of the dispatcher's reach, no Instantly call", async () => {
    mockListCampaignStatuses.mockResolvedValue([campaign("camp-stopped", "stopped")]);
    mockExecute.mockResolvedValue({ rows: [queued("self:aaaa", "camp-stopped", "p@x.com")] });

    const { summary, notYetStopped } = await stopQueuedSequencesOfStoppedCampaigns(CALLER);

    expect(summary).toMatchObject({ stoppedSelfSend: 1, stoppedInstantly: 0, failed: 0 });
    expect(mockCancelRemainingProvisions).toHaveBeenCalledWith(
      expect.objectContaining({ instantlyCampaignId: "self:aaaa", campaignId: "camp-stopped" }),
      "p@x.com",
    );
    expect(mockUpdateSet).toHaveBeenCalledWith(expect.objectContaining({ status: "paused" }));
    // Holds cancelled BEFORE the row leaves the queue.
    expect(mockCancelRemainingProvisions.mock.invocationCallOrder[0]!).toBeLessThan(
      mockUpdateSet.mock.invocationCallOrder[0]!,
    );
    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
  });

  it("an INSTANTLY sequence is paused on Instantly first, then refunded and marked", async () => {
    mockListCampaignStatuses.mockResolvedValue([campaign("camp-stopped", "stopped")]);
    mockExecute.mockResolvedValue({
      rows: [queued("019f9856-0000-4000-8000-000000000000", "camp-stopped", "p@x.com")],
    });

    const { summary, notYetStopped } = await stopQueuedSequencesOfStoppedCampaigns(CALLER);

    expect(summary).toMatchObject({ stoppedInstantly: 1, failed: 0 });
    expect(mockUpdateCampaignStatus).toHaveBeenCalledWith(
      "k",
      "019f9856-0000-4000-8000-000000000000",
      "paused",
    );
    expect(mockUpdateCampaignStatus.mock.invocationCallOrder[0]!).toBeLessThan(
      mockCancelRemainingProvisions.mock.invocationCallOrder[0]!,
    );
    expect(mockUpdateSet).toHaveBeenCalledWith(expect.objectContaining({ status: "paused" }));
  });

  it("an Instantly pause that fails leaves the row (and its holds) for the next tick", async () => {
    mockListCampaignStatuses.mockResolvedValue([campaign("camp-stopped", "stopped")]);
    mockExecute.mockResolvedValue({
      rows: [queued("019f9856-0000-4000-8000-000000000000", "camp-stopped", "p@x.com")],
    });
    mockUpdateCampaignStatus.mockRejectedValue(new Error("Instantly 500"));

    const { summary, notYetStopped } = await stopQueuedSequencesOfStoppedCampaigns(CALLER);

    expect(summary).toMatchObject({ stoppedInstantly: 0, failed: 1 });
    expect(notYetStopped).toEqual(new Set(["019f9856-0000-4000-8000-000000000000"]));
    expect(mockCancelRemainingProvisions).not.toHaveBeenCalled();
    expect(mockUpdateSet).not.toHaveBeenCalled();
  });

  it("org teardown: every queued sequence of the torn-down org's campaigns is stopped", async () => {
    mockListCampaignStatuses.mockResolvedValue([
      campaign("0bfe2236", "stopped", { orgId: "f1b7b046", stopReason: "org_teardown" }),
      campaign("bfbc8c74", "stopped", { orgId: "f1b7b046", stopReason: "org_teardown", legKey: "start_to_website_visit" }),
    ]);
    mockExecute.mockResolvedValue({
      rows: [
        queued("self:1", "0bfe2236", "stefanie.heller@sunrise.net", "f1b7b046"),
        queued("self:2", "bfbc8c74", "philipp.seidel@trafag.com", "f1b7b046"),
        queued("self:3", "bfbc8c74", "javier.ramos@hitachienergy.com", "f1b7b046"),
      ],
    });

    const { summary, notYetStopped } = await stopQueuedSequencesOfStoppedCampaigns(CALLER);

    expect(summary).toMatchObject({ queuedSequences: 3, stoppedSelfSend: 3, failed: 0 });
    expect(mockCancelRemainingProvisions).toHaveBeenCalledTimes(3);
  });

  // No change to which leads get emailed on a live campaign.
  it("leaves a RUNNING campaign's queue untouched, and an unknown campaign too", async () => {
    mockListCampaignStatuses.mockResolvedValue([campaign("camp-live", "ongoing")]);
    mockExecute.mockResolvedValue({
      rows: [queued("self:live", "camp-live", "a@x.com"), queued("self:unknown", "camp-gone", "b@x.com")],
    });

    const { summary, notYetStopped } = await stopQueuedSequencesOfStoppedCampaigns(CALLER);

    expect(summary).toMatchObject({ queuedSequences: 2, unknownCampaign: 1, stoppedSelfSend: 0 });
    expect(mockCancelRemainingProvisions).not.toHaveBeenCalled();
    expect(mockUpdateSet).not.toHaveBeenCalled();
    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
  });

  it("over the per-tick limit: Instantly rows go first, the rest is handed back for the dispatcher to hold", async () => {
    mockListCampaignStatuses.mockResolvedValue([campaign("camp-stopped", "stopped")]);
    mockExecute.mockResolvedValue({
      rows: [
        queued("self:1", "camp-stopped", "a@x.com"),
        queued("019f9856-0000-4000-8000-000000000000", "camp-stopped", "b@x.com"),
      ],
    });

    const { summary, notYetStopped } = await stopQueuedSequencesOfStoppedCampaigns(CALLER, 1);

    expect(summary).toMatchObject({ stoppedInstantly: 1, stoppedSelfSend: 0, deferred: 1 });
    expect(notYetStopped).toEqual(new Set(["self:1"]));
  });

  it("fails LOUD when campaign-service cannot be read (the dispatcher then sends nothing)", async () => {
    mockListCampaignStatuses.mockRejectedValue(new Error("campaign-service GET /campaigns/list failed: 503"));

    await expect(stopQueuedSequencesOfStoppedCampaigns(CALLER)).rejects.toThrow(/503/);
    expect(mockExecute).not.toHaveBeenCalled();
  });
});
