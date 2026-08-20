import { describe, it, expect, vi, beforeEach } from "vitest";

const mockCancelRemainingProvisions = vi.fn();
const mockUpdateSet = vi.fn();
const mockGetCampaignFunnelKey = vi.fn();
const mockResolveInstantlyApiKey = vi.fn();
const mockUpdateCampaignStatus = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    update: () => ({
      set: (v: unknown) => ({ where: () => Promise.resolve(mockUpdateSet(v)) }),
    }),
  },
}));

vi.mock("../../src/db/schema", () => ({
  instantlyCampaigns: { instantlyCampaignId: "instantly_campaign_id" },
}));

vi.mock("../../src/lib/silver-promote", () => ({
  cancelRemainingProvisions: (...args: unknown[]) => mockCancelRemainingProvisions(...args),
}));

vi.mock("../../src/lib/campaign-client", async (importOriginal) => ({
  ...((await importOriginal()) as Record<string, unknown>),
  getCampaignFunnelKey: (...args: unknown[]) => mockGetCampaignFunnelKey(...args),
}));

vi.mock("../../src/lib/key-client", () => ({
  resolveInstantlyApiKey: (...args: unknown[]) => mockResolveInstantlyApiKey(...args),
}));

vi.mock("../../src/lib/instantly-client", () => ({
  updateCampaignStatus: (...args: unknown[]) => mockUpdateCampaignStatus(...args),
}));

const { stopSelfSendSequence } = await import("../../src/lib/self-send/stop-sequence");
const { maybeStopOnClickForFunnel } = await import("../../src/lib/stop-on-click");

const SELF = {
  instantlyCampaignId: "self:11111111-1111-4111-8111-111111111111",
  campaignId: "camp-1",
  orgId: "org-1",
  userId: null,
  runId: null,
};

const INSTANTLY = { ...SELF, instantlyCampaignId: "019f9856-0000-4000-8000-000000000000" };

beforeEach(() => {
  vi.resetAllMocks();
  mockResolveInstantlyApiKey.mockResolvedValue({ key: "k", keySource: "platform" });
  mockUpdateCampaignStatus.mockResolvedValue({});
  mockCancelRemainingProvisions.mockResolvedValue(undefined);
  mockUpdateSet.mockReturnValue([{}]);
});

describe("stopSelfSendSequence", () => {
  // Both halves have to happen here. reconcileAll skips a `self:` row outright,
  // so nothing downstream will ever discover the stop and cancel the holds.
  it("refunds the remaining holds AND takes the lead out of the worker's reach", async () => {
    await stopSelfSendSequence(SELF, "lead@x.com", "test");

    expect(mockCancelRemainingProvisions).toHaveBeenCalledWith(SELF, "lead@x.com");
    expect(mockUpdateSet).toHaveBeenCalledWith(
      expect.objectContaining({ status: "paused" }),
    );
  });

  // A crash between the two should leave a row that still sends, never one that
  // has silently lost its refund.
  it("cancels the holds BEFORE marking the row", async () => {
    const order: string[] = [];
    mockCancelRemainingProvisions.mockImplementation(async () => {
      order.push("cancel");
    });
    mockUpdateSet.mockImplementation(() => {
      order.push("mark");
      return [{}];
    });

    await stopSelfSendSequence(SELF, "lead@x.com", "test");

    expect(order).toEqual(["cancel", "mark"]);
  });
});

describe("maybeStopOnClickForFunnel — transport split", () => {
  it("stops a self-send sequence locally, never calling Instantly", async () => {
    mockGetCampaignFunnelKey.mockResolvedValue("visit_signup");

    await maybeStopOnClickForFunnel(SELF, "lead@x.com");

    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
    expect(mockCancelRemainingProvisions).toHaveBeenCalled();
    expect(mockUpdateSet).toHaveBeenCalledWith(
      expect.objectContaining({ status: "paused" }),
    );
  });

  it("still pauses on Instantly for an Instantly-dispatched sequence", async () => {
    mockGetCampaignFunnelKey.mockResolvedValue("visit_signup");

    await maybeStopOnClickForFunnel(INSTANTLY, "lead@x.com");

    expect(mockUpdateCampaignStatus).toHaveBeenCalledWith(
      "k",
      INSTANTLY.instantlyCampaignId,
      "paused",
    );
    // The Instantly path must NOT touch local state — reconcile's finish closure
    // owns the holds, and marking the row terminal here would make it skip.
    expect(mockCancelRemainingProvisions).not.toHaveBeenCalled();
    expect(mockUpdateSet).not.toHaveBeenCalled();
  });

  it("leaves a reply-first funnel running on both transports", async () => {
    mockGetCampaignFunnelKey.mockResolvedValue("reply_meeting");

    await maybeStopOnClickForFunnel(SELF, "lead@x.com");
    await maybeStopOnClickForFunnel(INSTANTLY, "lead@x.com");

    expect(mockCancelRemainingProvisions).not.toHaveBeenCalled();
    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
  });
});
