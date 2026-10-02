import { describe, it, expect, vi, beforeEach } from "vitest";
import express from "express";
import request from "supertest";

const mockSelectRows = vi.fn();
const mockUpdateSet = vi.fn();
const mockStopSelfSendSequence = vi.fn();
const mockResolveInstantlyApiKey = vi.fn();
const mockUpdateCampaignStatus = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    select: () => ({ from: () => ({ where: () => Promise.resolve(mockSelectRows()) }) }),
    update: () => ({
      set: (v: Record<string, unknown>) => ({
        where: () => ({ returning: () => Promise.resolve([mockUpdateSet(v)]) }),
      }),
    }),
  },
}));

vi.mock("../../src/lib/self-send/stop-sequence", () => ({
  stopSelfSendSequence: (...args: unknown[]) => mockStopSelfSendSequence(...args),
}));

vi.mock("../../src/lib/key-client", () => ({
  resolveInstantlyApiKey: (...args: unknown[]) => mockResolveInstantlyApiKey(...args),
}));

vi.mock("../../src/lib/instantly-client", () => ({
  updateCampaignStatus: (...args: unknown[]) => mockUpdateCampaignStatus(...args),
}));

vi.mock("../../src/lib/trace-event", () => ({ traceEvent: () => Promise.resolve() }));

const { default: campaignsRoutes } = await import("../../src/routes/campaigns");

const app = express();
app.use(express.json());
app.use((_req, res, next) => {
  res.locals.orgId = "org-1";
  res.locals.userId = "user-1";
  next();
});
app.use("/orgs/campaigns", campaignsRoutes);

const SELF_ROW = {
  id: "row-1",
  campaignId: "camp-1",
  instantlyCampaignId: "self:11111111-1111-4111-8111-111111111111",
  leadEmail: "stefanie.heller@sunrise.net",
  orgId: "org-1",
  userId: null,
  runId: "run-1",
  status: "active",
};

beforeEach(() => {
  vi.resetAllMocks();
  mockResolveInstantlyApiKey.mockResolvedValue({ key: "k", keySource: "platform" });
  mockUpdateCampaignStatus.mockResolvedValue({});
  mockStopSelfSendSequence.mockResolvedValue(undefined);
  mockUpdateSet.mockImplementation((v: Record<string, unknown>) => ({ ...SELF_ROW, ...v }));
});

describe("PATCH /orgs/campaigns/:campaignId/status — self-send", () => {
  // Prod 2026-10-02: this answered 500 — the route called Instantly with the
  // `self:` id ("params/id must match format uuid").
  it("pauses a self-send campaign with 200, refunding its holds, never calling Instantly", async () => {
    mockSelectRows
      .mockReturnValueOnce([SELF_ROW])
      .mockReturnValueOnce([{ ...SELF_ROW, status: "paused" }]);

    const res = await request(app)
      .patch("/orgs/campaigns/camp-1/status")
      .send({ status: "paused" });

    expect(res.status).toBe(200);
    expect(res.body.campaign.status).toBe("paused");
    expect(mockStopSelfSendSequence).toHaveBeenCalledWith(
      SELF_ROW,
      "stefanie.heller@sunrise.net",
      expect.any(String),
      "paused",
    );
    expect(mockResolveInstantlyApiKey).not.toHaveBeenCalled();
    expect(mockUpdateCampaignStatus).not.toHaveBeenCalled();
  });

  it("still pauses an Instantly row on Instantly, and only that row", async () => {
    const instantlyRow = { ...SELF_ROW, id: "row-2", instantlyCampaignId: "019f9856-0000-4000-8000-000000000000" };
    mockSelectRows
      .mockReturnValueOnce([SELF_ROW, instantlyRow])
      .mockReturnValueOnce([{ ...SELF_ROW, status: "paused" }]);

    const res = await request(app)
      .patch("/orgs/campaigns/camp-1/status")
      .send({ status: "paused" });

    expect(res.status).toBe(200);
    expect(mockUpdateCampaignStatus).toHaveBeenCalledTimes(1);
    expect(mockUpdateCampaignStatus).toHaveBeenCalledWith("k", instantlyRow.instantlyCampaignId, "paused");
    expect(mockStopSelfSendSequence).toHaveBeenCalledTimes(1);
  });
});
