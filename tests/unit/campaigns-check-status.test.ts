import { describe, it, expect, vi, beforeEach } from "vitest";
import request from "supertest";
import express from "express";

// Mock DB
const mockDbWhere = vi.fn();

vi.mock("../../src/db", () => ({
  db: {
    select: () => ({
      from: () => ({ where: mockDbWhere }),
    }),
    update: () => ({
      set: () => ({
        where: () => Promise.resolve([{}]),
      }),
    }),
  },
}));

vi.mock("../../src/db/schema", () => ({
  instantlyCampaigns: {
    id: "id",
    campaignId: "campaign_id",
    leadEmail: "lead_email",
    instantlyCampaignId: "instantly_campaign_id",
    status: "status",
    orgId: "org_id",
  },
  sequenceCosts: {
    id: "id",
    campaignId: "campaign_id",
    leadEmail: "lead_email",
    status: "status",
  },
}));

// Mock instantly-client
const mockGetCampaign = vi.fn();
const mockCreateCampaign = vi.fn();
const mockUpdateCampaign = vi.fn();
const mockUpdateCampaignStatus = vi.fn();

vi.mock("../../src/lib/instantly-client", () => ({
  getCampaign: (...args: unknown[]) => mockGetCampaign(...args),
  createCampaign: (...args: unknown[]) => mockCreateCampaign(...args),
  updateCampaign: (...args: unknown[]) => mockUpdateCampaign(...args),
  updateCampaignStatus: (...args: unknown[]) => mockUpdateCampaignStatus(...args),
}));

// Mock runs-client
const mockCreateRun = vi.fn();
const mockUpdateRun = vi.fn();
const mockAddCosts = vi.fn();
const mockUpdateCostStatus = vi.fn();

vi.mock("../../src/lib/runs-client", () => ({
  createRun: (...args: unknown[]) => mockCreateRun(...args),
  updateRun: (...args: unknown[]) => mockUpdateRun(...args),
  addCosts: (...args: unknown[]) => mockAddCosts(...args),
  updateCostStatus: (...args: unknown[]) => mockUpdateCostStatus(...args),
}));

// Mock key-client
const mockResolveInstantlyApiKey = vi.fn();

vi.mock("../../src/lib/key-client", () => ({
  resolveInstantlyApiKey: (...args: unknown[]) => mockResolveInstantlyApiKey(...args),
  KeyServiceError: class KeyServiceError extends Error {
    statusCode: number;
    constructor(statusCode: number, message: string) {
      super(message);
      this.name = "KeyServiceError";
      this.statusCode = statusCode;
    }
  },
}));

// Mock email-client
const mockSendEmail = vi.fn();

vi.mock("../../src/lib/email-client", () => ({
  sendEmail: (...args: unknown[]) => mockSendEmail(...args),
}));

async function createCampaignsApp() {
  const campaignsRouter = (await import("../../src/routes/campaigns")).default;
  const app = express();
  app.use(express.json());
  app.use("/campaigns", campaignsRouter);
  return app;
}

describe("POST /campaigns/check-status", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockResolveInstantlyApiKey.mockResolvedValue("test-instantly-key");
    mockUpdateRun.mockResolvedValue({});
    mockUpdateCostStatus.mockResolvedValue({});
    mockSendEmail.mockResolvedValue({});
  });

  it("should return empty errors when no active campaigns exist", async () => {
    // First call: check-status query for active campaigns
    mockDbWhere.mockResolvedValueOnce([]);

    const app = await createCampaignsApp();
    const res = await request(app).post("/campaigns/check-status");

    expect(res.status).toBe(200);
    expect(res.body).toEqual({ checked: 0, errors: [] });
  });

  it("should detect errored campaigns via not_sending_status", async () => {
    // check-status: active campaigns
    mockDbWhere.mockResolvedValueOnce([
      {
        id: "db-1",
        campaignId: "camp-1",
        leadEmail: "lead@test.com",
        instantlyCampaignId: "inst-1",
        status: "active",
        orgId: "org-1",
        runId: "run-1",
        metadata: null,
      },
    ]);

    // handleCampaignError: campaign lookup
    mockDbWhere.mockResolvedValueOnce([
      {
        id: "db-1",
        campaignId: "camp-1",
        leadEmail: "lead@test.com",
        instantlyCampaignId: "inst-1",
        status: "active",
        runId: "run-1",
        metadata: null,
      },
    ]);

    // handleCampaignError: provisioned costs lookup
    mockDbWhere.mockResolvedValueOnce([]);

    mockGetCampaign.mockResolvedValue({
      id: "inst-1",
      status: "error",
      not_sending_status: "account_disconnected",
    });

    const app = await createCampaignsApp();
    const res = await request(app).post("/campaigns/check-status");

    expect(res.status).toBe(200);
    expect(res.body.checked).toBe(1);
    expect(res.body.errors).toHaveLength(1);
    expect(res.body.errors[0]).toMatchObject({
      instantlyCampaignId: "inst-1",
      campaignId: "camp-1",
      leadEmail: "lead@test.com",
    });
  });

  it("should skip healthy campaigns", async () => {
    mockDbWhere.mockResolvedValueOnce([
      {
        id: "db-1",
        campaignId: "camp-1",
        leadEmail: "lead@test.com",
        instantlyCampaignId: "inst-1",
        status: "active",
        orgId: "org-1",
        runId: "run-1",
      },
    ]);

    mockGetCampaign.mockResolvedValue({
      id: "inst-1",
      status: "active",
      not_sending_status: null,
    });

    const app = await createCampaignsApp();
    const res = await request(app).post("/campaigns/check-status");

    expect(res.status).toBe(200);
    expect(res.body.checked).toBe(1);
    expect(res.body.errors).toHaveLength(0);
  });

  it("should continue checking other campaigns if one API call fails", async () => {
    mockDbWhere.mockResolvedValueOnce([
      {
        id: "db-1",
        campaignId: "camp-1",
        leadEmail: "lead1@test.com",
        instantlyCampaignId: "inst-1",
        status: "active",
        orgId: "org-1",
        runId: "run-1",
      },
      {
        id: "db-2",
        campaignId: "camp-1",
        leadEmail: "lead2@test.com",
        instantlyCampaignId: "inst-2",
        status: "active",
        orgId: "org-1",
        runId: "run-2",
      },
    ]);

    // First campaign fails API call
    mockGetCampaign.mockRejectedValueOnce(new Error("API timeout"));
    // Second campaign is healthy
    mockGetCampaign.mockResolvedValueOnce({
      id: "inst-2",
      status: "active",
      not_sending_status: null,
    });

    const app = await createCampaignsApp();
    const res = await request(app).post("/campaigns/check-status");

    expect(res.status).toBe(200);
    expect(res.body.checked).toBe(2);
    expect(res.body.errors).toHaveLength(0);
  });
});
